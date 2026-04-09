from __future__ import annotations

from datetime import date
import io
from pathlib import Path

import lz4.frame
import orjson
import pyarrow as pa
import pyarrow.parquet as pq

from poochon_backtest_data.hyperliquid import (
    backfill_day,
    iso_to_epoch_ms,
    l2_source_key,
    normalize_day,
    parse_l2_snapshot,
    parse_trade,
    trade_source_key,
)
from poochon_backtest_data.models import (
    CoverageRecord,
    CoverageStatus,
    DatasetKind,
    IngestionRequest,
    MarketRef,
    coverage_pk,
    normalized_l2_s3_key,
    normalized_trade_s3_key,
    raw_l2_s3_key,
    raw_trade_s3_key,
    utc_now_iso,
)


class FakeBody:
    def __init__(self, data: bytes):
        self._data = data

    def read(self) -> bytes:
        return self._data


class FakeS3Client:
    def __init__(self):
        self.objects: dict[tuple[str, str], bytes] = {}
        self.get_requests: list[tuple[str, str]] = []

    def head_object(self, *, Bucket: str, Key: str):
        payload = self.objects.get((Bucket, Key))
        if payload is None:
            raise RuntimeError("missing object")
        return {"ContentLength": len(payload)}

    def get_object(self, *, Bucket: str, Key: str, RequestPayer: str | None = None):
        self.get_requests.append((Bucket, Key))
        payload = self.objects[(Bucket, Key)]
        return {"Body": FakeBody(payload)}


class FakeS3Store:
    def __init__(self):
        self.bucket = "test-bucket"
        self.client = FakeS3Client()
        self.objects: dict[str, bytes] = {}
        self.get_bytes_calls = 0

    def put_bytes(
        self,
        key: str,
        data: bytes,
        *,
        content_type: str | None = None,
        content_encoding: str | None = None,
    ) -> None:
        self.objects[key] = data
        self.client.objects[(self.bucket, key)] = data

    def put_file(self, key: str, path: str, *, content_type: str | None = None) -> None:
        data = Path(path).read_bytes()
        self.objects[key] = data
        self.client.objects[(self.bucket, key)] = data

    def get_bytes(self, key: str) -> bytes:
        self.get_bytes_calls += 1
        return self.objects[key]

    def exists(self, key: str) -> bool:
        return key in self.objects

    def object_size(self, key: str) -> int | None:
        payload = self.objects.get(key)
        return None if payload is None else len(payload)


class FakeCoverageRepository:
    def __init__(self, items: dict[str, CoverageRecord] | None = None):
        self.items = items or {}

    def get(self, pk: str) -> CoverageRecord | None:
        return self.items.get(pk)

    def put(self, record: CoverageRecord) -> None:
        self.items[record.pk] = record


def ready_coverage(
    dataset_kind: DatasetKind,
    market: MarketRef,
    date: str,
    hour: str,
    *,
    byte_count: int,
    row_count: int = 0,
) -> CoverageRecord:
    return CoverageRecord(
        pk=coverage_pk(dataset_kind, market, date, hour),
        dataset_kind=dataset_kind,
        venue=market.venue,
        market_type=market.market_type,
        instrument=market.instrument,
        date=date,
        hour=hour,
        status=CoverageStatus.READY,
        object_count=1 if hour != "daily" else 24,
        byte_count=byte_count,
        row_count=row_count,
        updated_at=utc_now_iso(),
        source="test",
    )


def lz4_bytes(lines: list[dict]) -> bytes:
    payload = b"".join(orjson.dumps(line) + b"\n" for line in lines)
    return lz4.frame.compress(payload)


def parquet_bytes(rows: list[dict], schema: pa.Schema) -> bytes:
    table = pa.Table.from_pylist(rows, schema=schema)
    buffer = io.BytesIO()
    pq.write_table(table, buffer, compression="zstd")
    return buffer.getvalue()


def test_iso_to_epoch_ms_handles_nanoseconds() -> None:
    assert iso_to_epoch_ms("2025-03-31T23:59:59.962208772") == 1743465599962


def test_parse_trade_filters_non_target_instrument() -> None:
    raw = {
        "coin": "DOGE",
        "side": "B",
        "time": "2025-03-31T23:59:59.962208772",
        "px": "0.16656",
        "sz": "600.0",
        "hash": "0xabc",
    }
    assert parse_trade(raw, instrument="BTC", source_hour=0, source_line_number=1) is None


def test_parse_l2_snapshot_keeps_book_payload_for_materialization() -> None:
    raw = {
        "raw": {
            "data": {
                "coin": "BTC",
                "time": 1705309199653,
                "levels": [
                    [{"px": "42706.0", "sz": "0.02342", "n": 1}],
                    [{"px": "42707.0", "sz": "0.09689", "n": 3}],
                ],
            }
        }
    }
    snapshot = parse_l2_snapshot(raw, source_hour=9, source_line_number=14)
    assert snapshot.instrument == "BTC"
    assert snapshot.ts_ms == 1705309199653
    assert '"px":"42706.0"' in snapshot.bids_json
    assert snapshot.source_line_number == 14


def test_source_keys_use_typed_market_paths() -> None:
    perp = MarketRef(market_type="perp", instrument="BTC")
    spot = MarketRef(market_type="spot", instrument="UBTC/USDC")
    assert l2_source_key(perp, "2025-05-24", 0) == "market_data/20250524/0/l2Book/BTC.lz4"
    assert trade_source_key("2025-05-24", 0) == "node_trades/hourly/20250524/0.lz4"
    assert raw_l2_s3_key(spot, "2025-05-24", 0).endswith("instrument=UBTC%2FUSDC/UBTC%2FUSDC.lz4")


def test_ingestion_request_resolves_relative_window() -> None:
    request = IngestionRequest(
        market_type="perp",
        instrument="BTC",
        start_offset_days=-2,
        end_offset_days=-1,
    )
    assert request.resolve_window(today=date(2025, 5, 27)) == ("2025-05-25", "2025-05-26")


def test_backfill_day_skips_existing_hours() -> None:
    market = MarketRef(market_type="perp", instrument="BTC")
    date = "2025-05-25"
    store = FakeS3Store()
    coverage = FakeCoverageRepository()

    for hour in range(24):
        l2_key = raw_l2_s3_key(market, date, hour)
        trade_key = raw_trade_s3_key(market, date, hour)
        store.put_bytes(l2_key, f"l2-{hour}".encode("utf-8"))
        store.put_bytes(trade_key, f"trades-{hour}".encode("utf-8"))
        coverage.put(
            ready_coverage(
                DatasetKind.RAW_L2,
                market,
                date,
                f"{hour:02d}",
                byte_count=len(store.objects[l2_key]),
            )
        )
        coverage.put(
            ready_coverage(
                DatasetKind.RAW_TRADES,
                market,
                date,
                f"{hour:02d}",
                byte_count=len(store.objects[trade_key]),
            )
        )
    coverage.put(ready_coverage(DatasetKind.RAW_L2, market, date, "daily", byte_count=24))
    coverage.put(ready_coverage(DatasetKind.RAW_TRADES, market, date, "daily", byte_count=24))

    backfill_day(store, coverage, market=market, date=date)

    assert store.client.get_requests == []


def test_normalize_day_skips_when_daily_outputs_are_ready() -> None:
    market = MarketRef(market_type="perp", instrument="BTC")
    date = "2025-05-25"
    store = FakeS3Store()
    coverage = FakeCoverageRepository()

    for hour in range(24):
        l2_key = normalized_l2_s3_key(market, date, hour)
        trade_key = normalized_trade_s3_key(market, date, hour)
        store.put_bytes(l2_key, b"l2")
        store.put_bytes(trade_key, b"trades")
    coverage.put(ready_coverage(DatasetKind.NORMALIZED_L2, market, date, "daily", byte_count=72))
    coverage.put(
        ready_coverage(DatasetKind.NORMALIZED_TRADES, market, date, "daily", byte_count=144)
    )

    normalize_day(store, coverage, market=market, date=date)

    assert store.get_bytes_calls == 0
