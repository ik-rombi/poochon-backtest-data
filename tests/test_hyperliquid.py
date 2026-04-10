from __future__ import annotations

import io
from pathlib import Path

import lz4.frame
import orjson
import pyarrow as pa
import pyarrow.parquet as pq
import zstandard

from poochon_backtest_data.canonical import build_hyperliquid_canonical_day
from poochon_backtest_data.hyperliquid import collapse_fill_trades, trade_source_key
from poochon_backtest_data.models import (
    CanonicalShardRecord,
    CoverageRecord,
    CoverageStatus,
    DatasetKind,
    MarketRef,
    coverage_pk,
    normalized_l2_s3_key,
    normalized_trade_s3_key,
    utc_now_iso,
)


class FakeS3Store:
    def __init__(self):
        self.objects: dict[str, bytes] = {}

    def put_bytes(self, key: str, data: bytes, *, content_type: str | None = None, content_encoding: str | None = None) -> None:
        self.objects[key] = data

    def put_file(self, key: str, path: str, *, content_type: str | None = None) -> None:
        self.objects[key] = Path(path).read_bytes()

    def put_json(self, key: str, payload: dict) -> None:
        self.objects[key] = orjson.dumps(payload)

    def get_bytes(self, key: str) -> bytes:
        return self.objects[key]

    def exists(self, key: str) -> bool:
        return key in self.objects


class FakeCoverageRepository:
    def __init__(self, items: dict[str, CoverageRecord] | None = None):
        self.items = items or {}

    def get(self, pk: str) -> CoverageRecord | None:
        return self.items.get(pk)

    def put(self, record: CoverageRecord) -> None:
        self.items[record.pk] = record


class FakeShardRepository:
    def __init__(self):
        self.items: dict[str, CanonicalShardRecord] = {}

    def get(self, shard_id: str):
        return self.items.get(shard_id)

    def put(self, record: CanonicalShardRecord) -> None:
        self.items[record.shard_id] = record


def lz4_bytes(lines: list[dict]) -> bytes:
    payload = b"".join(orjson.dumps(line) + b"\n" for line in lines)
    return lz4.frame.compress(payload)


def parquet_bytes(rows: list[dict], schema: pa.Schema) -> bytes:
    table = pa.Table.from_pylist(rows, schema=schema)
    buffer = io.BytesIO()
    pq.write_table(table, buffer, compression="zstd")
    return buffer.getvalue()


def ready_coverage(dataset_kind: DatasetKind, market: MarketRef, date: str) -> CoverageRecord:
    return CoverageRecord(
        pk=coverage_pk(dataset_kind, market, date, "daily"),
        dataset_kind=dataset_kind,
        venue=market.venue,
        market_type=market.market_type,
        instrument=market.instrument,
        date=date,
        hour="daily",
        status=CoverageStatus.READY,
        object_count=24,
        byte_count=1,
        row_count=1,
        updated_at=utc_now_iso(),
        source="test",
    )


def test_trade_source_key_uses_node_fills_by_block() -> None:
    assert trade_source_key("2026-02-19", 0) == "node_fills_by_block/hourly/20260219/0.lz4"


def test_collapse_fill_trades_uses_crossed_fill_side_and_skips_non_trade_rows() -> None:
    payload = lz4_bytes(
        [
            {
                "events": [
                    [
                        "0xbuyer",
                        {
                            "coin": "BTC",
                            "px": "66436.0",
                            "sz": "0.00017",
                            "side": "B",
                            "time": 1771459202061,
                            "hash": "0x0",
                            "oid": 1,
                            "crossed": False,
                            "tid": 7,
                            "dir": "Close Short",
                        },
                    ],
                    [
                        "0xseller",
                        {
                            "coin": "BTC",
                            "px": "66436.0",
                            "sz": "0.00017",
                            "side": "A",
                            "time": 1771459202061,
                            "hash": "0x0",
                            "oid": 2,
                            "crossed": True,
                            "tid": 7,
                            "dir": "Open Short",
                        },
                    ],
                    [
                        "0xdust",
                        {
                            "coin": "BTC",
                            "px": "1.0",
                            "sz": "1.0",
                            "side": "B",
                            "time": 1771459202061,
                            "hash": "0xskip",
                            "oid": 3,
                            "crossed": False,
                            "tid": 0,
                            "dir": "Spot Dust Conversion",
                        },
                    ],
                ]
            }
        ]
    )

    rows = collapse_fill_trades(payload, instrument="BTC", source_hour=0)

    assert len(rows) == 1
    assert rows[0].side == "Sell"
    assert rows[0].px == 66436.0
    assert rows[0].ts_ms == 1771459202061


def test_build_hyperliquid_canonical_day_orders_trade_before_snapshot() -> None:
    market = MarketRef(market_type="perp", instrument="BTC")
    date = "2026-02-19"
    store = FakeS3Store()
    coverage = FakeCoverageRepository(
        {
            coverage_pk(DatasetKind.NORMALIZED_L2, market, date, "daily"): ready_coverage(
                DatasetKind.NORMALIZED_L2, market, date
            ),
            coverage_pk(DatasetKind.NORMALIZED_TRADES, market, date, "daily"): ready_coverage(
                DatasetKind.NORMALIZED_TRADES, market, date
            ),
        }
    )
    shard_repo = FakeShardRepository()

    l2_schema = pa.schema(
        [
            ("ts_ms", pa.int64()),
            ("instrument", pa.string()),
            ("bids_json", pa.large_string()),
            ("asks_json", pa.large_string()),
            ("source_hour", pa.int8()),
            ("source_line_number", pa.int64()),
        ]
    )
    trade_schema = pa.schema(
        [
            ("ts_ms", pa.int64()),
            ("instrument", pa.string()),
            ("side", pa.string()),
            ("px", pa.float64()),
            ("sz", pa.float64()),
            ("hash", pa.string()),
            ("source_hour", pa.int8()),
            ("source_line_number", pa.int64()),
        ]
    )

    empty_l2 = parquet_bytes([], l2_schema)
    empty_trades = parquet_bytes([], trade_schema)
    for hour in range(24):
        store.objects[normalized_l2_s3_key(market, date, hour)] = empty_l2
        store.objects[normalized_trade_s3_key(market, date, hour)] = empty_trades

    store.objects[normalized_trade_s3_key(market, date, 0)] = parquet_bytes(
        [
            {
                "ts_ms": 1000,
                "instrument": "BTC",
                "side": "Buy",
                "px": 100.5,
                "sz": 0.25,
                "hash": "0xtrade",
                "source_hour": 0,
                "source_line_number": 1,
            }
        ],
        trade_schema,
    )
    store.objects[normalized_l2_s3_key(market, date, 0)] = parquet_bytes(
        [
            {
                "ts_ms": 1000,
                "instrument": "BTC",
                "bids_json": '[{"px":"100.0","sz":"1.0","n":2}]',
                "asks_json": '[{"px":"101.0","sz":"1.5","n":3}]',
                "source_hour": 0,
                "source_line_number": 2,
            }
        ],
        l2_schema,
    )

    record = build_hyperliquid_canonical_day(
        market=market,
        date=date,
        depth=1,
        s3_store=store,
        coverage_repo=coverage,
        shard_repo=shard_repo,
    )

    with zstandard.ZstdDecompressor().stream_reader(io.BytesIO(store.objects[record.shard_s3_key])) as reader:
        payload = reader.read().decode("utf-8").strip().splitlines()
    first = orjson.loads(payload[0])
    second = orjson.loads(payload[1])
    assert first["Market"]["Trade"]["side"] == "Buy"
    assert second["Market"]["L2Snapshot"]["bids"][0]["level_count"] == 2
    assert len(second["Market"]["L2Snapshot"]["bids"]) == 1
