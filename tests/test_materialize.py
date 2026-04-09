from __future__ import annotations

from dataclasses import dataclass
import io
from pathlib import Path

import orjson
import pyarrow as pa
import pyarrow.parquet as pq
import zstandard

from poochon_backtest_data.models import (
    CoverageRecord,
    CoverageStatus,
    DatasetKind,
    ReplayRequest,
    coverage_pk,
    normalized_l2_s3_key,
    normalized_trade_s3_key,
    replay_s3_key,
    utc_now_iso,
)
from poochon_backtest_data.service import materialize_replay


class FakeS3Store:
    def __init__(self):
        self.objects: dict[str, bytes] = {}

    def get_bytes(self, key: str) -> bytes:
        return self.objects[key]

    def put_file(self, key: str, path: str, *, content_type: str | None = None) -> None:
        self.objects[key] = Path(path).read_bytes()

    def put_json(self, key: str, payload: dict) -> None:
        self.objects[key] = orjson.dumps(payload)


class FakeCoverageRepository:
    def __init__(self, items: dict[str, CoverageRecord]):
        self.items = items

    def get(self, pk: str) -> CoverageRecord | None:
        return self.items.get(pk)


class FakeReplayRepository:
    def __init__(self):
        self.items = {}

    def get(self, replay_id: str):
        return self.items.get(replay_id)

    def put(self, record):
        self.items[record.replay_id] = record


def parquet_bytes(rows: list[dict], schema: pa.Schema) -> bytes:
    table = pa.Table.from_pylist(rows, schema=schema)
    buffer = io.BytesIO()
    pq.write_table(table, buffer, compression="zstd")
    return buffer.getvalue()


def test_materialize_replay_orders_trade_before_snapshot_and_emits_rust_shape() -> None:
    request = ReplayRequest(symbol="BTC", date="2025-05-24")
    s3 = FakeS3Store()
    coverage = FakeCoverageRepository(
        {
            coverage_pk(DatasetKind.NORMALIZED_L2, "BTC", "2025-05-24", "daily"): CoverageRecord(
                pk=coverage_pk(DatasetKind.NORMALIZED_L2, "BTC", "2025-05-24", "daily"),
                dataset_kind=DatasetKind.NORMALIZED_L2,
                symbol="BTC",
                date="2025-05-24",
                hour="daily",
                status=CoverageStatus.READY,
                object_count=24,
                byte_count=0,
                row_count=1,
                updated_at=utc_now_iso(),
                source="test",
            ),
            coverage_pk(DatasetKind.NORMALIZED_TRADES, "BTC", "2025-05-24", "daily"): CoverageRecord(
                pk=coverage_pk(DatasetKind.NORMALIZED_TRADES, "BTC", "2025-05-24", "daily"),
                dataset_kind=DatasetKind.NORMALIZED_TRADES,
                symbol="BTC",
                date="2025-05-24",
                hour="daily",
                status=CoverageStatus.READY,
                object_count=24,
                byte_count=0,
                row_count=1,
                updated_at=utc_now_iso(),
                source="test",
            ),
        }
    )
    replay_repo = FakeReplayRepository()

    l2_schema = pa.schema(
        [
            ("ts_ms", pa.int64()),
            ("symbol", pa.string()),
            ("bids_json", pa.large_string()),
            ("asks_json", pa.large_string()),
            ("source_hour", pa.int8()),
            ("source_line_number", pa.int64()),
        ]
    )
    trade_schema = pa.schema(
        [
            ("ts_ms", pa.int64()),
            ("symbol", pa.string()),
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
        s3.objects[normalized_l2_s3_key("BTC", "2025-05-24", hour)] = empty_l2
        s3.objects[normalized_trade_s3_key("BTC", "2025-05-24", hour)] = empty_trades

    s3.objects[normalized_trade_s3_key("BTC", "2025-05-24", 0)] = parquet_bytes(
        [
            {
                "ts_ms": 1000,
                "symbol": "BTC",
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
    s3.objects[normalized_l2_s3_key("BTC", "2025-05-24", 0)] = parquet_bytes(
        [
            {
                "ts_ms": 1000,
                "symbol": "BTC",
                "bids_json": '[{"px":"100.0","sz":"1.0","n":2}]',
                "asks_json": '[{"px":"101.0","sz":"1.5","n":3}]',
                "source_hour": 0,
                "source_line_number": 2,
            }
        ],
        l2_schema,
    )

    record = materialize_replay(
        request=request,
        s3_store=s3,
        coverage_repo=coverage,
        replay_repo=replay_repo,
    )

    compressed = s3.objects[replay_s3_key(request)]
    with zstandard.ZstdDecompressor().stream_reader(io.BytesIO(compressed)) as reader:
        payload = reader.read().decode("utf-8").strip().splitlines()
    assert record.event_count == 2
    assert len(payload) == 2

    first = orjson.loads(payload[0])
    second = orjson.loads(payload[1])
    assert first["Market"]["Trade"]["side"] == "Buy"
    assert second["Market"]["L2Snapshot"]["bids"][0]["level_count"] == 2
    assert second["Market"]["L2Snapshot"]["asks"][0]["px"] == 101.0
