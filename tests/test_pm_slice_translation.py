"""Round-trip translation of PMXT-shaped synthetic data into data.parquet rows."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
import io

import orjson
import pyarrow as pa
import pyarrow.parquet as pq

from poochon_backtest_data import canonical as canonical_module
from poochon_backtest_data.canonical import (
    PMSliceStats,
    _build_schedule_table,
    _slice_pmxt_for_date,
    _translate_pmxt_payload,
    DATA_PARQUET_SCHEMA,
    SCHEDULE_PARQUET_SCHEMA,
)
from poochon_backtest_data.models import (
    CanonicalFileFamily,
    CanonicalShardFile,
    CanonicalShardRecord,
    CanonicalShardStatus,
    MarketType,
    PolymarketMarketResolution,
    PolymarketTarget,
    PolymarketTargetKind,
    Venue,
)


PMXT_INPUT_SCHEMA = pa.schema(
    [
        pa.field("timestamp_received", pa.timestamp("ms", tz="UTC"), nullable=False),
        pa.field("timestamp", pa.timestamp("ms", tz="UTC"), nullable=False),
        pa.field("market", pa.binary(66), nullable=False),
        pa.field("event_type", pa.string(), nullable=False),
        pa.field("asset_id", pa.string(), nullable=False),
        pa.field("bids", pa.string()),
        pa.field("asks", pa.string()),
        pa.field("price", pa.decimal128(9, 4)),
        pa.field("size", pa.decimal128(18, 6)),
        pa.field("side", pa.string()),
        pa.field("best_bid", pa.decimal128(9, 4)),
        pa.field("best_ask", pa.decimal128(9, 4)),
        pa.field("fee_rate_bps", pa.uint16()),
        pa.field("transaction_hash", pa.string()),
        pa.field("old_tick_size", pa.decimal128(9, 4)),
        pa.field("new_tick_size", pa.decimal128(9, 4)),
    ]
)

PMXT_TRANSLATED_SCHEMA = pa.schema(
    [
        pa.field("ts_ms", pa.int64(), nullable=False),
        pa.field("source_hour", pa.uint8(), nullable=False),
        pa.field("source_row_number", pa.int64(), nullable=False),
        pa.field("instrument", pa.string(), nullable=False),
        pa.field("kind", pa.string(), nullable=False),
        pa.field("bids", DATA_PARQUET_SCHEMA.field("bids").type),
        pa.field("asks", DATA_PARQUET_SCHEMA.field("asks").type),
        pa.field("full_bids", DATA_PARQUET_SCHEMA.field("bids").type),
        pa.field("full_asks", DATA_PARQUET_SCHEMA.field("asks").type),
        pa.field("delta_levels", DATA_PARQUET_SCHEMA.field("delta_levels").type),
        pa.field("px", pa.float64()),
        pa.field("sz", pa.float64()),
        pa.field("side", pa.string()),
        pa.field("best_bid", pa.float64()),
        pa.field("best_ask", pa.float64()),
    ]
)


def _ts(ms: int) -> datetime:
    return datetime.fromtimestamp(ms / 1000, tz=UTC)


def _market_bytes() -> bytes:
    # Polymarket condition IDs are 0x + 64 hex = 66 chars total.
    return b"0x" + b"a" * 64


def _make_pmxt_payload(rows: list[dict]) -> bytes:
    columns: dict[str, list] = {field.name: [] for field in PMXT_INPUT_SCHEMA}
    for row in rows:
        for field in PMXT_INPUT_SCHEMA:
            columns[field.name].append(row.get(field.name))
    table = pa.Table.from_pydict(columns, schema=PMXT_INPUT_SCHEMA)
    buf = io.BytesIO()
    pq.write_table(table, buf)
    return buf.getvalue()


def _write_translated_trade_rows(
    path,
    *,
    hour: int,
    row_count: int,
    instrument: str,
) -> None:
    table = pa.Table.from_pydict(
        {
            "ts_ms": [1_000 + idx for idx in range(row_count)],
            "source_hour": [hour] * row_count,
            "source_row_number": list(range(row_count)),
            "instrument": [instrument] * row_count,
            "kind": ["trade"] * row_count,
            "bids": [None] * row_count,
            "asks": [None] * row_count,
            "full_bids": [None] * row_count,
            "full_asks": [None] * row_count,
            "delta_levels": [None] * row_count,
            "px": [0.5] * row_count,
            "sz": [1.0] * row_count,
            "side": ["Buy"] * row_count,
            "best_bid": [None] * row_count,
            "best_ask": [None] * row_count,
        },
        schema=PMXT_TRANSLATED_SCHEMA,
    )
    pq.write_table(table, path)


def test_translate_pmxt_payload_handles_book_delta_and_trade() -> None:
    asset_id = "asset-1"
    rows = [
        {
            "timestamp_received": _ts(1_000_000),
            "timestamp": _ts(1_000_000),
            "market": _market_bytes(),
            "event_type": "book",
            "asset_id": asset_id,
            "bids": orjson.dumps(
                [{"price": "0.51", "size": "100"}, {"price": "0.50", "size": "200"}]
            ).decode(),
            "asks": orjson.dumps([{"price": "0.52", "size": "75"}]).decode(),
        },
        {
            "timestamp_received": _ts(1_000_100),
            "timestamp": _ts(1_000_100),
            "market": _market_bytes(),
            "event_type": "price_change",
            "asset_id": asset_id,
            "price": Decimal("0.51"),
            "size": Decimal("0"),
            "side": "BUY",
        },
        {
            "timestamp_received": _ts(1_000_200),
            "timestamp": _ts(1_000_200),
            "market": _market_bytes(),
            "event_type": "last_trade_price",
            "asset_id": asset_id,
            "price": Decimal("0.515"),
            "size": Decimal("10"),
            "side": "BUY",
        },
        {
            "timestamp_received": _ts(1_000_300),
            "timestamp": _ts(1_000_300),
            "market": _market_bytes(),
            "event_type": "tick_size_change",
            "asset_id": asset_id,
            "old_tick_size": Decimal("0.001"),
            "new_tick_size": Decimal("0.0005"),
        },
        # noise: irrelevant asset id should be dropped by predicate filter.
        {
            "timestamp_received": _ts(1_000_400),
            "timestamp": _ts(1_000_400),
            "market": _market_bytes(),
            "event_type": "last_trade_price",
            "asset_id": "different-asset",
            "price": Decimal("0.999"),
            "size": Decimal("1"),
            "side": "SELL",
        },
    ]
    payload = _make_pmxt_payload(rows)
    asset_to_instrument = {asset_id: "btc-updown-5m-1:Up"}
    asset_id_set = pa.array([asset_id], type=pa.string())

    stats = PMSliceStats()
    out = _translate_pmxt_payload(
        payload,
        asset_to_instrument=asset_to_instrument,
        asset_id_set=asset_id_set,
        depth=5,
        stats=stats,
    )

    assert stats.rows_in == 5
    assert len(out["ts_ms"]) == 3
    assert out["kind"] == ["l2_snapshot", "delta_batch", "trade"]
    assert out["instrument"] == ["btc-updown-5m-1:Up"] * 3

    # Snapshot
    assert out["bids"][0] == [
        {"px": 0.51, "sz": 100.0, "n": 0},
        {"px": 0.50, "sz": 200.0, "n": 0},
    ]
    assert out["asks"][0] == [{"px": 0.52, "sz": 75.0, "n": 0}]
    assert out["delta_levels"][0] is None
    assert out["px"][0] is None

    # Delta
    assert out["bids"][1] is None
    assert out["asks"][1] is None
    assert out["delta_levels"][1] == [
        {"side": "Buy", "px": 0.51, "sz": 0.0, "n": 0},
    ]

    # Trade
    assert out["bids"][2] is None
    assert out["delta_levels"][2] is None
    assert out["px"][2] == 0.515
    assert out["sz"][2] == 10.0
    assert out["side"][2] == "Buy"

    # Round-trip into the canonical schema cleanly.
    table = pa.Table.from_pydict(out, schema=DATA_PARQUET_SCHEMA)
    assert table.num_rows == 3


def test_slice_pmxt_for_date_writes_chronological_day_file(monkeypatch, tmp_path) -> None:
    asset_id = "asset-1"
    empty_payload = _make_pmxt_payload([])
    payloads = {
        0: _make_pmxt_payload(
            [
                {
                    "timestamp_received": _ts(2_000),
                    "timestamp": _ts(2_000),
                    "market": _market_bytes(),
                    "event_type": "book",
                    "asset_id": asset_id,
                    "bids": orjson.dumps([{"price": "0.61", "size": "100"}]).decode(),
                    "asks": orjson.dumps([{"price": "0.62", "size": "75"}]).decode(),
                },
                {
                    "timestamp_received": _ts(1_000),
                    "timestamp": _ts(1_000),
                    "market": _market_bytes(),
                    "event_type": "price_change",
                    "asset_id": asset_id,
                    "price": Decimal("0.41"),
                    "size": Decimal("3"),
                    "side": "BUY",
                },
                {
                    "timestamp_received": _ts(1_000),
                    "timestamp": _ts(1_000),
                    "market": _market_bytes(),
                    "event_type": "last_trade_price",
                    "asset_id": asset_id,
                    "price": Decimal("0.42"),
                    "size": Decimal("2"),
                    "side": "SELL",
                },
            ]
        ),
        1: _make_pmxt_payload(
            [
                {
                    "timestamp_received": _ts(1_000),
                    "timestamp": _ts(1_000),
                    "market": _market_bytes(),
                    "event_type": "book",
                    "asset_id": asset_id,
                    "bids": orjson.dumps([{"price": "0.51", "size": "100"}]).decode(),
                    "asks": orjson.dumps([{"price": "0.52", "size": "75"}]).decode(),
                },
                {
                    "timestamp_received": _ts(1_500),
                    "timestamp": _ts(1_500),
                    "market": _market_bytes(),
                    "event_type": "last_trade_price",
                    "asset_id": asset_id,
                    "price": Decimal("0.52"),
                    "size": Decimal("1"),
                    "side": "BUY",
                },
            ]
        ),
    }

    def fake_fetch(_s3_store, _date: str, hour: int) -> bytes:
        return payloads.get(hour, empty_payload)

    monkeypatch.setattr(canonical_module, "_fetch_pmxt_payload", fake_fetch)

    stats = PMSliceStats()
    out_path = tmp_path / "data.parquet"
    rows_out = _slice_pmxt_for_date(
        s3_store=object(),  # type: ignore[arg-type]
        date="2026-04-17",
        asset_to_instrument={asset_id: "btc-updown-5m-1:Up"},
        asset_ids=[asset_id],
        depth=5,
        stats=stats,
        data_parquet_path=out_path,
    )

    assert rows_out == 5
    assert stats.rows_in == 5
    assert stats.rows_out == 5

    table = pq.read_table(out_path)
    assert table.schema.names == DATA_PARQUET_SCHEMA.names
    for actual, expected in zip(table.schema, DATA_PARQUET_SCHEMA):
        assert actual.type == expected.type

    rows = table.to_pylist()
    assert [row["ts_ms"] for row in rows] == [1_000, 1_000, 1_000, 1_500, 2_000]
    assert [row["kind"] for row in rows] == [
        "delta_batch",
        "trade",
        "l2_snapshot",
        "trade",
        "l2_snapshot",
    ]
    assert rows[1]["px"] == 0.42
    assert rows[2]["bids"][0]["px"] == 0.51
    assert rows[3]["px"] == 0.52
    assert rows[4]["bids"][0]["px"] == 0.61


def test_slice_pmxt_for_date_parses_pmxt_array_books_at_top_of_book(
    monkeypatch, tmp_path
) -> None:
    asset_id = "asset-1"
    empty_payload = _make_pmxt_payload([])
    payloads = {
        0: _make_pmxt_payload(
            [
                {
                    "timestamp_received": _ts(1_000),
                    "timestamp": _ts(1_000),
                    "market": _market_bytes(),
                    "event_type": "book",
                    "asset_id": asset_id,
                    "bids": orjson.dumps(
                        [
                            ["0.01", "100"],
                            ["0.68", "451.62"],
                            ["0.69", "373.22"],
                            ["0.70", "48"],
                        ]
                    ).decode(),
                    "asks": orjson.dumps(
                        [
                            ["0.99", "11186.09"],
                            ["0.80", "5"],
                            ["0.74", "52.04"],
                            ["0.73", "20.17"],
                        ]
                    ).decode(),
                },
            ]
        )
    }

    def fake_fetch(_s3_store, _date: str, hour: int) -> bytes:
        return payloads.get(hour, empty_payload)

    monkeypatch.setattr(canonical_module, "_fetch_pmxt_payload", fake_fetch)

    stats = PMSliceStats()
    out_path = tmp_path / "data.parquet"
    rows_out = _slice_pmxt_for_date(
        s3_store=object(),  # type: ignore[arg-type]
        date="2026-04-17",
        asset_to_instrument={asset_id: "btc-updown-5m-1:Down"},
        asset_ids=[asset_id],
        depth=2,
        stats=stats,
        data_parquet_path=out_path,
    )

    assert rows_out == 1
    row = pq.read_table(out_path).to_pylist()[0]
    assert row["bids"] == [
        {"px": 0.70, "sz": 48.0, "n": 0},
        {"px": 0.69, "sz": 373.22, "n": 0},
    ]
    assert row["asks"] == [
        {"px": 0.73, "sz": 20.17, "n": 0},
        {"px": 0.74, "sz": 52.04, "n": 0},
    ]


def test_repaired_day_file_consumes_all_batches_from_all_translated_hours(tmp_path) -> None:
    import duckdb

    row_count = 70_000
    first_hour = tmp_path / "translated-00.parquet"
    second_hour = tmp_path / "translated-01.parquet"
    out_path = tmp_path / "data.parquet"
    _write_translated_trade_rows(
        first_hour,
        hour=0,
        row_count=row_count,
        instrument="btc-updown-5m-1:Up",
    )
    _write_translated_trade_rows(
        second_hour,
        hour=1,
        row_count=row_count,
        instrument="btc-updown-5m-1:Up",
    )

    con = duckdb.connect(":memory:")
    try:
        rows_out = canonical_module._write_pm_repaired_day_file(
            con=con,
            translated_paths=[first_hour, second_hour],
            data_parquet_path=out_path,
            depth=5,
            stats=PMSliceStats(),
        )
    finally:
        con.close()

    assert rows_out == row_count * 2
    assert pq.ParquetFile(out_path).metadata.num_rows == row_count * 2


def test_slice_pmxt_for_date_prunes_stale_levels_with_pmxt_best_bounds(
    monkeypatch, tmp_path
) -> None:
    asset_id = "asset-1"
    empty_payload = _make_pmxt_payload([])
    payloads = {
        0: _make_pmxt_payload(
            [
                {
                    "timestamp_received": _ts(1_000),
                    "timestamp": _ts(1_000),
                    "market": _market_bytes(),
                    "event_type": "book",
                    "asset_id": asset_id,
                    "bids": orjson.dumps([{"price": "0.69", "size": "100"}]).decode(),
                    "asks": orjson.dumps(
                        [
                            {"price": "0.35", "size": "7.69"},
                            {"price": "0.70", "size": "122.35"},
                        ]
                    ).decode(),
                },
                {
                    "timestamp_received": _ts(2_000),
                    "timestamp": _ts(2_000),
                    "market": _market_bytes(),
                    "event_type": "price_change",
                    "asset_id": asset_id,
                    "price": Decimal("0.69"),
                    "size": Decimal("116.29"),
                    "side": "BUY",
                    "best_bid": Decimal("0.69"),
                    "best_ask": Decimal("0.70"),
                },
            ]
        )
    }

    def fake_fetch(_s3_store, _date: str, hour: int) -> bytes:
        return payloads.get(hour, empty_payload)

    monkeypatch.setattr(canonical_module, "_fetch_pmxt_payload", fake_fetch)

    stats = PMSliceStats()
    out_path = tmp_path / "data.parquet"
    rows_out = _slice_pmxt_for_date(
        s3_store=object(),  # type: ignore[arg-type]
        date="2026-04-17",
        asset_to_instrument={asset_id: "btc-updown-5m-1:Up"},
        asset_ids=[asset_id],
        depth=5,
        stats=stats,
        data_parquet_path=out_path,
    )

    assert rows_out == 2
    assert stats.best_bound_deletes == 1
    rows = pq.read_table(out_path).to_pylist()
    assert rows[1]["delta_levels"] == [
        {"side": "Buy", "px": 0.69, "sz": 116.29, "n": 0},
        {"side": "Sell", "px": 0.35, "sz": 0.0, "n": 0},
    ]


def test_slice_pmxt_for_date_prunes_new_stale_level_when_best_bound_is_unchanged(
    monkeypatch, tmp_path
) -> None:
    asset_id = "asset-1"
    empty_payload = _make_pmxt_payload([])
    payloads = {
        0: _make_pmxt_payload(
            [
                {
                    "timestamp_received": _ts(1_000),
                    "timestamp": _ts(1_000),
                    "market": _market_bytes(),
                    "event_type": "book",
                    "asset_id": asset_id,
                    "bids": orjson.dumps([{"price": "0.69", "size": "100"}]).decode(),
                    "asks": orjson.dumps([{"price": "0.70", "size": "100"}]).decode(),
                },
                {
                    "timestamp_received": _ts(2_000),
                    "timestamp": _ts(2_000),
                    "market": _market_bytes(),
                    "event_type": "price_change",
                    "asset_id": asset_id,
                    "price": Decimal("0.71"),
                    "size": Decimal("12"),
                    "side": "BUY",
                    "best_bid": Decimal("0.69"),
                    "best_ask": Decimal("0.70"),
                },
            ]
        )
    }

    def fake_fetch(_s3_store, _date: str, hour: int) -> bytes:
        return payloads.get(hour, empty_payload)

    monkeypatch.setattr(canonical_module, "_fetch_pmxt_payload", fake_fetch)

    stats = PMSliceStats()
    out_path = tmp_path / "data.parquet"
    rows_out = _slice_pmxt_for_date(
        s3_store=object(),  # type: ignore[arg-type]
        date="2026-04-17",
        asset_to_instrument={asset_id: "btc-updown-5m-1:Up"},
        asset_ids=[asset_id],
        depth=5,
        stats=stats,
        data_parquet_path=out_path,
    )

    assert rows_out == 2
    assert stats.best_bound_deletes == 1
    rows = pq.read_table(out_path).to_pylist()
    assert rows[1]["delta_levels"] == [
        {"side": "Buy", "px": 0.71, "sz": 12.0, "n": 0},
        {"side": "Buy", "px": 0.71, "sz": 0.0, "n": 0},
    ]


def test_slice_pmxt_for_date_publishes_deeper_level_exposed_by_delete(
    monkeypatch, tmp_path
) -> None:
    asset_id = "asset-1"
    empty_payload = _make_pmxt_payload([])
    payloads = {
        0: _make_pmxt_payload(
            [
                {
                    "timestamp_received": _ts(1_000),
                    "timestamp": _ts(1_000),
                    "market": _market_bytes(),
                    "event_type": "book",
                    "asset_id": asset_id,
                    "bids": orjson.dumps([{"price": "0.60", "size": "100"}]).decode(),
                    "asks": orjson.dumps(
                        [
                            {"price": "0.70", "size": "10"},
                            {"price": "0.71", "size": "20"},
                            {"price": "0.72", "size": "30"},
                        ]
                    ).decode(),
                },
                {
                    "timestamp_received": _ts(2_000),
                    "timestamp": _ts(2_000),
                    "market": _market_bytes(),
                    "event_type": "price_change",
                    "asset_id": asset_id,
                    "price": Decimal("0.70"),
                    "size": Decimal("0"),
                    "side": "SELL",
                    "best_bid": Decimal("0.60"),
                    "best_ask": Decimal("0.71"),
                },
            ]
        )
    }

    def fake_fetch(_s3_store, _date: str, hour: int) -> bytes:
        return payloads.get(hour, empty_payload)

    monkeypatch.setattr(canonical_module, "_fetch_pmxt_payload", fake_fetch)

    stats = PMSliceStats()
    out_path = tmp_path / "data.parquet"
    rows_out = _slice_pmxt_for_date(
        s3_store=object(),  # type: ignore[arg-type]
        date="2026-04-17",
        asset_to_instrument={asset_id: "btc-updown-5m-1:Up"},
        asset_ids=[asset_id],
        depth=2,
        stats=stats,
        data_parquet_path=out_path,
    )

    assert rows_out == 2
    assert stats.exposed_level_inserts == 1
    rows = pq.read_table(out_path).to_pylist()
    assert rows[0]["asks"] == [
        {"px": 0.70, "sz": 10.0, "n": 0},
        {"px": 0.71, "sz": 20.0, "n": 0},
    ]
    assert rows[1]["delta_levels"] == [
        {"side": "Sell", "px": 0.70, "sz": 0.0, "n": 0},
        {"side": "Sell", "px": 0.72, "sz": 30.0, "n": 0},
    ]


def test_slice_pmxt_for_date_does_not_prune_level_touched_at_same_timestamp(
    monkeypatch, tmp_path
) -> None:
    asset_id = "asset-1"
    empty_payload = _make_pmxt_payload([])
    payloads = {
        0: _make_pmxt_payload(
            [
                {
                    "timestamp_received": _ts(1_000),
                    "timestamp": _ts(1_000),
                    "market": _market_bytes(),
                    "event_type": "book",
                    "asset_id": asset_id,
                    "bids": orjson.dumps([{"price": "0.19", "size": "100"}]).decode(),
                    "asks": orjson.dumps([{"price": "0.21", "size": "20"}]).decode(),
                },
                {
                    "timestamp_received": _ts(2_000),
                    "timestamp": _ts(2_000),
                    "market": _market_bytes(),
                    "event_type": "price_change",
                    "asset_id": asset_id,
                    "price": Decimal("0.20"),
                    "size": Decimal("31.62"),
                    "side": "SELL",
                    "best_bid": Decimal("0.19"),
                    "best_ask": Decimal("0.20"),
                },
                {
                    "timestamp_received": _ts(2_001),
                    "timestamp": _ts(2_000),
                    "market": _market_bytes(),
                    "event_type": "price_change",
                    "asset_id": asset_id,
                    "price": Decimal("0.06"),
                    "size": Decimal("347.67"),
                    "side": "BUY",
                    "best_bid": Decimal("0.19"),
                    "best_ask": Decimal("0.21"),
                },
            ]
        )
    }

    def fake_fetch(_s3_store, _date: str, hour: int) -> bytes:
        return payloads.get(hour, empty_payload)

    monkeypatch.setattr(canonical_module, "_fetch_pmxt_payload", fake_fetch)

    stats = PMSliceStats()
    out_path = tmp_path / "data.parquet"
    rows_out = _slice_pmxt_for_date(
        s3_store=object(),  # type: ignore[arg-type]
        date="2026-04-17",
        asset_to_instrument={asset_id: "btc-updown-5m-1:Up"},
        asset_ids=[asset_id],
        depth=5,
        stats=stats,
        data_parquet_path=out_path,
    )

    assert rows_out == 2
    rows = pq.read_table(out_path).to_pylist()
    assert rows[1]["delta_levels"] == [
        {"side": "Buy", "px": 0.06, "sz": 347.67, "n": 0},
        {"side": "Sell", "px": 0.20, "sz": 31.62, "n": 0},
    ]


def test_slice_pmxt_for_date_coalesces_same_timestamp_duplicate_levels(
    monkeypatch, tmp_path
) -> None:
    asset_id = "asset-1"
    empty_payload = _make_pmxt_payload([])
    payloads = {
        0: _make_pmxt_payload(
            [
                {
                    "timestamp_received": _ts(1_000),
                    "timestamp": _ts(1_000),
                    "market": _market_bytes(),
                    "event_type": "book",
                    "asset_id": asset_id,
                    "bids": orjson.dumps([{"price": "0.40", "size": "10"}]).decode(),
                    "asks": orjson.dumps([{"price": "0.60", "size": "20"}]).decode(),
                },
                {
                    "timestamp_received": _ts(2_000),
                    "timestamp": _ts(2_000),
                    "market": _market_bytes(),
                    "event_type": "price_change",
                    "asset_id": asset_id,
                    "price": Decimal("0.40"),
                    "size": Decimal("25"),
                    "side": "BUY",
                    "best_bid": Decimal("0.40"),
                    "best_ask": Decimal("0.60"),
                },
                {
                    "timestamp_received": _ts(2_000),
                    "timestamp": _ts(2_000),
                    "market": _market_bytes(),
                    "event_type": "price_change",
                    "asset_id": asset_id,
                    "price": Decimal("0.40"),
                    "size": Decimal("7"),
                    "side": "BUY",
                    "best_bid": Decimal("0.40"),
                    "best_ask": Decimal("0.60"),
                },
                {
                    "timestamp_received": _ts(2_000),
                    "timestamp": _ts(2_000),
                    "market": _market_bytes(),
                    "event_type": "price_change",
                    "asset_id": asset_id,
                    "price": Decimal("0.60"),
                    "size": Decimal("18"),
                    "side": "SELL",
                    "best_bid": Decimal("0.40"),
                    "best_ask": Decimal("0.60"),
                },
                {
                    "timestamp_received": _ts(2_000),
                    "timestamp": _ts(2_000),
                    "market": _market_bytes(),
                    "event_type": "price_change",
                    "asset_id": asset_id,
                    "price": Decimal("0.60"),
                    "size": Decimal("0"),
                    "side": "SELL",
                    "best_bid": Decimal("0.40"),
                    "best_ask": Decimal("0.60"),
                },
            ]
        )
    }

    def fake_fetch(_s3_store, _date: str, hour: int) -> bytes:
        return payloads.get(hour, empty_payload)

    monkeypatch.setattr(canonical_module, "_fetch_pmxt_payload", fake_fetch)

    stats = PMSliceStats()
    out_path = tmp_path / "data.parquet"
    rows_out = _slice_pmxt_for_date(
        s3_store=object(),  # type: ignore[arg-type]
        date="2026-04-17",
        asset_to_instrument={asset_id: "btc-updown-5m-1:Up"},
        asset_ids=[asset_id],
        depth=5,
        stats=stats,
        data_parquet_path=out_path,
    )

    assert rows_out == 2
    rows = pq.read_table(out_path).to_pylist()
    assert rows[1]["delta_levels"] == [
        {"side": "Buy", "px": 0.40, "sz": 7.0, "n": 0},
        {"side": "Sell", "px": 0.60, "sz": 0.0, "n": 0},
    ]


def test_build_schedule_table_groups_by_slug() -> None:
    target = PolymarketTarget(
        target_kind=PolymarketTargetKind.SERIES, target_key="btc-updown-5m"
    )
    resolutions = [
        PolymarketMarketResolution(
            slug="btc-updown-5m-1771459200",
            outcome="Up",
            market_id="0xa",
            asset_id="up-1",
            instrument="btc-updown-5m-1771459200:Up",
            start_ts_ms=1_771_459_200_000,
            end_ts_ms=1_771_459_500_000,
            price_to_beat=101000.0,
            price_to_beat_source="vatic",
            price_to_beat_quality="exact",
            settlement_payout=1.0,
        ),
        PolymarketMarketResolution(
            slug="btc-updown-5m-1771459200",
            outcome="Down",
            market_id="0xa",
            asset_id="down-1",
            instrument="btc-updown-5m-1771459200:Down",
            start_ts_ms=1_771_459_200_000,
            end_ts_ms=1_771_459_500_000,
            price_to_beat=101000.0,
            price_to_beat_source="vatic",
            price_to_beat_quality="exact",
            settlement_payout=0.0,
        ),
    ]
    table = _build_schedule_table(target, resolutions)
    assert table.schema == SCHEDULE_PARQUET_SCHEMA
    assert table.num_rows == 1
    row = table.to_pylist()[0]
    assert row["slug"] == "btc-updown-5m-1771459200"
    assert row["target_kind"] == "series"
    assert row["target_key"] == "btc-updown-5m"
    assert row["start_ts_ms"] == 1_771_459_200_000
    assert {o["outcome"] for o in row["outcomes"]} == {"Up", "Down"}
    payouts = {o["outcome"]: o["settlement_payout"] for o in row["outcomes"]}
    assert payouts == {"Up": 1.0, "Down": 0.0}


def test_load_existing_pm_schedule_table_reuses_valid_schedule(tmp_path) -> None:
    target = PolymarketTarget(
        target_kind=PolymarketTargetKind.SERIES, target_key="btc-updown-5m"
    )
    schedule_path = tmp_path / "schedule.parquet"
    schedule_table = _build_schedule_table(
        target,
        [
            PolymarketMarketResolution(
                slug="btc-updown-5m-1771459200",
                outcome="Up",
                market_id="0xa",
                asset_id="up-1",
                instrument="btc-updown-5m-1771459200:Up",
                start_ts_ms=1_771_459_200_000,
                end_ts_ms=1_771_459_500_000,
            ),
            PolymarketMarketResolution(
                slug="btc-updown-5m-1771459200",
                outcome="Down",
                market_id="0xa",
                asset_id="down-1",
                instrument="btc-updown-5m-1771459200:Down",
                start_ts_ms=1_771_459_200_000,
                end_ts_ms=1_771_459_500_000,
            ),
        ],
    )
    pq.write_table(schedule_table, schedule_path)

    class FakeS3:
        def object_size(self, key: str) -> int | None:
            assert key == "schedule.parquet"
            return schedule_path.stat().st_size

        def get_bytes(self, key: str) -> bytes:
            assert key == "schedule.parquet"
            return schedule_path.read_bytes()

    existing = CanonicalShardRecord(
        shard_id="shard",
        status=CanonicalShardStatus.READY,
        venue=Venue.POLYMARKET,
        market_type=MarketType.BINARY,
        date="2026-04-17",
        depth=5,
        shard_prefix="canonical/polymarket/series/btc-updown-5m/date=2026-04-17/depth=5/",
        manifest_s3_key="manifest.json",
        target_kind=PolymarketTargetKind.SERIES,
        target_key="btc-updown-5m",
        schedule_file=CanonicalShardFile(
            family=CanonicalFileFamily.SCHEDULE,
            file_name="schedule.parquet",
            s3_key="schedule.parquet",
            row_count=1,
        ),
        created_at="2026-05-18T00:00:00Z",
        updated_at="2026-05-18T00:00:00Z",
    )

    loaded = canonical_module._load_existing_pm_schedule_table(
        existing=existing,
        target=target,
        s3_store=FakeS3(),  # type: ignore[arg-type]
        schedule_key="schedule.parquet",
    )

    assert loaded is not None
    assert loaded.schema == SCHEDULE_PARQUET_SCHEMA
    assert canonical_module._asset_to_instrument_from_schedule(loaded) == {
        "down-1": "btc-updown-5m-1771459200:Down",
        "up-1": "btc-updown-5m-1771459200:Up",
    }
