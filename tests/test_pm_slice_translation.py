"""Round-trip translation of PMXT-shaped synthetic data into data.parquet rows."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
import io

import orjson
import pyarrow as pa
import pyarrow.parquet as pq

from poochon_backtest_data.canonical import (
    PMSliceStats,
    _build_schedule_table,
    _translate_pmxt_payload,
    DATA_PARQUET_SCHEMA,
    SCHEDULE_PARQUET_SCHEMA,
)
from poochon_backtest_data.models import (
    PolymarketMarketResolution,
    PolymarketTarget,
    PolymarketTargetKind,
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
