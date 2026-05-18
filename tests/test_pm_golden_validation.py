from __future__ import annotations

import json
import os

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from poochon_backtest_data.canonical import DATA_PARQUET_SCHEMA
from poochon_backtest_data.golden import (
    check_polymarket_golden_thresholds,
    compare_canonical_to_live,
    run_polymarket_golden_fixture,
)


def _write_canonical(path) -> None:
    table = pa.Table.from_pydict(
        {
            "ts_ms": [1_000, 2_000],
            "instrument": ["btc-updown-5m-1:Up", "btc-updown-5m-1:Up"],
            "kind": ["l2_snapshot", "delta_batch"],
            "bids": [[{"px": 0.49, "sz": 10.0, "n": 0}], None],
            "asks": [[{"px": 0.51, "sz": 11.0, "n": 0}], None],
            "delta_levels": [None, [{"side": "Buy", "px": 0.50, "sz": 12.0, "n": 0}]],
            "px": [None, None],
            "sz": [None, None],
            "side": [None, None],
        },
        schema=DATA_PARQUET_SCHEMA,
    )
    pq.write_table(table, path)


def _write_live(path) -> None:
    rows = [
        {
            "timestamp": 1_000,
            "capture_seq": 1,
            "event_type": "book",
            "token_id": "asset-1",
            "slug": "btc-updown-5m-1",
            "outcome": "Up",
            "bids": json.dumps([["0.49", "10"]]),
            "asks": json.dumps([["0.51", "11"]]),
        },
        {
            "timestamp": 2_000,
            "capture_seq": 2,
            "event_type": "price_change",
            "token_id": "asset-1",
            "slug": "btc-updown-5m-1",
            "outcome": "Up",
            "side": "BUY",
            "price": "0.50",
            "size": "12",
        },
    ]
    path.write_text("".join(json.dumps(row) + "\n" for row in rows))


def test_compare_canonical_to_live_reports_book_shape_rates(tmp_path) -> None:
    canonical = tmp_path / "canonical.parquet"
    live = tmp_path / "live.jsonl"
    _write_canonical(canonical)
    _write_live(live)

    summary = compare_canonical_to_live(canonical_path=canonical, live_path=live)

    assert summary["missing"] == 0
    assert summary["crossed_at_sample"] == 0
    assert summary["exact_top1_price_rate"] == 1.0
    assert summary["within_1_tick_top1_rate"] == 1.0
    assert summary["exact_top5_price_rate"] == 1.0
    assert check_polymarket_golden_thresholds(summary) == []


def test_polymarket_golden_thresholds_fail_on_price_shape_regression() -> None:
    errors = check_polymarket_golden_thresholds(
        {
            "missing": 1,
            "crossed_at_sample": 0,
            "exact_top1_price_rate": 0.99,
            "within_1_tick_top1_rate": 0.999,
            "exact_top5_price_rate": 0.98,
        }
    )

    assert errors == [
        "missing=1 expected 0",
        "exact_top1_price_rate=0.990000000000 below 0.999000000000",
        "within_1_tick_top1_rate=0.999000000000 below 0.999900000000",
        "exact_top5_price_rate=0.980000000000 below 0.990000000000",
    ]


def test_polymarket_golden_fixture_from_s3(tmp_path) -> None:
    fixture_prefix = os.environ.get("POOCHON_PM_GOLDEN_FIXTURE")
    if not fixture_prefix:
        pytest.skip("set POOCHON_PM_GOLDEN_FIXTURE=s3://... to run golden fixture validation")

    summary = run_polymarket_golden_fixture(
        fixture_prefix=fixture_prefix,
        work_dir=tmp_path,
    )

    assert summary["threshold_errors"] == []
