"""Round-trip Hyperliquid lz4 JSONL → parser output."""

from __future__ import annotations

import io

import lz4.frame
import orjson

from poochon_backtest_data.hyperliquid import (
    collapse_fill_trades,
    parse_l2_lz4_payload,
)


def _lz4_jsonl(rows: list[dict]) -> bytes:
    buf = io.BytesIO()
    with lz4.frame.open(buf, mode="wb") as writer:
        for row in rows:
            writer.write(orjson.dumps(row) + b"\n")
    return buf.getvalue()


def test_parse_l2_lz4_payload_orders_by_ts_and_line() -> None:
    payload = _lz4_jsonl(
        [
            {
                "raw": {
                    "data": {
                        "time": 1_771_459_200_500,
                        "coin": "BTC",
                        "levels": [
                            [{"px": "60000.5", "sz": "0.5", "n": 2}],
                            [{"px": "60001.0", "sz": "1.5", "n": 3}],
                        ],
                    }
                }
            },
            {
                "raw": {
                    "data": {
                        "time": 1_771_459_200_500,  # same ts, later line
                        "coin": "BTC",
                        "levels": [
                            [{"px": "60000.4", "sz": "1.0", "n": 1}],
                            [{"px": "60001.5", "sz": "0.8", "n": 2}],
                        ],
                    }
                }
            },
            {
                "raw": {
                    "data": {
                        "time": 1_771_459_100_000,  # earlier ts, comes first
                        "coin": "BTC",
                        "levels": [[], []],
                    }
                }
            },
        ]
    )
    snapshots = parse_l2_lz4_payload(payload, source_hour=3)
    assert len(snapshots) == 3
    assert [s.ts_ms for s in snapshots] == [
        1_771_459_100_000,
        1_771_459_200_500,
        1_771_459_200_500,
    ]
    # JSON content preserved
    parsed_bids = orjson.loads(snapshots[1].bids_json)
    assert parsed_bids == [{"px": "60000.5", "sz": "0.5", "n": 2}]


def test_collapse_fill_trades_filters_dedupes_and_picks_crossed() -> None:
    payload = _lz4_jsonl(
        [
            {
                "events": [
                    [
                        "0xMaker",
                        {
                            "coin": "BTC",
                            "time": 1_771_459_200_000,
                            "tid": 100,
                            "hash": "0xH",
                            "px": "60000",
                            "sz": "0.5",
                            "side": "B",
                            "crossed": False,
                            "oid": 1,
                            "dir": "Open Long",
                        },
                    ],
                    [
                        "0xTaker",
                        {
                            "coin": "BTC",
                            "time": 1_771_459_200_000,
                            "tid": 100,
                            "hash": "0xH",
                            "px": "60000",
                            "sz": "0.5",
                            "side": "A",
                            "crossed": True,
                            "oid": 5,
                            "dir": "Close Short",
                        },
                    ],
                    [
                        "0xOther",
                        {
                            "coin": "ETH",  # filtered out
                            "time": 1_771_459_200_000,
                            "tid": 200,
                            "hash": "0xZ",
                            "px": "3000",
                            "sz": "1",
                            "side": "B",
                            "crossed": True,
                            "oid": 3,
                            "dir": "Open Long",
                        },
                    ],
                    [
                        "0xZero",
                        {
                            "coin": "BTC",
                            "time": 1_771_459_200_500,
                            "tid": 0,  # tid==0 dropped
                            "hash": "0xZ",
                            "px": "60000",
                            "sz": "0.1",
                            "side": "B",
                            "crossed": True,
                            "oid": 9,
                            "dir": "Open Long",
                        },
                    ],
                    [
                        "0xDust",
                        {
                            "coin": "BTC",
                            "time": 1_771_459_200_500,
                            "tid": 99,
                            "hash": "0xD",
                            "px": "60000",
                            "sz": "0.01",
                            "side": "B",
                            "crossed": True,
                            "oid": 11,
                            "dir": "Spot Dust Conversion",  # filtered out
                        },
                    ],
                ]
            }
        ]
    )

    trades = collapse_fill_trades(payload, instrument="BTC", source_hour=3)
    assert len(trades) == 1
    trade = trades[0]
    # Crossed candidate wins: side "A" → "Sell"
    assert trade.side == "Sell"
    assert trade.ts_ms == 1_771_459_200_000
    assert trade.px == 60000.0
    assert trade.sz == 0.5
    assert trade.instrument == "BTC"
