from __future__ import annotations

from poochon_backtest_data.hyperliquid import iso_to_epoch_ms, parse_l2_snapshot, parse_trade


def test_iso_to_epoch_ms_handles_nanoseconds() -> None:
    assert iso_to_epoch_ms("2025-03-31T23:59:59.962208772") == 1743465599962


def test_parse_trade_filters_non_target_symbol() -> None:
    raw = {
        "coin": "DOGE",
        "side": "B",
        "time": "2025-03-31T23:59:59.962208772",
        "px": "0.16656",
        "sz": "600.0",
        "hash": "0xabc",
    }
    assert parse_trade(raw, symbol="BTC", source_hour=0, source_line_number=1) is None


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
    assert snapshot.symbol == "BTC"
    assert snapshot.ts_ms == 1705309199653
    assert '"px":"42706.0"' in snapshot.bids_json
    assert snapshot.source_line_number == 14
