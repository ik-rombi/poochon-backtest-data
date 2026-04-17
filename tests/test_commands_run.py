from __future__ import annotations

from unittest.mock import MagicMock, patch
from argparse import Namespace

import pytest

from poochon_backtest_data.commands import run as run_cmd
from poochon_backtest_data.commands._session import SessionBundle
from poochon_backtest_data.models import MarketRef, MarketType, OutcomesMode, Venue


@pytest.fixture
def fake_bundle():
    bundle = MagicMock(spec=SessionBundle)
    bundle.s3_store = MagicMock()
    bundle.coverage_repo = MagicMock()
    bundle.shard_repo = MagicMock()
    bundle.settings = MagicMock()
    bundle.settings.request_payer = "requester"
    return bundle


def _args(**kwargs) -> Namespace:
    defaults = {
        "command": "run",
        "venue": None,
        "stage": None,
        "market_type": "perp",
        "instrument": "BTC",
        "series": "btc-updown-5m",
        "start_date": "2026-02-19",
        "end_date": "2026-02-19",
        "depth": 20,
        "outcomes": OutcomesMode.BOTH.value,
        "force": False,
    }
    defaults.update(kwargs)
    return Namespace(**defaults)


class TestHyperliquidDispatch:
    def test_raw_calls_backfill_day_per_date(self, fake_bundle):
        args = _args(venue="hyperliquid", stage="raw", start_date="2026-02-19", end_date="2026-02-20")
        with patch.object(run_cmd, "open_session", return_value=fake_bundle), \
             patch.object(run_cmd, "backfill_day") as mock_fn:
            exit_code = run_cmd.handle(args)
        assert exit_code == 0
        assert mock_fn.call_count == 2
        dates = [call.kwargs["date"] for call in mock_fn.call_args_list]
        assert dates == ["2026-02-19", "2026-02-20"]

    def test_normalize_calls_normalize_day_per_date(self, fake_bundle):
        args = _args(venue="hyperliquid", stage="normalize")
        with patch.object(run_cmd, "open_session", return_value=fake_bundle), \
             patch.object(run_cmd, "normalize_day") as mock_fn:
            run_cmd.handle(args)
        assert mock_fn.call_count == 1

    def test_canonical_passes_force_flag(self, fake_bundle):
        args = _args(venue="hyperliquid", stage="canonical", force=True)
        with patch.object(run_cmd, "open_session", return_value=fake_bundle), \
             patch.object(run_cmd, "build_hyperliquid_canonical_day") as mock_fn:
            run_cmd.handle(args)
        assert mock_fn.call_args.kwargs["force"] is True

    def test_all_calls_sync_window(self, fake_bundle):
        args = _args(venue="hyperliquid", stage="all")
        with patch.object(run_cmd, "open_session", return_value=fake_bundle), \
             patch.object(run_cmd, "sync_window") as mock_fn:
            run_cmd.handle(args)
        mock_fn.assert_called_once()
        assert mock_fn.call_args.kwargs["depth"] == 20


class TestPolymarketDispatch:
    def test_all_calls_sync_series(self, fake_bundle):
        args = _args(venue="polymarket", stage="all", depth=5)
        with patch.object(run_cmd, "open_session", return_value=fake_bundle), \
             patch.object(run_cmd, "sync_series") as mock_fn, \
             patch.object(run_cmd, "require_telonex_api_key", return_value="test-key"):
            run_cmd.handle(args)
        mock_fn.assert_called_once()

    def test_canonical_calls_build_polymarket_per_date(self, fake_bundle):
        args = _args(venue="polymarket", stage="canonical", start_date="2026-02-19", end_date="2026-02-21")
        with patch.object(run_cmd, "open_session", return_value=fake_bundle), \
             patch.object(run_cmd, "build_polymarket_canonical_day_from_storage") as mock_fn:
            run_cmd.handle(args)
        assert mock_fn.call_count == 3

    def test_discover_writes_metadata_per_resolution(self, fake_bundle):
        args = _args(venue="polymarket", stage="discover")
        mock_resolutions = [MagicMock(), MagicMock()]
        for i, res in enumerate(mock_resolutions):
            res.model_dump.return_value = {"market_id": f"0x{i}"}
        with patch.object(run_cmd, "open_session", return_value=fake_bundle), \
             patch.object(run_cmd, "_discovered_resolutions", return_value=mock_resolutions), \
             patch.object(run_cmd, "polymarket_metadata_s3_key", side_effect=lambda r: f"metadata/{id(r)}"):
            run_cmd.handle(args)
        assert fake_bundle.s3_store.put_json.call_count == 2
