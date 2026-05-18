from __future__ import annotations

import pytest

from poochon_backtest_data import polymarket_metadata as metadata
from poochon_backtest_data.models import PolymarketTarget, PolymarketTargetKind


def test_discover_resolutions_retries_and_fails_on_gamma_errors(monkeypatch) -> None:
    calls = 0

    def fake_fetch_gamma_market(_client, *, urls, slug):  # noqa: ANN001
        nonlocal calls
        calls += 1
        raise RuntimeError(f"temporary gamma error for {slug}")

    monkeypatch.setattr(
        metadata,
        "_iter_series_candidate_slugs",
        lambda _series_key, _start_date, _end_date: ["btc-updown-5m-1"],
    )
    monkeypatch.setattr(metadata, "_fetch_gamma_market", fake_fetch_gamma_market)
    monkeypatch.setattr(metadata, "sleep", lambda _seconds: None)

    with pytest.raises(RuntimeError, match="finished with 1 failures"):
        metadata.discover_resolutions(
            PolymarketTarget(
                target_kind=PolymarketTargetKind.SERIES,
                target_key="btc-updown-5m",
            ),
            start_date="2026-04-17",
            end_date="2026-04-17",
        )

    assert calls == 3
