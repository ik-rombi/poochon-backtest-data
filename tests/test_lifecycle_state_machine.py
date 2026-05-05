from __future__ import annotations

from poochon_backtest_data.canonical import (
    _PolymarketContract,
    _PolymarketContractOutcome,
    _PolymarketContractSchedule,
)


def _contract(slug: str, start_ts_ms: int) -> _PolymarketContract:
    return _PolymarketContract(
        series_key="btc-updown-5m",
        slug=slug,
        market_id=f"market-{slug}",
        start_ts_ms=start_ts_ms,
        end_ts_ms=start_ts_ms + 300_000,
        price_to_beat=None,
        price_to_beat_source=None,
        price_to_beat_quality=None,
        outcomes=(
            _PolymarketContractOutcome(
                outcome="Up",
                asset_id=f"{slug}-up",
                instrument=f"{slug}:Up",
                settlement_payout=None,
            ),
            _PolymarketContractOutcome(
                outcome="Down",
                asset_id=f"{slug}-down",
                instrument=f"{slug}:Down",
                settlement_payout=None,
            ),
        ),
    )


def test_cold_start_emits_listed_current_and_listed_next() -> None:
    schedule = _PolymarketContractSchedule(
        [_contract("c1", 1_771_459_200_000), _contract("c2", 1_771_459_500_000)]
    )
    events = schedule.lifecycle_events(1_771_459_201_000)
    kinds = [e["Contract"]["Polymarket"]["kind"] for e in events]
    slugs = [e["Contract"]["Polymarket"]["slug"] for e in events]
    assert kinds == ["ListedCurrent", "ListedNext"]
    assert slugs == ["c1", "c2"]


def test_rollover_emits_resolved_activated_and_listed_next() -> None:
    schedule = _PolymarketContractSchedule(
        [
            _contract("c1", 1_771_459_200_000),
            _contract("c2", 1_771_459_500_000),
            _contract("c3", 1_771_459_800_000),
        ]
    )
    schedule.lifecycle_events(1_771_459_201_000)  # cold-start step
    events = schedule.lifecycle_events(1_771_459_500_000)
    kinds = [e["Contract"]["Polymarket"]["kind"] for e in events]
    slugs = [e["Contract"]["Polymarket"]["slug"] for e in events]
    # The new "current" was already advertised as next, so we expect Resolved + Activated
    # (no extra ListedCurrent), then ListedNext for c3.
    assert kinds == ["Resolved", "Activated", "ListedNext"]
    assert slugs == ["c1", "c2", "c3"]


def test_steady_state_emits_nothing_when_unchanged() -> None:
    schedule = _PolymarketContractSchedule(
        [_contract("c1", 1_771_459_200_000), _contract("c2", 1_771_459_500_000)]
    )
    schedule.lifecycle_events(1_771_459_201_000)
    assert schedule.lifecycle_events(1_771_459_400_000) == []
