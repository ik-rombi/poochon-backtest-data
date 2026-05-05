from __future__ import annotations

from datetime import date as date_cls

import pytest

from poochon_backtest_data.models import (
    HyperliquidIngestionRequest,
    MarketRef,
    MarketType,
    PolymarketMarketResolution,
    PolymarketMirrorRequest,
    PolymarketSliceRequest,
    PolymarketTarget,
    PolymarketTargetKind,
    Venue,
    WindowSpec,
    canonical_hl_shard_id,
    canonical_hl_shard_prefix,
    canonical_pm_shard_id,
    canonical_pm_shard_prefix,
    canonical_shard_data_s3_key,
    canonical_shard_manifest_s3_key,
    canonical_shard_schedule_s3_key,
    coverage_pk_canonical_hl,
    coverage_pk_canonical_pm,
    coverage_pk_raw_hl_fills,
    coverage_pk_raw_hl_l2,
    coverage_pk_raw_pmxt,
    iter_dates_inclusive,
    raw_hl_fills_s3_key,
    raw_hl_l2_s3_key,
    raw_pmxt_s3_key,
    raw_pmxt_upstream_url,
)


def test_window_spec_resolves_absolute() -> None:
    spec = WindowSpec(start_date="2026-04-17", end_date="2026-04-21")
    assert spec.resolve_window() == ("2026-04-17", "2026-04-21")
    assert spec.iter_dates() == [
        "2026-04-17",
        "2026-04-18",
        "2026-04-19",
        "2026-04-20",
        "2026-04-21",
    ]


def test_window_spec_resolves_relative() -> None:
    spec = WindowSpec(start_offset_days=-2, end_offset_days=0)
    today = date_cls(2026, 4, 27)
    assert spec.resolve_window(today=today) == ("2026-04-25", "2026-04-27")


def test_window_spec_rejects_mixed() -> None:
    with pytest.raises(ValueError):
        WindowSpec(start_date="2026-04-17", start_offset_days=-1)


def test_window_spec_rejects_reversed() -> None:
    with pytest.raises(ValueError):
        WindowSpec(start_date="2026-04-21", end_date="2026-04-17")


def test_polymarket_target_validates() -> None:
    t = PolymarketTarget(target_kind=PolymarketTargetKind.SERIES, target_key="btc-updown-5m")
    assert t.encoded_key() == "btc-updown-5m"
    with pytest.raises(ValueError):
        PolymarketTarget(target_kind=PolymarketTargetKind.SLUG, target_key="   ")


def test_market_ref_validates() -> None:
    market = MarketRef(market_type=MarketType.PERP, instrument="BTC")
    assert market.venue == Venue.HYPERLIQUID
    with pytest.raises(ValueError):
        MarketRef(market_type=MarketType.BINARY, instrument="BTC")  # HL with binary
    with pytest.raises(ValueError):
        MarketRef(venue=Venue.POLYMARKET, market_type=MarketType.PERP, instrument="x")


def test_hyperliquid_ingestion_request_window_round_trip() -> None:
    request = HyperliquidIngestionRequest(
        market_type=MarketType.PERP,
        instrument="BTC",
        start_date="2026-04-17",
        end_date="2026-04-17",
    )
    market = request.market_ref()
    assert market.instrument == "BTC"
    assert market.market_type == MarketType.PERP


def test_polymarket_slice_request_target() -> None:
    request = PolymarketSliceRequest(
        target_kind=PolymarketTargetKind.SLUG,
        target_key="will-trump-win-2026",
        start_date="2026-04-17",
        end_date="2026-04-17",
    )
    target = request.target()
    assert target.target_kind == PolymarketTargetKind.SLUG
    assert target.target_key == "will-trump-win-2026"


def test_polymarket_mirror_request_no_identity() -> None:
    request = PolymarketMirrorRequest(start_offset_days=-1, end_offset_days=0)
    assert request.iter_dates(today=date_cls(2026, 4, 27)) == ["2026-04-26", "2026-04-27"]


def test_polymarket_market_resolution_series_key() -> None:
    res = PolymarketMarketResolution(
        slug="btc-updown-5m-1771459200",
        outcome="Up",
        market_id="0xabc",
        asset_id="0x1",
        instrument="btc-updown-5m-1771459200:Up",
        start_ts_ms=1771459200000,
        end_ts_ms=1771459500000,
    )
    assert res.series_key == "btc-updown-5m"


def test_canonical_pm_shard_identity() -> None:
    target = PolymarketTarget(
        target_kind=PolymarketTargetKind.SERIES, target_key="btc-updown-5m"
    )
    sid = canonical_pm_shard_id(target=target, date="2026-04-17", depth=5)
    assert len(sid) == 32
    prefix = canonical_pm_shard_prefix(target=target, date="2026-04-17", depth=5)
    assert prefix == "canonical/polymarket/series/btc-updown-5m/date=2026-04-17/depth=5/"
    assert canonical_shard_data_s3_key(prefix).endswith("/data.parquet")
    assert canonical_shard_schedule_s3_key(prefix).endswith("/schedule.parquet")
    assert canonical_shard_manifest_s3_key(prefix).endswith("/manifest.json")


def test_canonical_hl_shard_identity() -> None:
    market = MarketRef(market_type=MarketType.PERP, instrument="BTC")
    sid = canonical_hl_shard_id(market, "2026-04-17", 20)
    assert len(sid) == 32
    prefix = canonical_hl_shard_prefix(market, "2026-04-17", 20)
    assert prefix == "canonical/hyperliquid/market_type=perp/instrument=BTC/date=2026-04-17/depth=20/"


def test_raw_pmxt_keys() -> None:
    assert (
        raw_pmxt_s3_key("2026-04-17", 3)
        == "raw/pmxt/orderbook/date=2026-04-17/hour=03/polymarket_orderbook_2026-04-17T03.parquet"
    )
    assert (
        raw_pmxt_upstream_url("https://r2v2.pmxt.dev", "2026-04-17", 3)
        == "https://r2v2.pmxt.dev/polymarket_orderbook_2026-04-17T03.parquet"
    )


def test_raw_hl_keys() -> None:
    market = MarketRef(market_type=MarketType.PERP, instrument="BTC")
    assert (
        raw_hl_l2_s3_key(market, "2026-04-17", 3)
        == "raw/hyperliquid/l2book/market_type=perp/date=2026-04-17/hour=03/instrument=BTC/BTC.lz4"
    )
    assert (
        raw_hl_fills_s3_key("2026-04-17", 3)
        == "raw/hyperliquid/node_fills_by_block/date=2026-04-17/hour=03/fills.lz4"
    )


def test_coverage_pks() -> None:
    market = MarketRef(market_type=MarketType.PERP, instrument="BTC")
    target = PolymarketTarget(
        target_kind=PolymarketTargetKind.SERIES, target_key="btc-updown-5m"
    )
    assert coverage_pk_raw_pmxt("2026-04-17", 3) == "raw_pmxt#2026-04-17#03"
    assert (
        coverage_pk_raw_hl_l2(market, "2026-04-17", 3)
        == "raw_hl_l2#perp#BTC#2026-04-17#03"
    )
    assert coverage_pk_raw_hl_fills("2026-04-17", 3) == "raw_hl_fills#2026-04-17#03"
    assert (
        coverage_pk_canonical_pm(target, "2026-04-17")
        == "canonical_pm#series#btc-updown-5m#2026-04-17"
    )
    assert (
        coverage_pk_canonical_hl(market, "2026-04-17")
        == "canonical_hl#perp#BTC#2026-04-17"
    )


def test_iter_dates_inclusive() -> None:
    assert iter_dates_inclusive("2026-04-17", "2026-04-19") == [
        "2026-04-17",
        "2026-04-18",
        "2026-04-19",
    ]
