"""Gamma + Vatic + Binance discovery helpers used by the slice builder.

Builds `PolymarketMarketResolution` records from Gamma payloads, with optional
Vatic / Binance price-to-beat lookups and Gamma settlement payouts.
"""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import UTC, date as date_cls, datetime, time as time_cls, timedelta
import logging
from threading import local
from time import sleep
from typing import Any

import httpx
import orjson

from .models import (
    PolymarketMarketResolution,
    PolymarketTarget,
    PolymarketTargetKind,
)

logger = logging.getLogger(__name__)

DEFAULT_GAMMA_BASE_URL = "https://gamma-api.polymarket.com"
DEFAULT_VATIC_BASE_URL = "https://api.vatic.trading"
DEFAULT_BINANCE_BASE_URL = "https://api.binance.com"
DEFAULT_BINANCE_US_BASE_URL = "https://api.binance.us"
POLYMARKET_5M_INTERVAL_MS = 300_000
DISCOVER_MAX_WORKERS = 16


@dataclass(frozen=True)
class PriceToBeat:
    price: float
    source: str
    quality: str


@dataclass(frozen=True)
class GammaUrls:
    gamma_base_url: str = DEFAULT_GAMMA_BASE_URL
    vatic_base_url: str = DEFAULT_VATIC_BASE_URL
    binance_base_url: str = DEFAULT_BINANCE_BASE_URL
    binance_us_base_url: str = DEFAULT_BINANCE_US_BASE_URL


def _client(client: httpx.Client | None = None) -> httpx.Client:
    return client or httpx.Client(timeout=60.0, follow_redirects=True)


def _parse_utc_timestamp(value: str) -> datetime:
    if value.endswith("Z"):
        value = value[:-1] + "+00:00"
    return datetime.fromisoformat(value).astimezone(UTC)


def _parse_json_list(raw: str) -> list[str]:
    values = orjson.loads(raw)
    if not isinstance(values, list):
        raise ValueError("expected JSON list")
    return [str(value) for value in values]


def _series_key_from_slug(slug: str) -> str:
    head, sep, tail = slug.rpartition("-")
    if sep and len(tail) >= 8 and tail.isdigit():
        return head
    return slug


def _window_bounds_from_slug(slug: str) -> tuple[int, int] | None:
    head, sep, tail = slug.rpartition("-")
    if not sep or len(tail) < 8 or not tail.isdigit():
        return None
    if "5m" not in head:
        return None
    start_ts_ms = int(tail) * 1000
    return (start_ts_ms, start_ts_ms + POLYMARKET_5M_INTERVAL_MS)


def _contract_window_from_payload(payload: dict[str, Any]) -> tuple[datetime, datetime, int, int]:
    window = _window_bounds_from_slug(str(payload["slug"]))
    if window is not None:
        start_ts_ms, end_ts_ms = window
        return (
            datetime.fromtimestamp(start_ts_ms / 1000, tz=UTC),
            datetime.fromtimestamp(end_ts_ms / 1000, tz=UTC),
            start_ts_ms,
            end_ts_ms,
        )
    fallback_start = _parse_utc_timestamp(payload["startDate"])
    fallback_end = _parse_utc_timestamp(payload["endDate"])
    return (
        fallback_start,
        fallback_end,
        int(fallback_start.timestamp() * 1000),
        int(fallback_end.timestamp() * 1000),
    )


def _resolution_from_payload(
    payload: dict[str, Any],
    outcome: str,
    *,
    price_to_beat: PriceToBeat | None = None,
    settlement_payout: float | None = None,
) -> PolymarketMarketResolution:
    outcomes = _parse_json_list(payload["outcomes"])
    token_ids = _parse_json_list(payload["clobTokenIds"])
    if outcome not in outcomes:
        valid = ", ".join(f"'{value}'" for value in outcomes)
        raise ValueError(f"unknown outcome '{outcome}'. Valid outcomes: {valid}")
    outcome_index = outcomes.index(outcome)
    start, end, start_ts_ms, end_ts_ms = _contract_window_from_payload(payload)
    instrument = f"{payload['slug']}:{outcome}"
    return PolymarketMarketResolution(
        slug=str(payload["slug"]),
        question=str(payload.get("question") or ""),
        outcome=outcome,
        market_id=str(payload["conditionId"]),
        asset_id=token_ids[outcome_index],
        instrument=instrument,
        start_time=start.isoformat(),
        end_time=end.isoformat(),
        start_ts_ms=start_ts_ms,
        end_ts_ms=end_ts_ms,
        price_to_beat=None if price_to_beat is None else price_to_beat.price,
        price_to_beat_source=None if price_to_beat is None else price_to_beat.source,
        price_to_beat_quality=None if price_to_beat is None else price_to_beat.quality,
        settlement_payout=settlement_payout,
    )


def _settlement_payout_from_payload(
    payload: dict[str, Any],
    outcome: str,
    *,
    price_to_beat: PriceToBeat | None = None,
    settlement_price: PriceToBeat | None = None,
) -> float | None:
    winning_outcome = payload.get("winningOutcome")
    if isinstance(winning_outcome, str) and winning_outcome:
        return 1.0 if outcome == winning_outcome else 0.0

    outcome_prices = payload.get("outcomePrices")
    if isinstance(outcome_prices, str):
        parsed = orjson.loads(outcome_prices)
        outcome_prices = parsed if isinstance(parsed, list) else None
    if isinstance(outcome_prices, list):
        raw_outcomes = payload.get("outcomes")
        if isinstance(raw_outcomes, str):
            outcomes = _parse_json_list(raw_outcomes)
        elif isinstance(raw_outcomes, list):
            outcomes = [str(value) for value in raw_outcomes]
        else:
            outcomes = []
        if outcome in outcomes:
            outcome_index = outcomes.index(outcome)
            if outcome_index < len(outcome_prices):
                try:
                    return float(outcome_prices[outcome_index])
                except (TypeError, ValueError):
                    pass

    tokens = payload.get("tokens")
    if isinstance(tokens, list):
        for token in tokens:
            if not isinstance(token, dict):
                continue
            if str(token.get("outcome")) != outcome:
                continue
            winner = token.get("winner")
            if isinstance(winner, bool):
                return 1.0 if winner else 0.0

    if price_to_beat is None or settlement_price is None:
        return None
    series_key = _series_key_from_slug(str(payload["slug"])).lower()
    if "5m" not in series_key:
        return None
    if outcome not in {"Up", "Down"}:
        return None
    if settlement_price.price == price_to_beat.price:
        return None
    winning_outcome = "Up" if settlement_price.price > price_to_beat.price else "Down"
    return 1.0 if outcome == winning_outcome else 0.0


def _asset_from_series_key(series_key: str) -> str:
    return series_key.split("-", 1)[0].lower()


def _fetch_vatic_price_to_beat(
    client: httpx.Client,
    *,
    asset: str,
    start_ts_ms: int,
    base_url: str,
) -> PriceToBeat | None:
    timestamp = start_ts_ms // 1000
    base = base_url.rstrip("/")
    response = client.get(
        f"{base}/api/v1/targets/timestamp",
        params={"asset": asset, "type": "5min", "timestamp": timestamp},
    )
    if response.status_code < 400:
        payload = response.json()
        for key in ("target_price", "price"):
            value = payload.get(key)
            if value is not None:
                return PriceToBeat(price=float(value), source="vatic", quality="exact")
    response = client.get(
        f"{base}/api/v1/targets/{timestamp}",
        params={"asset": asset, "type": "5min"},
    )
    if response.status_code < 400:
        payload = response.json()
        for key in ("target_price", "price"):
            value = payload.get(key)
            if value is not None:
                return PriceToBeat(price=float(value), source="vatic", quality="exact")
    return None


def _fetch_binance_price_to_beat(
    client: httpx.Client,
    *,
    asset: str,
    start_ts_ms: int,
    base_url: str,
    source: str,
) -> PriceToBeat | None:
    base = base_url.rstrip("/")
    response = client.get(
        f"{base}/api/v3/klines",
        params={
            "symbol": f"{asset.upper()}USDT",
            "interval": "1m",
            "startTime": start_ts_ms,
            "limit": 1,
        },
    )
    if response.status_code >= 400:
        return None
    payload = response.json()
    if not payload:
        return None
    first = payload[0]
    if len(first) < 2:
        return None
    return PriceToBeat(price=float(first[1]), source=source, quality="proxy")


def _fetch_price_to_beat(
    client: httpx.Client,
    *,
    slug: str,
    start_ts_ms: int,
    urls: GammaUrls,
) -> PriceToBeat | None:
    series_key = _series_key_from_slug(slug)
    if "5m" not in series_key:
        return None
    asset = _asset_from_series_key(series_key)
    try:
        if exact := _fetch_vatic_price_to_beat(
            client,
            asset=asset,
            start_ts_ms=start_ts_ms,
            base_url=urls.vatic_base_url,
        ):
            return exact
    except (httpx.HTTPError, ValueError):
        pass
    try:
        if proxy := _fetch_binance_price_to_beat(
            client,
            asset=asset,
            start_ts_ms=start_ts_ms,
            base_url=urls.binance_base_url,
            source="binance_open_1m",
        ):
            return proxy
    except (httpx.HTTPError, ValueError):
        pass
    try:
        return _fetch_binance_price_to_beat(
            client,
            asset=asset,
            start_ts_ms=start_ts_ms,
            base_url=urls.binance_us_base_url,
            source="binance_us_open_1m",
        )
    except (httpx.HTTPError, ValueError):
        return None


def _iter_series_candidate_slugs(series_key: str, start_date: str, end_date: str) -> list[str]:
    start = datetime.combine(date_cls.fromisoformat(start_date), time_cls.min, tzinfo=UTC)
    end = datetime.combine(
        date_cls.fromisoformat(end_date) + timedelta(days=1), time_cls.min, tzinfo=UTC
    )
    current = start
    slugs: list[str] = []
    while current < end:
        slugs.append(f"{series_key}-{int(current.timestamp())}")
        current += timedelta(minutes=5)
    return slugs


def _fetch_gamma_market(
    client: httpx.Client, *, urls: GammaUrls, slug: str
) -> dict[str, Any] | None:
    response = client.get(f"{urls.gamma_base_url.rstrip('/')}/markets/slug/{slug}")
    if response.status_code == 404:
        return None
    response.raise_for_status()
    return response.json()


def discover_resolutions(
    target: PolymarketTarget,
    *,
    start_date: str,
    end_date: str,
    urls: GammaUrls | None = None,
    client: httpx.Client | None = None,
) -> list[PolymarketMarketResolution]:
    """Discover all per-outcome resolutions covering the requested window for a target.

    For series targets: enumerates slugs on the 5-min grid for the date span,
    fetches each from Gamma, builds Up/Down resolutions, and attaches
    price_to_beat + settlement_payout when available.

    For slug targets: fetches the single slug and builds one resolution per outcome.
    """
    urls = urls or GammaUrls()
    own_client = client is None
    http = _client(client)
    try:
        if target.target_kind == PolymarketTargetKind.SLUG:
            return _discover_slug(http, target.target_key, urls=urls)
        if target.target_kind == PolymarketTargetKind.SERIES:
            return _discover_series(
                http,
                series_key=target.target_key,
                start_date=start_date,
                end_date=end_date,
                urls=urls,
            )
        raise ValueError(f"unsupported target_kind: {target.target_kind}")
    finally:
        if own_client:
            http.close()


def _discover_slug(
    client: httpx.Client, slug: str, *, urls: GammaUrls
) -> list[PolymarketMarketResolution]:
    payload = _fetch_gamma_market(client, urls=urls, slug=slug)
    if payload is None:
        raise ValueError(f"polymarket slug not found on Gamma: {slug}")

    _, _, start_ts_ms, end_ts_ms = _contract_window_from_payload(payload)
    price_to_beat = _fetch_price_to_beat(
        client, slug=slug, start_ts_ms=start_ts_ms, urls=urls
    )
    settlement_price = _fetch_price_to_beat(
        client, slug=slug, start_ts_ms=end_ts_ms, urls=urls
    )

    resolutions: list[PolymarketMarketResolution] = []
    outcomes = _parse_json_list(payload["outcomes"])
    for outcome in outcomes:
        resolutions.append(
            _resolution_from_payload(
                payload,
                outcome,
                price_to_beat=price_to_beat,
                settlement_payout=_settlement_payout_from_payload(
                    payload,
                    outcome,
                    price_to_beat=price_to_beat,
                    settlement_price=settlement_price,
                ),
            )
        )
    return sorted(resolutions, key=lambda item: (item.slug, item.outcome))


def _discover_series(
    client: httpx.Client | None,
    *,
    series_key: str,
    start_date: str,
    end_date: str,
    urls: GammaUrls,
) -> list[PolymarketMarketResolution]:
    window_start = datetime.combine(
        date_cls.fromisoformat(start_date), time_cls.min, tzinfo=UTC
    )
    window_end = datetime.combine(
        date_cls.fromisoformat(end_date), time_cls.max, tzinfo=UTC
    )
    slugs = _iter_series_candidate_slugs(series_key, start_date, end_date)

    worker_state = local()

    def worker_client() -> httpx.Client:
        if not hasattr(worker_state, "client"):
            worker_state.client = client if client is not None else httpx.Client(timeout=30.0)
        return worker_state.client

    failures: list[dict[str, Any]] = []

    def process_slug(slug: str) -> list[PolymarketMarketResolution]:
        http = worker_client()
        payload: dict[str, Any] | None = None
        for attempt in range(3):
            try:
                payload = _fetch_gamma_market(http, urls=urls, slug=slug)
                break
            except Exception as error:
                if attempt < 2:
                    sleep(0.25 * (attempt + 1))
                    continue
                failures.append(
                    {
                        "slug": slug,
                        "phase": "gamma-fetch",
                        "error": f"{type(error).__name__}: {error}",
                    }
                )
                return []
        if payload is None:
            return []
        try:
            start, end, start_ts_ms, end_ts_ms = _contract_window_from_payload(payload)
        except Exception as error:
            failures.append(
                {
                    "slug": slug,
                    "phase": "parse-window",
                    "error": f"{type(error).__name__}: {error}",
                }
            )
            return []
        if end < window_start or start > window_end:
            return []
        try:
            price_to_beat = _fetch_price_to_beat(
                http, slug=str(payload["slug"]), start_ts_ms=start_ts_ms, urls=urls
            )
            settlement_price = _fetch_price_to_beat(
                http, slug=str(payload["slug"]), start_ts_ms=end_ts_ms, urls=urls
            )
        except Exception as error:
            failures.append(
                {
                    "slug": slug,
                    "phase": "price-to-beat",
                    "error": f"{type(error).__name__}: {error}",
                }
            )
            return []
        out: list[PolymarketMarketResolution] = []
        try:
            for outcome in ("Up", "Down"):
                resolution = _resolution_from_payload(
                    payload,
                    outcome,
                    price_to_beat=price_to_beat,
                    settlement_payout=_settlement_payout_from_payload(
                        payload,
                        outcome,
                        price_to_beat=price_to_beat,
                        settlement_price=settlement_price,
                    ),
                )
                out.append(resolution)
        except Exception as error:
            failures.append(
                {
                    "slug": slug,
                    "phase": "build-resolution",
                    "error": f"{type(error).__name__}: {error}",
                }
            )
            return []
        return out

    logger.info(
        "discover_resolutions: probing %d candidate slugs for series=%s (workers=%d)",
        len(slugs),
        series_key,
        DISCOVER_MAX_WORKERS,
    )

    results: list[PolymarketMarketResolution] = []
    seen: set[tuple[str, str]] = set()
    completed = 0
    with ThreadPoolExecutor(max_workers=DISCOVER_MAX_WORKERS) as executor:
        future_to_slug = {executor.submit(process_slug, slug): slug for slug in slugs}
        for future in as_completed(future_to_slug):
            completed += 1
            if completed % 500 == 0:
                logger.info(
                    "discover_resolutions: %d/%d probed, %d resolutions, %d failures",
                    completed,
                    len(slugs),
                    len(results),
                    len(failures),
                )
            for resolution in future.result():
                key = (resolution.slug, resolution.outcome)
                if key in seen:
                    continue
                seen.add(key)
                results.append(resolution)

    if failures:
        message = (
            f"discover_resolutions: series={series_key} finished with {len(failures)} "
            f"failures; sample: {failures[:5]}"
        )
        logger.error(message)
        raise RuntimeError(message)
    logger.info(
        "discover_resolutions: series=%s resolved=%d failed=%d",
        series_key,
        len(results),
        len(failures),
    )
    return sorted(results, key=lambda item: (item.slug, item.outcome))
