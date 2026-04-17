from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import UTC, date as date_cls, datetime, time as time_cls, timedelta
import io
import logging
from pathlib import Path
import time
from threading import local
from typing import Any

import httpx
import orjson
import pyarrow as pa
import pyarrow.parquet as pq

logger = logging.getLogger(__name__)

TELONEX_MAX_RETRIES = 3
TELONEX_RETRYABLE_STATUS = {500, 502, 503, 504}
TELONEX_BACKOFF_BASE_SECONDS = 2.0

from .canonical import build_polymarket_canonical_day, build_polymarket_canonical_day_from_storage
from .models import (
    CoverageRecord,
    CoverageStatus,
    DatasetKind,
    MarketRef,
    OutcomesMode,
    PolymarketMarketResolution,
    PolymarketReplayCreateRequest,
    PolymarketSeriesSyncRequest,
    coverage_pk,
    iter_dates_inclusive,
    polymarket_metadata_s3_key,
    polymarket_normalized_l2_s3_key,
    polymarket_normalized_trade_s3_key,
    polymarket_raw_l2_s3_key,
    polymarket_raw_trade_s3_key,
    utc_now_iso,
)
from .storage import CanonicalShardRepository, CoverageRepository, S3Store

GAMMA_BASE_URL = "https://gamma-api.polymarket.com"
VATIC_BASE_URL = "https://api.vatic.trading"
BINANCE_BASE_URL = "https://api.binance.com"
BINANCE_US_BASE_URL = "https://api.binance.us"
TELONEX_DOWNLOAD_BASE_URL = "https://api.telonex.io/v1/downloads/polymarket"
BOOK_CHANNEL = "book_snapshot_5"
TRADE_CHANNEL = "trades"
POLYMARKET_5M_INTERVAL_MS = 300_000

BOOK_RAW_SCHEMA = pa.schema(
    [
        ("timestamp_us", pa.int64()),
        ("local_timestamp_us", pa.int64()),
        ("exchange", pa.string()),
        ("market_id", pa.string()),
        ("slug", pa.string()),
        ("asset_id", pa.string()),
        ("outcome", pa.string()),
        ("bid_price_0", pa.string()),
        ("bid_size_0", pa.string()),
        ("bid_price_1", pa.string()),
        ("bid_size_1", pa.string()),
        ("bid_price_2", pa.string()),
        ("bid_size_2", pa.string()),
        ("bid_price_3", pa.string()),
        ("bid_size_3", pa.string()),
        ("bid_price_4", pa.string()),
        ("bid_size_4", pa.string()),
        ("ask_price_0", pa.string()),
        ("ask_size_0", pa.string()),
        ("ask_price_1", pa.string()),
        ("ask_size_1", pa.string()),
        ("ask_price_2", pa.string()),
        ("ask_size_2", pa.string()),
        ("ask_price_3", pa.string()),
        ("ask_size_3", pa.string()),
        ("ask_price_4", pa.string()),
        ("ask_size_4", pa.string()),
    ]
)

TRADE_RAW_SCHEMA = pa.schema(
    [
        ("timestamp_us", pa.int64()),
        ("local_timestamp_us", pa.int64()),
        ("exchange", pa.string()),
        ("market_id", pa.string()),
        ("slug", pa.string()),
        ("asset_id", pa.string()),
        ("outcome", pa.string()),
        ("price", pa.string()),
        ("size", pa.string()),
        ("side", pa.string()),
        ("trade_id", pa.string()),
        ("origin_asset_id", pa.string()),
    ]
)


@dataclass(frozen=True)
class SeriesCandidate:
    slug: str
    market_id: str
    question: str
    start_time: str
    end_time: str
    start_ts_ms: int
    end_ts_ms: int
    dates: tuple[str, ...]
    outcomes: tuple[PolymarketMarketResolution, ...]


@dataclass(frozen=True)
class PriceToBeat:
    price: float
    source: str
    quality: str


def _parse_utc_timestamp(value: str) -> datetime:
    if value.endswith("Z"):
        value = value[:-1] + "+00:00"
    return datetime.fromisoformat(value).astimezone(UTC)


def _date_span(start: datetime, end: datetime) -> tuple[str, ...]:
    current = start.date()
    target = end.date()
    result: list[str] = []
    while current <= target:
        result.append(current.isoformat())
        current += timedelta(days=1)
    return tuple(result)


def _client(client: httpx.Client | None = None) -> httpx.Client:
    return client or httpx.Client(timeout=60.0, follow_redirects=True)


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
    # Prefer the slug-derived window — it's the authoritative source for 5m
    # series contracts and doesn't depend on Gamma's startDate/endDate fields,
    # which are sometimes missing on newer payloads.
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
        slug=payload["slug"],
        question=payload["question"],
        outcome=outcome,
        market_id=payload["conditionId"],
        asset_id=token_ids[outcome_index],
        instrument=instrument,
        start_time=start.isoformat(),
        end_time=end.isoformat(),
        start_ts_ms=start_ts_ms,
        end_ts_ms=end_ts_ms,
        dates=_date_span(start, end),
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
) -> PriceToBeat | None:
    timestamp = start_ts_ms // 1000
    requests = [
        client.get(
            f"{VATIC_BASE_URL}/api/v1/targets/timestamp",
            params={"asset": asset, "type": "5min", "timestamp": timestamp},
        ),
    ]
    for response in requests:
        if response.status_code >= 400:
            continue
        payload = response.json()
        for key in ("target_price", "price"):
            value = payload.get(key)
            if value is not None:
                return PriceToBeat(price=float(value), source="vatic", quality="exact")
    response = client.get(
        f"{VATIC_BASE_URL}/api/v1/targets/{timestamp}",
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
    base_url: str = BINANCE_BASE_URL,
    source: str = "binance_open_1m",
) -> PriceToBeat | None:
    response = client.get(
        f"{base_url}/api/v3/klines",
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
) -> PriceToBeat | None:
    series_key = _series_key_from_slug(slug)
    if "5m" not in series_key:
        return None
    asset = _asset_from_series_key(series_key)
    try:
        if exact := _fetch_vatic_price_to_beat(client, asset=asset, start_ts_ms=start_ts_ms):
            return exact
    except (httpx.HTTPError, ValueError):
        pass
    try:
        if proxy := _fetch_binance_price_to_beat(client, asset=asset, start_ts_ms=start_ts_ms):
            return proxy
    except (httpx.HTTPError, ValueError):
        pass
    try:
        return _fetch_binance_price_to_beat(
            client,
            asset=asset,
            start_ts_ms=start_ts_ms,
            base_url=BINANCE_US_BASE_URL,
            source="binance_us_open_1m",
        )
    except (httpx.HTTPError, ValueError):
        return None


def resolve_market(
    request: PolymarketReplayCreateRequest,
    *,
    client: httpx.Client | None = None,
) -> PolymarketMarketResolution:
    own_client = client is None
    http = _client(client)
    try:
        response = http.get(f"{GAMMA_BASE_URL}/markets/slug/{request.slug}")
        response.raise_for_status()
        payload = response.json()
        _, _, start_ts_ms, end_ts_ms = _contract_window_from_payload(payload)
        price_to_beat = _fetch_price_to_beat(
            http,
            slug=str(payload["slug"]),
            start_ts_ms=start_ts_ms,
        )
        settlement_price = _fetch_price_to_beat(
            http,
            slug=str(payload["slug"]),
            start_ts_ms=end_ts_ms,
        )
    finally:
        if own_client:
            http.close()
    return _resolution_from_payload(
        payload,
        request.outcome,
        price_to_beat=price_to_beat,
        settlement_payout=_settlement_payout_from_payload(
            payload,
            request.outcome,
            price_to_beat=price_to_beat,
            settlement_price=settlement_price,
        ),
    )


def _put_coverage(
    coverage: CoverageRepository,
    *,
    dataset_kind: DatasetKind,
    market: MarketRef,
    date: str,
    status: CoverageStatus,
    object_count: int,
    byte_count: int,
    row_count: int,
    source: str,
) -> CoverageRecord:
    record = CoverageRecord(
        pk=coverage_pk(dataset_kind, market, date, "daily"),
        dataset_kind=dataset_kind,
        venue=market.venue,
        market_type=market.market_type,
        instrument=market.instrument,
        date=date,
        hour="daily",
        status=status,
        object_count=object_count,
        byte_count=byte_count,
        row_count=row_count,
        updated_at=utc_now_iso(),
        source=source,
    )
    coverage.put(record)
    return record


def _coverage_ready(
    coverage: CoverageRepository,
    dataset_kind: DatasetKind,
    market: MarketRef,
    date: str,
) -> CoverageRecord | None:
    record = coverage.get(coverage_pk(dataset_kind, market, date, "daily"))
    if record is None or record.status != CoverageStatus.READY:
        return None
    return record


def _download_channel(
    *,
    telonex_api_key: str,
    channel: str,
    date: str,
    market_id: str,
    outcome: str,
    client: httpx.Client | None = None,
) -> tuple[bytes, bool]:
    own_client = client is None
    http = _client(client)
    try:
        last_error_detail = ""
        for attempt in range(TELONEX_MAX_RETRIES):
            try:
                response = http.get(
                    f"{TELONEX_DOWNLOAD_BASE_URL}/{channel}/{date}",
                    params={"market_id": market_id, "outcome": outcome},
                    headers={"Authorization": f"Bearer {telonex_api_key}"},
                )
            except (httpx.RequestError, httpx.TimeoutException) as error:
                last_error_detail = f"{type(error).__name__}: {error}"
                if attempt + 1 < TELONEX_MAX_RETRIES:
                    wait = TELONEX_BACKOFF_BASE_SECONDS * (2 ** attempt)
                    logger.warning(
                        "telonex %s network error (attempt %d/%d), retrying in %.1fs: %s",
                        channel, attempt + 1, TELONEX_MAX_RETRIES, wait, error,
                    )
                    time.sleep(wait)
                    continue
                break

            if response.status_code == 404:
                return _empty_channel_payload(channel), True
            if response.status_code in TELONEX_RETRYABLE_STATUS:
                last_error_detail = f"HTTP {response.status_code}: {response.text.strip()[:200]}"
                if attempt + 1 < TELONEX_MAX_RETRIES:
                    wait = TELONEX_BACKOFF_BASE_SECONDS * (2 ** attempt)
                    logger.warning(
                        "telonex %s %d for market_id=%s outcome=%s date=%s (attempt %d/%d), retrying in %.1fs",
                        channel, response.status_code, market_id, outcome, date,
                        attempt + 1, TELONEX_MAX_RETRIES, wait,
                    )
                    time.sleep(wait)
                    continue
                break
            if response.status_code >= 400:
                detail = response.text.strip()
                raise ValueError(
                    f"Telonex {channel} request failed for market_id={market_id} outcome={outcome} date={date}: {detail}"
                )
            return response.content, False

        raise ValueError(
            f"Telonex {channel} exhausted {TELONEX_MAX_RETRIES} retries for "
            f"market_id={market_id} outcome={outcome} date={date}: {last_error_detail}"
        )
    finally:
        if own_client:
            http.close()


def _write_parquet(rows: list[dict[str, Any]], schema: pa.Schema, path: Path) -> None:
    table = pa.Table.from_pylist(rows, schema=schema)
    pq.write_table(table, path, compression="zstd")


def _empty_channel_payload(channel: str) -> bytes:
    if channel == BOOK_CHANNEL:
        schema = BOOK_RAW_SCHEMA
    elif channel == TRADE_CHANNEL:
        schema = TRADE_RAW_SCHEMA
    else:
        raise ValueError(f"unsupported Telonex channel: {channel}")
    table = pa.Table.from_pylist([], schema=schema)
    buffer = io.BytesIO()
    pq.write_table(table, buffer, compression="zstd")
    return buffer.getvalue()


def _book_levels(row: dict[str, Any], side: str) -> str:
    levels: list[dict[str, Any]] = []
    for index in range(5):
        px = row.get(f"{side}_price_{index}")
        sz = row.get(f"{side}_size_{index}")
        if px in (None, "") or sz in (None, ""):
            continue
        levels.append({"px": str(px), "sz": str(sz), "n": 0})
    return orjson.dumps(levels).decode("utf-8")


def _parse_snapshot_row(
    row: dict[str, Any],
    *,
    instrument: str,
    source_line_number: int,
) -> dict[str, Any]:
    return {
        "ts_ms": int(row["timestamp_us"]) // 1000,
        "instrument": instrument,
        "bids_json": _book_levels(row, "bid"),
        "asks_json": _book_levels(row, "ask"),
        "source_hour": 0,
        "source_line_number": source_line_number,
    }


def _parse_trade_row(
    row: dict[str, Any],
    *,
    instrument: str,
    source_line_number: int,
) -> dict[str, Any]:
    side = str(row["side"]).strip().lower()
    if side == "buy":
        normalized_side = "Buy"
    elif side == "sell":
        normalized_side = "Sell"
    else:
        raise ValueError(f"unknown trade side: {row['side']}")
    return {
        "ts_ms": int(row["timestamp_us"]) // 1000,
        "instrument": instrument,
        "side": normalized_side,
        "px": float(row["price"]),
        "sz": float(row["size"]),
        "hash": str(row["trade_id"]),
        "source_hour": 0,
        "source_line_number": source_line_number,
    }


def backfill_market(
    destination: S3Store,
    coverage: CoverageRepository,
    *,
    resolution: PolymarketMarketResolution,
    telonex_api_key: str,
    client: httpx.Client | None = None,
) -> None:
    market = resolution.market_ref()
    metadata_key = polymarket_metadata_s3_key(resolution)
    destination.put_json(metadata_key, resolution.model_dump(mode="json"))

    downloads: list[tuple[DatasetKind, str, str, str, str]] = []
    for date in resolution.dates:
        l2_key = polymarket_raw_l2_s3_key(market, resolution.market_id, date)
        trade_key = polymarket_raw_trade_s3_key(market, resolution.market_id, date)
        l2_record = _coverage_ready(coverage, DatasetKind.RAW_L2, market, date)
        trade_record = _coverage_ready(coverage, DatasetKind.RAW_TRADES, market, date)
        if l2_record is None or not destination.exists(l2_key):
            downloads.append((DatasetKind.RAW_L2, BOOK_CHANNEL, date, l2_key, f"telonex:{BOOK_CHANNEL}"))
        if trade_record is None or not destination.exists(trade_key):
            downloads.append((DatasetKind.RAW_TRADES, TRADE_CHANNEL, date, trade_key, f"telonex:{TRADE_CHANNEL}"))

    if not downloads:
        return

    results: dict[tuple[DatasetKind, str], tuple[bytes, bool]] = {}
    with ThreadPoolExecutor(max_workers=min(4, len(downloads))) as executor:
        future_map = {
            executor.submit(
                _download_channel,
                telonex_api_key=telonex_api_key,
                channel=channel,
                date=date,
                market_id=resolution.market_id,
                outcome=resolution.outcome,
                client=client,
            ): (dataset_kind, channel, date, key, source)
            for dataset_kind, channel, date, key, source in downloads
        }
        for future in as_completed(future_map):
            dataset_kind, channel, date, key, source = future_map[future]
            try:
                results[(dataset_kind, date)] = future.result()
            except Exception:
                _put_coverage(
                    coverage,
                    dataset_kind=dataset_kind,
                    market=market,
                    date=date,
                    status=CoverageStatus.FAILED,
                    object_count=0,
                    byte_count=0,
                    row_count=0,
                    source=source,
                )
                raise

    for dataset_kind, channel, date, key, source in downloads:
        payload, was_missing = results[(dataset_kind, date)]
        destination.put_bytes(key, payload, content_type="application/octet-stream")
        _put_coverage(
            coverage,
            dataset_kind=dataset_kind,
            market=market,
            date=date,
            status=CoverageStatus.READY,
            object_count=1,
            byte_count=len(payload),
            row_count=0,
            source=f"{source}:empty" if was_missing else source,
        )


def normalize_market(
    destination: S3Store,
    coverage: CoverageRepository,
    *,
    resolution: PolymarketMarketResolution,
) -> None:
    market = resolution.market_ref()
    l2_schema = pa.schema(
        [
            ("ts_ms", pa.int64()),
            ("instrument", pa.string()),
            ("bids_json", pa.large_string()),
            ("asks_json", pa.large_string()),
            ("source_hour", pa.int8()),
            ("source_line_number", pa.int64()),
        ]
    )
    trade_schema = pa.schema(
        [
            ("ts_ms", pa.int64()),
            ("instrument", pa.string()),
            ("side", pa.string()),
            ("px", pa.float64()),
            ("sz", pa.float64()),
            ("hash", pa.string()),
            ("source_hour", pa.int8()),
            ("source_line_number", pa.int64()),
        ]
    )
    tmp_dir = Path("/tmp") / "poochon-backtest-data-polymarket"
    tmp_dir.mkdir(parents=True, exist_ok=True)

    for date in resolution.dates:
        normalized_l2_key = polymarket_normalized_l2_s3_key(market, resolution.market_id, date)
        normalized_trade_key = polymarket_normalized_trade_s3_key(market, resolution.market_id, date)
        l2_record = _coverage_ready(coverage, DatasetKind.NORMALIZED_L2, market, date)
        trade_record = _coverage_ready(coverage, DatasetKind.NORMALIZED_TRADES, market, date)
        if (
            l2_record is not None
            and trade_record is not None
            and destination.exists(normalized_l2_key)
            and destination.exists(normalized_trade_key)
        ):
            continue

        raw_l2_bytes = destination.get_bytes(polymarket_raw_l2_s3_key(market, resolution.market_id, date))
        raw_trade_bytes = destination.get_bytes(
            polymarket_raw_trade_s3_key(market, resolution.market_id, date)
        )

        l2_table = pq.read_table(io.BytesIO(raw_l2_bytes))
        trade_table = pq.read_table(io.BytesIO(raw_trade_bytes))
        l2_rows = [
            _parse_snapshot_row(row, instrument=resolution.instrument, source_line_number=index)
            for index, row in enumerate(l2_table.to_pylist(), start=1)
        ]
        trade_rows = [
            _parse_trade_row(row, instrument=resolution.instrument, source_line_number=index)
            for index, row in enumerate(trade_table.to_pylist(), start=1)
        ]
        l2_rows.sort(key=lambda item: (int(item["ts_ms"]), int(item["source_line_number"])))
        trade_rows.sort(key=lambda item: (int(item["ts_ms"]), int(item["source_line_number"])))

        l2_path = tmp_dir / f"{quote_component(resolution.instrument)}-{date}-book5.parquet"
        trade_path = tmp_dir / f"{quote_component(resolution.instrument)}-{date}-trades.parquet"
        _write_parquet(l2_rows, l2_schema, l2_path)
        _write_parquet(trade_rows, trade_schema, trade_path)
        destination.put_file(normalized_l2_key, str(l2_path), content_type="application/octet-stream")
        destination.put_file(normalized_trade_key, str(trade_path), content_type="application/octet-stream")

        _put_coverage(
            coverage,
            dataset_kind=DatasetKind.NORMALIZED_L2,
            market=market,
            date=date,
            status=CoverageStatus.READY,
            object_count=1,
            byte_count=l2_path.stat().st_size,
            row_count=len(l2_rows),
            source=normalized_l2_key,
        )
        _put_coverage(
            coverage,
            dataset_kind=DatasetKind.NORMALIZED_TRADES,
            market=market,
            date=date,
            status=CoverageStatus.READY,
            object_count=1,
            byte_count=trade_path.stat().st_size,
            row_count=len(trade_rows),
            source=normalized_trade_key,
        )


def ingest_market(
    destination: S3Store,
    coverage: CoverageRepository,
    *,
    request: PolymarketReplayCreateRequest,
    telonex_api_key: str,
    client: httpx.Client | None = None,
) -> PolymarketMarketResolution:
    resolution = resolve_market(request, client=client)
    backfill_market(
        destination,
        coverage,
        resolution=resolution,
        telonex_api_key=telonex_api_key,
        client=client,
    )
    normalize_market(destination, coverage, resolution=resolution)
    return resolution


def _iter_series_candidate_slugs(series_key: str, start_date: str, end_date: str) -> list[str]:
    start = datetime.combine(date_cls.fromisoformat(start_date), time_cls.min, tzinfo=UTC)
    end = datetime.combine(date_cls.fromisoformat(end_date) + timedelta(days=1), time_cls.max, tzinfo=UTC)
    current = start
    slugs: list[str] = []
    while current <= end:
        slugs.append(f"{series_key}-{int(current.timestamp())}")
        current += timedelta(minutes=5)
    return slugs


DISCOVER_MAX_WORKERS = 16


def discover_series_markets(
    request: PolymarketSeriesSyncRequest,
    *,
    client: httpx.Client | None = None,
) -> list[PolymarketMarketResolution]:
    window_start = datetime.combine(
        date_cls.fromisoformat(request.start_date),
        time_cls.min,
        tzinfo=UTC,
    )
    window_end = datetime.combine(
        date_cls.fromisoformat(request.end_date),
        time_cls.max,
        tzinfo=UTC,
    )
    slugs = _iter_series_candidate_slugs(
        request.series, request.start_date, request.end_date
    )

    worker_state = local()

    def worker_client() -> httpx.Client:
        if not hasattr(worker_state, "client"):
            worker_state.client = client if client is not None else httpx.Client(timeout=30.0)
        return worker_state.client

    failures: list[dict[str, Any]] = []
    failures_lock = local()  # appends from threads; Python list.append is GIL-safe

    def process_slug(slug: str) -> list[PolymarketMarketResolution]:
        http = worker_client()
        try:
            response = http.get(f"{GAMMA_BASE_URL}/markets/slug/{slug}")
            if response.status_code == 404:
                return []
            response.raise_for_status()
            payload = response.json()
        except Exception as error:
            failures.append({
                "slug": slug,
                "phase": "gamma-fetch",
                "error": f"{type(error).__name__}: {error}",
            })
            return []

        try:
            start, end, start_ts_ms, end_ts_ms = _contract_window_from_payload(payload)
        except Exception as error:
            failures.append({
                "slug": slug,
                "phase": "parse-window",
                "error": f"{type(error).__name__}: {error}",
                "payload_keys": sorted(payload.keys()) if isinstance(payload, dict) else None,
                "payload_sample": {k: payload.get(k) for k in ("slug", "startDate", "endDate", "startDateIso", "endDateIso", "gameStartTime")} if isinstance(payload, dict) else None,
            })
            return []

        if end < window_start or start > window_end:
            return []

        try:
            price_to_beat = _fetch_price_to_beat(http, slug=str(payload["slug"]), start_ts_ms=start_ts_ms)
            settlement_price = _fetch_price_to_beat(http, slug=str(payload["slug"]), start_ts_ms=end_ts_ms)
        except Exception as error:
            failures.append({
                "slug": slug,
                "phase": "price-to-beat",
                "error": f"{type(error).__name__}: {error}",
            })
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
            failures.append({
                "slug": slug,
                "phase": "build-resolution",
                "error": f"{type(error).__name__}: {error}",
            })
            return []
        return out

    logger.info(
        "discover_series_markets: probing %d candidate slugs for series=%s (workers=%d)",
        len(slugs), request.series, DISCOVER_MAX_WORKERS,
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
                    "discover_series_markets: %d/%d probed, %d resolutions so far, %d failures",
                    completed, len(slugs), len(results), len(failures),
                )
            for resolution in future.result():
                key = (resolution.slug, resolution.outcome)
                if key in seen:
                    continue
                seen.add(key)
                results.append(resolution)

    if failures:
        logger.warning(
            "discover_series_markets finished with %d failures; sample: %s",
            len(failures), failures[:5],
        )
    logger.info(
        "discover_series_markets: series=%s resolved=%d failed=%d",
        request.series, len(results), len(failures),
    )
    return sorted(results, key=lambda item: (item.slug, item.outcome))


def _clip_resolution_to_window(
    resolution: PolymarketMarketResolution,
    *,
    start_date: str,
    end_date: str,
) -> PolymarketMarketResolution | None:
    requested_dates = set(iter_dates_inclusive(start_date, end_date))
    clipped_dates = tuple(date for date in resolution.dates if date in requested_dates)
    if not clipped_dates:
        return None
    return resolution.model_copy(update={"dates": clipped_dates})


def sync_series(
    destination: S3Store,
    coverage: CoverageRepository,
    shard_repo: CanonicalShardRepository,
    *,
    request: PolymarketSeriesSyncRequest,
    telonex_api_key: str,
    client: httpx.Client | None = None,
) -> list[PolymarketMarketResolution]:
    discovered = discover_series_markets(request, client=client)
    resolutions = [
        clipped
        for resolution in discovered
        if (clipped := _clip_resolution_to_window(
            resolution,
            start_date=request.start_date,
            end_date=request.end_date,
        ))
        is not None
    ]
    if not resolutions:
        raise ValueError(
            f"no polymarket markets were discovered for series={request.series} "
            f"between {request.start_date} and {request.end_date}"
        )

    worker_state = local()

    def process_resolution(resolution: PolymarketMarketResolution) -> None:
        if not hasattr(worker_state, "store"):
            worker_state.store = destination.clone() if hasattr(destination, "clone") else destination
            worker_state.coverage_repo = coverage.clone() if hasattr(coverage, "clone") else coverage
        store = worker_state.store
        coverage_repo = worker_state.coverage_repo
        backfill_market(
            store,
            coverage_repo,
            resolution=resolution,
            telonex_api_key=telonex_api_key,
            client=None,
        )
        normalize_market(store, coverage_repo, resolution=resolution)

    with ThreadPoolExecutor(max_workers=min(8, len(resolutions))) as executor:
        futures = [executor.submit(process_resolution, resolution) for resolution in resolutions]
        for future in as_completed(futures):
            future.result()

    for date in iter_dates_inclusive(request.start_date, request.end_date):
        build_polymarket_canonical_day_from_storage(
            date=date,
            series_key=request.series,
            outcomes=request.outcomes,
            depth=request.depth,
            s3_store=destination,
            shard_repo=shard_repo,
            force=True,
        )
    return resolutions


def quote_component(value: str) -> str:
    return value.replace("/", "_").replace(":", "_")
