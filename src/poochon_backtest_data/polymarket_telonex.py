from __future__ import annotations

from datetime import UTC, date as date_cls, datetime, timedelta
import io
from pathlib import Path
from typing import Any

import httpx
import orjson
import pyarrow as pa
import pyarrow.parquet as pq

from .models import (
    CoverageRecord,
    CoverageStatus,
    DatasetKind,
    MarketRef,
    NormalizedL2Snapshot,
    NormalizedTrade,
    PolymarketMarketResolution,
    PolymarketReplayCreateRequest,
    coverage_pk,
    polymarket_metadata_s3_key,
    polymarket_normalized_l2_s3_key,
    polymarket_normalized_trade_s3_key,
    polymarket_raw_l2_s3_key,
    polymarket_raw_trade_s3_key,
    utc_now_iso,
)
from .storage import CoverageRepository, S3Store

GAMMA_BASE_URL = "https://gamma-api.polymarket.com"
TELONEX_DOWNLOAD_BASE_URL = "https://api.telonex.io/v1/downloads/polymarket"
BOOK_CHANNEL = "book_snapshot_5"
TRADE_CHANNEL = "trades"


def _parse_utc_timestamp(value: str) -> datetime:
    if value.endswith("Z"):
        value = value[:-1] + "+00:00"
    return datetime.fromisoformat(value).astimezone(UTC)


def _iso_to_epoch_ms(value: str) -> int:
    return int(_parse_utc_timestamp(value).timestamp() * 1000)


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
    finally:
        if own_client:
            http.close()

    outcomes = _parse_json_list(payload["outcomes"])
    token_ids = _parse_json_list(payload["clobTokenIds"])
    if request.outcome not in outcomes:
        valid = ", ".join(f"'{value}'" for value in outcomes)
        raise ValueError(f"unknown outcome '{request.outcome}'. Valid outcomes: {valid}")

    outcome_index = outcomes.index(request.outcome)
    start = _parse_utc_timestamp(payload["startDate"])
    end = _parse_utc_timestamp(payload["endDate"])
    instrument = f"{payload['slug']}:{request.outcome}"
    return PolymarketMarketResolution(
        slug=payload["slug"],
        question=payload["question"],
        outcome=request.outcome,
        market_id=payload["conditionId"],
        asset_id=token_ids[outcome_index],
        instrument=instrument,
        start_time=start.isoformat(),
        end_time=end.isoformat(),
        start_ts_ms=int(start.timestamp() * 1000),
        end_ts_ms=int(end.timestamp() * 1000),
        dates=_date_span(start, end),
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
) -> bytes:
    own_client = client is None
    http = _client(client)
    try:
        response = http.get(
            f"{TELONEX_DOWNLOAD_BASE_URL}/{channel}/{date}",
            params={"market_id": market_id, "outcome": outcome},
            headers={"Authorization": f"Bearer {telonex_api_key}"},
        )
        if response.status_code == 404:
            raise FileNotFoundError(
                f"missing Telonex {channel} dataset for market_id={market_id} outcome={outcome} date={date}"
            )
        if response.status_code >= 400:
            detail = response.text.strip()
            raise ValueError(
                f"Telonex {channel} request failed for market_id={market_id} outcome={outcome} date={date}: {detail}"
            )
        return response.content
    finally:
        if own_client:
            http.close()


def _write_parquet(rows: list[dict[str, Any]], schema: pa.Schema, path: Path) -> None:
    table = pa.Table.from_pylist(rows, schema=schema)
    pq.write_table(table, path, compression="zstd")


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
) -> NormalizedL2Snapshot:
    return NormalizedL2Snapshot(
        ts_ms=int(row["timestamp_us"]) // 1000,
        instrument=instrument,
        bids_json=_book_levels(row, "bid"),
        asks_json=_book_levels(row, "ask"),
        source_hour=0,
        source_line_number=source_line_number,
    )


def _parse_trade_row(
    row: dict[str, Any],
    *,
    instrument: str,
    source_line_number: int,
) -> NormalizedTrade:
    side = str(row["side"]).strip().lower()
    if side == "buy":
        normalized_side = "Buy"
    elif side == "sell":
        normalized_side = "Sell"
    else:
        raise ValueError(f"unknown trade side: {row['side']}")
    return NormalizedTrade(
        ts_ms=int(row["timestamp_us"]) // 1000,
        instrument=instrument,
        side=normalized_side,
        px=float(row["price"]),
        sz=float(row["size"]),
        hash=str(row["trade_id"]),
        source_hour=0,
        source_line_number=source_line_number,
    )


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

    own_client = client is None
    http = _client(client)
    try:
        for date in resolution.dates:
            l2_key = polymarket_raw_l2_s3_key(market, resolution.market_id, date)
            trade_key = polymarket_raw_trade_s3_key(market, resolution.market_id, date)
            l2_record = _coverage_ready(coverage, DatasetKind.RAW_L2, market, date)
            trade_record = _coverage_ready(coverage, DatasetKind.RAW_TRADES, market, date)

            if l2_record is None or not destination.exists(l2_key):
                try:
                    payload = _download_channel(
                        telonex_api_key=telonex_api_key,
                        channel=BOOK_CHANNEL,
                        date=date,
                        market_id=resolution.market_id,
                        outcome=resolution.outcome,
                        client=http,
                    )
                except Exception:
                    _put_coverage(
                        coverage,
                        dataset_kind=DatasetKind.RAW_L2,
                        market=market,
                        date=date,
                        status=CoverageStatus.FAILED,
                        object_count=0,
                        byte_count=0,
                        row_count=0,
                        source=f"telonex:{BOOK_CHANNEL}",
                    )
                    raise
                destination.put_bytes(l2_key, payload, content_type="application/octet-stream")
                _put_coverage(
                    coverage,
                    dataset_kind=DatasetKind.RAW_L2,
                    market=market,
                    date=date,
                    status=CoverageStatus.READY,
                    object_count=1,
                    byte_count=len(payload),
                    row_count=0,
                    source=f"telonex:{BOOK_CHANNEL}",
                )

            if trade_record is None or not destination.exists(trade_key):
                try:
                    payload = _download_channel(
                        telonex_api_key=telonex_api_key,
                        channel=TRADE_CHANNEL,
                        date=date,
                        market_id=resolution.market_id,
                        outcome=resolution.outcome,
                        client=http,
                    )
                except Exception:
                    _put_coverage(
                        coverage,
                        dataset_kind=DatasetKind.RAW_TRADES,
                        market=market,
                        date=date,
                        status=CoverageStatus.FAILED,
                        object_count=0,
                        byte_count=0,
                        row_count=0,
                        source=f"telonex:{TRADE_CHANNEL}",
                    )
                    raise
                destination.put_bytes(trade_key, payload, content_type="application/octet-stream")
                _put_coverage(
                    coverage,
                    dataset_kind=DatasetKind.RAW_TRADES,
                    market=market,
                    date=date,
                    status=CoverageStatus.READY,
                    object_count=1,
                    byte_count=len(payload),
                    row_count=0,
                    source=f"telonex:{TRADE_CHANNEL}",
                )
    finally:
        if own_client:
            http.close()


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
            _parse_snapshot_row(row, instrument=resolution.instrument, source_line_number=index).__dict__
            for index, row in enumerate(l2_table.to_pylist(), start=1)
        ]
        trade_rows = [
            _parse_trade_row(row, instrument=resolution.instrument, source_line_number=index).__dict__
            for index, row in enumerate(trade_table.to_pylist(), start=1)
        ]

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


def quote_component(value: str) -> str:
    return value.replace("/", "_").replace(":", "_")
