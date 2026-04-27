"""Slice builders + replay-side lifecycle state machine for Polymarket.

`build_pm_slice` produces a per-(target, date) shard from mirrored PMXT raw.
`build_hl_slice` (Phase 4) does the same for Hyperliquid from raw lz4 archives.

`_PolymarketContractSchedule` is the lifecycle state machine that emits
Listed/Activated/Resolved transitions from a discovered schedule; it is consumed
at replay time, not at slice-build time.
"""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import UTC, datetime
import io
import logging
from pathlib import Path
import tempfile
from time import perf_counter
from typing import Any, Iterable, Iterator

import orjson
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from .hyperliquid import collapse_fill_trades, parse_l2_lz4_payload
from .models import (
    CanonicalFileFamily,
    CanonicalShardFile,
    CanonicalShardRecord,
    CanonicalShardStatus,
    CoverageStatus,
    DataEventKind,
    MarketRef,
    MarketType,
    PolymarketTarget,
    PolymarketTargetKind,
    Venue,
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
    raw_hl_fills_s3_key,
    raw_hl_l2_s3_key,
    raw_pmxt_s3_key,
    utc_now_iso,
    CANONICAL_DATA_FILE_NAME,
    CANONICAL_SCHEDULE_FILE_NAME,
)
from .polymarket_metadata import GammaUrls, discover_resolutions
from .storage import CanonicalShardRepository, CoverageRepository, S3Store

logger = logging.getLogger(__name__)

POLYMARKET_DEFAULT_DEPTH = 5
PMXT_FETCH_WORKERS = 1

# --- pyarrow schemas ---------------------------------------------------------

_PRICE_LEVEL_TYPE = pa.struct(
    [
        pa.field("px", pa.float64()),
        pa.field("sz", pa.float64()),
        pa.field("n", pa.uint32()),
    ]
)

_DELTA_LEVEL_TYPE = pa.struct(
    [
        pa.field("side", pa.string()),
        pa.field("px", pa.float64()),
        pa.field("sz", pa.float64()),
        pa.field("n", pa.uint32()),
    ]
)

DATA_PARQUET_SCHEMA = pa.schema(
    [
        pa.field("ts_ms", pa.int64(), nullable=False),
        pa.field("instrument", pa.string(), nullable=False),
        pa.field("kind", pa.string(), nullable=False),
        pa.field("bids", pa.list_(_PRICE_LEVEL_TYPE), nullable=True),
        pa.field("asks", pa.list_(_PRICE_LEVEL_TYPE), nullable=True),
        pa.field("delta_levels", pa.list_(_DELTA_LEVEL_TYPE), nullable=True),
        pa.field("px", pa.float64(), nullable=True),
        pa.field("sz", pa.float64(), nullable=True),
        pa.field("side", pa.string(), nullable=True),
    ]
)

_SCHEDULE_OUTCOME_TYPE = pa.struct(
    [
        pa.field("outcome", pa.string()),
        pa.field("asset_id", pa.string()),
        pa.field("instrument", pa.string()),
        pa.field("settlement_payout", pa.float64()),
    ]
)

SCHEDULE_PARQUET_SCHEMA = pa.schema(
    [
        pa.field("target_kind", pa.string(), nullable=False),
        pa.field("target_key", pa.string(), nullable=False),
        pa.field("slug", pa.string(), nullable=False),
        pa.field("market_id", pa.string(), nullable=False),
        pa.field("start_ts_ms", pa.int64(), nullable=False),
        pa.field("end_ts_ms", pa.int64(), nullable=False),
        pa.field("price_to_beat", pa.float64(), nullable=True),
        pa.field("price_to_beat_source", pa.string(), nullable=True),
        pa.field("price_to_beat_quality", pa.string(), nullable=True),
        pa.field("outcomes", pa.list_(_SCHEDULE_OUTCOME_TYPE), nullable=False),
    ]
)


# --- strategy event shape helpers (kept for tests + future tooling) ----------


def _hyperliquid_market_type_variant(value: str | MarketType) -> str:
    market_type = value if isinstance(value, MarketType) else MarketType(value)
    if market_type == MarketType.PERP:
        return "Perp"
    if market_type == MarketType.SPOT:
        return "Spot"
    raise ValueError(f"unsupported hyperliquid market type for strategy event: {market_type}")


def _instrument_payload(
    *,
    venue: str | Venue,
    instrument: str,
    market_type: str | MarketType | None = None,
) -> dict[str, Any]:
    venue_value = venue if isinstance(venue, Venue) else Venue(venue)
    if venue_value == Venue.HYPERLIQUID:
        if market_type is None:
            raise ValueError("hyperliquid strategy events require market_type")
        return {
            "Hyperliquid": {
                "market_type": _hyperliquid_market_type_variant(market_type),
                "symbol": instrument,
            }
        }
    if venue_value == Venue.POLYMARKET:
        return {"Polymarket": {"symbol": instrument}}
    raise ValueError(f"unsupported venue for strategy event instrument: {venue_value}")


# --- Polymarket lifecycle state machine (replay-side; tests) ----------------


@dataclass(frozen=True)
class _PolymarketContractOutcome:
    outcome: str
    asset_id: str
    instrument: str
    settlement_payout: float | None


@dataclass(frozen=True)
class _PolymarketContract:
    series_key: str
    slug: str
    market_id: str
    start_ts_ms: int
    end_ts_ms: int
    price_to_beat: float | None
    price_to_beat_source: str | None
    price_to_beat_quality: str | None
    outcomes: tuple[_PolymarketContractOutcome, ...]

    def event(self, kind: str, *, ts_ms: int) -> dict[str, Any]:
        return {
            "Contract": {
                "Polymarket": {
                    "kind": kind,
                    "ts_ms": ts_ms,
                    "series_key": self.series_key,
                    "slug": self.slug,
                    "market_id": self.market_id,
                    "start_ts_ms": self.start_ts_ms,
                    "end_ts_ms": self.end_ts_ms,
                    "price_to_beat": self.price_to_beat,
                    "price_to_beat_source": self.price_to_beat_source,
                    "price_to_beat_quality": self.price_to_beat_quality,
                    "outcomes": [
                        {
                            "outcome": item.outcome,
                            "asset_id": item.asset_id,
                            "instrument": _instrument_payload(
                                venue=Venue.POLYMARKET, instrument=item.instrument
                            ),
                            "settlement_payout": item.settlement_payout,
                        }
                        for item in self.outcomes
                    ],
                }
            }
        }


class _PolymarketContractSchedule:
    """Steps the (current, next) state machine forward as time advances.

    Built at replay time from `schedule.parquet` rows; each call to
    `lifecycle_events(ts)` returns the events to emit at that timestamp.
    """

    def __init__(self, contracts: list[_PolymarketContract]):
        self.contracts = sorted(contracts, key=lambda item: (item.start_ts_ms, item.slug))
        self.by_start = {item.start_ts_ms: item for item in self.contracts}
        starts = sorted({item.start_ts_ms for item in self.contracts})
        diffs = [
            current - previous
            for previous, current in zip(starts, starts[1:], strict=False)
            if current > previous
        ]
        durations = [
            item.end_ts_ms - item.start_ts_ms
            for item in self.contracts
            if item.end_ts_ms > item.start_ts_ms
        ]
        self.interval_ms = min(diffs) if diffs else min(durations, default=300_000)
        self.current: _PolymarketContract | None = None
        self.next: _PolymarketContract | None = None

    def current_and_next_for_ts(
        self, ts_ms: int
    ) -> tuple[_PolymarketContract | None, _PolymarketContract | None]:
        current: _PolymarketContract | None = None
        for contract in self.contracts:
            if contract.start_ts_ms <= ts_ms <= contract.end_ts_ms:
                current = contract
                continue
            if contract.start_ts_ms > ts_ms:
                break
        if current is None:
            return None, None
        next_contract = self.by_start.get(current.start_ts_ms + self.interval_ms)
        return current, next_contract

    def lifecycle_events(self, ts_ms: int) -> list[dict[str, Any]]:
        current, next_contract = self.current_and_next_for_ts(ts_ms)
        if current is None:
            return []

        events: list[dict[str, Any]] = []
        if self.current is None:
            events.append(current.event("ListedCurrent", ts_ms=ts_ms))
            if next_contract is not None:
                events.append(next_contract.event("ListedNext", ts_ms=ts_ms))
        elif self.current.slug != current.slug:
            events.append(self.current.event("Resolved", ts_ms=ts_ms))
            if self.next is None or self.next.slug != current.slug:
                events.append(current.event("ListedCurrent", ts_ms=ts_ms))
            events.append(current.event("Activated", ts_ms=ts_ms))
            if next_contract is not None and (
                self.next is None or self.next.slug != next_contract.slug
            ):
                events.append(next_contract.event("ListedNext", ts_ms=ts_ms))
        elif (
            (self.next is None and next_contract is not None)
            or (
                self.next is not None
                and next_contract is not None
                and self.next.slug != next_contract.slug
            )
        ):
            events.append(next_contract.event("ListedNext", ts_ms=ts_ms))

        self.current = current
        self.next = next_contract
        return events


# --- PM slice build ----------------------------------------------------------


@dataclass
class PMSliceStats:
    contracts_discovered: int = 0
    asset_ids_kept: int = 0
    rows_in: int = 0
    rows_out: int = 0
    bytes_in: int = 0
    bytes_out: int = 0
    fetch_seconds: float = 0.0
    translate_seconds: float = 0.0
    write_seconds: float = 0.0


def build_pm_slice(
    *,
    target: PolymarketTarget,
    date: str,
    s3_store: S3Store,
    coverage_repo: CoverageRepository,
    shard_repo: CanonicalShardRepository,
    gamma_base_url: str = "https://gamma-api.polymarket.com",
    vatic_base_url: str = "https://api.vatic.trading",
    binance_base_url: str = "https://api.binance.com",
    binance_us_base_url: str = "https://api.binance.us",
    depth: int = POLYMARKET_DEFAULT_DEPTH,
    force: bool = False,
) -> CanonicalShardRecord:
    """Build one canonical PM shard (data.parquet + schedule.parquet + manifest.json).

    Idempotent: a fully-READY shard with all referenced files present is returned
    unchanged unless `force=True`.
    """
    shard_id = canonical_pm_shard_id(target=target, date=date, depth=depth)
    shard_prefix = canonical_pm_shard_prefix(target=target, date=date, depth=depth)
    manifest_key = canonical_shard_manifest_s3_key(shard_prefix)
    data_key = canonical_shard_data_s3_key(shard_prefix)
    schedule_key = canonical_shard_schedule_s3_key(shard_prefix)

    existing = shard_repo.get(shard_id)
    if not force and _pm_shard_ready(existing, s3_store, data_key, schedule_key):
        logger.info(
            "pm slice skip target=%s:%s date=%s (already READY)",
            target.target_kind.value,
            target.target_key,
            date,
        )
        return existing  # type: ignore[return-value]

    started_at = perf_counter()
    stats = PMSliceStats()
    logger.info(
        "pm slice start target=%s:%s date=%s depth=%d force=%s",
        target.target_kind.value,
        target.target_key,
        date,
        depth,
        force,
    )

    # Step 1 — pre-flight: raw_pmxt coverage for all 24 hours of `date`.
    _assert_raw_pmxt_ready(coverage_repo, s3_store, date)

    # Step 2 — discover schedule.
    discover_started = perf_counter()
    urls = GammaUrls(
        gamma_base_url=gamma_base_url,
        vatic_base_url=vatic_base_url,
        binance_base_url=binance_base_url,
        binance_us_base_url=binance_us_base_url,
    )
    resolutions = discover_resolutions(target, start_date=date, end_date=date, urls=urls)
    if not resolutions:
        raise RuntimeError(
            f"no Polymarket resolutions discovered for target={target.target_kind.value}:"
            f"{target.target_key} date={date}"
        )
    schedule_table = _build_schedule_table(target, resolutions)
    asset_to_instrument = {res.asset_id: res.instrument for res in resolutions}
    asset_ids = list(asset_to_instrument)
    stats.contracts_discovered = len({res.slug for res in resolutions})
    stats.asset_ids_kept = len(asset_ids)
    logger.info(
        "pm slice discovered contracts=%d asset_ids=%d (%.2fs)",
        stats.contracts_discovered,
        stats.asset_ids_kept,
        perf_counter() - discover_started,
    )

    # Step 3 — slice raw_pmxt directly to a local data.parquet (streaming).
    write_started = perf_counter()
    source_refs = [raw_pmxt_s3_key(date, hour) for hour in range(24)]
    with tempfile.TemporaryDirectory(prefix=f"{shard_id}-") as temp_dir_raw:
        temp_dir = Path(temp_dir_raw)
        data_parquet_path = temp_dir / CANONICAL_DATA_FILE_NAME
        schedule_parquet_path = temp_dir / CANONICAL_SCHEDULE_FILE_NAME

        fetch_started = perf_counter()
        rows_out = _slice_pmxt_for_date(
            s3_store=s3_store,
            date=date,
            asset_to_instrument=asset_to_instrument,
            asset_ids=asset_ids,
            depth=depth,
            stats=stats,
            data_parquet_path=data_parquet_path,
        )
        stats.fetch_seconds = perf_counter() - fetch_started

        pq.write_table(
            schedule_table,
            schedule_parquet_path,
            compression="zstd",
            use_dictionary=[
                "target_kind",
                "target_key",
                "price_to_beat_source",
                "price_to_beat_quality",
            ],
        )

        record = _write_pm_shard_from_files(
            shard_id=shard_id,
            shard_prefix=shard_prefix,
            manifest_key=manifest_key,
            data_key=data_key,
            schedule_key=schedule_key,
            target=target,
            date=date,
            depth=depth,
            data_parquet_path=data_parquet_path,
            schedule_parquet_path=schedule_parquet_path,
            schedule_row_count=schedule_table.num_rows,
            data_row_count=rows_out,
            source_refs=source_refs,
            s3_store=s3_store,
            shard_repo=shard_repo,
            coverage_repo=coverage_repo,
            stats=stats,
        )
    stats.write_seconds = perf_counter() - write_started

    logger.info(
        "pm slice done target=%s:%s date=%s events=%d data_bytes=%d "
        "rows_in=%d rows_out=%d total=%.3fs (discover+fetch=%.3fs translate=%.3fs write=%.3fs)",
        target.target_kind.value,
        target.target_key,
        date,
        record.event_count,
        record.byte_count,
        stats.rows_in,
        stats.rows_out,
        perf_counter() - started_at,
        stats.fetch_seconds,
        stats.translate_seconds,
        stats.write_seconds,
    )
    return record


def _pm_shard_ready(
    existing: CanonicalShardRecord | None,
    s3_store: S3Store,
    data_key: str,
    schedule_key: str,
) -> bool:
    if existing is None or existing.status != CanonicalShardStatus.READY:
        return False
    if existing.data_file is None or existing.schedule_file is None:
        return False
    return s3_store.exists(data_key) and s3_store.exists(schedule_key)


def _assert_raw_pmxt_ready(
    coverage_repo: CoverageRepository, s3_store: S3Store, date: str
) -> None:
    pks = [coverage_pk_raw_pmxt(date, hour) for hour in range(24)]
    records = coverage_repo.batch_get(pks)
    missing: list[int] = []
    for hour in range(24):
        record = records.get(coverage_pk_raw_pmxt(date, hour))
        if record is None or record.status != CoverageStatus.READY:
            missing.append(hour)
            continue
        if not s3_store.exists(raw_pmxt_s3_key(date, hour)):
            missing.append(hour)
    if missing:
        gaps = ", ".join(f"{hour:02d}" for hour in missing)
        raise RuntimeError(
            f"raw_pmxt incomplete for {date} (missing {len(missing)}/24 hours: {gaps}). "
            f"run `run polymarket mirror --start-date {date} --end-date {date}` first."
        )


def _build_schedule_table(
    target: PolymarketTarget, resolutions: list
) -> pa.Table:
    grouped: dict[str, list] = {}
    for resolution in resolutions:
        grouped.setdefault(resolution.slug, []).append(resolution)

    rows: list[dict[str, Any]] = []
    for slug, items in sorted(grouped.items(), key=lambda item: (item[1][0].start_ts_ms, item[0])):
        first = items[0]
        outcomes = [
            {
                "outcome": item.outcome,
                "asset_id": item.asset_id,
                "instrument": item.instrument,
                "settlement_payout": item.settlement_payout,
            }
            for item in sorted(items, key=lambda res: res.outcome)
        ]
        rows.append(
            {
                "target_kind": target.target_kind.value,
                "target_key": target.target_key,
                "slug": slug,
                "market_id": first.market_id,
                "start_ts_ms": first.start_ts_ms,
                "end_ts_ms": first.end_ts_ms,
                "price_to_beat": first.price_to_beat,
                "price_to_beat_source": first.price_to_beat_source,
                "price_to_beat_quality": first.price_to_beat_quality,
                "outcomes": outcomes,
            }
        )
    return pa.Table.from_pylist(rows, schema=SCHEDULE_PARQUET_SCHEMA)


_PMXT_TRANSLATE_SQL_TEMPLATE = """
WITH filtered AS (
    SELECT
        CAST(epoch_ms(p.timestamp) AS BIGINT) AS ts_ms,
        l.instrument,
        p.event_type,
        p.bids,
        p.asks,
        CASE WHEN p.price IS NULL THEN NULL ELSE CAST(p.price AS DOUBLE) END AS px_raw,
        CASE WHEN p."size" IS NULL THEN NULL ELSE CAST(p."size" AS DOUBLE) END AS sz_raw,
        CASE
            WHEN UPPER(p.side) = 'BUY'  THEN 'Buy'
            WHEN UPPER(p.side) = 'SELL' THEN 'Sell'
            ELSE NULL
        END AS side_norm
    FROM read_parquet(?) AS p
    INNER JOIN asset_lookup AS l USING (asset_id)
    WHERE p.event_type IN ('book', 'price_change', 'last_trade_price')
),
parsed AS (
    SELECT
        ts_ms,
        instrument,
        CASE event_type
            WHEN 'book'             THEN 'l2_snapshot'
            WHEN 'price_change'     THEN 'delta_batch'
            WHEN 'last_trade_price' THEN 'trade'
        END AS kind,
        CASE WHEN event_type = 'book' THEN
            list_transform(
                json_extract(bids, '$[*]')[1:{depth}],
                x -> {{
                    'px': CAST(json_extract_string(x, '$.price') AS DOUBLE),
                    'sz': CAST(json_extract_string(x, '$.size')  AS DOUBLE),
                    'n':  CAST(0 AS UINTEGER)
                }}
            )
        END AS bids,
        CASE WHEN event_type = 'book' THEN
            list_transform(
                json_extract(asks, '$[*]')[1:{depth}],
                x -> {{
                    'px': CAST(json_extract_string(x, '$.price') AS DOUBLE),
                    'sz': CAST(json_extract_string(x, '$.size')  AS DOUBLE),
                    'n':  CAST(0 AS UINTEGER)
                }}
            )
        END AS asks,
        CASE WHEN event_type = 'price_change'
                  AND side_norm IS NOT NULL
                  AND px_raw IS NOT NULL
                  AND sz_raw IS NOT NULL THEN
            [{{
                'side': side_norm,
                'px':   px_raw,
                'sz':   sz_raw,
                'n':    CAST(0 AS UINTEGER)
            }}]
        END AS delta_levels,
        CASE WHEN event_type = 'last_trade_price' THEN px_raw END AS px,
        CASE WHEN event_type = 'last_trade_price' THEN sz_raw END AS sz,
        CASE WHEN event_type = 'last_trade_price' THEN side_norm END AS side
    FROM filtered
)
SELECT
    ts_ms,
    instrument,
    kind,
    bids,
    asks,
    delta_levels,
    px,
    sz,
    side
FROM parsed
WHERE
    (kind = 'l2_snapshot') OR
    (kind = 'delta_batch'  AND delta_levels IS NOT NULL) OR
    (kind = 'trade'        AND px IS NOT NULL AND sz IS NOT NULL AND side IS NOT NULL)
"""


def _slice_pmxt_for_date(
    *,
    s3_store: S3Store,
    date: str,
    asset_to_instrument: dict[str, str],
    asset_ids: list[str],
    depth: int,
    stats: PMSliceStats,
    data_parquet_path: Path,
) -> int:
    """Translate 24 hourly PMXT files into a single data.parquet using DuckDB.

    For each hour:
      - download the PMXT file from S3 to a local temp file
      - run a DuckDB SQL pipeline (filter by asset_id, translate event_type → kind,
        parse JSON bids/asks for `book`, vectorize `price_change`/`last_trade_price`)
      - stream-append the result as a row group to `data_parquet_path`

    Memory stays bounded because DuckDB processes batches; the local PMXT temp
    file is removed after each hour. Returns total rows written.
    """
    import duckdb

    asset_lookup = pa.Table.from_pydict(
        {
            "asset_id": list(asset_to_instrument),
            "instrument": list(asset_to_instrument.values()),
        }
    )
    sql = _PMXT_TRANSLATE_SQL_TEMPLATE.format(depth=depth)
    rows_out_total = 0
    writer: pq.ParquetWriter | None = None
    con = duckdb.connect(":memory:")
    try:
        con.execute("INSTALL json; LOAD json;")
        con.register("asset_lookup", asset_lookup)

        with tempfile.TemporaryDirectory(prefix=f"pmxt-day-{date}-") as work_dir_raw:
            work_dir = Path(work_dir_raw)
            for hour in range(24):
                hour_payload = _fetch_pmxt_payload(s3_store, date, hour)
                stats.bytes_in += len(hour_payload)
                stats.rows_in += pq.ParquetFile(io.BytesIO(hour_payload)).metadata.num_rows
                hour_path = work_dir / f"pmxt-{date}-{hour:02d}.parquet"
                hour_path.write_bytes(hour_payload)
                del hour_payload

                translate_started = perf_counter()
                hour_table = con.execute(sql, [str(hour_path)]).to_arrow_table()
                stats.translate_seconds += perf_counter() - translate_started

                hour_rows = hour_table.num_rows
                if hour_rows > 0:
                    if writer is None:
                        writer = pq.ParquetWriter(
                            data_parquet_path,
                            schema=DATA_PARQUET_SCHEMA,
                            compression="zstd",
                            use_dictionary=["instrument", "kind", "side"],
                        )
                    # Cast to canonical schema in case DuckDB inferred slightly different types
                    hour_table = hour_table.cast(DATA_PARQUET_SCHEMA)
                    writer.write_table(hour_table)
                    rows_out_total += hour_rows
                del hour_table
                hour_path.unlink(missing_ok=True)

                logger.info(
                    "pm slice translated hour=%02d (%d/24)  rows_in=%d rows_out=%d (cum=%d)",
                    hour,
                    hour + 1,
                    stats.rows_in,
                    hour_rows,
                    rows_out_total,
                )

        if writer is None:
            # No rows for any hour — still write an empty data.parquet so the
            # consumer never sees a missing file.
            writer = pq.ParquetWriter(
                data_parquet_path,
                schema=DATA_PARQUET_SCHEMA,
                compression="zstd",
                use_dictionary=["instrument", "kind", "side"],
            )
    finally:
        if writer is not None:
            writer.close()
        con.close()

    stats.rows_out = rows_out_total
    return rows_out_total


def _fetch_pmxt_payload(s3_store: S3Store, date: str, hour: int) -> bytes:
    """Multipart-download a PMXT hourly file from S3 with retry on mid-stream drops."""
    from boto3.s3.transfer import TransferConfig
    from botocore.exceptions import ClientError, ConnectionClosedError, ResponseStreamingError
    import time

    key = raw_pmxt_s3_key(date, hour)
    config = TransferConfig(
        multipart_threshold=16 * 1024 * 1024,
        multipart_chunksize=16 * 1024 * 1024,
        max_concurrency=4,
    )
    last_error: Exception | None = None
    for attempt in range(5):
        try:
            buf = io.BytesIO()
            s3_store.client.download_fileobj(
                s3_store.bucket, key, buf, Config=config
            )
            return buf.getvalue()
        except (ResponseStreamingError, ConnectionClosedError, ConnectionResetError, ClientError) as error:
            last_error = error
            if attempt + 1 < 5:
                time.sleep(2 ** attempt)
    assert last_error is not None
    raise last_error


def _translate_pmxt_payload(
    payload: bytes,
    *,
    asset_to_instrument: dict[str, str],
    asset_id_set: pa.Array,
    depth: int,
    stats: PMSliceStats,
) -> dict[str, list]:
    table = pq.read_table(io.BytesIO(payload))
    stats.rows_in += table.num_rows
    if table.num_rows == 0:
        return {field.name: [] for field in DATA_PARQUET_SCHEMA}

    asset_filter = pc.is_in(table["asset_id"], value_set=asset_id_set)
    table = table.filter(asset_filter)
    if table.num_rows == 0:
        return {field.name: [] for field in DATA_PARQUET_SCHEMA}

    # Drop tick_size_change rows (irrelevant for backtest market data).
    keep = pc.not_equal(table["event_type"], pa.scalar("tick_size_change"))
    table = table.filter(keep)
    if table.num_rows == 0:
        return {field.name: [] for field in DATA_PARQUET_SCHEMA}

    timestamps = table.column("timestamp")
    asset_ids = table.column("asset_id").to_pylist()
    event_types = table.column("event_type").to_pylist()
    bids_raw = table.column("bids").to_pylist()
    asks_raw = table.column("asks").to_pylist()
    prices = table.column("price").to_pylist()
    sizes = table.column("size").to_pylist()
    sides = table.column("side").to_pylist()

    # timestamps is timestamp[ms, tz=UTC]; cast to int64 ms.
    ts_ms_array = pc.cast(timestamps, pa.int64()).to_pylist()

    out: dict[str, list] = {field.name: [] for field in DATA_PARQUET_SCHEMA}
    for idx, event_type in enumerate(event_types):
        asset_id = asset_ids[idx]
        instrument = asset_to_instrument.get(asset_id)
        if instrument is None:
            continue
        ts_ms = ts_ms_array[idx]
        if ts_ms is None:
            continue

        out["ts_ms"].append(int(ts_ms))
        out["instrument"].append(instrument)

        if event_type == "book":
            out["kind"].append(DataEventKind.L2_SNAPSHOT.value)
            out["bids"].append(_parse_book_levels(bids_raw[idx], depth=depth))
            out["asks"].append(_parse_book_levels(asks_raw[idx], depth=depth))
            out["delta_levels"].append(None)
            out["px"].append(None)
            out["sz"].append(None)
            out["side"].append(None)
        elif event_type == "price_change":
            level = _build_delta_level(side=sides[idx], price=prices[idx], size=sizes[idx])
            if level is None:
                # malformed delta — skip
                out["ts_ms"].pop()
                out["instrument"].pop()
                continue
            out["kind"].append(DataEventKind.DELTA_BATCH.value)
            out["bids"].append(None)
            out["asks"].append(None)
            out["delta_levels"].append([level])
            out["px"].append(None)
            out["sz"].append(None)
            out["side"].append(None)
        elif event_type == "last_trade_price":
            normalized_side = _normalize_side(sides[idx])
            if normalized_side is None or prices[idx] is None or sizes[idx] is None:
                out["ts_ms"].pop()
                out["instrument"].pop()
                continue
            out["kind"].append(DataEventKind.TRADE.value)
            out["bids"].append(None)
            out["asks"].append(None)
            out["delta_levels"].append(None)
            out["px"].append(float(prices[idx]))
            out["sz"].append(float(sizes[idx]))
            out["side"].append(normalized_side)
        else:
            out["ts_ms"].pop()
            out["instrument"].pop()
            continue
    return out


def _parse_book_levels(raw: str | None, *, depth: int) -> list[dict[str, Any]] | None:
    if not raw:
        return []
    try:
        levels = orjson.loads(raw)
    except orjson.JSONDecodeError:
        return []
    if not isinstance(levels, list):
        return []
    out: list[dict[str, Any]] = []
    for level in levels[:depth]:
        if not isinstance(level, dict):
            continue
        try:
            px = float(level.get("price", level.get("p")))
            sz = float(level.get("size", level.get("s", 0.0)))
        except (TypeError, ValueError):
            continue
        out.append({"px": px, "sz": sz, "n": 0})
    return out


def _build_delta_level(*, side: Any, price: Any, size: Any) -> dict[str, Any] | None:
    normalized_side = _normalize_side(side)
    if normalized_side is None or price is None or size is None:
        return None
    try:
        px = float(price)
        sz = float(size)
    except (TypeError, ValueError):
        return None
    return {"side": normalized_side, "px": px, "sz": sz, "n": 0}


def _normalize_side(value: Any) -> str | None:
    if value is None:
        return None
    upper = str(value).strip().upper()
    if upper == "BUY":
        return "Buy"
    if upper == "SELL":
        return "Sell"
    return None


def _write_pm_shard_from_files(
    *,
    shard_id: str,
    shard_prefix: str,
    manifest_key: str,
    data_key: str,
    schedule_key: str,
    target: PolymarketTarget,
    date: str,
    depth: int,
    data_parquet_path: Path,
    schedule_parquet_path: Path,
    schedule_row_count: int,
    data_row_count: int,
    source_refs: list[str],
    s3_store: S3Store,
    shard_repo: CanonicalShardRepository,
    coverage_repo: CoverageRepository,
    stats: PMSliceStats,
) -> CanonicalShardRecord:
    """Upload pre-written local data.parquet + schedule.parquet to S3 and persist records."""
    # Read ts_ms column (only this column) to get min/max without loading full file.
    if data_row_count == 0:
        ts_min: int | None = None
        ts_max: int | None = None
    else:
        ts_table = pq.read_table(data_parquet_path, columns=["ts_ms"])
        ts_array = ts_table.column("ts_ms")
        ts_min = int(pc.min(ts_array).as_py())
        ts_max = int(pc.max(ts_array).as_py())
        del ts_table

    data_size = data_parquet_path.stat().st_size
    schedule_size = schedule_parquet_path.stat().st_size
    stats.bytes_out = data_size + schedule_size

    s3_store.put_file(
        data_key,
        str(data_parquet_path),
        content_type="application/vnd.apache.parquet",
    )
    s3_store.put_file(
        schedule_key,
        str(schedule_parquet_path),
        content_type="application/vnd.apache.parquet",
    )

    data_file = CanonicalShardFile(
        family=CanonicalFileFamily.DATA,
        file_name=CANONICAL_DATA_FILE_NAME,
        s3_key=data_key,
        row_count=data_row_count,
        size_bytes=data_size,
    )
    schedule_file = CanonicalShardFile(
        family=CanonicalFileFamily.SCHEDULE,
        file_name=CANONICAL_SCHEDULE_FILE_NAME,
        s3_key=schedule_key,
        row_count=schedule_row_count,
        size_bytes=schedule_size,
    )

    now = utc_now_iso()
    record = CanonicalShardRecord(
        shard_id=shard_id,
        status=CanonicalShardStatus.READY,
        venue=Venue.POLYMARKET,
        market_type=MarketType.BINARY,
        date=date,
        depth=depth,
        shard_prefix=shard_prefix,
        manifest_s3_key=manifest_key,
        target_kind=target.target_kind,
        target_key=target.target_key,
        data_file=data_file,
        schedule_file=schedule_file,
        event_count=data_row_count,
        byte_count=data_size + schedule_size,
        start_ts_ms=ts_min,
        end_ts_ms=ts_max,
        source_refs=tuple(source_refs),
        created_at=now,
        updated_at=now,
    )
    s3_store.put_json(manifest_key, orjson.loads(record.model_dump_json()))
    shard_repo.put(record)

    coverage_record_pk = coverage_pk_canonical_pm(target, date)
    from .models import CoverageRecord, DatasetKind

    coverage_repo.put(
        CoverageRecord(
            pk=coverage_record_pk,
            dataset_kind=DatasetKind.CANONICAL_PM,
            status=CoverageStatus.READY,
            object_count=2,
            byte_count=data_size + schedule_size,
            row_count=data_row_count,
            updated_at=now,
            source=manifest_key,
            target_kind=target.target_kind,
            target_key=target.target_key,
            date=date,
        )
    )
    return record


# Backwards-compatible shim used by scripts/pm_writepath_smoketest.py
def _write_pm_shard(
    *,
    shard_id: str,
    shard_prefix: str,
    manifest_key: str,
    data_key: str,
    schedule_key: str,
    target: PolymarketTarget,
    date: str,
    depth: int,
    data_table: pa.Table,
    schedule_table: pa.Table,
    source_refs: list[str],
    s3_store: S3Store,
    shard_repo: CanonicalShardRepository,
    coverage_repo: CoverageRepository,
    stats: PMSliceStats,
) -> CanonicalShardRecord:
    """Write tables to local parquet then delegate to _write_pm_shard_from_files."""
    with tempfile.TemporaryDirectory(prefix=f"{shard_id}-") as temp_dir_raw:
        temp_dir = Path(temp_dir_raw)
        data_path = temp_dir / CANONICAL_DATA_FILE_NAME
        schedule_path = temp_dir / CANONICAL_SCHEDULE_FILE_NAME
        pq.write_table(
            data_table,
            data_path,
            compression="zstd",
            use_dictionary=["instrument", "kind", "side"],
        )
        pq.write_table(
            schedule_table,
            schedule_path,
            compression="zstd",
            use_dictionary=[
                "target_kind",
                "target_key",
                "price_to_beat_source",
                "price_to_beat_quality",
            ],
        )
        return _write_pm_shard_from_files(
            shard_id=shard_id,
            shard_prefix=shard_prefix,
            manifest_key=manifest_key,
            data_key=data_key,
            schedule_key=schedule_key,
            target=target,
            date=date,
            depth=depth,
            data_parquet_path=data_path,
            schedule_parquet_path=schedule_path,
            schedule_row_count=schedule_table.num_rows,
            data_row_count=data_table.num_rows,
            source_refs=source_refs,
            s3_store=s3_store,
            shard_repo=shard_repo,
            coverage_repo=coverage_repo,
            stats=stats,
        )


# --- HL slice ----------------------------------------------------------------


HL_DEFAULT_DEPTH = 20
HL_FETCH_WORKERS = 1


@dataclass
class HLSliceStats:
    rows_in_l2: int = 0
    rows_in_trades: int = 0
    rows_out: int = 0
    bytes_in: int = 0
    bytes_out: int = 0
    fetch_seconds: float = 0.0
    translate_seconds: float = 0.0
    write_seconds: float = 0.0


def build_hl_slice(
    *,
    market: MarketRef,
    date: str,
    s3_store: S3Store,
    coverage_repo: CoverageRepository,
    shard_repo: CanonicalShardRepository,
    depth: int = HL_DEFAULT_DEPTH,
    force: bool = False,
) -> CanonicalShardRecord:
    shard_id = canonical_hl_shard_id(market, date, depth)
    shard_prefix = canonical_hl_shard_prefix(market, date, depth)
    manifest_key = canonical_shard_manifest_s3_key(shard_prefix)
    data_key = canonical_shard_data_s3_key(shard_prefix)

    existing = shard_repo.get(shard_id)
    if not force and _hl_shard_ready(existing, s3_store, data_key):
        logger.info(
            "hl slice skip market=%s/%s date=%s (already READY)",
            market.market_type.value,
            market.instrument,
            date,
        )
        return existing  # type: ignore[return-value]

    started_at = perf_counter()
    stats = HLSliceStats()
    logger.info(
        "hl slice start market=%s/%s date=%s depth=%d force=%s",
        market.market_type.value,
        market.instrument,
        date,
        depth,
        force,
    )

    _assert_raw_hl_ready(coverage_repo, s3_store, market, date)

    fetch_started = perf_counter()
    l2_payloads, fills_payloads = _fetch_hl_payloads(s3_store, market, date, stats)
    stats.fetch_seconds = perf_counter() - fetch_started

    translate_started = perf_counter()
    rows = _translate_hl_payloads(
        l2_payloads=l2_payloads,
        fills_payloads=fills_payloads,
        instrument=market.instrument,
        depth=depth,
        stats=stats,
    )
    stats.translate_seconds = perf_counter() - translate_started

    data_table = _hl_rows_to_table(rows)
    stats.rows_out = data_table.num_rows

    write_started = perf_counter()
    source_refs: list[str] = []
    for hour in range(24):
        source_refs.append(raw_hl_l2_s3_key(market, date, hour))
        source_refs.append(raw_hl_fills_s3_key(date, hour))

    record = _write_hl_shard(
        shard_id=shard_id,
        shard_prefix=shard_prefix,
        manifest_key=manifest_key,
        data_key=data_key,
        market=market,
        date=date,
        depth=depth,
        data_table=data_table,
        source_refs=source_refs,
        s3_store=s3_store,
        shard_repo=shard_repo,
        coverage_repo=coverage_repo,
        stats=stats,
    )
    stats.write_seconds = perf_counter() - write_started

    logger.info(
        "hl slice done market=%s/%s date=%s events=%d data_bytes=%d "
        "rows_in_l2=%d rows_in_trades=%d total=%.3fs (fetch=%.3fs translate=%.3fs write=%.3fs)",
        market.market_type.value,
        market.instrument,
        date,
        record.event_count,
        record.byte_count,
        stats.rows_in_l2,
        stats.rows_in_trades,
        perf_counter() - started_at,
        stats.fetch_seconds,
        stats.translate_seconds,
        stats.write_seconds,
    )
    return record


def _hl_shard_ready(
    existing: CanonicalShardRecord | None,
    s3_store: S3Store,
    data_key: str,
) -> bool:
    if existing is None or existing.status != CanonicalShardStatus.READY:
        return False
    if existing.data_file is None:
        return False
    return s3_store.exists(data_key)


def _assert_raw_hl_ready(
    coverage_repo: CoverageRepository,
    s3_store: S3Store,
    market: MarketRef,
    date: str,
) -> None:
    l2_pks = [coverage_pk_raw_hl_l2(market, date, hour) for hour in range(24)]
    fills_pks = [coverage_pk_raw_hl_fills(date, hour) for hour in range(24)]
    records = coverage_repo.batch_get(l2_pks + fills_pks)

    missing_l2: list[int] = []
    missing_fills: list[int] = []
    for hour in range(24):
        l2_record = records.get(coverage_pk_raw_hl_l2(market, date, hour))
        if (
            l2_record is None
            or l2_record.status != CoverageStatus.READY
            or not s3_store.exists(raw_hl_l2_s3_key(market, date, hour))
        ):
            missing_l2.append(hour)
        fills_record = records.get(coverage_pk_raw_hl_fills(date, hour))
        if (
            fills_record is None
            or fills_record.status != CoverageStatus.READY
            or not s3_store.exists(raw_hl_fills_s3_key(date, hour))
        ):
            missing_fills.append(hour)

    if missing_l2 or missing_fills:
        gaps = []
        if missing_l2:
            gaps.append(
                f"raw_hl_l2 missing hours: {', '.join(f'{h:02d}' for h in missing_l2)}"
            )
        if missing_fills:
            gaps.append(
                f"raw_hl_fills missing hours: {', '.join(f'{h:02d}' for h in missing_fills)}"
            )
        raise RuntimeError(
            f"raw_hl incomplete for {market.market_type.value}/{market.instrument} {date}: "
            + "; ".join(gaps)
            + ". run mirror first."
        )


def _fetch_hl_payloads(
    s3_store: S3Store, market: MarketRef, date: str, stats: HLSliceStats
) -> tuple[dict[int, bytes], dict[int, bytes]]:
    l2_payloads: dict[int, bytes] = {}
    fills_payloads: dict[int, bytes] = {}
    with ThreadPoolExecutor(max_workers=HL_FETCH_WORKERS) as executor:
        futures: dict[Any, tuple[str, int]] = {}
        for hour in range(24):
            futures[
                executor.submit(s3_store.get_bytes, raw_hl_l2_s3_key(market, date, hour))
            ] = ("l2", hour)
            futures[
                executor.submit(s3_store.get_bytes, raw_hl_fills_s3_key(date, hour))
            ] = ("fills", hour)
        for future in as_completed(futures):
            kind, hour = futures[future]
            payload = future.result()
            stats.bytes_in += len(payload)
            if kind == "l2":
                l2_payloads[hour] = payload
            else:
                fills_payloads[hour] = payload
    return l2_payloads, fills_payloads


def _translate_hl_payloads(
    *,
    l2_payloads: dict[int, bytes],
    fills_payloads: dict[int, bytes],
    instrument: str,
    depth: int,
    stats: HLSliceStats,
) -> list[dict[str, Any]]:
    rows: list[tuple[int, int, int, dict[str, Any]]] = []
    # priority: trade=0, l2_snapshot=1
    for hour in range(24):
        l2_payload = l2_payloads[hour]
        for snapshot in parse_l2_lz4_payload(l2_payload, source_hour=hour):
            stats.rows_in_l2 += 1
            row = {
                "ts_ms": snapshot.ts_ms,
                "instrument": snapshot.instrument,
                "kind": DataEventKind.L2_SNAPSHOT.value,
                "bids": _parse_hl_levels(snapshot.bids_json, depth=depth),
                "asks": _parse_hl_levels(snapshot.asks_json, depth=depth),
                "delta_levels": None,
                "px": None,
                "sz": None,
                "side": None,
            }
            order_key = (snapshot.ts_ms, 1, snapshot.source_line_number + hour * 1_000_000)
            rows.append((*order_key, row))

        fills_payload = fills_payloads[hour]
        trades = collapse_fill_trades(
            fills_payload, instrument=instrument, source_hour=hour
        )
        for trade in trades:
            stats.rows_in_trades += 1
            row = {
                "ts_ms": trade.ts_ms,
                "instrument": trade.instrument,
                "kind": DataEventKind.TRADE.value,
                "bids": None,
                "asks": None,
                "delta_levels": None,
                "px": float(trade.px),
                "sz": float(trade.sz),
                "side": trade.side,
            }
            order_key = (trade.ts_ms, 0, trade.source_line_number + hour * 1_000_000)
            rows.append((*order_key, row))

    rows.sort(key=lambda item: (item[0], item[1], item[2]))
    return [row for _ts, _prio, _ord, row in rows]


def _parse_hl_levels(json_payload: str, *, depth: int) -> list[dict[str, Any]]:
    try:
        levels = orjson.loads(json_payload)
    except orjson.JSONDecodeError:
        return []
    out: list[dict[str, Any]] = []
    if not isinstance(levels, list):
        return out
    for level in levels[:depth]:
        if not isinstance(level, dict):
            continue
        try:
            px = float(level.get("px"))
            sz = float(level.get("sz"))
            n = int(level.get("n", 0))
        except (TypeError, ValueError):
            continue
        out.append({"px": px, "sz": sz, "n": max(0, n)})
    return out


def _hl_rows_to_table(rows: list[dict[str, Any]]) -> pa.Table:
    out_arrays: dict[str, list] = {field.name: [] for field in DATA_PARQUET_SCHEMA}
    for row in rows:
        for column in out_arrays:
            out_arrays[column].append(row[column])
    return pa.Table.from_pydict(out_arrays, schema=DATA_PARQUET_SCHEMA)


def _write_hl_shard(
    *,
    shard_id: str,
    shard_prefix: str,
    manifest_key: str,
    data_key: str,
    market: MarketRef,
    date: str,
    depth: int,
    data_table: pa.Table,
    source_refs: list[str],
    s3_store: S3Store,
    shard_repo: CanonicalShardRepository,
    coverage_repo: CoverageRepository,
    stats: HLSliceStats,
) -> CanonicalShardRecord:
    if data_table.num_rows == 0:
        ts_min: int | None = None
        ts_max: int | None = None
    else:
        ts_array = data_table.column("ts_ms")
        ts_min = int(pc.min(ts_array).as_py())
        ts_max = int(pc.max(ts_array).as_py())

    with tempfile.TemporaryDirectory(prefix=f"{shard_id}-") as temp_dir_raw:
        temp_dir = Path(temp_dir_raw)
        data_path = temp_dir / CANONICAL_DATA_FILE_NAME
        pq.write_table(
            data_table,
            data_path,
            compression="zstd",
            use_dictionary=["instrument", "kind", "side"],
        )
        data_size = data_path.stat().st_size
        stats.bytes_out = data_size
        s3_store.put_file(
            data_key, str(data_path), content_type="application/vnd.apache.parquet"
        )

    data_file = CanonicalShardFile(
        family=CanonicalFileFamily.DATA,
        file_name=CANONICAL_DATA_FILE_NAME,
        s3_key=data_key,
        row_count=data_table.num_rows,
        size_bytes=data_size,
    )

    now = utc_now_iso()
    record = CanonicalShardRecord(
        shard_id=shard_id,
        status=CanonicalShardStatus.READY,
        venue=Venue.HYPERLIQUID,
        market_type=market.market_type,
        date=date,
        depth=depth,
        shard_prefix=shard_prefix,
        manifest_s3_key=manifest_key,
        instrument=market.instrument,
        data_file=data_file,
        schedule_file=None,
        event_count=data_table.num_rows,
        byte_count=data_size,
        start_ts_ms=ts_min,
        end_ts_ms=ts_max,
        source_refs=tuple(source_refs),
        created_at=now,
        updated_at=now,
    )
    s3_store.put_json(manifest_key, orjson.loads(record.model_dump_json()))
    shard_repo.put(record)

    from .models import CoverageRecord, DatasetKind

    coverage_repo.put(
        CoverageRecord(
            pk=coverage_pk_canonical_hl(market, date),
            dataset_kind=DatasetKind.CANONICAL_HL,
            status=CoverageStatus.READY,
            object_count=1,
            byte_count=data_size,
            row_count=data_table.num_rows,
            updated_at=now,
            source=manifest_key,
            venue=Venue.HYPERLIQUID,
            market_type=market.market_type,
            instrument=market.instrument,
            date=date,
        )
    )
    return record
