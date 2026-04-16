from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import UTC, date as date_cls, datetime
import heapq
import io
import logging
import os
from pathlib import Path
import tempfile
from time import perf_counter
from typing import Any, Callable, Iterator
from urllib.parse import unquote

import orjson
import pyarrow as pa
import pyarrow.parquet as pq
import zstandard

from .models import (
    CanonicalFileFamily,
    CanonicalShardFile,
    CanonicalShardRecord,
    CanonicalShardStatus,
    CoverageRecord,
    CoverageStatus,
    DatasetKind,
    MarketRef,
    MarketType,
    OutcomesMode,
    PolymarketMarketResolution,
    Venue,
    canonical_hyperliquid_manifest_s3_key,
    canonical_hyperliquid_shard_prefix,
    canonical_hyperliquid_shard_id,
    canonical_polymarket_manifest_s3_key,
    canonical_polymarket_shard_prefix,
    canonical_polymarket_shard_id,
    canonical_shard_family_file_name,
    canonical_shard_family_s3_key,
    coverage_pk,
    normalized_l2_s3_key,
    normalized_trade_s3_key,
    polymarket_normalized_l2_s3_key,
    polymarket_normalized_trade_s3_key,
    utc_now_iso,
)
from .storage import CanonicalShardRepository, CoverageRepository, S3Store

logger = logging.getLogger(__name__)


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


def _trade_event(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "Market": {
            "Trade": {
                "instrument": _instrument_payload(
                    venue=row["canonical_venue"],
                    instrument=str(row["instrument"]),
                    market_type=row.get("canonical_market_type"),
                ),
                "ts_ms": int(row["ts_ms"]),
                "px": float(row["px"]),
                "sz": float(row["sz"]),
                "side": row["side"],
            }
        }
    }


def _snapshot_event(row: dict[str, Any], *, depth: int) -> dict[str, Any]:
    def decode_levels(raw_levels: str) -> list[dict[str, Any]]:
        return [
            {
                "px": float(level["px"]),
                "sz": float(level["sz"]),
                "level_count": int(level.get("n", 0)),
            }
            for level in orjson.loads(raw_levels)[:depth]
        ]

    return {
        "Market": {
            "L2Snapshot": {
                "instrument": _instrument_payload(
                    venue=row["canonical_venue"],
                    instrument=str(row["instrument"]),
                    market_type=row.get("canonical_market_type"),
                ),
                "ts_ms": int(row["ts_ms"]),
                "bids": decode_levels(row["bids_json"]),
                "asks": decode_levels(row["asks_json"]),
            }
        }
    }


def _coverage_record(
    coverage_repo: CoverageRepository,
    dataset_kind: DatasetKind,
    market: MarketRef,
    date: str,
    hour: str,
) -> CoverageRecord | None:
    record = coverage_repo.get(coverage_pk(dataset_kind, market, date, hour))
    if record is None or record.status != CoverageStatus.READY:
        return None
    return record


def _iter_parquet_rows(
    parquet_bytes: bytes,
    *,
    extra_fields: dict[str, Any],
    filter_fn: Callable[[dict[str, Any]], bool] | None = None,
) -> Iterator[dict[str, Any]]:
    parquet = pq.ParquetFile(io.BytesIO(parquet_bytes))
    for batch in parquet.iter_batches(batch_size=4096):
        for row in batch.to_pylist():
            if filter_fn is not None and not filter_fn(row):
                continue
            yield {**row, **extra_fields}


def _trade_schema() -> pa.Schema:
    return pa.schema(
        [
            ("event_seq", pa.int64()),
            ("ts_ms", pa.int64()),
            ("instrument", pa.string()),
            ("side", pa.string()),
            ("px", pa.float64()),
            ("sz", pa.float64()),
        ]
    )


def _book_schema(depth: int) -> pa.Schema:
    fields: list[pa.Field] = [
        pa.field("event_seq", pa.int64()),
        pa.field("ts_ms", pa.int64()),
        pa.field("instrument", pa.string()),
    ]
    for side in ("bid", "ask"):
        for index in range(depth):
            fields.extend(
                [
                    pa.field(f"{side}_px_{index}", pa.float64()),
                    pa.field(f"{side}_sz_{index}", pa.float64()),
                    pa.field(f"{side}_level_count_{index}", pa.int32()),
                ]
            )
    return pa.schema(fields)


def _contract_schema() -> pa.Schema:
    return pa.schema(
        [
            ("event_seq", pa.int64()),
            ("ts_ms", pa.int64()),
            ("kind", pa.string()),
            ("series_key", pa.string()),
            ("slug", pa.string()),
            ("market_id", pa.string()),
            ("start_ts_ms", pa.int64()),
            ("end_ts_ms", pa.int64()),
            ("price_to_beat", pa.float64()),
            ("price_to_beat_source", pa.string()),
            ("price_to_beat_quality", pa.string()),
            ("outcome_0", pa.string()),
            ("outcome_0_asset_id", pa.string()),
            ("outcome_0_instrument", pa.string()),
            ("outcome_0_settlement_payout", pa.float64()),
            ("outcome_1", pa.string()),
            ("outcome_1_asset_id", pa.string()),
            ("outcome_1_instrument", pa.string()),
            ("outcome_1_settlement_payout", pa.float64()),
        ]
    )


def _decode_book_levels(raw_levels: str, *, depth: int) -> list[dict[str, Any]]:
    levels = orjson.loads(raw_levels)
    if not isinstance(levels, list):
        return []
    return [
        {
            "px": float(level["px"]),
            "sz": float(level["sz"]),
            "level_count": int(level.get("n", 0)),
        }
        for level in levels[:depth]
    ]


def _book_row(row: dict[str, Any], *, event_seq: int, depth: int) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "event_seq": event_seq,
        "ts_ms": int(row["ts_ms"]),
        "instrument": str(row["instrument"]),
    }
    bids = _decode_book_levels(row["bids_json"], depth=depth)
    asks = _decode_book_levels(row["asks_json"], depth=depth)
    for side, levels in (("bid", bids), ("ask", asks)):
        for index in range(depth):
            level = levels[index] if index < len(levels) else None
            payload[f"{side}_px_{index}"] = None if level is None else level["px"]
            payload[f"{side}_sz_{index}"] = None if level is None else level["sz"]
            payload[f"{side}_level_count_{index}"] = (
                None if level is None else level["level_count"]
            )
    return payload


def _trade_row(row: dict[str, Any], *, event_seq: int) -> dict[str, Any]:
    return {
        "event_seq": event_seq,
        "ts_ms": int(row["ts_ms"]),
        "instrument": str(row["instrument"]),
        "side": str(row["side"]),
        "px": float(row["px"]),
        "sz": float(row["sz"]),
    }


def _contract_row(event: dict[str, Any], *, event_seq: int) -> dict[str, Any]:
    payload = event["Contract"]["Polymarket"]
    outcomes = payload["outcomes"]
    first = outcomes[0]
    second = outcomes[1] if len(outcomes) > 1 else None
    return {
        "event_seq": event_seq,
        "ts_ms": int(payload["ts_ms"]),
        "kind": str(payload["kind"]),
        "series_key": str(payload["series_key"]),
        "slug": str(payload["slug"]),
        "market_id": str(payload["market_id"]),
        "start_ts_ms": int(payload["start_ts_ms"]),
        "end_ts_ms": int(payload["end_ts_ms"]),
        "price_to_beat": payload["price_to_beat"],
        "price_to_beat_source": payload["price_to_beat_source"],
        "price_to_beat_quality": payload["price_to_beat_quality"],
        "outcome_0": str(first["outcome"]),
        "outcome_0_asset_id": str(first["asset_id"]),
        "outcome_0_instrument": str(first["instrument"]["Polymarket"]["symbol"]),
        "outcome_0_settlement_payout": first.get("settlement_payout"),
        "outcome_1": None if second is None else str(second["outcome"]),
        "outcome_1_asset_id": None if second is None else str(second["asset_id"]),
        "outcome_1_instrument": (
            None
            if second is None
            else str(second["instrument"]["Polymarket"]["symbol"])
        ),
        "outcome_1_settlement_payout": (
            None if second is None else second.get("settlement_payout")
        ),
    }


class _ParquetFamilyWriter:
    def __init__(self, *, path: Path, schema: pa.Schema):
        self.path = path
        self.schema = schema
        self.rows: list[dict[str, Any]] = []
        self.row_count = 0
        self.writer: pq.ParquetWriter | None = None

    def write(self, row: dict[str, Any]) -> None:
        self.rows.append(row)
        if len(self.rows) >= 4096:
            self.flush()

    def flush(self) -> None:
        if not self.rows:
            return
        if self.writer is None:
            self.writer = pq.ParquetWriter(
                self.path,
                self.schema,
                compression="snappy",
                use_dictionary=False,
            )
        table = pa.Table.from_pylist(self.rows, schema=self.schema)
        self.writer.write_table(table)
        self.row_count += len(self.rows)
        self.rows.clear()

    def close(self) -> None:
        if self.writer is None:
            self.writer = pq.ParquetWriter(
                self.path,
                self.schema,
                compression="snappy",
                use_dictionary=False,
            )
        self.flush()
        self.writer.close()
        self.writer = None


class _CanonicalShardParquetWriter:
    def __init__(
        self,
        *,
        temp_dir: Path,
        shard_prefix: str,
        depth: int,
        families: tuple[CanonicalFileFamily, ...],
    ):
        self.shard_prefix = shard_prefix
        self.families = families
        self.writers: dict[CanonicalFileFamily, _ParquetFamilyWriter] = {}
        for family in families:
            path = temp_dir / canonical_shard_family_file_name(family)
            if family == CanonicalFileFamily.TRADES:
                schema = _trade_schema()
            elif family == CanonicalFileFamily.BOOKS:
                schema = _book_schema(depth)
            elif family == CanonicalFileFamily.CONTRACTS:
                schema = _contract_schema()
            else:
                raise ValueError(f"unsupported canonical family: {family}")
            self.writers[family] = _ParquetFamilyWriter(path=path, schema=schema)

    def write_trade(self, row: dict[str, Any], *, event_seq: int) -> None:
        self.writers[CanonicalFileFamily.TRADES].write(_trade_row(row, event_seq=event_seq))

    def write_book(self, row: dict[str, Any], *, event_seq: int, depth: int) -> None:
        self.writers[CanonicalFileFamily.BOOKS].write(
            _book_row(row, event_seq=event_seq, depth=depth)
        )

    def write_contract(self, event: dict[str, Any], *, event_seq: int) -> None:
        if CanonicalFileFamily.CONTRACTS not in self.writers:
            raise ValueError("contract writer is not configured for this shard")
        self.writers[CanonicalFileFamily.CONTRACTS].write(
            _contract_row(event, event_seq=event_seq)
        )

    def close(self) -> tuple[CanonicalShardFile, ...]:
        files: list[CanonicalShardFile] = []
        for family in self.families:
            writer = self.writers[family]
            writer.close()
            file_name = canonical_shard_family_file_name(family)
            files.append(
                CanonicalShardFile(
                    family=family,
                    file_name=file_name,
                    s3_key=canonical_shard_family_s3_key(self.shard_prefix, family),
                    row_count=writer.row_count,
                    size_bytes=os.path.getsize(writer.path),
                )
            )
        return tuple(files)


@dataclass
class _MergeStream:
    iterator: Iterator[dict[str, Any]]
    kind: str
    source_order: int


@dataclass
class _MergedRow:
    kind: str
    source_order: int
    row: dict[str, Any]


@dataclass(frozen=True)
class _PolymarketResolutionSource:
    resolution: PolymarketMarketResolution
    trade_key: str | None
    l2_key: str | None


@dataclass(frozen=True)
class _PolymarketWindow:
    current: _PolymarketContract
    next_contract: _PolymarketContract | None
    start_ts_ms: int
    end_ts_exclusive_ms: int
    sources: tuple[_PolymarketResolutionSource, ...]


@dataclass
class _PolymarketBuildStats:
    contracts_discovered: int = 0
    windows_planned: int = 0
    objects_fetched: int = 0
    bytes_fetched: int = 0
    rows_emitted: int = 0
    fetch_seconds: float = 0.0
    emit_seconds: float = 0.0


@dataclass(frozen=True)
class PolymarketCanonicalValidationSummary:
    path: str
    total_rows: int
    market_rows: int
    contract_rows: int
    timestamps_seen: int
    market_timestamps_seen: int
    contract_timestamps_seen: int
    max_market_slugs_per_ts: int
    max_contract_slugs_per_ts: int
    final_current_slug: str | None
    final_next_slug: str | None


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
                                venue=Venue.POLYMARKET,
                                instrument=item.instrument,
                            ),
                            "settlement_payout": item.settlement_payout,
                        }
                        for item in self.outcomes
                    ],
                }
            }
        }


class _PolymarketContractSchedule:
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
            if next_contract is not None and (self.next is None or self.next.slug != next_contract.slug):
                events.append(next_contract.event("ListedNext", ts_ms=ts_ms))
        elif (
            (self.next is None and next_contract is not None)
            or (self.next is not None and next_contract is not None and self.next.slug != next_contract.slug)
        ):
            events.append(next_contract.event("ListedNext", ts_ms=ts_ms))

        self.current = current
        self.next = next_contract
        return events


def _group_polymarket_contracts(
    resolutions: list[PolymarketMarketResolution],
) -> list[_PolymarketContract]:
    grouped: dict[str, list[PolymarketMarketResolution]] = {}
    for resolution in sorted(resolutions, key=lambda item: (item.start_ts_ms, item.slug, item.outcome)):
        grouped.setdefault(resolution.slug, []).append(resolution)

    contracts: list[_PolymarketContract] = []
    for slug, items in sorted(grouped.items(), key=lambda item: (item[1][0].start_ts_ms, item[0])):
        first = items[0]
        contracts.append(
            _PolymarketContract(
                series_key=first.series_key,
                slug=slug,
                market_id=first.market_id,
                start_ts_ms=first.start_ts_ms,
                end_ts_ms=first.end_ts_ms,
                price_to_beat=first.price_to_beat,
                price_to_beat_source=first.price_to_beat_source,
                price_to_beat_quality=first.price_to_beat_quality,
                outcomes=tuple(
                    _PolymarketContractOutcome(
                        outcome=item.outcome,
                        asset_id=item.asset_id,
                        instrument=item.instrument,
                        settlement_payout=item.settlement_payout,
                    )
                    for item in sorted(items, key=lambda item: item.outcome)
                ),
            )
        )
    return contracts


def _polymarket_day_bounds_ms(date: str) -> tuple[int, int]:
    day = date_cls.fromisoformat(date)
    start = datetime(day.year, day.month, day.day, tzinfo=UTC)
    end = datetime.fromtimestamp(start.timestamp(), tz=UTC).replace(hour=0, minute=0, second=0, microsecond=0)
    start_ts_ms = int(start.timestamp() * 1000)
    return start_ts_ms, start_ts_ms + 86_400_000


def _sort_resolution_sources(
    sources: list[_PolymarketResolutionSource],
) -> list[_PolymarketResolutionSource]:
    return sorted(
        sources,
        key=lambda item: (
            item.resolution.start_ts_ms,
            item.resolution.slug,
            item.resolution.outcome,
        ),
    )


def _plan_polymarket_windows(
    *,
    date: str,
    schedule: _PolymarketContractSchedule,
    sources_by_slug: dict[str, list[_PolymarketResolutionSource]],
) -> list[_PolymarketWindow]:
    day_start_ts_ms, day_end_ts_exclusive_ms = _polymarket_day_bounds_ms(date)
    windows: list[_PolymarketWindow] = []

    for current in schedule.contracts:
        next_contract = schedule.by_start.get(current.start_ts_ms + schedule.interval_ms)
        window_start_ts_ms = max(day_start_ts_ms, current.start_ts_ms)
        natural_end_ts_exclusive_ms = (
            next_contract.start_ts_ms if next_contract is not None else current.end_ts_ms + 1
        )
        window_end_ts_exclusive_ms = min(day_end_ts_exclusive_ms, natural_end_ts_exclusive_ms)
        if window_start_ts_ms >= window_end_ts_exclusive_ms:
            continue

        selected_sources = _sort_resolution_sources(sources_by_slug.get(current.slug, []))
        if next_contract is not None:
            selected_sources.extend(_sort_resolution_sources(sources_by_slug.get(next_contract.slug, [])))

        windows.append(
            _PolymarketWindow(
                current=current,
                next_contract=next_contract,
                start_ts_ms=window_start_ts_ms,
                end_ts_exclusive_ms=window_end_ts_exclusive_ms,
                sources=tuple(selected_sources),
            )
        )

    return windows


def _stream_sort_key(row: dict[str, Any], kind: str, source_order: int) -> tuple[int, int, int, int]:
    priority = 0 if kind == "trade" else 1
    return (
        int(row["ts_ms"]),
        priority,
        source_order,
        int(row["source_line_number"]),
    )


def _merge_sorted_streams(streams: list[_MergeStream]) -> Iterator[_MergedRow]:
    heap: list[tuple[tuple[int, int, int, int], int, dict[str, Any]]] = []
    for index, stream in enumerate(streams):
        try:
            row = next(stream.iterator)
        except StopIteration:
            continue
        heapq.heappush(heap, (_stream_sort_key(row, stream.kind, stream.source_order), index, row))

    while heap:
        _, stream_index, row = heapq.heappop(heap)
        stream = streams[stream_index]
        yield _MergedRow(kind=stream.kind, source_order=stream.source_order, row=row)
        try:
            next_row = next(stream.iterator)
        except StopIteration:
            continue
        heapq.heappush(
            heap,
            (_stream_sort_key(next_row, stream.kind, stream.source_order), stream_index, next_row),
        )


def _open_jsonl_reader(path: str | Path) -> Iterator[str]:
    file_path = Path(path)
    with file_path.open("rb") as raw_file:
        if file_path.suffix == ".zst":
            with zstandard.ZstdDecompressor().stream_reader(raw_file) as reader:
                with io.TextIOWrapper(reader, encoding="utf-8") as text_reader:
                    yield from text_reader
            return
        with io.TextIOWrapper(raw_file, encoding="utf-8") as text_reader:
            yield from text_reader


def _polymarket_slug_start_secs(slug: str) -> int:
    try:
        _, timestamp = slug.rsplit("-", 1)
        return int(timestamp)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"invalid polymarket slug: {slug}") from exc


def _validate_polymarket_slug_sequence(
    *,
    ts_ms: int,
    slugs: set[str],
    limit: int,
    context: str,
) -> None:
    if len(slugs) > limit:
        ordered = sorted(slugs, key=_polymarket_slug_start_secs)
        raise ValueError(
            f"{context} at ts_ms={ts_ms} exposed {len(slugs)} slugs; expected at most {limit}: {ordered}"
        )
    ordered_starts = sorted(_polymarket_slug_start_secs(slug) for slug in slugs)
    for previous, current in zip(ordered_starts, ordered_starts[1:]):
        if current - previous != 300:
            raise ValueError(
                f"{context} at ts_ms={ts_ms} exposed non-adjacent slugs: {sorted(slugs)}"
            )


def validate_polymarket_current_next_file(
    path: str | Path,
) -> PolymarketCanonicalValidationSummary:
    current_slug: str | None = None
    next_slug: str | None = None
    current_ts_ms: int | None = None
    timestamps_seen = 0
    market_timestamps_seen = 0
    contract_timestamps_seen = 0
    total_rows = 0
    market_rows = 0
    contract_rows = 0
    max_market_slugs_per_ts = 0
    max_contract_slugs_per_ts = 0
    market_slugs_at_ts: set[str] = set()
    contract_slugs_at_ts: set[str] = set()
    saw_market_at_ts = False
    saw_contract_at_ts = False

    def flush_timestamp() -> None:
        nonlocal market_timestamps_seen, contract_timestamps_seen
        nonlocal max_market_slugs_per_ts, max_contract_slugs_per_ts
        nonlocal market_slugs_at_ts, contract_slugs_at_ts, saw_market_at_ts, saw_contract_at_ts
        nonlocal current_ts_ms
        if current_ts_ms is None:
            return
        _validate_polymarket_slug_sequence(
            ts_ms=current_ts_ms,
            slugs=market_slugs_at_ts,
            limit=2,
            context="market rows",
        )
        _validate_polymarket_slug_sequence(
            ts_ms=current_ts_ms,
            slugs=contract_slugs_at_ts,
            limit=3,
            context="contract rows",
        )
        if saw_market_at_ts:
            market_timestamps_seen += 1
            max_market_slugs_per_ts = max(max_market_slugs_per_ts, len(market_slugs_at_ts))
        if saw_contract_at_ts:
            contract_timestamps_seen += 1
            max_contract_slugs_per_ts = max(max_contract_slugs_per_ts, len(contract_slugs_at_ts))
        market_slugs_at_ts = set()
        contract_slugs_at_ts = set()
        saw_market_at_ts = False
        saw_contract_at_ts = False

    for line in _open_jsonl_reader(path):
        line = line.strip()
        if not line:
            continue
        payload = orjson.loads(line)
        total_rows += 1
        if "Contract" in payload and "Polymarket" in payload["Contract"]:
            contract = payload["Contract"]["Polymarket"]
            ts_ms = int(contract["ts_ms"])
            if current_ts_ms is None:
                current_ts_ms = ts_ms
                timestamps_seen += 1
            elif ts_ms != current_ts_ms:
                flush_timestamp()
                current_ts_ms = ts_ms
                timestamps_seen += 1

            saw_contract_at_ts = True
            contract_rows += 1
            slug = str(contract["slug"])
            kind = str(contract["kind"])
            contract_slugs_at_ts.add(slug)
            if kind == "ListedCurrent":
                current_slug = slug
            elif kind == "ListedNext":
                next_slug = slug
            elif kind == "Resolved":
                if current_slug is not None and slug != current_slug:
                    raise ValueError(
                        f"resolved slug mismatch at ts_ms={ts_ms}: expected current={current_slug}, got={slug}"
                    )
            elif kind == "Activated":
                if next_slug is not None and slug != next_slug:
                    raise ValueError(
                        f"activated slug mismatch at ts_ms={ts_ms}: expected next={next_slug}, got={slug}"
                    )
                current_slug = slug
                next_slug = None
            else:
                raise ValueError(f"unsupported contract kind at ts_ms={ts_ms}: {kind}")
            continue

        market = payload.get("Market")
        if not isinstance(market, dict):
            continue
        event = next(iter(market.values()))
        instrument = event.get("instrument")
        if not isinstance(instrument, dict) or "Polymarket" not in instrument:
            continue
        symbol = instrument["Polymarket"].get("symbol")
        if not isinstance(symbol, str):
            continue
        slug, separator, _ = symbol.rpartition(":")
        if not separator:
            raise ValueError(f"invalid polymarket instrument symbol: {symbol}")
        ts_ms = int(event["ts_ms"])
        if current_ts_ms is None:
            current_ts_ms = ts_ms
            timestamps_seen += 1
        elif ts_ms != current_ts_ms:
            flush_timestamp()
            current_ts_ms = ts_ms
            timestamps_seen += 1

        if current_slug is None:
            raise ValueError(f"market row encountered before ListedCurrent at ts_ms={ts_ms}: {symbol}")
        allowed_slugs = {current_slug}
        if next_slug is not None:
            allowed_slugs.add(next_slug)
        if slug not in allowed_slugs:
            raise ValueError(
                f"market row at ts_ms={ts_ms} referenced {slug}; active current/next={sorted(allowed_slugs)}"
            )
        saw_market_at_ts = True
        market_rows += 1
        market_slugs_at_ts.add(slug)

    flush_timestamp()
    return PolymarketCanonicalValidationSummary(
        path=str(path),
        total_rows=total_rows,
        market_rows=market_rows,
        contract_rows=contract_rows,
        timestamps_seen=timestamps_seen,
        market_timestamps_seen=market_timestamps_seen,
        contract_timestamps_seen=contract_timestamps_seen,
        max_market_slugs_per_ts=max_market_slugs_per_ts,
        max_contract_slugs_per_ts=max_contract_slugs_per_ts,
        final_current_slug=current_slug,
        final_next_slug=next_slug,
    )


def _collect_polymarket_source_refs(windows: list[_PolymarketWindow]) -> list[str]:
    seen: set[str] = set()
    refs: list[str] = []
    for window in windows:
        for source in window.sources:
            for key in (source.trade_key, source.l2_key):
                if key is None or key in seen:
                    continue
                seen.add(key)
                refs.append(key)
    return refs


def _prefetch_polymarket_payloads(
    *,
    keys: list[str],
    s3_store: S3Store,
    payload_cache: dict[str, bytes],
    stats: _PolymarketBuildStats,
) -> None:
    missing_keys = [key for key in keys if key not in payload_cache]
    if not missing_keys:
        return

    fetch_started_at = perf_counter()
    with ThreadPoolExecutor(max_workers=min(8, len(missing_keys))) as executor:
        future_map = {executor.submit(s3_store.get_bytes, key): key for key in missing_keys}
        for future in as_completed(future_map):
            key = future_map[future]
            payload = future.result()
            payload_cache[key] = payload
            stats.objects_fetched += 1
            stats.bytes_fetched += len(payload)
    stats.fetch_seconds += perf_counter() - fetch_started_at


def _iter_polymarket_window_rows(
    *,
    windows: list[_PolymarketWindow],
    s3_store: S3Store,
    stats: _PolymarketBuildStats,
) -> Iterator[_MergedRow]:
    payload_cache: dict[str, bytes] = {}

    for window in windows:
        stream_specs: list[tuple[str, str]] = []
        for source in window.sources:
            if source.trade_key is not None:
                stream_specs.append(("trade", source.trade_key))
            if source.l2_key is not None:
                stream_specs.append(("l2", source.l2_key))
        if not stream_specs:
            continue

        ordered_keys = list(dict.fromkeys(key for _, key in stream_specs))
        _prefetch_polymarket_payloads(
            keys=ordered_keys,
            s3_store=s3_store,
            payload_cache=payload_cache,
            stats=stats,
        )

        streams: list[_MergeStream] = []
        for source_order, (kind, key) in enumerate(stream_specs):
            start_ts_ms = window.start_ts_ms
            end_ts_exclusive_ms = window.end_ts_exclusive_ms

            def within_window(
                row: dict[str, Any],
                *,
                start_ts_ms: int = start_ts_ms,
                end_ts_exclusive_ms: int = end_ts_exclusive_ms,
            ) -> bool:
                ts_ms = int(row["ts_ms"])
                return start_ts_ms <= ts_ms < end_ts_exclusive_ms

            streams.append(
                _MergeStream(
                    iterator=_iter_parquet_rows(
                        payload_cache[key],
                        extra_fields={"canonical_venue": Venue.POLYMARKET.value},
                        filter_fn=within_window,
                    ),
                    kind=kind,
                    source_order=source_order,
                )
            )

        yield from _merge_sorted_streams(streams)


def _canonical_shard_exists(
    existing: CanonicalShardRecord | None,
    *,
    s3_store: S3Store,
) -> bool:
    if existing is None or existing.status != CanonicalShardStatus.READY:
        return False
    if not s3_store.exists(existing.manifest_s3_key):
        return False
    if not existing.files:
        return False
    return all(s3_store.exists(file.s3_key) for file in existing.files)


def _build_polymarket_shard(
    *,
    shard_id: str,
    shard_prefix: str,
    manifest_key: str,
    date: str,
    series_key: str,
    outcomes: OutcomesMode,
    depth: int,
    contracts: list[_PolymarketContract],
    windows: list[_PolymarketWindow],
    s3_store: S3Store,
    shard_repo: CanonicalShardRepository,
    force: bool = False,
) -> CanonicalShardRecord:
    stats = _PolymarketBuildStats(
        contracts_discovered=len(contracts),
        windows_planned=len(windows),
    )
    source_refs = _collect_polymarket_source_refs(windows)
    total_started_at = perf_counter()
    logger.info(
        "building polymarket canonical shard series=%s date=%s contracts=%d windows=%d sources=%d depth=%d",
        series_key,
        date,
        stats.contracts_discovered,
        stats.windows_planned,
        len(source_refs),
        depth,
    )
    record = _materialize_polymarket_shard(
        shard_id=shard_id,
        shard_prefix=shard_prefix,
        manifest_key=manifest_key,
        series_key=series_key,
        outcomes=outcomes.value,
        date=date,
        depth=depth,
        contracts=contracts,
        merged_rows=_iter_polymarket_window_rows(
            windows=windows,
            s3_store=s3_store,
            stats=stats,
        ),
        s3_store=s3_store,
        shard_repo=shard_repo,
        source_refs=source_refs,
        force=force,
        stats=stats,
    )
    logger.info(
        "completed polymarket canonical shard series=%s date=%s contracts=%d windows=%d fetched_objects=%d fetched_bytes=%d emitted_rows=%d fetch_seconds=%.3f emit_seconds=%.3f total_seconds=%.3f",
        series_key,
        date,
        stats.contracts_discovered,
        stats.windows_planned,
        stats.objects_fetched,
        stats.bytes_fetched,
        stats.rows_emitted,
        stats.fetch_seconds,
        stats.emit_seconds,
        perf_counter() - total_started_at,
    )
    return record


def _materialize_polymarket_shard(
    *,
    shard_id: str,
    shard_prefix: str,
    manifest_key: str,
    date: str,
    series_key: str,
    outcomes: str,
    depth: int,
    contracts: list[_PolymarketContract],
    merged_rows: Iterator[_MergedRow],
    s3_store: S3Store,
    shard_repo: CanonicalShardRepository,
    source_refs: list[str],
    force: bool = False,
    stats: _PolymarketBuildStats | None = None,
) -> CanonicalShardRecord:
    existing = shard_repo.get(shard_id)
    if not force and _canonical_shard_exists(existing, s3_store=s3_store):
        return existing

    event_count = 0
    start_ts_ms: int | None = None
    end_ts_ms: int | None = None
    schedule = _PolymarketContractSchedule(contracts)
    emit_started_at = perf_counter()
    with tempfile.TemporaryDirectory(prefix=f"{shard_id}-") as temp_dir_raw:
        temp_dir = Path(temp_dir_raw)
        writers = _CanonicalShardParquetWriter(
            temp_dir=temp_dir,
            shard_prefix=shard_prefix,
            depth=depth,
            families=(
                CanonicalFileFamily.CONTRACTS,
                CanonicalFileFamily.TRADES,
                CanonicalFileFamily.BOOKS,
            ),
        )
        event_seq = 0

        for merged in merged_rows:
            row = merged.row
            ts_ms = int(row["ts_ms"])
            for event in schedule.lifecycle_events(ts_ms):
                if start_ts_ms is None:
                    start_ts_ms = ts_ms
                end_ts_ms = ts_ms
                writers.write_contract(event, event_seq=event_seq)
                event_seq += 1
                event_count += 1
                if stats is not None:
                    stats.rows_emitted += 1

            if start_ts_ms is None:
                start_ts_ms = ts_ms
            end_ts_ms = ts_ms
            if merged.kind == "trade":
                writers.write_trade(row, event_seq=event_seq)
            else:
                writers.write_book(row, event_seq=event_seq, depth=depth)
            event_seq += 1
            event_count += 1
            if stats is not None:
                stats.rows_emitted += 1

        if event_count == 0 and contracts:
            ts_ms = contracts[0].start_ts_ms
            for event in schedule.lifecycle_events(ts_ms):
                if start_ts_ms is None:
                    start_ts_ms = ts_ms
                end_ts_ms = ts_ms
                writers.write_contract(event, event_seq=event_seq)
                event_seq += 1
                event_count += 1
                if stats is not None:
                    stats.rows_emitted += 1

        files = writers.close()
        for file in files:
            s3_store.put_file(
                file.s3_key,
                str(temp_dir / file.file_name),
                content_type="application/vnd.apache.parquet",
            )
    if stats is not None:
        stats.emit_seconds += perf_counter() - emit_started_at

    created_at = utc_now_iso()
    record = CanonicalShardRecord(
        shard_id=shard_id,
        status=CanonicalShardStatus.READY,
        venue=Venue.POLYMARKET,
        market_type=MarketType.BINARY,
        instrument=None,
        series_key=series_key,
        outcomes=outcomes,
        date=date,
        depth=depth,
        shard_prefix=shard_prefix,
        manifest_s3_key=manifest_key,
        event_count=event_count,
        start_ts_ms=start_ts_ms,
        end_ts_ms=end_ts_ms,
        created_at=created_at,
        updated_at=created_at,
        source_refs=tuple(source_refs),
        files=files,
    )
    s3_store.put_json(manifest_key, record.model_dump(mode="json"))
    shard_repo.put(record)
    return record


def _materialize_shard(
    *,
    shard_id: str,
    shard_prefix: str,
    manifest_key: str,
    venue: Venue,
    market_type,
    date: str,
    depth: int,
    instrument: str | None,
    series_key: str | None,
    outcomes: str | None,
    merged_rows: Iterator[_MergedRow],
    s3_store: S3Store,
    shard_repo: CanonicalShardRepository,
    source_refs: list[str],
    force: bool = False,
) -> CanonicalShardRecord:
    existing = shard_repo.get(shard_id)
    if not force and _canonical_shard_exists(existing, s3_store=s3_store):
        return existing

    event_count = 0
    start_ts_ms: int | None = None
    end_ts_ms: int | None = None
    with tempfile.TemporaryDirectory(prefix=f"{shard_id}-") as temp_dir_raw:
        temp_dir = Path(temp_dir_raw)
        writers = _CanonicalShardParquetWriter(
            temp_dir=temp_dir,
            shard_prefix=shard_prefix,
            depth=depth,
            families=(
                CanonicalFileFamily.TRADES,
                CanonicalFileFamily.BOOKS,
            ),
        )
        event_seq = 0
        for merged in merged_rows:
            row = merged.row
            ts_ms = int(row["ts_ms"])
            if start_ts_ms is None:
                start_ts_ms = ts_ms
            end_ts_ms = ts_ms
            if merged.kind == "trade":
                writers.write_trade(row, event_seq=event_seq)
            else:
                writers.write_book(row, event_seq=event_seq, depth=depth)
            event_seq += 1
            event_count += 1
        files = writers.close()
        for file in files:
            s3_store.put_file(
                file.s3_key,
                str(temp_dir / file.file_name),
                content_type="application/vnd.apache.parquet",
            )

    created_at = utc_now_iso()
    record = CanonicalShardRecord(
        shard_id=shard_id,
        status=CanonicalShardStatus.READY,
        venue=venue,
        market_type=market_type,
        instrument=instrument,
        series_key=series_key,
        outcomes=outcomes,
        date=date,
        depth=depth,
        shard_prefix=shard_prefix,
        manifest_s3_key=manifest_key,
        event_count=event_count,
        start_ts_ms=start_ts_ms,
        end_ts_ms=end_ts_ms,
        created_at=created_at,
        updated_at=created_at,
        source_refs=tuple(source_refs),
        files=files,
    )
    s3_store.put_json(manifest_key, record.model_dump(mode="json"))
    shard_repo.put(record)
    return record


def _build_shard(
    *,
    shard_id: str,
    shard_prefix: str,
    manifest_key: str,
    venue: Venue,
    market_type,
    date: str,
    depth: int,
    instrument: str | None,
    series_key: str | None,
    outcomes: str | None,
    streams: list[_MergeStream],
    s3_store: S3Store,
    shard_repo: CanonicalShardRepository,
    source_refs: list[str],
    force: bool = False,
) -> CanonicalShardRecord:
    return _materialize_shard(
        shard_id=shard_id,
        shard_prefix=shard_prefix,
        manifest_key=manifest_key,
        venue=venue,
        market_type=market_type,
        date=date,
        depth=depth,
        instrument=instrument,
        series_key=series_key,
        outcomes=outcomes,
        merged_rows=_merge_sorted_streams(streams),
        s3_store=s3_store,
        shard_repo=shard_repo,
        source_refs=source_refs,
        force=force,
    )


def build_hyperliquid_canonical_day(
    *,
    market: MarketRef,
    date: str,
    depth: int,
    s3_store: S3Store,
    coverage_repo: CoverageRepository,
    shard_repo: CanonicalShardRepository,
    force: bool = False,
) -> CanonicalShardRecord:
    l2_daily = _coverage_record(coverage_repo, DatasetKind.NORMALIZED_L2, market, date, "daily")
    if l2_daily is None:
        raise ValueError(f"normalized L2 coverage is not ready for {market.instrument} {date}")
    trade_daily = _coverage_record(coverage_repo, DatasetKind.NORMALIZED_TRADES, market, date, "daily")
    if trade_daily is None:
        raise ValueError(f"normalized trade coverage is not ready for {market.instrument} {date}")

    shard_id = canonical_hyperliquid_shard_id(market, date, depth)
    shard_prefix = canonical_hyperliquid_shard_prefix(market, date, depth)
    manifest_key = canonical_hyperliquid_manifest_s3_key(market, date, depth)
    streams: list[_MergeStream] = []
    source_refs: list[str] = []

    for hour in range(24):
        trade_key = normalized_trade_s3_key(market, date, hour)
        l2_key = normalized_l2_s3_key(market, date, hour)
        trade_hour = _coverage_record(coverage_repo, DatasetKind.NORMALIZED_TRADES, market, date, f"{hour:02d}")
        l2_hour = _coverage_record(coverage_repo, DatasetKind.NORMALIZED_L2, market, date, f"{hour:02d}")
        if (trade_hour is not None and trade_hour.row_count > 0) or (
            trade_hour is None and s3_store.exists(trade_key)
        ):
            source_refs.append(trade_key)
            streams.append(
                _MergeStream(
                    iterator=_iter_parquet_rows(
                        s3_store.get_bytes(trade_key),
                        extra_fields={
                            "canonical_venue": market.venue.value,
                            "canonical_market_type": market.market_type.value,
                        },
                    ),
                    kind="trade",
                    source_order=hour * 2,
                )
            )
        if (l2_hour is not None and l2_hour.row_count > 0) or (
            l2_hour is None and s3_store.exists(l2_key)
        ):
            source_refs.append(l2_key)
            streams.append(
                _MergeStream(
                    iterator=_iter_parquet_rows(
                        s3_store.get_bytes(l2_key),
                        extra_fields={
                            "canonical_venue": market.venue.value,
                            "canonical_market_type": market.market_type.value,
                        },
                    ),
                    kind="l2",
                    source_order=hour * 2 + 1,
                )
            )

    return _build_shard(
        shard_id=shard_id,
        shard_prefix=shard_prefix,
        manifest_key=manifest_key,
        venue=market.venue,
        market_type=market.market_type,
        instrument=market.instrument,
        series_key=None,
        outcomes=None,
        date=date,
        depth=depth,
        streams=streams,
        s3_store=s3_store,
        shard_repo=shard_repo,
        source_refs=source_refs,
        force=force,
    )


def build_polymarket_canonical_day(
    *,
    date: str,
    series_key: str,
    outcomes: OutcomesMode,
    depth: int,
    resolutions: list[PolymarketMarketResolution],
    s3_store: S3Store,
    coverage_repo: CoverageRepository,
    shard_repo: CanonicalShardRepository,
    force: bool = False,
) -> CanonicalShardRecord:
    active_resolutions = [resolution for resolution in resolutions if date in resolution.dates]
    if not active_resolutions:
        raise ValueError(f"no polymarket markets were discovered for {series_key} on {date}")

    shard_id = canonical_polymarket_shard_id(
        series_key=series_key,
        date=date,
        outcomes=outcomes,
        depth=depth,
    )
    shard_prefix = canonical_polymarket_shard_prefix(
        series_key=series_key,
        date=date,
        outcomes=outcomes,
        depth=depth,
    )
    manifest_key = canonical_polymarket_manifest_s3_key(
        series_key=series_key,
        date=date,
        outcomes=outcomes,
        depth=depth,
    )
    contracts = _group_polymarket_contracts(active_resolutions)
    schedule = _PolymarketContractSchedule(contracts)
    sources = _polymarket_sources_from_resolutions(
        date=date,
        resolutions=active_resolutions,
        coverage_repo=coverage_repo,
    )
    sources_by_slug: dict[str, list[_PolymarketResolutionSource]] = {}
    for source in sources:
        sources_by_slug.setdefault(source.resolution.slug, []).append(source)
    windows = _plan_polymarket_windows(
        date=date,
        schedule=schedule,
        sources_by_slug=sources_by_slug,
    )

    return _build_polymarket_shard(
        shard_id=shard_id,
        shard_prefix=shard_prefix,
        manifest_key=manifest_key,
        date=date,
        series_key=series_key,
        outcomes=outcomes,
        depth=depth,
        contracts=contracts,
        windows=windows,
        s3_store=s3_store,
        shard_repo=shard_repo,
        force=force,
    )


def _load_polymarket_resolutions_from_storage(
    *,
    date: str,
    series_key: str,
    s3_store: S3Store,
) -> list[PolymarketMarketResolution]:
    paginator = s3_store.client.get_paginator("list_objects_v2")
    prefix = "metadata/polymarket/"
    candidate_keys: list[str] = []
    for page in paginator.paginate(Bucket=s3_store.bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if not key.endswith("/manifest.json"):
                continue
            parts = key.split("/")
            instrument = unquote(
                next(part.split("=", 1)[1] for part in parts if part.startswith("instrument="))
            )
            if instrument.startswith(f"{series_key}-"):
                candidate_keys.append(key)

    resolutions: list[PolymarketMarketResolution] = []
    seen: set[tuple[str, str]] = set()
    if candidate_keys:
        with ThreadPoolExecutor(max_workers=min(16, len(candidate_keys))) as executor:
            future_map = {executor.submit(s3_store.get_bytes, key): key for key in candidate_keys}
            for future in as_completed(future_map):
                resolution = PolymarketMarketResolution.model_validate(orjson.loads(future.result()))
                if date not in resolution.dates:
                    continue
                dedupe_key = (resolution.slug, resolution.outcome)
                if dedupe_key in seen:
                    continue
                seen.add(dedupe_key)
                resolutions.append(resolution)
    return sorted(resolutions, key=lambda item: (item.start_ts_ms, item.slug, item.outcome))


def _polymarket_sources_from_resolutions(
    *,
    date: str,
    resolutions: list[PolymarketMarketResolution],
    coverage_repo: CoverageRepository,
) -> list[_PolymarketResolutionSource]:
    sources: list[_PolymarketResolutionSource] = []
    for resolution in sorted(resolutions, key=lambda item: (item.start_ts_ms, item.slug, item.outcome)):
        market = resolution.market_ref()
        l2_record = _coverage_record(coverage_repo, DatasetKind.NORMALIZED_L2, market, date, "daily")
        if l2_record is None:
            raise ValueError(f"normalized L2 coverage is not ready for {resolution.instrument} {date}")
        trade_record = _coverage_record(coverage_repo, DatasetKind.NORMALIZED_TRADES, market, date, "daily")
        if trade_record is None:
            raise ValueError(f"normalized trade coverage is not ready for {resolution.instrument} {date}")

        sources.append(
            _PolymarketResolutionSource(
                resolution=resolution,
                trade_key=(
                    polymarket_normalized_trade_s3_key(market, resolution.market_id, date)
                    if trade_record.row_count > 0
                    else None
                ),
                l2_key=(
                    polymarket_normalized_l2_s3_key(market, resolution.market_id, date)
                    if l2_record.row_count > 0
                    else None
                ),
            )
        )
    return sources


def _list_polymarket_normalized_keys(
    *,
    date: str,
    series_key: str,
    kind: str,
    s3_store: S3Store,
) -> dict[tuple[str, str], str]:
    results: dict[tuple[str, str], str] = {}
    paginator = s3_store.client.get_paginator("list_objects_v2")
    prefix = f"normalized/polymarket/kind={kind}/"
    for page in paginator.paginate(Bucket=s3_store.bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if f"/date={date}/" not in key or not key.endswith(".parquet"):
                continue
            parts = key.split("/")
            market_id = next(part.split("=", 1)[1] for part in parts if part.startswith("market_id="))
            instrument = unquote(
                next(part.split("=", 1)[1] for part in parts if part.startswith("instrument="))
            )
            if not instrument.startswith(f"{series_key}-"):
                continue
            results[(instrument, market_id)] = key
    return results


def _infer_polymarket_resolution_from_key(
    *,
    instrument: str,
    market_id: str,
    date: str,
) -> PolymarketMarketResolution:
    slug, outcome = instrument.rsplit(":", 1)
    head, sep, tail = slug.rpartition("-")
    start_ts_ms = int(tail) * 1000 if sep and tail.isdigit() else 0
    interval_ms = 300_000 if "5m" in slug else 0
    return PolymarketMarketResolution(
        slug=slug,
        question="",
        outcome=outcome,
        market_id=market_id,
        asset_id="",
        instrument=instrument,
        start_time="",
        end_time="",
        start_ts_ms=start_ts_ms,
        end_ts_ms=start_ts_ms + interval_ms,
        dates=(date,),
    )


def _polymarket_sources_from_storage(
    *,
    date: str,
    series_key: str,
    s3_store: S3Store,
) -> tuple[list[PolymarketMarketResolution], list[_PolymarketResolutionSource]]:
    metadata_resolutions = _load_polymarket_resolutions_from_storage(
        date=date,
        series_key=series_key,
        s3_store=s3_store,
    )
    if metadata_resolutions:
        sources = [
            _PolymarketResolutionSource(
                resolution=resolution,
                trade_key=polymarket_normalized_trade_s3_key(
                    resolution.market_ref(),
                    resolution.market_id,
                    date,
                ),
                l2_key=polymarket_normalized_l2_s3_key(
                    resolution.market_ref(),
                    resolution.market_id,
                    date,
                ),
            )
            for resolution in sorted(
                metadata_resolutions,
                key=lambda item: (item.start_ts_ms, item.slug, item.outcome),
            )
        ]
        return metadata_resolutions, sources

    trade_keys = _list_polymarket_normalized_keys(
        date=date,
        series_key=series_key,
        kind="trade",
        s3_store=s3_store,
    )
    l2_keys = _list_polymarket_normalized_keys(
        date=date,
        series_key=series_key,
        kind="l2_snapshot",
        s3_store=s3_store,
    )
    all_keys = sorted(set(trade_keys) | set(l2_keys))
    if not all_keys:
        raise ValueError(f"no normalized polymarket objects were found for {series_key} on {date}")
    resolution_by_key = {
        (resolution.instrument, resolution.market_id): resolution
        for resolution in metadata_resolutions
    }
    resolutions: list[PolymarketMarketResolution] = []
    sources: list[_PolymarketResolutionSource] = []
    for key_parts in all_keys:
        resolution = resolution_by_key.get(key_parts) or _infer_polymarket_resolution_from_key(
            instrument=key_parts[0],
            market_id=key_parts[1],
            date=date,
        )
        resolutions.append(resolution)
        sources.append(
            _PolymarketResolutionSource(
                resolution=resolution,
                trade_key=trade_keys.get(key_parts),
                l2_key=l2_keys.get(key_parts),
            )
        )
    return resolutions, _sort_resolution_sources(sources)


def build_polymarket_canonical_day_from_storage(
    *,
    date: str,
    series_key: str,
    outcomes: OutcomesMode,
    depth: int,
    s3_store: S3Store,
    shard_repo: CanonicalShardRepository,
    force: bool = False,
) -> CanonicalShardRecord:
    shard_id = canonical_polymarket_shard_id(
        series_key=series_key,
        date=date,
        outcomes=outcomes,
        depth=depth,
    )
    shard_prefix = canonical_polymarket_shard_prefix(
        series_key=series_key,
        date=date,
        outcomes=outcomes,
        depth=depth,
    )
    manifest_key = canonical_polymarket_manifest_s3_key(
        series_key=series_key,
        date=date,
        outcomes=outcomes,
        depth=depth,
    )
    resolutions, sources = _polymarket_sources_from_storage(
        date=date,
        series_key=series_key,
        s3_store=s3_store,
    )
    contracts = _group_polymarket_contracts(resolutions)
    schedule = _PolymarketContractSchedule(contracts)
    sources_by_slug: dict[str, list[_PolymarketResolutionSource]] = {}
    for source in sources:
        sources_by_slug.setdefault(source.resolution.slug, []).append(source)
    windows = _plan_polymarket_windows(
        date=date,
        schedule=schedule,
        sources_by_slug=sources_by_slug,
    )

    return _build_polymarket_shard(
        shard_id=shard_id,
        shard_prefix=shard_prefix,
        manifest_key=manifest_key,
        date=date,
        series_key=series_key,
        outcomes=outcomes,
        depth=depth,
        contracts=contracts,
        windows=windows,
        s3_store=s3_store,
        shard_repo=shard_repo,
        force=force,
    )
