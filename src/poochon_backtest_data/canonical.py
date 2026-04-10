from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
import heapq
import io
import shutil
import tempfile
from typing import Any, Callable, Iterator
from urllib.parse import unquote

import orjson
import pyarrow.parquet as pq
import zstandard

from .models import (
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
    canonical_hyperliquid_s3_key,
    canonical_hyperliquid_shard_id,
    canonical_polymarket_manifest_s3_key,
    canonical_polymarket_s3_key,
    canonical_polymarket_shard_id,
    coverage_pk,
    normalized_l2_s3_key,
    normalized_trade_s3_key,
    polymarket_normalized_l2_s3_key,
    polymarket_normalized_trade_s3_key,
    utc_now_iso,
)
from .storage import CanonicalShardRepository, CoverageRepository, S3Store


def _venue_label(venue: Venue) -> str:
    if venue == Venue.HYPERLIQUID:
        return "Hyperliquid"
    if venue == Venue.POLYMARKET:
        return "Polymarket"
    return str(venue)


def _trade_event(row: dict[str, Any]) -> dict[str, Any]:
    venue = str(row["venue_label"])
    return {
        "Market": {
            "Trade": {
                "instrument": {"venue": venue, "symbol": row["instrument"]},
                "ts_ms": int(row["ts_ms"]),
                "px": float(row["px"]),
                "sz": float(row["sz"]),
                "side": row["side"],
            }
        }
    }


def _snapshot_event(row: dict[str, Any], *, depth: int) -> dict[str, Any]:
    venue = str(row["venue_label"])

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
                "instrument": {"venue": venue, "symbol": row["instrument"]},
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


def _chunked[T](items: list[T], chunk_size: int) -> Iterator[list[T]]:
    for start in range(0, len(items), chunk_size):
        yield items[start : start + chunk_size]


def _fragment_payload(merged: _MergedRow) -> bytes:
    return orjson.dumps(
        {
            "kind": merged.kind,
            "source_order": merged.source_order,
            "row": merged.row,
        }
    )


def _write_fragment(path: str, streams: list[_MergeStream]) -> None:
    with open(path, "wb") as handle:
        for merged in _merge_sorted_streams(streams):
            handle.write(_fragment_payload(merged))
            handle.write(b"\n")


def _iter_fragment_rows(path: str) -> Iterator[_MergedRow]:
    with open(path, "rb") as handle:
        for line in handle:
            payload = orjson.loads(line)
            yield _MergedRow(
                kind=str(payload["kind"]),
                source_order=int(payload["source_order"]),
                row=payload["row"],
            )


def _merge_fragment_files(paths: list[str]) -> Iterator[_MergedRow]:
    iterators = [_iter_fragment_rows(path) for path in paths]
    heap: list[tuple[tuple[int, int, int, int], int, _MergedRow]] = []
    for index, iterator in enumerate(iterators):
        try:
            merged = next(iterator)
        except StopIteration:
            continue
        heapq.heappush(
            heap,
            (_stream_sort_key(merged.row, merged.kind, merged.source_order), index, merged),
        )

    while heap:
        _, iterator_index, merged = heapq.heappop(heap)
        yield merged
        iterator = iterators[iterator_index]
        try:
            next_merged = next(iterator)
        except StopIteration:
            continue
        heapq.heappush(
            heap,
            (
                _stream_sort_key(next_merged.row, next_merged.kind, next_merged.source_order),
                iterator_index,
                next_merged,
            ),
        )


def _build_polymarket_shard_from_tasks(
    *,
    shard_id: str,
    shard_key: str,
    manifest_key: str,
    date: str,
    series_key: str,
    outcomes: OutcomesMode,
    depth: int,
    pending_fetches: list[tuple[int, str, str, Callable[[dict[str, Any]], bool] | None]],
    s3_store: S3Store,
    shard_repo: CanonicalShardRepository,
) -> CanonicalShardRecord:
    if not pending_fetches:
        return _materialize_shard(
            shard_id=shard_id,
            shard_key=shard_key,
            manifest_key=manifest_key,
            venue=Venue.POLYMARKET,
            market_type=MarketType.BINARY,
            instrument=None,
            series_key=series_key,
            outcomes=outcomes.value,
            date=date,
            depth=depth,
            merged_rows=iter(()),
            s3_store=s3_store,
            shard_repo=shard_repo,
            source_refs=[],
        )

    fragment_stream_limit = 64
    venue_label = _venue_label(Venue.POLYMARKET)
    fragment_dir = tempfile.mkdtemp(prefix=f"{shard_id}-")
    fragment_paths: list[str] = []
    source_refs: list[str] = []
    try:
        for fragment_index, task_chunk in enumerate(_chunked(pending_fetches, fragment_stream_limit)):
            streams = []
            fetched: list[tuple[int, str, str, Callable[[dict[str, Any]], bool] | None, bytes]] = []
            with ThreadPoolExecutor(max_workers=min(8, len(task_chunk))) as executor:
                future_map = {
                    executor.submit(s3_store.get_bytes, key): (source_order, kind, key, filter_fn)
                    for source_order, kind, key, filter_fn in task_chunk
                }
                for future in as_completed(future_map):
                    source_order, kind, key, filter_fn = future_map[future]
                    fetched.append((source_order, kind, key, filter_fn, future.result()))

            for source_order, kind, key, filter_fn, payload in sorted(
                fetched, key=lambda item: item[0]
            ):
                streams.append(
                    _MergeStream(
                        iterator=_iter_parquet_rows(
                            payload,
                            extra_fields={"venue_label": venue_label},
                            filter_fn=filter_fn,
                        ),
                        kind=kind,
                        source_order=source_order,
                    )
                )
                source_refs.append(key)

            if not streams:
                continue
            fragment_path = f"{fragment_dir}/fragment-{fragment_index:04d}.jsonl"
            _write_fragment(fragment_path, streams)
            fragment_paths.append(fragment_path)

        return _materialize_shard(
            shard_id=shard_id,
            shard_key=shard_key,
            manifest_key=manifest_key,
            venue=Venue.POLYMARKET,
            market_type=MarketType.BINARY,
            instrument=None,
            series_key=series_key,
            outcomes=outcomes.value,
            date=date,
            depth=depth,
            merged_rows=_merge_fragment_files(fragment_paths),
            s3_store=s3_store,
            shard_repo=shard_repo,
            source_refs=source_refs,
        )
    finally:
        shutil.rmtree(fragment_dir, ignore_errors=True)


def _materialize_shard(
    *,
    shard_id: str,
    shard_key: str,
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
) -> CanonicalShardRecord:
    existing = shard_repo.get(shard_id)
    if existing is not None and existing.status == CanonicalShardStatus.READY and s3_store.exists(
        existing.shard_s3_key
    ):
        return existing

    temp_path = f"/tmp/{shard_id}.jsonl.zst"
    event_count = 0
    start_ts_ms: int | None = None
    end_ts_ms: int | None = None
    with open(temp_path, "wb") as raw_file:
        with zstandard.ZstdCompressor(level=3).stream_writer(raw_file) as writer:
            for merged in merged_rows:
                row = merged.row
                ts_ms = int(row["ts_ms"])
                if start_ts_ms is None:
                    start_ts_ms = ts_ms
                end_ts_ms = ts_ms
                if merged.kind == "trade":
                    writer.write(orjson.dumps(_trade_event(row)))
                else:
                    writer.write(orjson.dumps(_snapshot_event(row, depth=depth)))
                writer.write(b"\n")
                event_count += 1

    s3_store.put_file(shard_key, temp_path, content_type="application/zstd")
    created_at = utc_now_iso()
    s3_store.put_json(
        manifest_key,
        {
            "shard_id": shard_id,
            "venue": venue.value,
            "market_type": market_type.value,
            "instrument": instrument,
            "series_key": series_key,
            "outcomes": outcomes,
            "date": date,
            "depth": depth,
            "event_count": event_count,
            "start_ts_ms": start_ts_ms,
            "end_ts_ms": end_ts_ms,
            "shard_s3_key": shard_key,
            "source_refs": source_refs,
            "created_at": created_at,
        },
    )
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
        shard_s3_key=shard_key,
        manifest_s3_key=manifest_key,
        event_count=event_count,
        start_ts_ms=start_ts_ms,
        end_ts_ms=end_ts_ms,
        created_at=created_at,
        updated_at=created_at,
    )
    shard_repo.put(record)
    return record


def _build_shard(
    *,
    shard_id: str,
    shard_key: str,
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
) -> CanonicalShardRecord:
    return _materialize_shard(
        shard_id=shard_id,
        shard_key=shard_key,
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
    )


def build_hyperliquid_canonical_day(
    *,
    market: MarketRef,
    date: str,
    depth: int,
    s3_store: S3Store,
    coverage_repo: CoverageRepository,
    shard_repo: CanonicalShardRepository,
) -> CanonicalShardRecord:
    l2_daily = _coverage_record(coverage_repo, DatasetKind.NORMALIZED_L2, market, date, "daily")
    if l2_daily is None:
        raise ValueError(f"normalized L2 coverage is not ready for {market.instrument} {date}")
    trade_daily = _coverage_record(coverage_repo, DatasetKind.NORMALIZED_TRADES, market, date, "daily")
    if trade_daily is None:
        raise ValueError(f"normalized trade coverage is not ready for {market.instrument} {date}")

    shard_id = canonical_hyperliquid_shard_id(market, date, depth)
    shard_key = canonical_hyperliquid_s3_key(market, date, depth)
    manifest_key = canonical_hyperliquid_manifest_s3_key(market, date, depth)
    streams: list[_MergeStream] = []
    source_refs: list[str] = []
    venue_label = _venue_label(market.venue)

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
                        extra_fields={"venue_label": venue_label},
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
                        extra_fields={"venue_label": venue_label},
                    ),
                    kind="l2",
                    source_order=hour * 2 + 1,
                )
            )

    return _build_shard(
        shard_id=shard_id,
        shard_key=shard_key,
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
) -> CanonicalShardRecord:
    shard_id = canonical_polymarket_shard_id(
        series_key=series_key,
        date=date,
        outcomes=outcomes,
        depth=depth,
    )
    shard_key = canonical_polymarket_s3_key(
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
    active_resolutions = [resolution for resolution in resolutions if date in resolution.dates]
    if not active_resolutions:
        raise ValueError(f"no polymarket markets were discovered for {series_key} on {date}")

    sorted_resolutions = sorted(active_resolutions, key=lambda item: (item.slug, item.outcome))
    pending_fetches: list[tuple[int, str, str, Callable[[dict[str, Any]], bool] | None]] = []
    for source_order, resolution in enumerate(sorted_resolutions):
        market = resolution.market_ref()
        l2_record = _coverage_record(coverage_repo, DatasetKind.NORMALIZED_L2, market, date, "daily")
        if l2_record is None:
            raise ValueError(f"normalized L2 coverage is not ready for {resolution.instrument} {date}")
        trade_record = _coverage_record(coverage_repo, DatasetKind.NORMALIZED_TRADES, market, date, "daily")
        if trade_record is None:
            raise ValueError(
                f"normalized trade coverage is not ready for {resolution.instrument} {date}"
            )

        l2_key = polymarket_normalized_l2_s3_key(market, resolution.market_id, date)
        trade_key = polymarket_normalized_trade_s3_key(market, resolution.market_id, date)

        def within_lifetime(row: dict[str, Any], *, item=resolution) -> bool:
            ts_ms = int(row["ts_ms"])
            return item.start_ts_ms <= ts_ms <= item.end_ts_ms

        if trade_record.row_count > 0:
            pending_fetches.append((source_order * 2, "trade", trade_key, within_lifetime))
        if l2_record.row_count > 0:
            pending_fetches.append((source_order * 2 + 1, "l2", l2_key, within_lifetime))

    return _build_polymarket_shard_from_tasks(
        shard_id=shard_id,
        shard_key=shard_key,
        manifest_key=manifest_key,
        date=date,
        series_key=series_key,
        outcomes=outcomes,
        depth=depth,
        pending_fetches=pending_fetches,
        s3_store=s3_store,
        shard_repo=shard_repo,
    )


def build_polymarket_canonical_day_from_storage(
    *,
    date: str,
    series_key: str,
    outcomes: OutcomesMode,
    depth: int,
    s3_store: S3Store,
    shard_repo: CanonicalShardRepository,
) -> CanonicalShardRecord:
    shard_id = canonical_polymarket_shard_id(
        series_key=series_key,
        date=date,
        outcomes=outcomes,
        depth=depth,
    )
    shard_key = canonical_polymarket_s3_key(
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

    def list_keys(kind: str) -> dict[tuple[str, str], str]:
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

    trade_keys = list_keys("trade")
    l2_keys = list_keys("l2_snapshot")
    all_keys = sorted(set(trade_keys) | set(l2_keys))
    if not all_keys:
        raise ValueError(f"no normalized polymarket objects were found for {series_key} on {date}")

    pending_fetches: list[tuple[int, str, str, Callable[[dict[str, Any]], bool] | None]] = []
    for source_order, key_parts in enumerate(all_keys):
        trade_key = trade_keys.get(key_parts)
        l2_key = l2_keys.get(key_parts)
        if trade_key is not None:
            pending_fetches.append((source_order * 2, "trade", trade_key, None))
        if l2_key is not None:
            pending_fetches.append((source_order * 2 + 1, "l2", l2_key, None))

    return _build_polymarket_shard_from_tasks(
        shard_id=shard_id,
        shard_key=shard_key,
        manifest_key=manifest_key,
        date=date,
        series_key=series_key,
        outcomes=outcomes,
        depth=depth,
        pending_fetches=pending_fetches,
        s3_store=s3_store,
        shard_repo=shard_repo,
    )
