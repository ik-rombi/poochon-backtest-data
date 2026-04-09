from __future__ import annotations

import io
import time
from typing import Any

import orjson
import pyarrow.parquet as pq
import zstandard

from .models import (
    CoverageStatus,
    DatasetKind,
    PolymarketReplayCreateRequest,
    ReplayRecord,
    ReplayRequest,
    ReplayStatus,
    Venue,
    coverage_pk,
    new_pending_replay,
    normalized_l2_s3_key,
    normalized_trade_s3_key,
    polymarket_normalized_l2_s3_key,
    polymarket_normalized_trade_s3_key,
    replay_manifest_s3_key,
    replay_s3_key,
    utc_now_iso,
)
from .polymarket_telonex import resolve_market
from .storage import CoverageRepository, ReplayRepository, S3Store


class ReplayService:
    def __init__(
        self,
        *,
        s3_store: S3Store,
        coverage_repo: CoverageRepository,
        replay_repo: ReplayRepository,
        stepfunctions_client: Any | None = None,
        replay_state_machine_arn: str | None = None,
    ):
        self.s3_store = s3_store
        self.coverage_repo = coverage_repo
        self.replay_repo = replay_repo
        self.stepfunctions_client = stepfunctions_client
        self.replay_state_machine_arn = replay_state_machine_arn

    def submit_replay(self, request: ReplayRequest) -> ReplayRecord:
        self._assert_coverage_ready(request)
        replay_id = request.replay_id()
        existing = self.replay_repo.get(replay_id)
        if existing is not None:
            return existing

        pending = new_pending_replay(request)
        record = self.replay_repo.create_if_absent(pending)
        if record.status == ReplayStatus.PENDING:
            self._start_materialize_workflow(record.request)
        return record

    def submit_polymarket_replay(
        self,
        create_request: PolymarketReplayCreateRequest,
    ) -> ReplayRecord:
        resolution = resolve_market(create_request)
        return self.submit_replay(resolution.replay_request(depth=create_request.depth))

    def _assert_coverage_ready(self, request: ReplayRequest) -> None:
        market = request.market_ref()
        for date in request.replay_dates():
            needed = [
                coverage_pk(DatasetKind.NORMALIZED_L2, market, date, "daily"),
                coverage_pk(DatasetKind.NORMALIZED_TRADES, market, date, "daily"),
            ]
            for item in needed:
                record = self.coverage_repo.get(item)
                if record is None or record.status != CoverageStatus.READY:
                    raise ValueError(f"normalized coverage is not ready for {item}")

    def get_replay(self, replay_id: str) -> ReplayRecord | None:
        return self.replay_repo.get(replay_id)

    def stream_replay(self, replay_id: str):
        record = self.get_replay(replay_id)
        if record is None:
            raise KeyError(replay_id)
        if record.status != ReplayStatus.READY:
            raise RuntimeError(f"replay {replay_id} is not ready")
        return self.s3_store.stream_zstd(record.replay_s3_key)

    def _start_materialize_workflow(self, request: ReplayRequest) -> None:
        if not self.stepfunctions_client or not self.replay_state_machine_arn:
            return
        execution_name = f"{request.replay_id()}-{int(time.time())}"
        self.stepfunctions_client.start_execution(
            stateMachineArn=self.replay_state_machine_arn,
            name=execution_name,
            input=request.model_dump_json(),
        )


def _trade_event(row: dict[str, Any]) -> dict[str, Any]:
    venue = str(row["venue_label"])
    return {
        "Market": {
            "Trade": {
                "instrument": {"venue": venue, "symbol": row["instrument"]},
                "ts_ms": row["ts_ms"],
                "px": row["px"],
                "sz": row["sz"],
                "side": row["side"],
            }
        }
    }


def _snapshot_event(row: dict[str, Any], *, depth: int) -> dict[str, Any]:
    venue = str(row["venue_label"])
    def decode_levels(raw_levels: str) -> list[dict[str, Any]]:
        levels = []
        for level in orjson.loads(raw_levels)[:depth]:
            levels.append(
                {
                    "px": float(level["px"]),
                    "sz": float(level["sz"]),
                    "level_count": int(level.get("n", 0)),
                }
            )
        return levels

    return {
        "Market": {
            "L2Snapshot": {
                "instrument": {"venue": venue, "symbol": row["instrument"]},
                "ts_ms": row["ts_ms"],
                "bids": decode_levels(row["bids_json"]),
                "asks": decode_levels(row["asks_json"]),
            }
        }
    }


def _event_sort_key(row: dict[str, Any], priority: int) -> tuple[int, int, int, int]:
    return (
        int(row["ts_ms"]),
        priority,
        int(row["source_hour"]),
        int(row["source_line_number"]),
    )


def _venue_label(venue: Venue) -> str:
    if venue == Venue.HYPERLIQUID:
        return "Hyperliquid"
    if venue == Venue.POLYMARKET:
        return "Polymarket"
    return str(venue)


def _materialize_hyperliquid_replay(
    *,
    request: ReplayRequest,
    s3_store: S3Store,
    writer,
) -> int:
    event_count = 0
    venue_label = _venue_label(request.venue)
    for hour in range(24):
        l2_bytes = s3_store.get_bytes(normalized_l2_s3_key(request.market_ref(), request.date, hour))
        trade_bytes = s3_store.get_bytes(normalized_trade_s3_key(request.market_ref(), request.date, hour))
        l2_rows = pq.read_table(io.BytesIO(l2_bytes)).to_pylist()
        trade_rows = pq.read_table(io.BytesIO(trade_bytes)).to_pylist()
        for row in l2_rows:
            row["venue_label"] = venue_label
        for row in trade_rows:
            row["venue_label"] = venue_label
        l2_index = 0
        trade_index = 0
        while trade_index < len(trade_rows) or l2_index < len(l2_rows):
            next_trade = trade_rows[trade_index] if trade_index < len(trade_rows) else None
            next_l2 = l2_rows[l2_index] if l2_index < len(l2_rows) else None
            if next_trade is not None and (
                next_l2 is None or _event_sort_key(next_trade, 0) <= _event_sort_key(next_l2, 1)
            ):
                writer.write(orjson.dumps(_trade_event(next_trade)))
                writer.write(b"\n")
                trade_index += 1
            else:
                writer.write(orjson.dumps(_snapshot_event(next_l2, depth=request.depth)))
                writer.write(b"\n")
                l2_index += 1
            event_count += 1
    return event_count


def _materialize_polymarket_replay(
    *,
    request: ReplayRequest,
    s3_store: S3Store,
    writer,
) -> int:
    assert request.market_id is not None
    assert request.start_ts_ms is not None
    assert request.end_ts_ms is not None
    market = request.market_ref()
    venue_label = _venue_label(request.venue)
    event_count = 0

    for date in request.dates:
        l2_bytes = s3_store.get_bytes(polymarket_normalized_l2_s3_key(market, request.market_id, date))
        trade_bytes = s3_store.get_bytes(
            polymarket_normalized_trade_s3_key(market, request.market_id, date)
        )
        l2_rows = [
            {**row, "venue_label": venue_label}
            for row in pq.read_table(io.BytesIO(l2_bytes)).to_pylist()
            if request.start_ts_ms <= int(row["ts_ms"]) <= request.end_ts_ms
        ]
        trade_rows = [
            {**row, "venue_label": venue_label}
            for row in pq.read_table(io.BytesIO(trade_bytes)).to_pylist()
            if request.start_ts_ms <= int(row["ts_ms"]) <= request.end_ts_ms
        ]
        l2_rows.sort(key=lambda row: (int(row["ts_ms"]), int(row["source_line_number"])))
        trade_rows.sort(key=lambda row: (int(row["ts_ms"]), int(row["source_line_number"])))
        l2_index = 0
        trade_index = 0
        while trade_index < len(trade_rows) or l2_index < len(l2_rows):
            next_trade = trade_rows[trade_index] if trade_index < len(trade_rows) else None
            next_l2 = l2_rows[l2_index] if l2_index < len(l2_rows) else None
            if next_trade is not None and (
                next_l2 is None or _event_sort_key(next_trade, 0) <= _event_sort_key(next_l2, 1)
            ):
                writer.write(orjson.dumps(_trade_event(next_trade)))
                writer.write(b"\n")
                trade_index += 1
            else:
                writer.write(orjson.dumps(_snapshot_event(next_l2, depth=request.depth)))
                writer.write(b"\n")
                l2_index += 1
            event_count += 1
    return event_count


def materialize_replay(
    *,
    request: ReplayRequest,
    s3_store: S3Store,
    coverage_repo: CoverageRepository,
    replay_repo: ReplayRepository,
) -> ReplayRecord:
    service = ReplayService(
        s3_store=s3_store,
        coverage_repo=coverage_repo,
        replay_repo=replay_repo,
    )
    service._assert_coverage_ready(request)

    existing = replay_repo.get(request.replay_id())
    if existing is not None and existing.status == ReplayStatus.READY and s3_store.exists(
        existing.replay_s3_key
    ):
        return existing

    record = existing or new_pending_replay(request)
    temp_path = f"/tmp/{request.replay_id()}.jsonl.zst"
    with open(temp_path, "wb") as raw_file:
        with zstandard.ZstdCompressor(level=3).stream_writer(raw_file) as writer:
            if request.venue == Venue.POLYMARKET:
                event_count = _materialize_polymarket_replay(
                    request=request,
                    s3_store=s3_store,
                    writer=writer,
                )
            else:
                event_count = _materialize_hyperliquid_replay(
                    request=request,
                    s3_store=s3_store,
                    writer=writer,
                )

    replay_key = replay_s3_key(request)
    manifest_key = replay_manifest_s3_key(request)
    s3_store.put_file(replay_key, temp_path, content_type="application/zstd")
    manifest = {
        "replay_id": request.replay_id(),
        "request": request.model_dump(mode="json"),
        "event_count": event_count,
        "replay_s3_key": replay_key,
        "created_at": utc_now_iso(),
    }
    s3_store.put_json(manifest_key, manifest)
    ready = ReplayRecord(
        replay_id=request.replay_id(),
        status=ReplayStatus.READY,
        request=request,
        replay_s3_key=replay_key,
        manifest_s3_key=manifest_key,
        event_count=event_count,
        error=None,
        created_at=record.created_at,
        updated_at=utc_now_iso(),
    )
    replay_repo.put(ready)
    return ready
