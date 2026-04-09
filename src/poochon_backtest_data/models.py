from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
import hashlib

from pydantic import BaseModel, ConfigDict, Field, model_validator


class ReplayStatus(StrEnum):
    PENDING = "PENDING"
    READY = "READY"
    FAILED = "FAILED"


class CoverageStatus(StrEnum):
    READY = "READY"
    FAILED = "FAILED"


class DatasetKind(StrEnum):
    RAW_L2 = "raw_l2"
    RAW_TRADES = "raw_trades"
    NORMALIZED_L2 = "normalized_l2"
    NORMALIZED_TRADES = "normalized_trades"


class ReplayRequest(BaseModel):
    model_config = ConfigDict(frozen=True)

    venue: str = "hyperliquid"
    market: str = "perp"
    symbol: str
    date: str
    mode: str = "l2-trade"

    @model_validator(mode="after")
    def validate_request(self) -> "ReplayRequest":
        if self.venue != "hyperliquid":
            raise ValueError("only hyperliquid venue is supported")
        if self.market != "perp":
            raise ValueError("only perp market is supported")
        if self.mode != "l2-trade":
            raise ValueError("only l2-trade replay mode is supported")
        return self

    def replay_id(self) -> str:
        canonical = f"{self.venue}|{self.market}|{self.symbol}|{self.date}|{self.mode}"
        return hashlib.sha256(canonical.encode("utf-8")).hexdigest()[:32]


class ReplayRecord(BaseModel):
    replay_id: str
    status: ReplayStatus
    request: ReplayRequest
    replay_s3_key: str
    manifest_s3_key: str
    event_count: int = 0
    error: str | None = None
    created_at: str
    updated_at: str


class CoverageRecord(BaseModel):
    pk: str
    dataset_kind: DatasetKind
    symbol: str
    date: str
    hour: str
    status: CoverageStatus
    object_count: int = 0
    byte_count: int = 0
    row_count: int = 0
    updated_at: str
    source: str


def utc_now_iso() -> str:
    return datetime.now(tz=UTC).replace(microsecond=0).isoformat()


def coverage_pk(dataset_kind: DatasetKind, symbol: str, date: str, hour: str) -> str:
    return f"{dataset_kind.value}#{symbol}#{date}#{hour}"


def replay_s3_key(request: ReplayRequest) -> str:
    replay_id = request.replay_id()
    return (
        "replays/"
        f"venue={request.venue}/market={request.market}/symbol={request.symbol}/"
        f"date={request.date}/mode={request.mode}/replay_id={replay_id}/events.jsonl.zst"
    )


def replay_manifest_s3_key(request: ReplayRequest) -> str:
    replay_id = request.replay_id()
    return (
        "replays/"
        f"venue={request.venue}/market={request.market}/symbol={request.symbol}/"
        f"date={request.date}/mode={request.mode}/replay_id={replay_id}/manifest.json"
    )


def normalized_l2_s3_key(symbol: str, date: str, hour: int) -> str:
    return (
        "normalized/hyperliquid/l2_snapshot/"
        f"date={date}/hour={hour:02d}/symbol={symbol}/part-000.parquet"
    )


def normalized_trade_s3_key(symbol: str, date: str, hour: int) -> str:
    return (
        "normalized/hyperliquid/trade/"
        f"date={date}/hour={hour:02d}/symbol={symbol}/part-000.parquet"
    )


def raw_l2_s3_key(symbol: str, date: str, hour: int) -> str:
    return (
        "raw/hyperliquid/l2book/"
        f"date={date}/hour={hour:02d}/symbol={symbol}/{symbol}.lz4"
    )


def raw_trade_s3_key(symbol: str, date: str, hour: int) -> str:
    return (
        "raw/hyperliquid/node_trades/"
        f"date={date}/hour={hour:02d}/symbol={symbol}/part-{hour:02d}.lz4"
    )


@dataclass(frozen=True)
class NormalizedL2Snapshot:
    ts_ms: int
    symbol: str
    bids_json: str
    asks_json: str
    source_hour: int
    source_line_number: int


@dataclass(frozen=True)
class NormalizedTrade:
    ts_ms: int
    symbol: str
    side: str
    px: float
    sz: float
    hash: str
    source_hour: int
    source_line_number: int


def new_pending_replay(request: ReplayRequest) -> ReplayRecord:
    now = utc_now_iso()
    return ReplayRecord(
        replay_id=request.replay_id(),
        status=ReplayStatus.PENDING,
        request=request,
        replay_s3_key=replay_s3_key(request),
        manifest_s3_key=replay_manifest_s3_key(request),
        created_at=now,
        updated_at=now,
    )
