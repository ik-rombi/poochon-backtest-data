from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, date as date_cls, datetime, timedelta
from enum import StrEnum
import hashlib
from urllib.parse import quote

from pydantic import BaseModel, ConfigDict, model_validator


class Venue(StrEnum):
    HYPERLIQUID = "hyperliquid"
    POLYMARKET = "polymarket"


class CoverageStatus(StrEnum):
    READY = "READY"
    FAILED = "FAILED"


class DatasetKind(StrEnum):
    RAW_HL_L2 = "raw_hl_l2"
    RAW_HL_FILLS = "raw_hl_fills"
    RAW_PMXT = "raw_pmxt"
    CANONICAL_HL = "canonical_hl"
    CANONICAL_PM = "canonical_pm"


class MarketType(StrEnum):
    PERP = "perp"
    SPOT = "spot"
    BINARY = "binary"


class PolymarketTargetKind(StrEnum):
    SERIES = "series"
    SLUG = "slug"


class IngestionMode(StrEnum):
    DISABLED = "disabled"
    ONCE = "once"
    CRON = "cron"


class CanonicalShardStatus(StrEnum):
    READY = "READY"
    FAILED = "FAILED"


class CanonicalFileFamily(StrEnum):
    DATA = "data"
    SCHEDULE = "schedule"


class DataEventKind(StrEnum):
    L2_SNAPSHOT = "l2_snapshot"
    TRADE = "trade"
    DELTA_BATCH = "delta_batch"


def _parse_date(value: str) -> date_cls:
    try:
        return date_cls.fromisoformat(value)
    except ValueError as error:
        raise ValueError(f"invalid ISO date: {value}") from error


def iter_dates_inclusive(start_date: str, end_date: str) -> list[str]:
    start = _parse_date(start_date)
    end = _parse_date(end_date)
    if end < start:
        raise ValueError("end_date must be on or after start_date")
    return [
        (start + timedelta(days=offset)).isoformat()
        for offset in range((end - start).days + 1)
    ]


def utc_now_iso() -> str:
    return datetime.now(tz=UTC).replace(microsecond=0).isoformat()


class WindowSpec(BaseModel):
    start_date: str | None = None
    end_date: str | None = None
    start_offset_days: int | None = None
    end_offset_days: int | None = None

    @model_validator(mode="after")
    def validate_window(self) -> "WindowSpec":
        explicit = self.start_date is not None or self.end_date is not None
        relative = self.start_offset_days is not None or self.end_offset_days is not None
        if explicit == relative:
            raise ValueError(
                "provide either start_date/end_date or start_offset_days/end_offset_days"
            )
        if explicit:
            if self.start_date is None or self.end_date is None:
                raise ValueError("start_date and end_date are both required")
            _ = iter_dates_inclusive(self.start_date, self.end_date)
        else:
            if self.start_offset_days is None or self.end_offset_days is None:
                raise ValueError("start_offset_days and end_offset_days are both required")
            if self.end_offset_days < self.start_offset_days:
                raise ValueError("end_offset_days must be >= start_offset_days")
        return self

    def resolve_window(self, *, today: date_cls | None = None) -> tuple[str, str]:
        base = today or datetime.now(tz=UTC).date()
        if self.start_date is not None and self.end_date is not None:
            return self.start_date, self.end_date
        assert self.start_offset_days is not None
        assert self.end_offset_days is not None
        start = base + timedelta(days=self.start_offset_days)
        end = base + timedelta(days=self.end_offset_days)
        return start.isoformat(), end.isoformat()

    def iter_dates(self, *, today: date_cls | None = None) -> list[str]:
        start_raw, end_raw = self.resolve_window(today=today)
        return iter_dates_inclusive(start_raw, end_raw)


class MarketRef(BaseModel):
    model_config = ConfigDict(frozen=True)

    venue: Venue = Venue.HYPERLIQUID
    market_type: MarketType
    instrument: str

    @model_validator(mode="after")
    def validate_market(self) -> "MarketRef":
        if not self.instrument.strip():
            raise ValueError("instrument is required")
        if self.venue == Venue.HYPERLIQUID and self.market_type not in {
            MarketType.PERP,
            MarketType.SPOT,
        }:
            raise ValueError("hyperliquid only supports perp or spot market types")
        if self.venue == Venue.POLYMARKET and self.market_type != MarketType.BINARY:
            raise ValueError("polymarket only supports binary market type")
        return self

    def encoded_instrument(self) -> str:
        return quote(self.instrument, safe="")


class PolymarketTarget(BaseModel):
    model_config = ConfigDict(frozen=True)

    target_kind: PolymarketTargetKind
    target_key: str

    @model_validator(mode="after")
    def validate_target(self) -> "PolymarketTarget":
        if not self.target_key.strip():
            raise ValueError("target_key is required")
        return self

    def encoded_key(self) -> str:
        return quote(self.target_key, safe="")


class HyperliquidIngestionRequest(WindowSpec):
    market_type: MarketType
    instrument: str

    @model_validator(mode="after")
    def validate_request(self) -> "HyperliquidIngestionRequest":
        if not self.instrument.strip():
            raise ValueError("instrument is required")
        if self.market_type not in {MarketType.PERP, MarketType.SPOT}:
            raise ValueError("hyperliquid only supports perp or spot market types")
        return self

    def market_ref(self) -> MarketRef:
        return MarketRef(
            venue=Venue.HYPERLIQUID,
            market_type=self.market_type,
            instrument=self.instrument,
        )


class PolymarketMirrorRequest(WindowSpec):
    """PMXT raw mirror — no per-target identity (firehose)."""


class PolymarketSliceRequest(WindowSpec):
    target_kind: PolymarketTargetKind
    target_key: str

    @model_validator(mode="after")
    def validate_request(self) -> "PolymarketSliceRequest":
        if not self.target_key.strip():
            raise ValueError("target_key is required")
        return self

    def target(self) -> PolymarketTarget:
        return PolymarketTarget(target_kind=self.target_kind, target_key=self.target_key)


class PolymarketMarketResolution(BaseModel):
    venue: Venue = Venue.POLYMARKET
    market_type: MarketType = MarketType.BINARY
    slug: str
    question: str = ""
    outcome: str
    market_id: str
    asset_id: str
    instrument: str
    start_time: str = ""
    end_time: str = ""
    start_ts_ms: int
    end_ts_ms: int
    price_to_beat: float | None = None
    price_to_beat_source: str | None = None
    price_to_beat_quality: str | None = None
    settlement_payout: float | None = None

    @model_validator(mode="after")
    def validate_resolution(self) -> "PolymarketMarketResolution":
        if not self.slug.strip():
            raise ValueError("slug is required")
        if not self.outcome.strip():
            raise ValueError("outcome is required")
        if not self.market_id.strip():
            raise ValueError("market_id is required")
        if not self.asset_id.strip():
            raise ValueError("asset_id is required")
        if not self.instrument.strip():
            raise ValueError("instrument is required")
        if self.end_ts_ms < self.start_ts_ms:
            raise ValueError("end_ts_ms must be on or after start_ts_ms")
        return self

    @property
    def series_key(self) -> str:
        head, sep, tail = self.slug.rpartition("-")
        if sep and tail.isdigit():
            return head
        return self.slug


class CoverageRecord(BaseModel):
    """Health record for one pipeline-stage output unit.

    pk is the partition key in DynamoDB. Other fields beyond
    (pk, dataset_kind, status, source, updated_at) are sparsely populated based
    on dataset_kind:
      - raw_pmxt           date, hour
      - raw_hl_l2          market_type, instrument, date, hour
      - raw_hl_fills       date, hour                  (firehose; no instrument)
      - canonical_pm       target_kind, target_key, date
      - canonical_hl       market_type, instrument, date
    """

    pk: str
    dataset_kind: DatasetKind
    status: CoverageStatus
    object_count: int = 0
    byte_count: int = 0
    row_count: int = 0
    updated_at: str
    source: str
    error: str | None = None

    venue: Venue | None = None
    market_type: MarketType | None = None
    instrument: str | None = None
    target_kind: PolymarketTargetKind | None = None
    target_key: str | None = None
    date: str | None = None
    hour: str | None = None


class CanonicalShardFile(BaseModel):
    family: CanonicalFileFamily
    file_name: str
    s3_key: str
    row_count: int
    size_bytes: int = 0


class CanonicalShardRecord(BaseModel):
    """Per-(target, date) canonical shard record.

    Wraps a single data.parquet for both venues, plus an optional schedule.parquet
    sidecar for Polymarket. Identity is keyed by venue + (instrument | target_kind+target_key)
    + date + depth.
    """

    shard_id: str
    status: CanonicalShardStatus
    venue: Venue
    market_type: MarketType
    date: str
    depth: int
    shard_prefix: str
    manifest_s3_key: str

    instrument: str | None = None
    target_kind: PolymarketTargetKind | None = None
    target_key: str | None = None

    data_file: CanonicalShardFile | None = None
    schedule_file: CanonicalShardFile | None = None

    event_count: int = 0
    byte_count: int = 0
    start_ts_ms: int | None = None
    end_ts_ms: int | None = None
    source_refs: tuple[str, ...] = ()

    created_at: str
    updated_at: str
    error: str | None = None


class CanonicalWindowManifest(BaseModel):
    venue: Venue
    market_type: MarketType
    start_date: str
    end_date: str
    depth: int

    instrument: str | None = None
    target_kind: PolymarketTargetKind | None = None
    target_key: str | None = None

    shard_count: int
    event_count: int
    start_ts_ms: int | None
    end_ts_ms: int | None
    shard_ids: tuple[str, ...]
    shard_prefixes: tuple[str, ...]
    shards: tuple[CanonicalShardRecord, ...]


@dataclass(frozen=True)
class NormalizedL2Snapshot:
    ts_ms: int
    instrument: str
    bids_json: str
    asks_json: str
    source_hour: int
    source_line_number: int


@dataclass(frozen=True)
class NormalizedTrade:
    ts_ms: int
    instrument: str
    side: str
    px: float
    sz: float
    hash: str
    source_hour: int
    source_line_number: int


# --- S3 key helpers --------------------------------------------------------


def raw_pmxt_filename(date: str, hour: int) -> str:
    return f"polymarket_orderbook_{date}T{hour:02d}.parquet"


def raw_pmxt_s3_key(date: str, hour: int) -> str:
    return f"raw/pmxt/orderbook/date={date}/hour={hour:02d}/{raw_pmxt_filename(date, hour)}"


def raw_pmxt_upstream_url(base_url: str, date: str, hour: int) -> str:
    base = base_url.rstrip("/")
    return f"{base}/{raw_pmxt_filename(date, hour)}"


def raw_hl_l2_s3_key(market: MarketRef, date: str, hour: int) -> str:
    return (
        "raw/hyperliquid/l2book/"
        f"market_type={market.market_type.value}/date={date}/hour={hour:02d}/"
        f"instrument={market.encoded_instrument()}/{market.encoded_instrument()}.lz4"
    )


def raw_hl_fills_s3_key(date: str, hour: int) -> str:
    """Hyperliquid fills are a firehose — one lz4 per (date, hour) covering all coins."""
    return f"raw/hyperliquid/node_fills_by_block/date={date}/hour={hour:02d}/fills.lz4"


def canonical_hl_shard_id(market: MarketRef, date: str, depth: int) -> str:
    canonical = (
        f"canonical|hyperliquid|{market.market_type.value}|"
        f"{market.instrument}|{date}|depth={depth}"
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()[:32]


def canonical_pm_shard_id(
    *,
    target: PolymarketTarget,
    date: str,
    depth: int,
) -> str:
    canonical = (
        f"canonical|polymarket|{target.target_kind.value}|{target.target_key}|"
        f"{date}|depth={depth}"
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()[:32]


def canonical_hl_shard_prefix(market: MarketRef, date: str, depth: int) -> str:
    return (
        "canonical/hyperliquid/"
        f"market_type={market.market_type.value}/instrument={market.encoded_instrument()}/"
        f"date={date}/depth={depth}/"
    )


def canonical_pm_shard_prefix(
    *,
    target: PolymarketTarget,
    date: str,
    depth: int,
) -> str:
    return (
        f"canonical/polymarket/{target.target_kind.value}/"
        f"{target.encoded_key()}/date={date}/depth={depth}/"
    )


CANONICAL_DATA_FILE_NAME = "data.parquet"
CANONICAL_SCHEDULE_FILE_NAME = "schedule.parquet"
CANONICAL_MANIFEST_FILE_NAME = "manifest.json"


def canonical_shard_data_s3_key(shard_prefix: str) -> str:
    return f"{shard_prefix}{CANONICAL_DATA_FILE_NAME}"


def canonical_shard_schedule_s3_key(shard_prefix: str) -> str:
    return f"{shard_prefix}{CANONICAL_SCHEDULE_FILE_NAME}"


def canonical_shard_manifest_s3_key(shard_prefix: str) -> str:
    return f"{shard_prefix}{CANONICAL_MANIFEST_FILE_NAME}"


# --- Coverage PK builders --------------------------------------------------


def coverage_pk_raw_pmxt(date: str, hour: int) -> str:
    return f"raw_pmxt#{date}#{hour:02d}"


def coverage_pk_raw_hl_l2(market: MarketRef, date: str, hour: int) -> str:
    return (
        f"raw_hl_l2#{market.market_type.value}#"
        f"{market.encoded_instrument()}#{date}#{hour:02d}"
    )


def coverage_pk_raw_hl_fills(date: str, hour: int) -> str:
    return f"raw_hl_fills#{date}#{hour:02d}"


def coverage_pk_canonical_pm(target: PolymarketTarget, date: str) -> str:
    return f"canonical_pm#{target.target_kind.value}#{target.encoded_key()}#{date}"


def coverage_pk_canonical_hl(market: MarketRef, date: str) -> str:
    return f"canonical_hl#{market.market_type.value}#{market.encoded_instrument()}#{date}"
