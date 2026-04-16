from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, date as date_cls, datetime, timedelta
from enum import StrEnum
import hashlib
from urllib.parse import quote

from pydantic import BaseModel, ConfigDict, Field, model_validator


class Venue(StrEnum):
    HYPERLIQUID = "hyperliquid"
    POLYMARKET = "polymarket"


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


class MarketType(StrEnum):
    PERP = "perp"
    SPOT = "spot"
    BINARY = "binary"


class IngestionMode(StrEnum):
    DISABLED = "disabled"
    ONCE = "once"
    CRON = "cron"


class OutcomesMode(StrEnum):
    BOTH = "both"


class CanonicalShardStatus(StrEnum):
    READY = "READY"
    FAILED = "FAILED"


class CanonicalFileFamily(StrEnum):
    BOOKS = "books"
    TRADES = "trades"
    CONTRACTS = "contracts"


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


class MarketRef(BaseModel):
    model_config = ConfigDict(frozen=True)

    venue: Venue = Venue.HYPERLIQUID
    market_type: MarketType
    instrument: str

    @model_validator(mode="before")
    @classmethod
    def apply_legacy_aliases(cls, value):
        if isinstance(value, dict):
            value = dict(value)
            if "market_type" not in value and "market" in value:
                value["market_type"] = value["market"]
            if "instrument" not in value and "symbol" in value:
                value["instrument"] = value["symbol"]
        return value

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


class ReplayRequest(MarketRef):
    date: str | None = None
    mode: str = "l2-trade"
    depth: int = Field(default=20, ge=1)
    slug: str | None = None
    outcome: str | None = None
    market_id: str | None = None
    asset_id: str | None = None
    dates: tuple[str, ...] = ()
    start_ts_ms: int | None = None
    end_ts_ms: int | None = None

    @model_validator(mode="after")
    def validate_request(self) -> "ReplayRequest":
        if self.mode != "l2-trade":
            raise ValueError("only l2-trade replay mode is supported")
        if self.venue == Venue.HYPERLIQUID:
            if self.date is None:
                raise ValueError("date is required for hyperliquid replay requests")
            _parse_date(self.date)
            if self.depth < 1:
                raise ValueError("depth must be >= 1")
            return self
        if not self.slug:
            raise ValueError("slug is required for polymarket replay requests")
        if not self.outcome:
            raise ValueError("outcome is required for polymarket replay requests")
        if not self.market_id:
            raise ValueError("market_id is required for polymarket replay requests")
        if not self.asset_id:
            raise ValueError("asset_id is required for polymarket replay requests")
        if not self.dates:
            raise ValueError("dates are required for polymarket replay requests")
        if self.start_ts_ms is None or self.end_ts_ms is None:
            raise ValueError("start_ts_ms and end_ts_ms are required for polymarket replay requests")
        if self.end_ts_ms < self.start_ts_ms:
            raise ValueError("end_ts_ms must be on or after start_ts_ms")
        if self.depth > 5:
            raise ValueError("polymarket replay depth cannot exceed 5")
        for value in self.dates:
            _parse_date(value)
        return self

    def replay_id(self) -> str:
        if self.venue == Venue.HYPERLIQUID:
            canonical = (
                f"{self.venue.value}|{self.market_type.value}|{self.instrument}|"
                f"{self.date}|{self.mode}|depth={self.depth}"
            )
        else:
            canonical = (
                f"{self.venue.value}|{self.market_type.value}|{self.instrument}|"
                f"{self.market_id}|{self.asset_id}|{self.slug}|{self.outcome}|"
                f"{','.join(self.dates)}|{self.start_ts_ms}|{self.end_ts_ms}|"
                f"{self.mode}|depth={self.depth}"
            )
        return hashlib.sha256(canonical.encode("utf-8")).hexdigest()[:32]

    def market_ref(self) -> MarketRef:
        return MarketRef(
            venue=self.venue,
            market_type=self.market_type,
            instrument=self.instrument,
        )

    def replay_dates(self) -> tuple[str, ...]:
        if self.venue == Venue.HYPERLIQUID:
            assert self.date is not None
            return (self.date,)
        return self.dates


class IngestionRequest(MarketRef):
    start_date: str | None = None
    end_date: str | None = None
    start_offset_days: int | None = None
    end_offset_days: int | None = None

    @model_validator(mode="after")
    def validate_window(self) -> "IngestionRequest":
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

    def day_request(self, date: str) -> MarketRef:
        _parse_date(date)
        return MarketRef(
            venue=self.venue,
            market_type=self.market_type,
            instrument=self.instrument,
        )


class PolymarketReplayCreateRequest(BaseModel):
    slug: str
    outcome: str
    depth: int = Field(default=5, ge=1, le=5)

    @model_validator(mode="after")
    def validate_request(self) -> "PolymarketReplayCreateRequest":
        if not self.slug.strip():
            raise ValueError("slug is required")
        if not self.outcome.strip():
            raise ValueError("outcome is required")
        return self


class PolymarketSeriesSyncRequest(BaseModel):
    series: str
    start_date: str
    end_date: str
    outcomes: OutcomesMode = OutcomesMode.BOTH
    depth: int = Field(default=5, ge=1, le=5)

    @model_validator(mode="after")
    def validate_request(self) -> "PolymarketSeriesSyncRequest":
        if not self.series.strip():
            raise ValueError("series is required")
        _ = iter_dates_inclusive(self.start_date, self.end_date)
        return self


class PolymarketMarketResolution(BaseModel):
    venue: Venue = Venue.POLYMARKET
    market_type: MarketType = MarketType.BINARY
    slug: str
    question: str
    outcome: str
    market_id: str
    asset_id: str
    instrument: str
    start_time: str
    end_time: str
    start_ts_ms: int
    end_ts_ms: int
    dates: tuple[str, ...]
    price_to_beat: float | None = None
    price_to_beat_source: str | None = None
    price_to_beat_quality: str | None = None
    settlement_payout: float | None = None

    @model_validator(mode="after")
    def validate_resolution(self) -> "PolymarketMarketResolution":
        if not self.dates:
            raise ValueError("dates are required")
        for value in self.dates:
            _parse_date(value)
        if self.end_ts_ms < self.start_ts_ms:
            raise ValueError("end_ts_ms must be on or after start_ts_ms")
        return self

    def market_ref(self) -> MarketRef:
        return MarketRef(
            venue=self.venue,
            market_type=self.market_type,
            instrument=self.instrument,
        )

    @property
    def series_key(self) -> str:
        head, sep, tail = self.slug.rpartition("-")
        if sep and tail.isdigit():
            return head
        return self.slug

    def replay_request(self, *, depth: int) -> ReplayRequest:
        return ReplayRequest(
            venue=self.venue,
            market_type=self.market_type,
            instrument=self.instrument,
            depth=depth,
            slug=self.slug,
            outcome=self.outcome,
            market_id=self.market_id,
            asset_id=self.asset_id,
            dates=self.dates,
            start_ts_ms=self.start_ts_ms,
            end_ts_ms=self.end_ts_ms,
        )


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
    venue: Venue = Venue.HYPERLIQUID
    market_type: MarketType = MarketType.PERP
    instrument: str
    date: str
    hour: str
    status: CoverageStatus
    object_count: int = 0
    byte_count: int = 0
    row_count: int = 0
    updated_at: str
    source: str

    @model_validator(mode="before")
    @classmethod
    def apply_legacy_aliases(cls, value):
        if isinstance(value, dict):
            value = dict(value)
            if "instrument" not in value and "symbol" in value:
                value["instrument"] = value["symbol"]
            if "market_type" not in value and "market" in value:
                value["market_type"] = value["market"]
        return value


class CanonicalShardRecord(BaseModel):
    shard_id: str
    status: CanonicalShardStatus
    venue: Venue
    market_type: MarketType
    date: str
    depth: int
    shard_prefix: str
    manifest_s3_key: str
    event_count: int = 0
    instrument: str | None = None
    series_key: str | None = None
    outcomes: str | None = None
    start_ts_ms: int | None = None
    end_ts_ms: int | None = None
    created_at: str
    updated_at: str
    source_refs: tuple[str, ...] = ()
    files: tuple["CanonicalShardFile", ...] = ()
    error: str | None = None

    @model_validator(mode="before")
    @classmethod
    def apply_legacy_aliases(cls, value):
        if isinstance(value, dict):
            value = dict(value)
            if "shard_prefix" not in value:
                shard_key = value.get("shard_s3_key") or value.get("manifest_s3_key")
                if shard_key:
                    prefix = str(shard_key).rsplit("/", 1)[0]
                    value["shard_prefix"] = f"{prefix}/"
            if "files" not in value:
                value["files"] = ()
        return value


class CanonicalShardFile(BaseModel):
    family: CanonicalFileFamily
    file_name: str
    s3_key: str
    row_count: int
    size_bytes: int = 0


class CanonicalWindowManifest(BaseModel):
    venue: Venue
    market_type: MarketType
    start_date: str
    end_date: str
    depth: int
    instrument: str | None = None
    series_key: str | None = None
    outcomes: str | None = None
    shard_count: int
    event_count: int
    start_ts_ms: int | None
    end_ts_ms: int | None
    shard_ids: tuple[str, ...]
    shard_prefixes: tuple[str, ...]
    shards: tuple[CanonicalShardRecord, ...]
    files_path_template: str


def utc_now_iso() -> str:
    return datetime.now(tz=UTC).replace(microsecond=0).isoformat()


def coverage_pk(dataset_kind: DatasetKind, market: MarketRef, date: str, hour: str) -> str:
    return (
        f"{dataset_kind.value}#{market.venue}#{market.market_type.value}#"
        f"{market.encoded_instrument()}#{date}#{hour}"
    )


def replay_s3_key(request: ReplayRequest) -> str:
    replay_id = request.replay_id()
    if request.venue == Venue.POLYMARKET:
        assert request.market_id is not None
        return (
            "replays/"
            f"venue={request.venue.value}/market_type={request.market_type.value}/"
            f"instrument={request.encoded_instrument()}/market_id={quote(request.market_id, safe='')}/"
            f"depth={request.depth}/replay_id={replay_id}/events.jsonl.zst"
        )
    return (
        "replays/"
        f"venue={request.venue.value}/market_type={request.market_type.value}/"
        f"instrument={request.encoded_instrument()}/date={request.date}/"
        f"mode={request.mode}/depth={request.depth}/replay_id={replay_id}/events.jsonl.zst"
    )


def replay_manifest_s3_key(request: ReplayRequest) -> str:
    replay_id = request.replay_id()
    if request.venue == Venue.POLYMARKET:
        assert request.market_id is not None
        return (
            "replays/"
            f"venue={request.venue.value}/market_type={request.market_type.value}/"
            f"instrument={request.encoded_instrument()}/market_id={quote(request.market_id, safe='')}/"
            f"depth={request.depth}/replay_id={replay_id}/manifest.json"
        )
    return (
        "replays/"
        f"venue={request.venue.value}/market_type={request.market_type.value}/"
        f"instrument={request.encoded_instrument()}/date={request.date}/"
        f"mode={request.mode}/depth={request.depth}/replay_id={replay_id}/manifest.json"
    )


def canonical_hyperliquid_shard_id(market: MarketRef, date: str, depth: int) -> str:
    canonical = (
        f"canonical|{market.venue.value}|{market.market_type.value}|"
        f"{market.instrument}|{date}|depth={depth}"
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()[:32]


def canonical_polymarket_shard_id(
    *,
    series_key: str,
    date: str,
    outcomes: OutcomesMode,
    depth: int,
) -> str:
    canonical = f"canonical|polymarket|{series_key}|{date}|outcomes={outcomes.value}|depth={depth}"
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()[:32]


def canonical_hyperliquid_shard_prefix(market: MarketRef, date: str, depth: int) -> str:
    return (
        "canonical/hyperliquid/"
        f"market_type={market.market_type.value}/instrument={market.encoded_instrument()}/"
        f"date={date}/depth={depth}/"
    )


def canonical_hyperliquid_manifest_s3_key(market: MarketRef, date: str, depth: int) -> str:
    return f"{canonical_hyperliquid_shard_prefix(market, date, depth)}manifest.json"


def canonical_polymarket_shard_prefix(
    *,
    series_key: str,
    date: str,
    outcomes: OutcomesMode,
    depth: int,
) -> str:
    return (
        "canonical/polymarket/"
        f"series={quote(series_key, safe='')}/outcomes={outcomes.value}/"
        f"date={date}/depth={depth}/"
    )


def canonical_polymarket_manifest_s3_key(
    *,
    series_key: str,
    date: str,
    outcomes: OutcomesMode,
    depth: int,
) -> str:
    return f"{canonical_polymarket_shard_prefix(series_key=series_key, date=date, outcomes=outcomes, depth=depth)}manifest.json"


def canonical_shard_family_file_name(family: CanonicalFileFamily) -> str:
    return f"{family.value}.parquet"


def canonical_shard_family_s3_key(shard_prefix: str, family: CanonicalFileFamily) -> str:
    return f"{shard_prefix}{canonical_shard_family_file_name(family)}"


def normalized_l2_s3_key(market: MarketRef, date: str, hour: int) -> str:
    return (
        "normalized/hyperliquid/l2_snapshot/"
        f"market_type={market.market_type.value}/date={date}/hour={hour:02d}/"
        f"instrument={market.encoded_instrument()}/part-000.parquet"
    )


def normalized_trade_s3_key(market: MarketRef, date: str, hour: int) -> str:
    return (
        "normalized/hyperliquid/trade/"
        f"market_type={market.market_type.value}/date={date}/hour={hour:02d}/"
        f"instrument={market.encoded_instrument()}/part-000.parquet"
    )


def raw_l2_s3_key(market: MarketRef, date: str, hour: int) -> str:
    return (
        "raw/hyperliquid/l2book/"
        f"market_type={market.market_type.value}/date={date}/hour={hour:02d}/"
        f"instrument={market.encoded_instrument()}/{market.encoded_instrument()}.lz4"
    )


def raw_fill_s3_key(market: MarketRef, date: str, hour: int) -> str:
    return (
        "raw/hyperliquid/node_fills_by_block/"
        f"market_type={market.market_type.value}/date={date}/hour={hour:02d}/"
        f"instrument={market.encoded_instrument()}/part-{hour:02d}.lz4"
    )


def raw_trade_s3_key(market: MarketRef, date: str, hour: int) -> str:
    return raw_fill_s3_key(market, date, hour)


def polymarket_metadata_s3_key(resolution: PolymarketMarketResolution) -> str:
    return (
        "metadata/polymarket/"
        f"market_id={quote(resolution.market_id, safe='')}/"
        f"instrument={quote(resolution.instrument, safe='')}/manifest.json"
    )


def polymarket_raw_l2_s3_key(market: MarketRef, market_id: str, date: str) -> str:
    return (
        "raw/telonex/polymarket/channel=book_snapshot_5/"
        f"market_id={quote(market_id, safe='')}/instrument={market.encoded_instrument()}/"
        f"date={date}/part-000.parquet"
    )


def polymarket_raw_trade_s3_key(market: MarketRef, market_id: str, date: str) -> str:
    return (
        "raw/telonex/polymarket/channel=trades/"
        f"market_id={quote(market_id, safe='')}/instrument={market.encoded_instrument()}/"
        f"date={date}/part-000.parquet"
    )


def polymarket_normalized_l2_s3_key(market: MarketRef, market_id: str, date: str) -> str:
    return (
        "normalized/polymarket/kind=l2_snapshot/"
        f"market_id={quote(market_id, safe='')}/instrument={market.encoded_instrument()}/"
        f"date={date}/part-000.parquet"
    )


def polymarket_normalized_trade_s3_key(market: MarketRef, market_id: str, date: str) -> str:
    return (
        "normalized/polymarket/kind=trade/"
        f"market_id={quote(market_id, safe='')}/instrument={market.encoded_instrument()}/"
        f"date={date}/part-000.parquet"
    )


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
