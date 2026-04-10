from __future__ import annotations

from typing import Iterator

from .models import (
    CanonicalShardRecord,
    CanonicalWindowManifest,
    MarketRef,
    MarketType,
    OutcomesMode,
    Venue,
    canonical_hyperliquid_shard_id,
    canonical_polymarket_shard_id,
    iter_dates_inclusive,
)
from .storage import CanonicalShardRepository, S3Store


class CanonicalReplayService:
    def __init__(
        self,
        *,
        s3_store: S3Store,
        shard_repo: CanonicalShardRepository,
    ):
        self.s3_store = s3_store
        self.shard_repo = shard_repo

    def get_hyperliquid_manifest(
        self,
        *,
        market_type: MarketType,
        instrument: str,
        start_date: str,
        end_date: str,
        depth: int = 20,
    ) -> CanonicalWindowManifest:
        market = MarketRef(
            venue=Venue.HYPERLIQUID,
            market_type=market_type,
            instrument=instrument,
        )
        shards = self._load_hyperliquid_shards(
            market=market,
            start_date=start_date,
            end_date=end_date,
            depth=depth,
        )
        return self._manifest_from_shards(
            venue=Venue.HYPERLIQUID,
            market_type=market_type,
            instrument=instrument,
            series_key=None,
            outcomes=None,
            start_date=start_date,
            end_date=end_date,
            depth=depth,
            shards=shards,
            stream_path=f"/api/v1/canonical/hyperliquid/{market_type.value}/{instrument}/stream",
        )

    def get_polymarket_manifest(
        self,
        *,
        series_key: str,
        start_date: str,
        end_date: str,
        outcomes: OutcomesMode = OutcomesMode.BOTH,
        depth: int = 5,
    ) -> CanonicalWindowManifest:
        shards = self._load_polymarket_shards(
            series_key=series_key,
            start_date=start_date,
            end_date=end_date,
            outcomes=outcomes,
            depth=depth,
        )
        return self._manifest_from_shards(
            venue=Venue.POLYMARKET,
            market_type=MarketType.BINARY,
            instrument=None,
            series_key=series_key,
            outcomes=outcomes.value,
            start_date=start_date,
            end_date=end_date,
            depth=depth,
            shards=shards,
            stream_path=f"/api/v1/canonical/polymarket/{series_key}/stream",
        )

    def stream_manifest(self, manifest: CanonicalWindowManifest) -> Iterator[bytes]:
        for shard in manifest.shards:
            yield from self.s3_store.stream_zstd(shard.shard_s3_key)

    def _load_hyperliquid_shards(
        self,
        *,
        market: MarketRef,
        start_date: str,
        end_date: str,
        depth: int,
    ) -> tuple[CanonicalShardRecord, ...]:
        shards: list[CanonicalShardRecord] = []
        missing: list[str] = []
        for date in iter_dates_inclusive(start_date, end_date):
            shard_id = canonical_hyperliquid_shard_id(market, date, depth)
            shard = self.shard_repo.get(shard_id)
            if shard is None or not self.s3_store.exists(shard.shard_s3_key):
                missing.append(date)
                continue
            shards.append(shard)
        if missing:
            missing_csv = ", ".join(missing)
            raise ValueError(
                f"missing canonical hyperliquid shards for {market.instrument} on: {missing_csv}"
            )
        return tuple(shards)

    def _load_polymarket_shards(
        self,
        *,
        series_key: str,
        start_date: str,
        end_date: str,
        outcomes: OutcomesMode,
        depth: int,
    ) -> tuple[CanonicalShardRecord, ...]:
        shards: list[CanonicalShardRecord] = []
        missing: list[str] = []
        for date in iter_dates_inclusive(start_date, end_date):
            shard_id = canonical_polymarket_shard_id(
                series_key=series_key,
                date=date,
                outcomes=outcomes,
                depth=depth,
            )
            shard = self.shard_repo.get(shard_id)
            if shard is None or not self.s3_store.exists(shard.shard_s3_key):
                missing.append(date)
                continue
            shards.append(shard)
        if missing:
            missing_csv = ", ".join(missing)
            raise ValueError(
                f"missing canonical polymarket shards for {series_key} on: {missing_csv}"
            )
        return tuple(shards)

    def _manifest_from_shards(
        self,
        *,
        venue: Venue,
        market_type: MarketType,
        instrument: str | None,
        series_key: str | None,
        outcomes: str | None,
        start_date: str,
        end_date: str,
        depth: int,
        shards: tuple[CanonicalShardRecord, ...],
        stream_path: str,
    ) -> CanonicalWindowManifest:
        event_count = sum(shard.event_count for shard in shards)
        start_ts_ms = min(
            (shard.start_ts_ms for shard in shards if shard.start_ts_ms is not None),
            default=None,
        )
        end_ts_ms = max(
            (shard.end_ts_ms for shard in shards if shard.end_ts_ms is not None),
            default=None,
        )
        return CanonicalWindowManifest(
            venue=venue,
            market_type=market_type,
            instrument=instrument,
            series_key=series_key,
            outcomes=outcomes,
            start_date=start_date,
            end_date=end_date,
            depth=depth,
            shard_count=len(shards),
            event_count=event_count,
            start_ts_ms=start_ts_ms,
            end_ts_ms=end_ts_ms,
            shard_ids=tuple(shard.shard_id for shard in shards),
            shard_keys=tuple(shard.shard_s3_key for shard in shards),
            shards=shards,
            stream_path=stream_path,
        )
