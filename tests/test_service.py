from __future__ import annotations

from poochon_backtest_data.models import (
    CanonicalFileFamily,
    CanonicalShardFile,
    CanonicalShardRecord,
    CanonicalShardStatus,
    MarketRef,
    MarketType,
    Venue,
    canonical_hyperliquid_shard_id,
)
from poochon_backtest_data.service import CanonicalReplayService


class FakeS3Store:
    def __init__(self):
        self.objects: dict[str, bytes] = {}

    def exists(self, key: str) -> bool:
        return key in self.objects

    def get_bytes(self, key: str) -> bytes:
        return self.objects[key]


class FakeShardRepository:
    def __init__(self, items: dict[str, CanonicalShardRecord]):
        self.items = items

    def get(self, shard_id: str):
        return self.items.get(shard_id)


def test_canonical_replay_service_loads_manifest_and_downloads_shard_file() -> None:
    market = MarketRef(market_type=MarketType.PERP, instrument="BTC")
    shard = CanonicalShardRecord(
        shard_id=canonical_hyperliquid_shard_id(market, "2026-02-19", 20),
        status=CanonicalShardStatus.READY,
        venue=Venue.HYPERLIQUID,
        market_type=MarketType.PERP,
        instrument="BTC",
        series_key=None,
        outcomes=None,
        date="2026-02-19",
        depth=20,
        shard_prefix="canonical/hyperliquid/one/",
        manifest_s3_key="canonical/hyperliquid/one/manifest.json",
        event_count=1,
        start_ts_ms=1,
        end_ts_ms=1,
        created_at="2026-04-09T00:00:00+00:00",
        updated_at="2026-04-09T00:00:00+00:00",
        files=(
            CanonicalShardFile(
                family=CanonicalFileFamily.TRADES,
                file_name="trades.parquet",
                s3_key="canonical/hyperliquid/one/trades.parquet",
                row_count=1,
                size_bytes=12,
            ),
            CanonicalShardFile(
                family=CanonicalFileFamily.BOOKS,
                file_name="books.parquet",
                s3_key="canonical/hyperliquid/one/books.parquet",
                row_count=0,
                size_bytes=8,
            ),
        ),
    )
    s3 = FakeS3Store()
    s3.objects[shard.manifest_s3_key] = b"{}"
    s3.objects["canonical/hyperliquid/one/trades.parquet"] = b"PAR1-trades"
    s3.objects["canonical/hyperliquid/one/books.parquet"] = b"PAR1-books"
    service = CanonicalReplayService(
        s3_store=s3,
        shard_repo=FakeShardRepository({shard.shard_id: shard}),
    )

    manifest = service.get_hyperliquid_manifest(
        market_type=MarketType.PERP,
        instrument="BTC",
        start_date="2026-02-19",
        end_date="2026-02-19",
        depth=20,
    )
    payload, media_type = service.get_shard_file(
        shard_id=shard.shard_id,
        file_name="trades.parquet",
    )

    assert manifest.event_count == 1
    assert manifest.files_path_template.endswith("/files/{file_name}")
    assert manifest.shard_prefixes == ("canonical/hyperliquid/one/",)
    assert payload == b"PAR1-trades"
    assert media_type == "application/vnd.apache.parquet"


def test_canonical_shard_record_accepts_legacy_shard_shape() -> None:
    shard = CanonicalShardRecord.model_validate(
        {
            "shard_id": "legacy",
            "status": "READY",
            "venue": "hyperliquid",
            "market_type": "perp",
            "date": "2026-02-19",
            "depth": 20,
            "shard_s3_key": "canonical/hyperliquid/market_type=perp/instrument=BTC/date=2026-02-19/depth=20/events.jsonl.zst",
            "manifest_s3_key": "canonical/hyperliquid/market_type=perp/instrument=BTC/date=2026-02-19/depth=20/manifest.json",
            "event_count": 10,
            "instrument": "BTC",
            "series_key": None,
            "outcomes": None,
            "start_ts_ms": 1,
            "end_ts_ms": 2,
            "created_at": "2026-04-13T00:00:00+00:00",
            "updated_at": "2026-04-13T00:00:00+00:00",
            "source_refs": [],
            "error": None,
        }
    )

    assert (
        shard.shard_prefix
        == "canonical/hyperliquid/market_type=perp/instrument=BTC/date=2026-02-19/depth=20/"
    )
    assert shard.files == ()


def test_canonical_replay_service_raises_when_window_is_missing() -> None:
    service = CanonicalReplayService(
        s3_store=FakeS3Store(),
        shard_repo=FakeShardRepository({}),
    )
    try:
        service.get_polymarket_manifest(
            series_key="btc-updown-5m",
            start_date="2026-02-19",
            end_date="2026-02-21",
        )
    except ValueError as error:
        assert "missing canonical polymarket shards" in str(error)
    else:
        raise AssertionError("expected missing shard error")
