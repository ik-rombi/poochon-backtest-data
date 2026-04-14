from __future__ import annotations

from fastapi.testclient import TestClient

from poochon_backtest_data.api import create_app
from poochon_backtest_data.models import (
    CanonicalFileFamily,
    CanonicalShardFile,
    CanonicalShardRecord,
    CanonicalShardStatus,
    CanonicalWindowManifest,
    MarketType,
    Venue,
)
from poochon_backtest_data.settings import Settings


class FakeCanonicalReplayService:
    def __init__(self):
        self.hyper_manifest = CanonicalWindowManifest(
            venue=Venue.HYPERLIQUID,
            market_type=MarketType.PERP,
            instrument="BTC",
            series_key=None,
            outcomes=None,
            start_date="2026-02-19",
            end_date="2026-02-21",
            depth=20,
            shard_count=1,
            event_count=2,
            start_ts_ms=1,
            end_ts_ms=2,
            shard_ids=("shard-1",),
            shard_prefixes=("canonical/hyperliquid/one/",),
            shards=(
                CanonicalShardRecord(
                    shard_id="shard-1",
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
                    event_count=2,
                    start_ts_ms=1,
                    end_ts_ms=2,
                    created_at="2026-04-09T00:00:00+00:00",
                    updated_at="2026-04-09T00:00:00+00:00",
                    files=(
                        CanonicalShardFile(
                            family=CanonicalFileFamily.TRADES,
                            file_name="trades.parquet",
                            s3_key="canonical/hyperliquid/one/trades.parquet",
                            row_count=1,
                            size_bytes=11,
                        ),
                        CanonicalShardFile(
                            family=CanonicalFileFamily.BOOKS,
                            file_name="books.parquet",
                            s3_key="canonical/hyperliquid/one/books.parquet",
                            row_count=1,
                            size_bytes=12,
                        ),
                    ),
                ),
            ),
            files_path_template="/api/v1/canonical/shards/{shard_id}/files/{file_name}",
        )

    def get_hyperliquid_manifest(self, **_: object):
        return self.hyper_manifest

    def get_polymarket_manifest(self, **_: object):
        return self.hyper_manifest.model_copy(
            update={
                "venue": Venue.POLYMARKET,
                "market_type": MarketType.BINARY,
                "instrument": None,
                "series_key": "btc-updown-5m",
                "outcomes": "both",
            }
        )

    def get_shard_file(self, *, shard_id: str, file_name: str):
        assert shard_id == "shard-1"
        return (f"payload:{file_name}".encode("utf-8"), "application/vnd.apache.parquet")


def test_post_replay_endpoints_are_deprecated() -> None:
    client = TestClient(create_app(Settings(), replay_service=FakeCanonicalReplayService()))
    assert client.post("/api/v1/replays").status_code == 410
    assert client.post("/api/v1/polymarket/replays").status_code == 410


def test_get_hyperliquid_manifest_and_download_file() -> None:
    client = TestClient(create_app(Settings(), replay_service=FakeCanonicalReplayService()))
    manifest = client.get(
        "/api/v1/canonical/hyperliquid/perp/BTC",
        params={"start_date": "2026-02-19", "end_date": "2026-02-21", "depth": 20},
    )
    assert manifest.status_code == 200
    assert manifest.json()["event_count"] == 2
    assert manifest.json()["files_path_template"].endswith("/files/{file_name}")

    download = client.get("/api/v1/canonical/shards/shard-1/files/trades.parquet")
    assert download.status_code == 200
    assert download.headers["content-type"].startswith("application/vnd.apache.parquet")
    assert download.content == b"payload:trades.parquet"


def test_get_polymarket_manifest_and_download_file() -> None:
    client = TestClient(create_app(Settings(), replay_service=FakeCanonicalReplayService()))
    manifest = client.get(
        "/api/v1/canonical/polymarket/btc-updown-5m",
        params={
            "start_date": "2026-02-19",
            "end_date": "2026-02-21",
            "outcomes": "both",
            "depth": 5,
        },
    )
    assert manifest.status_code == 200
    assert manifest.json()["series_key"] == "btc-updown-5m"

    download = client.get("/api/v1/canonical/shards/shard-1/files/books.parquet")
    assert download.status_code == 200
    assert download.content == b"payload:books.parquet"
