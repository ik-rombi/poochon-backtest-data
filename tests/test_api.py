from __future__ import annotations

from fastapi.testclient import TestClient

from poochon_backtest_data.api import create_app
from poochon_backtest_data.models import (
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
            shard_keys=("canonical/hyperliquid/one",),
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
                    shard_s3_key="canonical/hyperliquid/one",
                    manifest_s3_key="canonical/hyperliquid/one.json",
                    event_count=2,
                    start_ts_ms=1,
                    end_ts_ms=2,
                    created_at="2026-04-09T00:00:00+00:00",
                    updated_at="2026-04-09T00:00:00+00:00",
                ),
            ),
            stream_path="/api/v1/canonical/hyperliquid/perp/BTC/stream",
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
                "stream_path": "/api/v1/canonical/polymarket/btc-updown-5m/stream",
            }
        )

    def stream_manifest(self, manifest: CanonicalWindowManifest):
        if manifest.venue == Venue.HYPERLIQUID:
            return iter(
                [
                    b'{"Market":{"Trade":{"instrument":{"Hyperliquid":{"market_type":"Perp","symbol":"BTC"}},"ts_ms":1,"px":100.0,"sz":0.1,"side":"Buy"}}}\n'
                ]
            )
        return iter(
            [
                b'{"Market":{"Trade":{"instrument":{"Polymarket":{"symbol":"btc-updown-5m-1:Up"}},"ts_ms":1,"px":0.5,"sz":10.0,"side":"Buy"}}}\n'
            ]
        )


def test_post_replay_endpoints_are_deprecated() -> None:
    client = TestClient(create_app(Settings(), replay_service=FakeCanonicalReplayService()))
    assert client.post("/api/v1/replays").status_code == 410
    assert client.post("/api/v1/polymarket/replays").status_code == 410


def test_get_hyperliquid_manifest_and_stream() -> None:
    client = TestClient(create_app(Settings(), replay_service=FakeCanonicalReplayService()))
    manifest = client.get(
        "/api/v1/canonical/hyperliquid/perp/BTC",
        params={"start_date": "2026-02-19", "end_date": "2026-02-21", "depth": 20},
    )
    assert manifest.status_code == 200
    assert manifest.json()["event_count"] == 2

    stream = client.get(
        "/api/v1/canonical/hyperliquid/perp/BTC/stream",
        params={"start_date": "2026-02-19", "end_date": "2026-02-21", "depth": 20},
    )
    assert stream.status_code == 200
    assert stream.headers["content-type"].startswith("application/x-ndjson")
    assert '"Hyperliquid"' in stream.text


def test_get_polymarket_manifest_and_stream() -> None:
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

    stream = client.get(
        "/api/v1/canonical/polymarket/btc-updown-5m/stream",
        params={
            "start_date": "2026-02-19",
            "end_date": "2026-02-21",
            "outcomes": "both",
            "depth": 5,
        },
    )
    assert stream.status_code == 200
    assert '"Polymarket"' in stream.text
