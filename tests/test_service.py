from __future__ import annotations

import io

import orjson
import zstandard

from poochon_backtest_data.models import (
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

    def stream_zstd(self, key: str, chunk_size: int = 64 * 1024):
        with zstandard.ZstdDecompressor().stream_reader(io.BytesIO(self.objects[key])) as reader:
            while True:
                chunk = reader.read(chunk_size)
                if not chunk:
                    break
                yield chunk


class FakeShardRepository:
    def __init__(self, items: dict[str, CanonicalShardRecord]):
        self.items = items

    def get(self, shard_id: str):
        return self.items.get(shard_id)


def zstd_bytes(*lines: dict) -> bytes:
    payload = b"".join(orjson.dumps(line) + b"\n" for line in lines)
    return zstandard.ZstdCompressor(level=1).compress(payload)


def test_canonical_replay_service_loads_shards_and_streams() -> None:
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
        shard_s3_key="canonical/hyperliquid/one",
        manifest_s3_key="canonical/hyperliquid/one.json",
        event_count=1,
        start_ts_ms=1,
        end_ts_ms=1,
        created_at="2026-04-09T00:00:00+00:00",
        updated_at="2026-04-09T00:00:00+00:00",
    )
    s3 = FakeS3Store()
    s3.objects["canonical/hyperliquid/one"] = zstd_bytes(
        {
            "Market": {
                "Trade": {
                    "instrument": {
                        "Hyperliquid": {
                            "market_type": "Perp",
                            "symbol": "BTC",
                        }
                    },
                    "ts_ms": 1,
                    "px": 1.0,
                    "sz": 1.0,
                    "side": "Buy",
                }
            }
        }
    )
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

    stream = b"".join(service.stream_manifest(manifest)).decode("utf-8")
    assert manifest.event_count == 1
    assert '"Hyperliquid"' in stream


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
