from __future__ import annotations

from fastapi.testclient import TestClient

from poochon_backtest_data.api import create_app
from poochon_backtest_data.models import (
    PolymarketReplayCreateRequest,
    ReplayRequest,
    ReplayStatus,
    new_pending_replay,
)
from poochon_backtest_data.settings import Settings


class FakeReplayService:
    def __init__(self):
        self.record = new_pending_replay(
            ReplayRequest(market_type="perp", instrument="BTC", date="2025-05-24")
        )

    def submit_replay(self, request: ReplayRequest):
        return self.record

    def submit_polymarket_replay(self, request: PolymarketReplayCreateRequest):
        return self.record

    def get_replay(self, replay_id: str):
        if replay_id != self.record.replay_id:
            return None
        return self.record

    def stream_replay(self, replay_id: str):
        if replay_id != self.record.replay_id:
            raise KeyError(replay_id)
        if self.record.status != ReplayStatus.READY:
            raise RuntimeError(f"replay {replay_id} is not ready")
        return iter([b'{"Market":{"Trade":{"instrument":{"venue":"Hyperliquid","symbol":"BTC"},"ts_ms":1,"px":100.0,"sz":0.1,"side":"Buy"}}}\n'])


def test_api_returns_202_for_pending_replay() -> None:
    client = TestClient(create_app(Settings(), replay_service=FakeReplayService()))
    response = client.post(
        "/api/v1/replays",
        json={"market_type": "perp", "instrument": "BTC", "date": "2025-05-24"},
    )
    assert response.status_code == 202


def test_api_streams_ndjson_when_replay_ready() -> None:
    service = FakeReplayService()
    service.record.status = ReplayStatus.READY
    client = TestClient(create_app(Settings(), replay_service=service))
    response = client.get(f"/api/v1/replays/{service.record.replay_id}/stream")
    assert response.status_code == 200
    assert response.headers["content-type"].startswith("application/x-ndjson")
    assert '"Trade"' in response.text


def test_api_accepts_polymarket_replay_creation() -> None:
    client = TestClient(create_app(Settings(), replay_service=FakeReplayService()))
    response = client.post(
        "/api/v1/polymarket/replays",
        json={"slug": "btc-updown-5m-1775181000", "outcome": "Up", "depth": 5},
    )
    assert response.status_code == 202
