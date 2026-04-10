from __future__ import annotations

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse, StreamingResponse

from .models import MarketType, OutcomesMode
from .service import CanonicalReplayService
from .settings import Settings, get_settings
from .storage import CanonicalShardRepository, S3Store, boto3_session


def create_app(
    settings: Settings | None = None,
    replay_service: CanonicalReplayService | None = None,
) -> FastAPI:
    settings = settings or get_settings()
    if replay_service is None:
        if not settings.data_bucket or not settings.shard_table_name:
            raise RuntimeError("POOCHON_DATA_BUCKET and POOCHON_SHARD_TABLE_NAME are required")

        session = boto3_session(settings.aws_region)
        replay_service = CanonicalReplayService(
            s3_store=S3Store(session, settings.data_bucket),
            shard_repo=CanonicalShardRepository(session, settings.shard_table_name),
        )

    app = FastAPI(title="Poochon Backtest Data API", version="0.2.0")
    app.state.replay_service = replay_service

    @app.get("/api/v1/health")
    def health() -> dict[str, str]:
        return {"status": "ok"}

    @app.post("/api/v1/replays")
    def create_replay():
        return JSONResponse(
            status_code=410,
            content={"detail": "replay creation is deprecated; use GET /api/v1/canonical/..."},
        )

    @app.post("/api/v1/polymarket/replays")
    def create_polymarket_replay():
        return JSONResponse(
            status_code=410,
            content={"detail": "replay creation is deprecated; use GET /api/v1/canonical/..."},
        )

    @app.get("/api/v1/canonical/hyperliquid/{market_type}/{instrument}")
    def get_hyperliquid_manifest(
        market_type: MarketType,
        instrument: str,
        start_date: str = Query(...),
        end_date: str = Query(...),
        depth: int = Query(20, ge=1),
    ):
        try:
            manifest = replay_service.get_hyperliquid_manifest(
                market_type=market_type,
                instrument=instrument,
                start_date=start_date,
                end_date=end_date,
                depth=depth,
            )
        except ValueError as error:
            raise HTTPException(status_code=409, detail=str(error)) from error
        return manifest

    @app.get("/api/v1/canonical/hyperliquid/{market_type}/{instrument}/stream")
    def stream_hyperliquid_window(
        market_type: MarketType,
        instrument: str,
        start_date: str = Query(...),
        end_date: str = Query(...),
        depth: int = Query(20, ge=1),
    ):
        try:
            manifest = replay_service.get_hyperliquid_manifest(
                market_type=market_type,
                instrument=instrument,
                start_date=start_date,
                end_date=end_date,
                depth=depth,
            )
        except ValueError as error:
            raise HTTPException(status_code=409, detail=str(error)) from error
        return StreamingResponse(
            replay_service.stream_manifest(manifest),
            media_type="application/x-ndjson",
        )

    @app.get("/api/v1/canonical/polymarket/{series_key}")
    def get_polymarket_manifest(
        series_key: str,
        start_date: str = Query(...),
        end_date: str = Query(...),
        outcomes: OutcomesMode = Query(OutcomesMode.BOTH),
        depth: int = Query(5, ge=1),
    ):
        try:
            manifest = replay_service.get_polymarket_manifest(
                series_key=series_key,
                start_date=start_date,
                end_date=end_date,
                outcomes=outcomes,
                depth=depth,
            )
        except ValueError as error:
            raise HTTPException(status_code=409, detail=str(error)) from error
        return manifest

    @app.get("/api/v1/canonical/polymarket/{series_key}/stream")
    def stream_polymarket_window(
        series_key: str,
        start_date: str = Query(...),
        end_date: str = Query(...),
        outcomes: OutcomesMode = Query(OutcomesMode.BOTH),
        depth: int = Query(5, ge=1),
    ):
        try:
            manifest = replay_service.get_polymarket_manifest(
                series_key=series_key,
                start_date=start_date,
                end_date=end_date,
                outcomes=outcomes,
                depth=depth,
            )
        except ValueError as error:
            raise HTTPException(status_code=409, detail=str(error)) from error
        return StreamingResponse(
            replay_service.stream_manifest(manifest),
            media_type="application/x-ndjson",
        )

    return app
