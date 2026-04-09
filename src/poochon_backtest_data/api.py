from __future__ import annotations

from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse, StreamingResponse

from .models import ReplayRequest, ReplayStatus
from .service import ReplayService
from .settings import Settings, get_settings
from .storage import CoverageRepository, ReplayRepository, S3Store, boto3_session


def create_app(
    settings: Settings | None = None,
    replay_service: ReplayService | None = None,
) -> FastAPI:
    settings = settings or get_settings()
    if replay_service is None:
        if not settings.data_bucket or not settings.coverage_table_name or not settings.replay_table_name:
            raise RuntimeError("POOCHON_DATA_BUCKET, POOCHON_COVERAGE_TABLE_NAME, and POOCHON_REPLAY_TABLE_NAME are required")

        session = boto3_session(settings.aws_region)
        replay_service = ReplayService(
            s3_store=S3Store(session, settings.data_bucket),
            coverage_repo=CoverageRepository(session, settings.coverage_table_name),
            replay_repo=ReplayRepository(session, settings.replay_table_name),
            stepfunctions_client=session.client("stepfunctions"),
            replay_state_machine_arn=settings.replay_state_machine_arn,
        )

    app = FastAPI(title="Poochon Backtest Data API", version="0.1.0")
    app.state.replay_service = replay_service

    @app.get("/api/v1/health")
    def health() -> dict[str, str]:
        return {"status": "ok"}

    @app.post("/api/v1/replays")
    def create_replay(request: ReplayRequest):
        try:
            record = replay_service.submit_replay(request)
        except ValueError as error:
            raise HTTPException(status_code=409, detail=str(error)) from error
        status_code = 200 if record.status == ReplayStatus.READY else 202
        return JSONResponse(status_code=status_code, content=record.model_dump(mode="json"))

    @app.get("/api/v1/replays/{replay_id}")
    def get_replay(replay_id: str):
        record = replay_service.get_replay(replay_id)
        if record is None:
            raise HTTPException(status_code=404, detail=f"unknown replay_id: {replay_id}")
        return record

    @app.get("/api/v1/replays/{replay_id}/stream")
    def stream_replay(replay_id: str):
        try:
            stream = replay_service.stream_replay(replay_id)
        except KeyError as error:
            raise HTTPException(status_code=404, detail=f"unknown replay_id: {replay_id}") from error
        except RuntimeError as error:
            raise HTTPException(status_code=409, detail=str(error)) from error
        return StreamingResponse(stream, media_type="application/x-ndjson")

    return app
