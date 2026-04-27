from __future__ import annotations

from dataclasses import dataclass

from ..settings import Settings, get_settings
from ..storage import (
    CanonicalShardRepository,
    CoverageRepository,
    S3Store,
    boto3_session,
)


@dataclass
class SessionBundle:
    settings: Settings
    s3_store: S3Store
    coverage_repo: CoverageRepository
    shard_repo: CanonicalShardRepository


def open_session() -> SessionBundle:
    settings = get_settings()
    if not settings.data_bucket or not settings.coverage_table_name or not settings.shard_table_name:
        raise RuntimeError(
            "POOCHON_DATA_BUCKET, POOCHON_COVERAGE_TABLE_NAME, and POOCHON_SHARD_TABLE_NAME are required"
        )
    session = boto3_session(settings.aws_region)
    return SessionBundle(
        settings=settings,
        s3_store=S3Store(session, settings.data_bucket),
        coverage_repo=CoverageRepository(session, settings.coverage_table_name),
        shard_repo=CanonicalShardRepository(session, settings.shard_table_name),
    )
