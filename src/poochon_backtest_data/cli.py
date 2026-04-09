from __future__ import annotations

import argparse
import logging

import boto3
import uvicorn

from .api import create_app
from .hyperliquid import backfill_day, normalize_day
from .models import ReplayRequest
from .service import materialize_replay
from .settings import get_settings
from .storage import CoverageRepository, ReplayRepository, S3Store, boto3_session


def _configure_logging(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )


def _session_bundle():
    settings = get_settings()
    if not settings.data_bucket or not settings.coverage_table_name or not settings.replay_table_name:
        raise RuntimeError("required AWS settings are missing")
    session = boto3_session(settings.aws_region)
    return (
        settings,
        S3Store(session, settings.data_bucket),
        CoverageRepository(session, settings.coverage_table_name),
        ReplayRepository(session, settings.replay_table_name),
    )


def main() -> None:
    parser = argparse.ArgumentParser(prog="poochon-backtest-data")
    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser("api")

    backfill_parser = subparsers.add_parser("backfill-day")
    backfill_parser.add_argument("--symbol", required=True)
    backfill_parser.add_argument("--date", required=True)

    normalize_parser = subparsers.add_parser("normalize-day")
    normalize_parser.add_argument("--symbol", required=True)
    normalize_parser.add_argument("--date", required=True)

    materialize_parser = subparsers.add_parser("materialize-replay")
    materialize_parser.add_argument("--symbol", required=True)
    materialize_parser.add_argument("--date", required=True)

    args = parser.parse_args()
    settings = get_settings()
    _configure_logging(settings.log_level)

    if args.command == "api":
        uvicorn.run(
            "poochon_backtest_data.api:create_app",
            factory=True,
            host="0.0.0.0",
            port=settings.port,
        )
        return

    settings, s3_store, coverage_repo, replay_repo = _session_bundle()
    if args.command == "backfill-day":
        backfill_day(
            s3_store,
            coverage_repo,
            symbol=args.symbol,
            date=args.date,
            request_payer=settings.request_payer,
        )
        return

    if args.command == "normalize-day":
        normalize_day(s3_store, coverage_repo, symbol=args.symbol, date=args.date)
        return

    if args.command == "materialize-replay":
        materialize_replay(
            request=ReplayRequest(symbol=args.symbol, date=args.date),
            s3_store=s3_store,
            coverage_repo=coverage_repo,
            replay_repo=replay_repo,
        )
        return

    raise RuntimeError(f"unsupported command: {args.command}")
