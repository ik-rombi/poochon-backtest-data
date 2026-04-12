from __future__ import annotations

import argparse
import logging

import uvicorn

from .api import create_app
from .hyperliquid import sync_window
from .models import (
    IngestionRequest,
    MarketType,
    OutcomesMode,
    PolymarketSeriesSyncRequest,
    iter_dates_inclusive,
)
from .polymarket_telonex import sync_series
from .canonical import build_polymarket_canonical_day_from_storage
from .settings import get_settings
from .storage import CanonicalShardRepository, CoverageRepository, S3Store, boto3_session


def _configure_logging(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    logging.getLogger("botocore.credentials").setLevel(logging.WARNING)
    logging.getLogger("httpx").setLevel(logging.WARNING)


def _session_bundle():
    settings = get_settings()
    if not settings.data_bucket or not settings.coverage_table_name or not settings.shard_table_name:
        raise RuntimeError("POOCHON_DATA_BUCKET, POOCHON_COVERAGE_TABLE_NAME, and POOCHON_SHARD_TABLE_NAME are required")
    session = boto3_session(settings.aws_region)
    return (
        settings,
        S3Store(session, settings.data_bucket),
        CoverageRepository(session, settings.coverage_table_name),
        CanonicalShardRepository(session, settings.shard_table_name),
    )


def _require_telonex_api_key() -> str:
    settings = get_settings()
    if not settings.telonex_api_key:
        raise RuntimeError("POOCHON_TELONEX_API_KEY is required")
    return settings.telonex_api_key


def main() -> None:
    parser = argparse.ArgumentParser(prog="poochon-backtest-data")
    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser("api")

    hyperliquid_sync_parser = subparsers.add_parser("hyperliquid-sync-window")
    hyperliquid_sync_parser.add_argument(
        "--market-type", choices=[MarketType.PERP.value, MarketType.SPOT.value], required=True
    )
    hyperliquid_sync_parser.add_argument("--instrument", required=True)
    hyperliquid_sync_parser.add_argument("--start-date")
    hyperliquid_sync_parser.add_argument("--end-date")
    hyperliquid_sync_parser.add_argument("--start-offset-days", type=int)
    hyperliquid_sync_parser.add_argument("--end-offset-days", type=int)
    hyperliquid_sync_parser.add_argument("--depth", type=int, default=20)

    polymarket_sync_parser = subparsers.add_parser("polymarket-sync-series")
    polymarket_sync_parser.add_argument("--series", required=True)
    polymarket_sync_parser.add_argument("--start-date", required=True)
    polymarket_sync_parser.add_argument("--end-date", required=True)
    polymarket_sync_parser.add_argument(
        "--outcomes",
        choices=[item.value for item in OutcomesMode],
        default=OutcomesMode.BOTH.value,
    )
    polymarket_sync_parser.add_argument("--depth", type=int, default=5)

    polymarket_build_parser = subparsers.add_parser("polymarket-build-canonical-window")
    polymarket_build_parser.add_argument("--series", required=True)
    polymarket_build_parser.add_argument("--start-date", required=True)
    polymarket_build_parser.add_argument("--end-date", required=True)
    polymarket_build_parser.add_argument(
        "--outcomes",
        choices=[item.value for item in OutcomesMode],
        default=OutcomesMode.BOTH.value,
    )
    polymarket_build_parser.add_argument("--depth", type=int, default=5)
    polymarket_build_parser.add_argument("--force", action="store_true")

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

    settings, s3_store, coverage_repo, shard_repo = _session_bundle()
    if args.command == "hyperliquid-sync-window":
        sync_window(
            s3_store,
            coverage_repo,
            shard_repo,
            request=IngestionRequest(
                market_type=args.market_type,
                instrument=args.instrument,
                start_date=args.start_date,
                end_date=args.end_date,
                start_offset_days=args.start_offset_days,
                end_offset_days=args.end_offset_days,
            ),
            request_payer=settings.request_payer,
            depth=args.depth,
        )
        return

    if args.command == "polymarket-sync-series":
        sync_series(
            s3_store,
            coverage_repo,
            shard_repo,
            request=PolymarketSeriesSyncRequest(
                series=args.series,
                start_date=args.start_date,
                end_date=args.end_date,
                outcomes=args.outcomes,
                depth=args.depth,
            ),
            telonex_api_key=_require_telonex_api_key(),
        )
        return

    if args.command == "polymarket-build-canonical-window":
        for date in iter_dates_inclusive(args.start_date, args.end_date):
            build_polymarket_canonical_day_from_storage(
                date=date,
                series_key=args.series,
                outcomes=OutcomesMode(args.outcomes),
                depth=args.depth,
                s3_store=s3_store,
                shard_repo=shard_repo,
                force=args.force,
            )
        return

    raise RuntimeError(f"unsupported command: {args.command}")


if __name__ == "__main__":
    main()
