from __future__ import annotations

import argparse
import logging

import uvicorn

from .api import create_app
from .hyperliquid import backfill_day, ingest_range, normalize_day
from .models import (
    IngestionRequest,
    MarketRef,
    MarketType,
    PolymarketReplayCreateRequest,
    ReplayRequest,
    Venue,
)
from .polymarket_telonex import ingest_market, resolve_market
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


def _add_market_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--market-type", choices=[item.value for item in MarketType], required=True)
    parser.add_argument("--instrument", required=True)


def _market_ref_from_args(args: argparse.Namespace) -> MarketRef:
    return MarketRef(
        market_type=args.market_type,
        instrument=args.instrument,
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

    ingest_parser = subparsers.add_parser("ingest-range")
    _add_market_args(ingest_parser)
    ingest_parser.add_argument("--start-date")
    ingest_parser.add_argument("--end-date")
    ingest_parser.add_argument("--start-offset-days", type=int)
    ingest_parser.add_argument("--end-offset-days", type=int)

    backfill_parser = subparsers.add_parser("backfill-day")
    _add_market_args(backfill_parser)
    backfill_parser.add_argument("--date", required=True)

    normalize_parser = subparsers.add_parser("normalize-day")
    _add_market_args(normalize_parser)
    normalize_parser.add_argument("--date", required=True)

    materialize_parser = subparsers.add_parser("materialize-replay")
    _add_market_args(materialize_parser)
    materialize_parser.add_argument("--date", required=True)
    materialize_parser.add_argument("--depth", type=int, default=20)

    polymarket_ingest_parser = subparsers.add_parser("polymarket-ingest-market")
    polymarket_ingest_parser.add_argument("--slug", required=True)
    polymarket_ingest_parser.add_argument("--outcome", required=True)

    polymarket_materialize_parser = subparsers.add_parser("polymarket-materialize-replay")
    polymarket_materialize_parser.add_argument("--slug", required=True)
    polymarket_materialize_parser.add_argument("--outcome", required=True)
    polymarket_materialize_parser.add_argument("--depth", type=int, default=5)

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
    if args.command == "ingest-range":
        ingest_range(
            s3_store,
            coverage_repo,
            request=IngestionRequest(
                market_type=args.market_type,
                instrument=args.instrument,
                start_date=args.start_date,
                end_date=args.end_date,
                start_offset_days=args.start_offset_days,
                end_offset_days=args.end_offset_days,
            ),
            request_payer=settings.request_payer,
        )
        return

    if args.command == "backfill-day":
        market = _market_ref_from_args(args)
        backfill_day(
            s3_store,
            coverage_repo,
            market=market,
            date=args.date,
            request_payer=settings.request_payer,
        )
        return

    if args.command == "normalize-day":
        market = _market_ref_from_args(args)
        normalize_day(s3_store, coverage_repo, market=market, date=args.date)
        return

    if args.command == "materialize-replay":
        materialize_replay(
            request=ReplayRequest(
                market_type=args.market_type,
                instrument=args.instrument,
                date=args.date,
                depth=args.depth,
            ),
            s3_store=s3_store,
            coverage_repo=coverage_repo,
            replay_repo=replay_repo,
        )
        return

    if args.command == "polymarket-ingest-market":
        ingest_market(
            s3_store,
            coverage_repo,
            request=PolymarketReplayCreateRequest(slug=args.slug, outcome=args.outcome),
            telonex_api_key=_require_telonex_api_key(),
        )
        return

    if args.command == "polymarket-materialize-replay":
        resolution = resolve_market(
            PolymarketReplayCreateRequest(
                slug=args.slug,
                outcome=args.outcome,
                depth=args.depth,
            )
        )
        materialize_replay(
            request=ReplayRequest(
                venue=Venue.POLYMARKET,
                market_type=MarketType.BINARY,
                instrument=resolution.instrument,
                depth=args.depth,
                slug=resolution.slug,
                outcome=resolution.outcome,
                market_id=resolution.market_id,
                asset_id=resolution.asset_id,
                dates=resolution.dates,
                start_ts_ms=resolution.start_ts_ms,
                end_ts_ms=resolution.end_ts_ms,
            ),
            s3_store=s3_store,
            coverage_repo=coverage_repo,
            replay_repo=replay_repo,
        )
        return

    raise RuntimeError(f"unsupported command: {args.command}")


if __name__ == "__main__":
    main()
