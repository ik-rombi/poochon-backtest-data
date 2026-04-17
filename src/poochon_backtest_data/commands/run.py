from __future__ import annotations

from argparse import Namespace
import logging

from ..canonical import (
    build_hyperliquid_canonical_day,
    build_polymarket_canonical_day_from_storage,
)
from ..hyperliquid import backfill_day, normalize_day, sync_window
from ..models import (
    IngestionRequest,
    MarketRef,
    MarketType,
    OutcomesMode,
    PolymarketSeriesSyncRequest,
    Venue,
    iter_dates_inclusive,
)
from ..models import polymarket_metadata_s3_key
from ..polymarket_telonex import (
    _clip_resolution_to_window,
    backfill_market,
    discover_series_markets,
    normalize_market,
    sync_series,
)
from ..storage import CanonicalShardRepository, S3Store
from ._session import open_session, require_telonex_api_key

logger = logging.getLogger(__name__)

name = "run"


def _add_common_hyperliquid_args(parser) -> None:
    parser.add_argument(
        "--market-type",
        choices=[MarketType.PERP.value, MarketType.SPOT.value],
        required=True,
    )
    parser.add_argument("--instrument", required=True)
    parser.add_argument("--start-date", required=True)
    parser.add_argument("--end-date", required=True)
    parser.add_argument("--depth", type=int, default=20)


def _add_common_polymarket_args(parser) -> None:
    parser.add_argument("--series", required=True)
    parser.add_argument("--start-date", required=True)
    parser.add_argument("--end-date", required=True)
    parser.add_argument(
        "--outcomes",
        choices=[item.value for item in OutcomesMode],
        default=OutcomesMode.BOTH.value,
    )
    parser.add_argument("--depth", type=int, default=5)


def register(subparsers) -> None:
    parser = subparsers.add_parser(name, help="Execute a pipeline stage locally")
    run_subparsers = parser.add_subparsers(dest="venue", required=True)

    hl = run_subparsers.add_parser("hyperliquid", help="Hyperliquid stages")
    hl_stage = hl.add_subparsers(dest="stage", required=True)

    for stage in ("raw", "normalize", "canonical", "all"):
        stage_parser = hl_stage.add_parser(stage)
        _add_common_hyperliquid_args(stage_parser)
        if stage in {"canonical", "all"}:
            stage_parser.add_argument("--force", action="store_true")

    pm = run_subparsers.add_parser("polymarket", help="Polymarket stages")
    pm_stage = pm.add_subparsers(dest="stage", required=True)

    for stage in ("discover", "raw", "normalize", "canonical", "all"):
        stage_parser = pm_stage.add_parser(stage)
        _add_common_polymarket_args(stage_parser)
        if stage in {"canonical", "all"}:
            stage_parser.add_argument("--force", action="store_true")


def handle(args: Namespace) -> int:
    if args.venue == "hyperliquid":
        return _handle_hyperliquid(args)
    if args.venue == "polymarket":
        return _handle_polymarket(args)
    raise RuntimeError(f"unsupported venue: {args.venue}")


def _hyperliquid_market(args: Namespace) -> MarketRef:
    return MarketRef(
        venue=Venue.HYPERLIQUID,
        market_type=args.market_type,
        instrument=args.instrument,
    )


def _handle_hyperliquid(args: Namespace) -> int:
    bundle = open_session()
    market = _hyperliquid_market(args)
    dates = iter_dates_inclusive(args.start_date, args.end_date)

    if args.stage == "all":
        sync_window(
            bundle.s3_store,
            bundle.coverage_repo,
            bundle.shard_repo,
            request=IngestionRequest(
                market_type=args.market_type,
                instrument=args.instrument,
                start_date=args.start_date,
                end_date=args.end_date,
            ),
            request_payer=bundle.settings.request_payer,
            depth=args.depth,
        )
        return 0

    if args.stage == "raw":
        for date in dates:
            logger.info("hyperliquid raw date=%s instrument=%s", date, args.instrument)
            backfill_day(
                bundle.s3_store,
                bundle.coverage_repo,
                market=market,
                date=date,
                request_payer=bundle.settings.request_payer,
            )
        return 0

    if args.stage == "normalize":
        for date in dates:
            logger.info("hyperliquid normalize date=%s instrument=%s", date, args.instrument)
            normalize_day(
                bundle.s3_store,
                bundle.coverage_repo,
                market=market,
                date=date,
            )
        return 0

    if args.stage == "canonical":
        for date in dates:
            logger.info(
                "hyperliquid canonical date=%s instrument=%s depth=%s force=%s",
                date,
                args.instrument,
                args.depth,
                args.force,
            )
            build_hyperliquid_canonical_day(
                market=market,
                date=date,
                depth=args.depth,
                s3_store=bundle.s3_store,
                coverage_repo=bundle.coverage_repo,
                shard_repo=bundle.shard_repo,
                force=args.force,
            )
        return 0

    raise RuntimeError(f"unsupported stage: {args.stage}")


def _polymarket_request(args: Namespace) -> PolymarketSeriesSyncRequest:
    return PolymarketSeriesSyncRequest(
        series=args.series,
        start_date=args.start_date,
        end_date=args.end_date,
        outcomes=args.outcomes,
        depth=args.depth,
    )


def _discovered_resolutions(args: Namespace, *, client=None):
    request = _polymarket_request(args)
    discovered = discover_series_markets(request, client=client)
    resolutions = []
    for resolution in discovered:
        clipped = _clip_resolution_to_window(
            resolution,
            start_date=request.start_date,
            end_date=request.end_date,
        )
        if clipped is not None:
            resolutions.append(clipped)
    if not resolutions:
        raise ValueError(
            f"no polymarket markets were discovered for series={request.series} "
            f"between {request.start_date} and {request.end_date}"
        )
    return resolutions


def _handle_polymarket(args: Namespace) -> int:
    bundle = open_session()

    if args.stage == "all":
        sync_series(
            bundle.s3_store,
            bundle.coverage_repo,
            bundle.shard_repo,
            request=_polymarket_request(args),
            telonex_api_key=require_telonex_api_key(),
        )
        return 0

    if args.stage == "discover":
        resolutions = _discovered_resolutions(args)
        for resolution in resolutions:
            bundle.s3_store.put_json(
                polymarket_metadata_s3_key(resolution),
                resolution.model_dump(mode="json"),
            )
        logger.info(
            "polymarket discover series=%s resolutions=%d",
            args.series,
            len(resolutions),
        )
        return 0

    if args.stage == "canonical":
        for date in iter_dates_inclusive(args.start_date, args.end_date):
            logger.info(
                "polymarket canonical date=%s series=%s outcomes=%s depth=%s force=%s",
                date,
                args.series,
                args.outcomes,
                args.depth,
                args.force,
            )
            build_polymarket_canonical_day_from_storage(
                date=date,
                series_key=args.series,
                outcomes=OutcomesMode(args.outcomes),
                depth=args.depth,
                s3_store=bundle.s3_store,
                shard_repo=bundle.shard_repo,
                force=args.force,
            )
        return 0

    # raw and normalize both need discovered resolutions first
    resolutions = _discovered_resolutions(args)
    telonex_api_key = require_telonex_api_key() if args.stage == "raw" else None

    if args.stage not in {"raw", "normalize"}:
        raise RuntimeError(f"unsupported stage: {args.stage}")

    # Mirror sync_series: per-thread clones of S3 + coverage to avoid
    # boto3 client sharing across threads.
    from threading import local as _local
    worker_state = _local()

    def process(resolution):
        if not hasattr(worker_state, "store"):
            worker_state.store = bundle.s3_store.clone() if hasattr(bundle.s3_store, "clone") else bundle.s3_store
            worker_state.coverage = bundle.coverage_repo.clone() if hasattr(bundle.coverage_repo, "clone") else bundle.coverage_repo
        try:
            if args.stage == "raw":
                logger.info(
                    "polymarket raw series=%s market_id=%s outcome=%s",
                    args.series, resolution.market_id, resolution.outcome,
                )
                backfill_market(
                    worker_state.store,
                    worker_state.coverage,
                    resolution=resolution,
                    telonex_api_key=telonex_api_key,
                )
            else:
                logger.info(
                    "polymarket normalize series=%s market_id=%s outcome=%s",
                    args.series, resolution.market_id, resolution.outcome,
                )
                normalize_market(
                    worker_state.store,
                    worker_state.coverage,
                    resolution=resolution,
                )
            return None
        except Exception as error:
            return {
                "market_id": resolution.market_id,
                "outcome": resolution.outcome,
                "error": f"{type(error).__name__}: {error}",
            }

    from concurrent.futures import ThreadPoolExecutor, as_completed as _as_completed

    max_workers = min(8, len(resolutions)) or 1
    failures: list[dict] = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(process, r) for r in resolutions]
        for future in _as_completed(futures):
            result = future.result()
            if result is not None:
                failures.append(result)

    if failures:
        logger.warning(
            "polymarket %s stage finished with %d failures (of %d); first: %s",
            args.stage, len(failures), len(resolutions), failures[0],
        )
    return 0
