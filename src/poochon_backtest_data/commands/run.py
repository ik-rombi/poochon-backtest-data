"""`run <venue> mirror|slice|all` — local pipeline execution.

Stage handlers wire up the underlying mirror + slice functions. The slice
handlers will raise NotImplementedError until Phases 3 (PM) and 4 (HL) land.
"""

from __future__ import annotations

from argparse import Namespace
import logging
import sys

from ..models import (
    HyperliquidIngestionRequest,
    MarketType,
    PolymarketMirrorRequest,
    PolymarketSliceRequest,
    PolymarketTargetKind,
)

logger = logging.getLogger(__name__)
name = "run"


def _add_window_args(parser) -> None:
    parser.add_argument("--start-date")
    parser.add_argument("--end-date")
    parser.add_argument("--start-offset-days", type=int)
    parser.add_argument("--end-offset-days", type=int)
    parser.add_argument(
        "--date",
        help="Shorthand for a single-day window: YYYY-MM-DD, 'today', or 'yesterday'.",
    )


def _add_force(parser) -> None:
    parser.add_argument("--force", action="store_true")


def register(subparsers) -> None:
    parser = subparsers.add_parser(name, help="Execute a pipeline stage locally")
    run_subparsers = parser.add_subparsers(dest="venue", required=True)

    pm = run_subparsers.add_parser("polymarket", help="Polymarket stages")
    pm_stage = pm.add_subparsers(dest="stage", required=True)

    pm_mirror = pm_stage.add_parser("mirror", help="Mirror PMXT raw firehose for the window")
    _add_window_args(pm_mirror)

    pm_slice = pm_stage.add_parser("slice", help="Build canonical slices for a target")
    _add_window_args(pm_slice)
    pm_slice.add_argument(
        "--target",
        required=True,
        help="series:KEY or slug:KEY",
    )
    _add_force(pm_slice)

    pm_all = pm_stage.add_parser("all", help="Mirror + slice in one command")
    _add_window_args(pm_all)
    pm_all.add_argument("--target", required=True)
    _add_force(pm_all)

    hl = run_subparsers.add_parser("hyperliquid", help="Hyperliquid stages")
    hl_stage = hl.add_subparsers(dest="stage", required=True)

    hl_mirror = hl_stage.add_parser("mirror", help="Mirror HL raw archive for the window")
    _add_window_args(hl_mirror)
    hl_mirror.add_argument(
        "--instrument",
        required=True,
        help="Either 'BTC/perp' or '--instrument BTC --market-type perp'",
    )
    hl_mirror.add_argument(
        "--market-type",
        choices=[MarketType.PERP.value, MarketType.SPOT.value],
        default=None,
    )

    hl_slice = hl_stage.add_parser("slice", help="Build canonical slices for an instrument")
    _add_window_args(hl_slice)
    hl_slice.add_argument("--instrument", required=True)
    hl_slice.add_argument(
        "--market-type",
        choices=[MarketType.PERP.value, MarketType.SPOT.value],
        default=None,
    )
    hl_slice.add_argument("--depth", type=int, default=20)
    _add_force(hl_slice)

    hl_all = hl_stage.add_parser("all", help="Mirror + slice in one command")
    _add_window_args(hl_all)
    hl_all.add_argument("--instrument", required=True)
    hl_all.add_argument(
        "--market-type",
        choices=[MarketType.PERP.value, MarketType.SPOT.value],
        default=None,
    )
    hl_all.add_argument("--depth", type=int, default=20)
    _add_force(hl_all)


def handle(args: Namespace) -> int:
    if args.venue == "polymarket":
        return _handle_polymarket(args)
    if args.venue == "hyperliquid":
        return _handle_hyperliquid(args)
    raise RuntimeError(f"unsupported venue: {args.venue}")


def _resolve_window_args(args: Namespace) -> dict:
    """Map argparse window flags to WindowSpec kwargs.

    `--date` is sugar that sets start_date == end_date (or, for relative aliases,
    start_offset_days == end_offset_days).
    """
    if args.date is not None:
        if args.start_date or args.end_date or args.start_offset_days is not None or args.end_offset_days is not None:
            raise SystemExit("--date is incompatible with --start-date/--end-date/--*-offset-days")
        if args.date == "yesterday":
            return {"start_offset_days": -1, "end_offset_days": -1}
        if args.date == "today":
            return {"start_offset_days": 0, "end_offset_days": 0}
        return {"start_date": args.date, "end_date": args.date}

    if args.start_date or args.end_date:
        if not args.start_date or not args.end_date:
            raise SystemExit("provide both --start-date and --end-date")
        return {"start_date": args.start_date, "end_date": args.end_date}

    if args.start_offset_days is not None or args.end_offset_days is not None:
        if args.start_offset_days is None or args.end_offset_days is None:
            raise SystemExit("provide both --start-offset-days and --end-offset-days")
        return {
            "start_offset_days": args.start_offset_days,
            "end_offset_days": args.end_offset_days,
        }

    raise SystemExit("specify a date window (--date, --start-date/--end-date, or offsets)")


def _parse_target(raw: str) -> tuple[PolymarketTargetKind, str]:
    if ":" not in raw:
        raise SystemExit("--target must be 'series:KEY' or 'slug:KEY'")
    kind_raw, key = raw.split(":", 1)
    try:
        kind = PolymarketTargetKind(kind_raw)
    except ValueError as error:
        raise SystemExit(
            f"unsupported target_kind '{kind_raw}'; expected 'series' or 'slug'"
        ) from error
    if not key.strip():
        raise SystemExit("target_key is empty")
    return kind, key


def _parse_hl_market(args: Namespace) -> tuple[str, MarketType]:
    instrument = args.instrument
    market_type_raw = args.market_type
    if "/" in instrument and market_type_raw is None:
        instrument, market_type_raw = instrument.split("/", 1)
    if market_type_raw is None:
        raise SystemExit("hyperliquid commands require --market-type or 'INSTRUMENT/MARKET_TYPE' shorthand")
    return instrument, MarketType(market_type_raw)


def _handle_polymarket(args: Namespace) -> int:
    window_kwargs = _resolve_window_args(args)
    if args.stage == "mirror":
        request = PolymarketMirrorRequest(**window_kwargs)
        return _run_pm_mirror(request)
    target_kind, target_key = _parse_target(args.target)
    if args.stage == "slice":
        request = PolymarketSliceRequest(
            target_kind=target_kind, target_key=target_key, **window_kwargs
        )
        return _run_pm_slice(request, force=args.force)
    if args.stage == "all":
        mirror_request = PolymarketMirrorRequest(**window_kwargs)
        slice_request = PolymarketSliceRequest(
            target_kind=target_kind, target_key=target_key, **window_kwargs
        )
        rc = _run_pm_mirror(mirror_request)
        if rc != 0:
            return rc
        return _run_pm_slice(slice_request, force=args.force)
    raise RuntimeError(f"unsupported polymarket stage: {args.stage}")


def _handle_hyperliquid(args: Namespace) -> int:
    instrument, market_type = _parse_hl_market(args)
    window_kwargs = _resolve_window_args(args)
    if args.stage == "mirror":
        request = HyperliquidIngestionRequest(
            instrument=instrument, market_type=market_type, **window_kwargs
        )
        return _run_hl_mirror(request)
    if args.stage == "slice":
        request = HyperliquidIngestionRequest(
            instrument=instrument, market_type=market_type, **window_kwargs
        )
        return _run_hl_slice(request, depth=args.depth, force=args.force)
    if args.stage == "all":
        request = HyperliquidIngestionRequest(
            instrument=instrument, market_type=market_type, **window_kwargs
        )
        rc = _run_hl_mirror(request)
        if rc != 0:
            return rc
        return _run_hl_slice(request, depth=args.depth, force=args.force)
    raise RuntimeError(f"unsupported hyperliquid stage: {args.stage}")


def _run_pm_mirror(request: PolymarketMirrorRequest) -> int:
    from ._session import open_session
    from ..pmxt import mirror_pmxt_window

    bundle = open_session()
    start_date, end_date = request.resolve_window()
    logger.info("pm mirror start  window=%s..%s", start_date, end_date)
    summary = mirror_pmxt_window(
        s3_store=bundle.s3_store,
        coverage_repo=bundle.coverage_repo,
        start_date=start_date,
        end_date=end_date,
        pmxt_base_url=bundle.settings.pmxt_base_url,
    )
    logger.info(
        "pm mirror done   mirrored=%d skipped=%d failed=%d bytes=%d",
        summary.mirrored,
        summary.skipped,
        summary.failed,
        summary.bytes_total,
    )
    return 0 if summary.failed == 0 else 2


def _run_pm_slice(request: PolymarketSliceRequest, *, force: bool) -> int:
    from ._session import open_session
    from ..canonical import build_pm_slice

    bundle = open_session()
    target = request.target()
    start_date, end_date = request.resolve_window()
    logger.info(
        "pm slice start  target=%s:%s window=%s..%s force=%s",
        target.target_kind.value,
        target.target_key,
        start_date,
        end_date,
        force,
    )
    rc = 0
    for date in request.iter_dates():
        try:
            build_pm_slice(
                target=target,
                date=date,
                s3_store=bundle.s3_store,
                coverage_repo=bundle.coverage_repo,
                shard_repo=bundle.shard_repo,
                gamma_base_url=bundle.settings.gamma_base_url,
                vatic_base_url=bundle.settings.vatic_base_url,
                binance_base_url=bundle.settings.binance_base_url,
                binance_us_base_url=bundle.settings.binance_us_base_url,
                force=force,
            )
        except NotImplementedError as error:
            print(f"error: {error}", file=sys.stderr)
            return 2
        except Exception as error:  # noqa: BLE001
            logger.exception("pm slice failed date=%s: %s", date, error)
            rc = 2
    return rc


def _run_hl_mirror(request: HyperliquidIngestionRequest) -> int:
    from ._session import open_session
    from ..hyperliquid import mirror_hl_window

    bundle = open_session()
    market = request.market_ref()
    start_date, end_date = request.resolve_window()
    logger.info(
        "hl mirror start market=%s/%s window=%s..%s",
        market.market_type.value,
        market.instrument,
        start_date,
        end_date,
    )
    summary = mirror_hl_window(
        market=market,
        start_date=start_date,
        end_date=end_date,
        s3_store=bundle.s3_store,
        coverage_repo=bundle.coverage_repo,
        request_payer=bundle.settings.request_payer,
    )
    logger.info(
        "hl mirror done l2_mirrored=%d l2_skipped=%d l2_failed=%d "
        "fills_mirrored=%d fills_skipped=%d fills_failed=%d bytes=%d",
        summary.l2_mirrored,
        summary.l2_skipped,
        summary.l2_failed,
        summary.fills_mirrored,
        summary.fills_skipped,
        summary.fills_failed,
        summary.bytes_total,
    )
    return 0 if summary.l2_failed == 0 and summary.fills_failed == 0 else 2


def _run_hl_slice(request: HyperliquidIngestionRequest, *, depth: int, force: bool) -> int:
    from ._session import open_session
    from ..canonical import build_hl_slice

    bundle = open_session()
    market = request.market_ref()
    start_date, end_date = request.resolve_window()
    logger.info(
        "hl slice start market=%s/%s window=%s..%s depth=%d force=%s",
        market.market_type.value,
        market.instrument,
        start_date,
        end_date,
        depth,
        force,
    )
    rc = 0
    for date in request.iter_dates():
        try:
            build_hl_slice(
                market=market,
                date=date,
                s3_store=bundle.s3_store,
                coverage_repo=bundle.coverage_repo,
                shard_repo=bundle.shard_repo,
                depth=depth,
                force=force,
            )
        except Exception as error:  # noqa: BLE001
            logger.exception("hl slice failed date=%s: %s", date, error)
            rc = 2
    return rc
