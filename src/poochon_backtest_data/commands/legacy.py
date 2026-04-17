"""Backward-compat aliases for the pre-refactor flat subcommands.

The Step Functions state machine in infra/runtime (and anyone with a saved
script) still calls these. They forward to the new `run <venue> <stage>`
handlers so the pipeline keeps working during the transition.
"""

from __future__ import annotations

from argparse import Namespace
import logging

from ..models import MarketType, OutcomesMode
from . import run as run_module

logger = logging.getLogger(__name__)

name = "__legacy__"


def register(subparsers) -> None:
    hl_sync = subparsers.add_parser(
        "hyperliquid-sync-window",
        help="[deprecated] alias for `run hyperliquid all`",
    )
    hl_sync.add_argument(
        "--market-type",
        choices=[MarketType.PERP.value, MarketType.SPOT.value],
        required=True,
    )
    hl_sync.add_argument("--instrument", required=True)
    hl_sync.add_argument("--start-date")
    hl_sync.add_argument("--end-date")
    hl_sync.add_argument("--start-offset-days", type=int)
    hl_sync.add_argument("--end-offset-days", type=int)
    hl_sync.add_argument("--depth", type=int, default=20)

    hl_build = subparsers.add_parser(
        "hyperliquid-build-canonical-window",
        help="[deprecated] alias for `run hyperliquid canonical`",
    )
    hl_build.add_argument(
        "--market-type",
        choices=[MarketType.PERP.value, MarketType.SPOT.value],
        required=True,
    )
    hl_build.add_argument("--instrument", required=True)
    hl_build.add_argument("--start-date", required=True)
    hl_build.add_argument("--end-date", required=True)
    hl_build.add_argument("--depth", type=int, default=20)
    hl_build.add_argument("--force", action="store_true")

    pm_sync = subparsers.add_parser(
        "polymarket-sync-series",
        help="[deprecated] alias for `run polymarket all`",
    )
    pm_sync.add_argument("--series", required=True)
    pm_sync.add_argument("--start-date", required=True)
    pm_sync.add_argument("--end-date", required=True)
    pm_sync.add_argument(
        "--outcomes",
        choices=[item.value for item in OutcomesMode],
        default=OutcomesMode.BOTH.value,
    )
    pm_sync.add_argument("--depth", type=int, default=5)

    pm_build = subparsers.add_parser(
        "polymarket-build-canonical-window",
        help="[deprecated] alias for `run polymarket canonical`",
    )
    pm_build.add_argument("--series", required=True)
    pm_build.add_argument("--start-date", required=True)
    pm_build.add_argument("--end-date", required=True)
    pm_build.add_argument(
        "--outcomes",
        choices=[item.value for item in OutcomesMode],
        default=OutcomesMode.BOTH.value,
    )
    pm_build.add_argument("--depth", type=int, default=5)
    pm_build.add_argument("--force", action="store_true")


_LEGACY_COMMANDS = (
    "hyperliquid-sync-window",
    "hyperliquid-build-canonical-window",
    "polymarket-sync-series",
    "polymarket-build-canonical-window",
)


def legacy_command_names() -> tuple[str, ...]:
    return _LEGACY_COMMANDS


def handle(args: Namespace) -> int:
    command = args.command
    logger.warning(
        "command '%s' is deprecated; prefer the `run` subcommand tree",
        command,
    )

    if command == "hyperliquid-sync-window":
        args.venue = "hyperliquid"
        args.stage = "all"
        _resolve_hyperliquid_window(args)
        return run_module.handle(args)

    if command == "hyperliquid-build-canonical-window":
        args.venue = "hyperliquid"
        args.stage = "canonical"
        return run_module.handle(args)

    if command == "polymarket-sync-series":
        args.venue = "polymarket"
        args.stage = "all"
        return run_module.handle(args)

    if command == "polymarket-build-canonical-window":
        args.venue = "polymarket"
        args.stage = "canonical"
        return run_module.handle(args)

    raise RuntimeError(f"unsupported legacy command: {command}")


def _resolve_hyperliquid_window(args: Namespace) -> None:
    """The legacy hyperliquid-sync-window accepts either explicit or relative
    date windows. The new `run hyperliquid all` only takes explicit dates,
    so translate relative offsets here (same logic as IngestionRequest).
    """
    if args.start_date and args.end_date:
        return
    from datetime import UTC, date as date_cls, datetime, timedelta

    today = datetime.now(tz=UTC).date()
    if args.start_offset_days is None or args.end_offset_days is None:
        raise RuntimeError(
            "either --start-date/--end-date or --start-offset-days/--end-offset-days is required"
        )
    start = today - timedelta(days=args.start_offset_days)
    end = today - timedelta(days=args.end_offset_days)
    if end < start:
        start, end = end, start
    args.start_date = start.isoformat()
    args.end_date = end.isoformat()
