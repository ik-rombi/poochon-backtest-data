"""`submit <venue>` — start a Step Functions execution for the new state machines."""

from __future__ import annotations

from argparse import Namespace
from datetime import UTC, datetime
import json
import os
import re
import subprocess
import sys
from pathlib import Path

from ..models import MarketType, PolymarketTargetKind
from ..settings import get_settings
from ..storage import boto3_session

name = "submit"


def register(subparsers) -> None:
    parser = subparsers.add_parser(name, help="Start a Step Functions execution")
    submit_subparsers = parser.add_subparsers(dest="venue", required=True)

    pm = submit_subparsers.add_parser("polymarket")
    pm_stage = pm.add_subparsers(dest="stage", required=True)
    for stage_name, output_key in (
        ("mirror", "pm_mirror_state_machine_arn"),
        ("slice", "pm_slice_state_machine_arn"),
    ):
        s = pm_stage.add_parser(stage_name)
        s.add_argument("--start-date")
        s.add_argument("--end-date")
        s.add_argument("--start-offset-days", type=int)
        s.add_argument("--end-offset-days", type=int)
        s.add_argument("--date")
        if stage_name == "slice":
            s.add_argument("--target", required=True, help="series:KEY or slug:KEY")
        s.add_argument("--stack", default=os.environ.get("POOCHON_PULUMI_STACK", "dev"))
        s.add_argument("--state-machine-arn", default=None)
        s.set_defaults(_pulumi_output_key=output_key)

    hl = submit_subparsers.add_parser("hyperliquid")
    hl_stage = hl.add_subparsers(dest="stage", required=True)
    for stage_name, output_key in (
        ("mirror", "hl_mirror_state_machine_arn"),
        ("slice", "hl_slice_state_machine_arn"),
    ):
        s = hl_stage.add_parser(stage_name)
        s.add_argument("--start-date")
        s.add_argument("--end-date")
        s.add_argument("--start-offset-days", type=int)
        s.add_argument("--end-offset-days", type=int)
        s.add_argument("--date")
        s.add_argument("--instrument", required=True)
        s.add_argument(
            "--market-type",
            choices=[MarketType.PERP.value, MarketType.SPOT.value],
            default=None,
        )
        if stage_name == "slice":
            s.add_argument("--depth", type=int, default=20)
        s.add_argument("--stack", default=os.environ.get("POOCHON_PULUMI_STACK", "dev"))
        s.add_argument("--state-machine-arn", default=None)
        s.set_defaults(_pulumi_output_key=output_key)


def handle(args: Namespace) -> int:
    payload = _build_payload(args)
    slug = _execution_slug(args)
    return _submit(args, payload=payload, execution_slug=slug)


def _build_payload(args: Namespace) -> dict:
    """Build SFN input payload. State machines only support offset-days, so
    absolute dates are converted at submit time relative to today (UTC)."""
    from datetime import UTC, date as date_cls, datetime

    payload: dict = {}
    today = datetime.now(tz=UTC).date()

    def _absolute_to_offsets(start_date: str, end_date: str) -> tuple[int, int]:
        start = date_cls.fromisoformat(start_date)
        end = date_cls.fromisoformat(end_date)
        return (start - today).days, (end - today).days

    if args.start_date and args.end_date:
        start_off, end_off = _absolute_to_offsets(args.start_date, args.end_date)
        payload["start_offset_days"] = start_off
        payload["end_offset_days"] = end_off
    elif args.start_offset_days is not None and args.end_offset_days is not None:
        payload["start_offset_days"] = args.start_offset_days
        payload["end_offset_days"] = args.end_offset_days
    elif args.date:
        if args.date == "yesterday":
            payload["start_offset_days"] = -1
            payload["end_offset_days"] = -1
        elif args.date == "today":
            payload["start_offset_days"] = 0
            payload["end_offset_days"] = 0
        else:
            start_off, end_off = _absolute_to_offsets(args.date, args.date)
            payload["start_offset_days"] = start_off
            payload["end_offset_days"] = end_off
    else:
        raise SystemExit("specify a date window")

    if args.venue == "polymarket":
        if args.stage == "slice":
            # Validate the target shape; the SFN command_template references $.target as a single string.
            kind_raw, _, key = args.target.partition(":")
            PolymarketTargetKind(kind_raw)  # validates
            if not key:
                raise SystemExit("--target must be 'series:KEY' or 'slug:KEY'")
            payload["target"] = args.target
    else:
        instrument = args.instrument
        market_type = args.market_type
        if "/" in instrument and market_type is None:
            instrument, market_type = instrument.split("/", 1)
        if market_type is None:
            raise SystemExit("hyperliquid commands require --market-type or 'INSTRUMENT/MARKET_TYPE' shorthand")
        payload["instrument"] = instrument
        payload["market_type"] = MarketType(market_type).value
        if args.stage == "slice":
            payload["depth"] = args.depth
    return payload


def _execution_slug(args: Namespace) -> str:
    pieces: list[str] = [args.venue, args.stage]
    if args.venue == "polymarket" and args.stage == "slice":
        pieces.append(args.target.replace(":", "-"))
    elif args.venue == "hyperliquid":
        instrument = args.instrument
        market_type = args.market_type
        if "/" in instrument and market_type is None:
            instrument, market_type = instrument.split("/", 1)
        pieces.append(f"{market_type}-{instrument}")
    if args.start_date:
        pieces.append(args.start_date)
    elif args.date:
        pieces.append(args.date)
    return "-".join(pieces)


def resolve_state_machine_arn(args_or_stack, output_key: str | None = None, explicit: str | None = None) -> str:
    """Resolve a state machine ARN.

    Two calling shapes for compatibility:
      - resolve_state_machine_arn(args)        — uses args._pulumi_output_key + args.stack + args.state_machine_arn
      - resolve_state_machine_arn(stack, output_key, explicit) — by-name lookup
    """
    if isinstance(args_or_stack, Namespace):
        args = args_or_stack
        output_key = args._pulumi_output_key
        explicit = args.state_machine_arn
        stack = args.stack
    else:
        stack = args_or_stack
    if explicit:
        return explicit
    settings = get_settings()
    settings_attr = getattr(settings, output_key, None)
    if settings_attr:
        return settings_attr
    return _pulumi_output(stack, output_key)


def _pulumi_output(stack: str, output_name: str) -> str:
    runtime_dir = _find_stack_dir(["runtime", "write"])
    if runtime_dir is None:
        raise RuntimeError(
            "cannot locate infra/runtime or infra/write; set --state-machine-arn"
        )
    try:
        result = subprocess.run(
            ["pulumi", "stack", "output", output_name, "--stack", stack],
            cwd=runtime_dir,
            capture_output=True,
            text=True,
            timeout=30,
            check=True,
        )
    except FileNotFoundError as error:
        raise RuntimeError("pulumi CLI not found on PATH") from error
    except subprocess.CalledProcessError as error:
        raise RuntimeError(
            f"pulumi stack output {output_name} (stack={stack}) failed: {error.stderr.strip()}"
        ) from error
    value = result.stdout.strip()
    if not value:
        raise RuntimeError(
            f"pulumi returned empty output for {output_name} in stack {stack}"
        )
    return value


def _find_stack_dir(candidates: list[str]) -> Path | None:
    root = _repo_root()
    for candidate in candidates:
        path = root / "infra" / candidate
        if path.exists():
            return path
    return None


def _repo_root() -> Path:
    env = os.environ.get("POOCHON_REPO_ROOT")
    if env:
        return Path(env).resolve()
    try:
        result = subprocess.run(
            ["git", "rev-parse", "--show-toplevel"],
            capture_output=True,
            text=True,
            check=True,
        )
        return Path(result.stdout.strip()).resolve()
    except (subprocess.CalledProcessError, FileNotFoundError):
        return Path.cwd().resolve()


_NAME_SAFE_CHARS = re.compile(r"[^A-Za-z0-9_-]+")


def _execution_name(slug: str) -> str:
    timestamp = datetime.now(tz=UTC).strftime("%Y%m%dT%H%M%SZ")
    sanitized = _NAME_SAFE_CHARS.sub("-", slug).strip("-")
    body = f"{sanitized[:50]}-{timestamp}"
    return body[:80]


def _submit(args: Namespace, *, payload: dict, execution_slug: str) -> int:
    try:
        state_machine_arn = resolve_state_machine_arn(args)
    except RuntimeError as error:
        print(f"error: {error}", file=sys.stderr)
        return 1

    settings = get_settings()
    session = boto3_session(settings.aws_region)
    sfn = session.client("stepfunctions")
    execution_name = _execution_name(execution_slug)

    response = sfn.start_execution(
        stateMachineArn=state_machine_arn,
        name=execution_name,
        input=json.dumps(payload),
    )

    execution_arn = response["executionArn"]
    print(execution_arn)
    print(
        f"\nfollow with:  poochon-backtest-data job status {execution_arn}",
        file=sys.stderr,
    )
    print(
        f"tail logs:     poochon-backtest-data job logs {execution_arn} --follow",
        file=sys.stderr,
    )
    return 0
