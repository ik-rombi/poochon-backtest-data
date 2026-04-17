from __future__ import annotations

from argparse import Namespace
from datetime import UTC, datetime
import json
import os
import re
import subprocess
import sys
from pathlib import Path

from ..models import MarketType, OutcomesMode
from ..settings import get_settings
from ..storage import boto3_session

name = "submit"


def register(subparsers) -> None:
    parser = subparsers.add_parser(name, help="Start a Step Functions execution")
    submit_subparsers = parser.add_subparsers(dest="venue", required=True)

    hl = submit_subparsers.add_parser("hyperliquid")
    hl.add_argument(
        "--market-type",
        choices=[MarketType.PERP.value, MarketType.SPOT.value],
        required=True,
    )
    hl.add_argument("--instrument", required=True)
    hl.add_argument("--start-date", required=True)
    hl.add_argument("--end-date", required=True)
    hl.add_argument("--depth", type=int, default=20)
    hl.add_argument("--stack", default=os.environ.get("POOCHON_PULUMI_STACK", "dev"))
    hl.add_argument("--state-machine-arn", default=None,
                    help="Override auto-discovery of the state machine ARN")

    pm = submit_subparsers.add_parser("polymarket")
    pm.add_argument("--series", required=True)
    pm.add_argument("--start-date", required=True)
    pm.add_argument("--end-date", required=True)
    pm.add_argument(
        "--outcomes",
        choices=[item.value for item in OutcomesMode],
        default=OutcomesMode.BOTH.value,
    )
    pm.add_argument("--depth", type=int, default=5)
    pm.add_argument("--stack", default=os.environ.get("POOCHON_PULUMI_STACK", "dev"))
    pm.add_argument("--state-machine-arn", default=None)


def handle(args: Namespace) -> int:
    if args.venue == "hyperliquid":
        return _submit(
            args,
            payload={
                "venue": "hyperliquid",
                "market_type": args.market_type,
                "instrument": args.instrument,
                "start_date": args.start_date,
                "end_date": args.end_date,
                "depth": args.depth,
            },
            execution_slug=f"hl-{args.instrument}-{args.start_date}-{args.end_date}",
        )
    if args.venue == "polymarket":
        return _submit(
            args,
            payload={
                "venue": "polymarket",
                "series": args.series,
                "start_date": args.start_date,
                "end_date": args.end_date,
                "outcomes": args.outcomes,
                "depth": args.depth,
            },
            execution_slug=f"pm-{args.series}-{args.start_date}-{args.end_date}",
        )
    raise RuntimeError(f"unsupported venue: {args.venue}")


def resolve_state_machine_arn(stack: str, explicit: str | None = None) -> str:
    if explicit:
        return explicit
    env_arn = os.environ.get("POOCHON_INGESTION_STATE_MACHINE_ARN")
    if env_arn:
        return env_arn
    return _pulumi_output(stack, "ingestion_state_machine_arn")


def _pulumi_output(stack: str, output_name: str) -> str:
    write_dir = _find_stack_dir(["write", "runtime"])
    if write_dir is None:
        raise RuntimeError(
            "cannot locate infra/write or infra/runtime; set --state-machine-arn or POOCHON_INGESTION_STATE_MACHINE_ARN"
        )
    try:
        result = subprocess.run(
            ["pulumi", "stack", "output", output_name, "--stack", stack],
            cwd=write_dir,
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
    # Step Functions execution names have an 80-char cap.
    body = f"{sanitized[:50]}-{timestamp}"
    return body[:80]


def _submit(args: Namespace, *, payload: dict, execution_slug: str) -> int:
    try:
        state_machine_arn = resolve_state_machine_arn(args.stack, args.state_machine_arn)
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
