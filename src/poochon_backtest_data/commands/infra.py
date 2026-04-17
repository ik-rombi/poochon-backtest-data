from __future__ import annotations

from argparse import Namespace
from dataclasses import dataclass
import json
import os
from pathlib import Path
import subprocess
import sys

name = "infra"

# Stacks listed in dependency order; later stacks depend on earlier ones.
_STACK_PATHS = ("core", "shared", "write", "read", "runtime")


@dataclass
class StackStatus:
    stack_name: str
    path: str
    up: bool
    outputs: dict
    error: str | None = None


def register(subparsers) -> None:
    parser = subparsers.add_parser(name, help="Inspect Pulumi stacks")
    infra_subparsers = parser.add_subparsers(dest="infra_command", required=True)

    status = infra_subparsers.add_parser("status", help="Report stack UP/DOWN and key outputs")
    status.add_argument("--stack", default=os.environ.get("POOCHON_PULUMI_STACK", "dev"),
                        help="Pulumi stack name (default: $POOCHON_PULUMI_STACK or dev)")
    status.add_argument("--repo-root", default=None,
                        help="Repository root (defaults to $POOCHON_REPO_ROOT or git toplevel)")
    status.add_argument("--json", action="store_true", dest="as_json")


def handle(args: Namespace) -> int:
    if args.infra_command == "status":
        return _handle_status(args)
    raise RuntimeError(f"unsupported infra command: {args.infra_command}")


def _resolve_repo_root(explicit: str | None) -> Path:
    if explicit:
        return Path(explicit).resolve()
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


def _pulumi_output(stack_name: str, stack_dir: Path) -> StackStatus:
    if not stack_dir.exists():
        return StackStatus(
            stack_name=stack_name,
            path=str(stack_dir),
            up=False,
            outputs={},
            error="stack directory does not exist",
        )
    try:
        result = subprocess.run(
            ["pulumi", "stack", "output", "--json", "--stack", stack_name],
            cwd=stack_dir,
            capture_output=True,
            text=True,
            timeout=30,
        )
    except FileNotFoundError:
        return StackStatus(
            stack_name=stack_name,
            path=str(stack_dir),
            up=False,
            outputs={},
            error="pulumi CLI not found on PATH",
        )
    except subprocess.TimeoutExpired:
        return StackStatus(
            stack_name=stack_name,
            path=str(stack_dir),
            up=False,
            outputs={},
            error="pulumi command timed out",
        )

    if result.returncode != 0:
        return StackStatus(
            stack_name=stack_name,
            path=str(stack_dir),
            up=False,
            outputs={},
            error=result.stderr.strip().splitlines()[-1] if result.stderr else "pulumi exited non-zero",
        )

    stdout = result.stdout.strip()
    if not stdout:
        return StackStatus(
            stack_name=stack_name,
            path=str(stack_dir),
            up=False,
            outputs={},
        )
    try:
        outputs = json.loads(stdout)
    except json.JSONDecodeError as error:
        return StackStatus(
            stack_name=stack_name,
            path=str(stack_dir),
            up=False,
            outputs={},
            error=f"invalid JSON from pulumi: {error}",
        )

    if not outputs:
        return StackStatus(
            stack_name=stack_name,
            path=str(stack_dir),
            up=False,
            outputs={},
        )

    return StackStatus(
        stack_name=stack_name,
        path=str(stack_dir),
        up=True,
        outputs=outputs,
    )


def _collect(stack_name: str, repo_root: Path) -> list[StackStatus]:
    results = []
    for subdir in _STACK_PATHS:
        stack_dir = repo_root / "infra" / subdir
        if not stack_dir.exists():
            continue
        results.append(_pulumi_output(stack_name, stack_dir))
    return results


def _format_output_summary(outputs: dict) -> str:
    # Highlight a handful of known-useful outputs; otherwise just show count.
    keys_of_interest = [
        "data_bucket_name",
        "coverage_table_name",
        "replay_shard_table_name",
        "replay_table_name",
        "ingestion_state_machine_arn",
        "ingestion_schedule_name",
        "api_url",
        "cluster_arn",
        "ecr_repo_url",
    ]
    highlights = {key: outputs[key] for key in keys_of_interest if key in outputs}
    if highlights:
        return ", ".join(f"{key}={value}" for key, value in highlights.items())
    return f"{len(outputs)} output(s)"


def _handle_status(args: Namespace) -> int:
    repo_root = _resolve_repo_root(args.repo_root)
    statuses = _collect(args.stack, repo_root)

    if not statuses:
        print(f"no Pulumi stacks found under {repo_root}/infra", file=sys.stderr)
        return 1

    if args.as_json:
        payload = [
            {
                "stack_name": s.stack_name,
                "path": s.path,
                "up": s.up,
                "outputs": s.outputs,
                "error": s.error,
            }
            for s in statuses
        ]
        print(json.dumps(payload, indent=2, default=str))
        return 0

    width = max(len(Path(s.path).name) for s in statuses)
    for status in statuses:
        stack_label = Path(status.path).name.ljust(width)
        up_label = "UP  " if status.up else "DOWN"
        if status.up:
            detail = _format_output_summary(status.outputs)
        elif status.error:
            detail = f"error: {status.error}"
        else:
            detail = "(no outputs)"
        print(f"{stack_label}  {up_label}  {detail}")

    return 0
