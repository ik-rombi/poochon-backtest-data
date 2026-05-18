from __future__ import annotations

from argparse import Namespace
import json
from pathlib import Path
import sys

from ..golden import (
    DEFAULT_PM_GOLDEN_THRESHOLDS,
    check_polymarket_golden_thresholds,
    run_polymarket_golden_fixture,
    run_polymarket_golden_validation,
)

name = "validate"


def register(subparsers) -> None:
    parser = subparsers.add_parser(name, help="Run offline or fixture-backed validations")
    validate_subparsers = parser.add_subparsers(dest="target", required=True)

    pm = validate_subparsers.add_parser(
        "polymarket-golden",
        help="Rebuild a PM canonical sample and compare it to WS/live book state",
    )
    pm.add_argument("--fixture-prefix", help="s3://... or local directory containing manifest.json")
    pm.add_argument("--work-dir", type=Path, default=Path(".tmp/pm-golden"))
    pm.add_argument("--date")
    pm.add_argument("--live", type=Path)
    pm.add_argument(
        "--pmxt-hour",
        action="append",
        default=[],
        help="Hour/path pair, e.g. 02=/path/polymarket_orderbook_2026-05-18T02.parquet",
    )
    pm.add_argument("--depth", type=int, default=5)
    pm.add_argument("--canonical-out", type=Path)
    pm.add_argument("--out", type=Path)
    pm.add_argument(
        "--report-only",
        action="store_true",
        help="Print threshold errors but return success",
    )


def handle(args: Namespace) -> int:
    if args.target == "polymarket-golden":
        return _handle_polymarket_golden(args)
    raise RuntimeError(f"unsupported validate target: {args.target}")


def _handle_polymarket_golden(args: Namespace) -> int:
    if args.fixture_prefix:
        summary = run_polymarket_golden_fixture(
            fixture_prefix=args.fixture_prefix,
            work_dir=args.work_dir,
            canonical_out=args.canonical_out,
            report_out=args.out,
        )
    else:
        if args.date is None or args.live is None or not args.pmxt_hour:
            raise SystemExit(
                "without --fixture-prefix, specify --date, --live, and at least one --pmxt-hour"
            )
        summary = run_polymarket_golden_validation(
            date=args.date,
            live_path=args.live,
            pmxt_hours=_parse_pmxt_hours(args.pmxt_hour),
            depth=args.depth,
            canonical_out=args.canonical_out or args.work_dir / "canonical-data.parquet",
            report_out=args.out or args.work_dir / "compare-report.json",
            thresholds=DEFAULT_PM_GOLDEN_THRESHOLDS,
        )

    print(json.dumps(summary, indent=2, sort_keys=True))
    errors = summary.get("threshold_errors")
    if errors is None:
        errors = check_polymarket_golden_thresholds(summary, thresholds=DEFAULT_PM_GOLDEN_THRESHOLDS)
    if errors:
        print("threshold errors:", file=sys.stderr)
        for error in errors:
            print(f"  - {error}", file=sys.stderr)
        return 0 if args.report_only else 2
    return 0


def _parse_pmxt_hours(values: list[str]) -> dict[int, Path]:
    parsed: dict[int, Path] = {}
    for value in values:
        hour_raw, sep, path_raw = value.partition("=")
        if not sep:
            raise SystemExit("--pmxt-hour must be HH=/path/to/file.parquet")
        hour = int(hour_raw)
        if hour < 0 or hour > 23:
            raise SystemExit(f"invalid PMXT hour: {hour_raw}")
        parsed[hour] = Path(path_raw)
    return parsed
