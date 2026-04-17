from __future__ import annotations

from argparse import Namespace
from collections import Counter
import json
import sys

from ..models import (
    CoverageStatus,
    DatasetKind,
    MarketRef,
    MarketType,
    OutcomesMode,
    PolymarketMarketResolution,
    Venue,
    iter_dates_inclusive,
)
from ..polymarket_telonex import _series_key_from_slug
from ._session import open_session

name = "data"


def register(subparsers) -> None:
    parser = subparsers.add_parser(name, help="Inspect ingested data")
    data_subparsers = parser.add_subparsers(dest="data_command", required=True)

    inventory = data_subparsers.add_parser(
        "inventory",
        help="Summarize pipeline coverage for a venue / window",
    )
    inventory.add_argument("--venue", choices=["hyperliquid", "polymarket"], required=True)
    inventory.add_argument("--market-type", default=MarketType.PERP.value,
                           help="Hyperliquid only (perp/spot); ignored for polymarket")
    inventory.add_argument("--instrument",
                           help="Hyperliquid instrument (required for --venue hyperliquid)")
    inventory.add_argument("--series",
                           help="Polymarket series key (required for --venue polymarket)")
    inventory.add_argument("--start-date", required=True)
    inventory.add_argument("--end-date", required=True)
    inventory.add_argument(
        "--outcomes",
        choices=[item.value for item in OutcomesMode],
        default=OutcomesMode.BOTH.value,
    )
    inventory.add_argument("--depth", type=int, default=20)
    inventory.add_argument("--json", action="store_true", dest="as_json")

    coverage = data_subparsers.add_parser(
        "coverage",
        help="Dump DynamoDB coverage rows matching a pk prefix",
    )
    coverage.add_argument("--pk-prefix", required=True,
                          help="e.g. raw_l2#hyperliquid#perp#BTC#2026-02-19")
    coverage.add_argument("--json", action="store_true", dest="as_json")


def handle(args: Namespace) -> int:
    if args.data_command == "inventory":
        return _handle_inventory(args)
    if args.data_command == "coverage":
        return _handle_coverage(args)
    raise RuntimeError(f"unsupported data command: {args.data_command}")


def _count_statuses(records) -> tuple[int, int, int]:
    """Return (ready, failed, missing) from an iterable of records (None = missing)."""
    ready = failed = missing = 0
    for record in records:
        if record is None:
            missing += 1
        elif record.status == CoverageStatus.READY:
            ready += 1
        else:
            failed += 1
    return ready, failed, missing


def _handle_inventory(args: Namespace) -> int:
    if args.venue == "hyperliquid":
        if not args.instrument:
            print("--instrument is required for --venue hyperliquid", file=sys.stderr)
            return 2
        return _hyperliquid_inventory(args)
    if args.venue == "polymarket":
        if not args.series:
            print("--series is required for --venue polymarket", file=sys.stderr)
            return 2
        return _polymarket_inventory(args)
    raise RuntimeError(f"unsupported venue: {args.venue}")


_HYPERLIQUID_STAGES = (
    ("raw_l2", DatasetKind.RAW_L2, [f"{h:02d}" for h in range(24)]),
    ("raw_trades", DatasetKind.RAW_TRADES, [f"{h:02d}" for h in range(24)]),
    ("normalized_l2", DatasetKind.NORMALIZED_L2, [f"{h:02d}" for h in range(24)]),
    ("normalized_trades", DatasetKind.NORMALIZED_TRADES, [f"{h:02d}" for h in range(24)]),
)


def _hyperliquid_inventory(args: Namespace) -> int:
    bundle = open_session()
    market = MarketRef(
        venue=Venue.HYPERLIQUID,
        market_type=args.market_type,
        instrument=args.instrument,
    )
    dates = iter_dates_inclusive(args.start_date, args.end_date)

    report_rows: list[dict] = []
    for stage_label, dataset_kind, hours in _HYPERLIQUID_STAGES:
        cells = bundle.coverage_repo.list_window(
            dataset_kind=dataset_kind,
            market=market,
            start_date=args.start_date,
            end_date=args.end_date,
            hours=hours,
        )
        ready, failed, missing = _count_statuses(cells.values())
        report_rows.append({
            "stage": stage_label,
            "expected": len(cells),
            "ready": ready,
            "failed": failed,
            "missing": missing,
        })

    shards = bundle.shard_repo.list_hyperliquid_window(
        market=market,
        start_date=args.start_date,
        end_date=args.end_date,
        depth=args.depth,
    )
    canonical_ready = sum(1 for r in shards.values() if r is not None and r.status.value == "READY")
    canonical_failed = sum(1 for r in shards.values() if r is not None and r.status.value != "READY")
    canonical_missing = sum(1 for r in shards.values() if r is None)
    report_rows.append({
        "stage": "canonical",
        "expected": len(shards),
        "ready": canonical_ready,
        "failed": canonical_failed,
        "missing": canonical_missing,
    })

    return _emit_inventory(args, report_rows, scope={
        "venue": "hyperliquid",
        "market_type": args.market_type,
        "instrument": args.instrument,
        "start_date": args.start_date,
        "end_date": args.end_date,
        "depth": args.depth,
        "dates": dates,
    })


def _polymarket_resolutions_from_metadata(bundle, series_key: str) -> list[PolymarketMarketResolution]:
    """Find already-discovered PolymarketMarketResolutions for a series_key
    by reading metadata manifests from S3. Returns empty list if none present.
    """
    prefix = "metadata/polymarket/"
    resolutions: list[PolymarketMarketResolution] = []
    for key in bundle.s3_store.list_prefix(prefix):
        if not key.endswith("/manifest.json"):
            continue
        try:
            payload = bundle.s3_store.get_bytes(key)
            data = json.loads(payload)
        except Exception:
            continue
        slug = data.get("slug") or ""
        if _series_key_from_slug(slug) != series_key:
            continue
        try:
            resolution = PolymarketMarketResolution.model_validate(data)
        except Exception:
            continue
        resolutions.append(resolution)
    return resolutions


def _polymarket_inventory(args: Namespace) -> int:
    bundle = open_session()
    resolutions = _polymarket_resolutions_from_metadata(bundle, args.series)

    dates_window = set(iter_dates_inclusive(args.start_date, args.end_date))
    relevant: list[PolymarketMarketResolution] = [
        r for r in resolutions if set(r.dates) & dates_window
    ]

    report_rows: list[dict] = []

    for stage_label, dataset_kind in (
        ("raw_l2", DatasetKind.RAW_L2),
        ("raw_trades", DatasetKind.RAW_TRADES),
        ("normalized_l2", DatasetKind.NORMALIZED_L2),
        ("normalized_trades", DatasetKind.NORMALIZED_TRADES),
    ):
        ready = failed = missing = 0
        expected = 0
        for resolution in relevant:
            market = resolution.market_ref()
            dates = [d for d in resolution.dates if d in dates_window]
            if not dates:
                continue
            cells = bundle.coverage_repo.list_window(
                dataset_kind=dataset_kind,
                market=market,
                start_date=min(dates),
                end_date=max(dates),
                hours=["daily"],
            )
            # Filter down to only the dates we actually care about.
            filtered = {
                cell: record for cell, record in cells.items() if cell[0] in dates_window
            }
            expected += len(filtered)
            r, f, m = _count_statuses(filtered.values())
            ready += r
            failed += f
            missing += m
        report_rows.append({
            "stage": stage_label,
            "expected": expected,
            "ready": ready,
            "failed": failed,
            "missing": missing,
        })

    shards = bundle.shard_repo.list_polymarket_window(
        series_key=args.series,
        outcomes=OutcomesMode(args.outcomes),
        start_date=args.start_date,
        end_date=args.end_date,
        depth=args.depth,
    )
    canonical_ready = sum(1 for r in shards.values() if r is not None and r.status.value == "READY")
    canonical_failed = sum(1 for r in shards.values() if r is not None and r.status.value != "READY")
    canonical_missing = sum(1 for r in shards.values() if r is None)
    report_rows.append({
        "stage": "canonical",
        "expected": len(shards),
        "ready": canonical_ready,
        "failed": canonical_failed,
        "missing": canonical_missing,
    })

    return _emit_inventory(args, report_rows, scope={
        "venue": "polymarket",
        "series": args.series,
        "start_date": args.start_date,
        "end_date": args.end_date,
        "outcomes": args.outcomes,
        "depth": args.depth,
        "resolutions_found": len(relevant),
        "metadata_manifests_total": len(resolutions),
    })


def _emit_inventory(args: Namespace, rows: list[dict], *, scope: dict) -> int:
    if args.as_json:
        print(json.dumps({"scope": scope, "stages": rows}, indent=2, default=str))
        return 0

    print(f"Inventory for {scope}")
    print()
    header = ("stage", "expected", "ready", "failed", "missing")
    widths = [
        max(len(str(row[col])) for row in rows + [dict(zip(header, header))])
        for col in header
    ]
    def fmt_row(row: dict) -> str:
        return "  ".join(str(row[col]).ljust(widths[i]) for i, col in enumerate(header))
    print(fmt_row(dict(zip(header, header))))
    print("  ".join("-" * w for w in widths))
    for row in rows:
        print(fmt_row(row))
    return 0


def _handle_coverage(args: Namespace) -> int:
    bundle = open_session()
    # DynamoDB doesn't support prefix queries without a GSI; scan with a filter.
    client = bundle.coverage_repo.table.meta.client
    table_name = bundle.coverage_repo.table_name
    paginator = client.get_paginator("scan")
    matches: list[dict] = []
    for page in paginator.paginate(
        TableName=table_name,
        FilterExpression="begins_with(pk, :prefix)",
        ExpressionAttributeValues={":prefix": args.pk_prefix},
    ):
        matches.extend(page.get("Items", []))

    if args.as_json:
        print(json.dumps(matches, indent=2, default=str))
        return 0

    if not matches:
        print(f"no coverage rows match prefix {args.pk_prefix!r}")
        return 0
    for item in sorted(matches, key=lambda x: x.get("pk", "")):
        print(f"{item.get('pk'):80}  status={item.get('status'):8}  bytes={item.get('byte_count'):>12}")
    print(f"\n{len(matches)} row(s)")
    return 0
