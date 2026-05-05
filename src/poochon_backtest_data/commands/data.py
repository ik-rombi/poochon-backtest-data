"""`data <venue> raw|slice` — operator inspection of mirrored raw + canonical slices.

Reads only DynamoDB + S3. Never compares against external services (Gamma, etc.).
"""

from __future__ import annotations

from argparse import Namespace
from collections import defaultdict

from ..models import (
    CoverageStatus,
    MarketRef,
    MarketType,
    PolymarketTarget,
    PolymarketTargetKind,
    Venue,
)

name = "data"


def register(subparsers) -> None:
    parser = subparsers.add_parser(name, help="Inspect mirrored raw and canonical slices")
    venue_subparsers = parser.add_subparsers(dest="venue", required=True)

    pm = venue_subparsers.add_parser("polymarket")
    pm_stage = pm.add_subparsers(dest="layer", required=True)

    pm_raw = pm_stage.add_parser("raw", help="raw_pmxt mirror status")
    pm_raw.add_argument("--since", default=None, help="e.g., 7d, 30d (ignored if --start-date set)")
    pm_raw.add_argument("--start-date")
    pm_raw.add_argument("--end-date")

    pm_slice = pm_stage.add_parser("slice", help="canonical PM slices")
    pm_slice.add_argument(
        "target",
        nargs="?",
        default=None,
        help="series:KEY or slug:KEY (omit to list all)",
    )
    pm_slice.add_argument("--since", default=None)
    pm_slice.add_argument("--start-date")
    pm_slice.add_argument("--end-date")

    hl = venue_subparsers.add_parser("hyperliquid")
    hl_stage = hl.add_subparsers(dest="layer", required=True)

    hl_raw = hl_stage.add_parser("raw", help="HL raw archive status")
    hl_raw.add_argument("instrument", nargs="?", default=None, help="INSTRUMENT/MARKET_TYPE filter")
    hl_raw.add_argument("--since", default=None)
    hl_raw.add_argument("--start-date")
    hl_raw.add_argument("--end-date")

    hl_slice = hl_stage.add_parser("slice", help="canonical HL slices")
    hl_slice.add_argument("instrument", nargs="?", default=None)
    hl_slice.add_argument("--since", default=None)
    hl_slice.add_argument("--start-date")
    hl_slice.add_argument("--end-date")
    hl_slice.add_argument("--depth", type=int, default=20)


def handle(args: Namespace) -> int:
    if args.venue == "polymarket" and args.layer == "raw":
        return _handle_pm_raw(args)
    if args.venue == "polymarket" and args.layer == "slice":
        return _handle_pm_slice(args)
    if args.venue == "hyperliquid" and args.layer == "raw":
        return _handle_hl_raw(args)
    if args.venue == "hyperliquid" and args.layer == "slice":
        return _handle_hl_slice(args)
    raise RuntimeError(f"unsupported data target: {args.venue}/{args.layer}")


def _resolve_window(args: Namespace) -> tuple[str, str]:
    if args.start_date and args.end_date:
        return args.start_date, args.end_date
    from datetime import UTC, datetime, timedelta

    today = datetime.now(tz=UTC).date()
    days = 7
    if args.since:
        if args.since.endswith("d") and args.since[:-1].isdigit():
            days = int(args.since[:-1])
        else:
            raise SystemExit(f"unsupported --since value: {args.since}")
    return (today - timedelta(days=days - 1)).isoformat(), today.isoformat()


def _parse_target(raw: str | None) -> PolymarketTarget | None:
    if raw is None:
        return None
    if ":" not in raw:
        raise SystemExit("target must be 'series:KEY' or 'slug:KEY'")
    kind_raw, key = raw.split(":", 1)
    return PolymarketTarget(target_kind=PolymarketTargetKind(kind_raw), target_key=key)


def _parse_hl_market(raw: str | None) -> MarketRef | None:
    if raw is None:
        return None
    if "/" not in raw:
        raise SystemExit("hyperliquid filter must be 'INSTRUMENT/MARKET_TYPE'")
    instrument, market_type = raw.split("/", 1)
    return MarketRef(
        venue=Venue.HYPERLIQUID,
        market_type=MarketType(market_type),
        instrument=instrument,
    )


def _handle_pm_raw(args: Namespace) -> int:
    from ._session import open_session

    bundle = open_session()
    start, end = _resolve_window(args)
    cells = bundle.coverage_repo.list_raw_pmxt_window(start_date=start, end_date=end)
    rows: dict[str, dict[str, str]] = defaultdict(dict)
    for (date, hour), record in cells.items():
        rows[date][hour] = record.status.value if record else "MISSING"

    print("raw_pmxt")
    print("  date        hours        missing")
    total_hours = 0
    total_present = 0
    for date in sorted(rows):
        hours = rows[date]
        present_count = sum(1 for status in hours.values() if status == CoverageStatus.READY.value)
        missing = sorted(
            hour for hour, status in hours.items() if status != CoverageStatus.READY.value
        )
        missing_str = ", ".join(missing) if missing else "-"
        print(f"  {date}  {present_count:2d}/{len(hours):2d}        {missing_str}")
        total_hours += len(hours)
        total_present += present_count
    days = len(rows)
    gaps = sum(
        1 for date in rows if any(s != CoverageStatus.READY.value for s in rows[date].values())
    )
    print()
    print(f"{days} days  •  {total_present}/{total_hours} hours  •  {gaps} day(s) with gaps")
    return 0


def _handle_pm_slice(args: Namespace) -> int:
    from ._session import open_session

    bundle = open_session()
    start, end = _resolve_window(args)
    target_filter = _parse_target(args.target)

    print("canonical/polymarket")
    if target_filter is not None:
        records = bundle.shard_repo.list_pm_window(
            target=target_filter, start_date=start, end_date=end, depth=5
        )
        _print_pm_slice_block(target_filter, records)
        return 0

    print("(no target specified — pass series:KEY or slug:KEY to inspect a specific target)")
    return 0


def _print_pm_slice_block(target: PolymarketTarget, records) -> None:
    print()
    print(f"  {target.target_kind.value}={target.target_key}")
    print("    date        status   events       bytes      ts range (UTC)")
    for date in sorted(records):
        record = records[date]
        if record is None:
            print(f"    {date}  MISSING       -            -        -")
            continue
        ts_range = (
            f"{_fmt_ts(record.start_ts_ms)}..{_fmt_ts(record.end_ts_ms)}"
            if record.start_ts_ms is not None and record.end_ts_ms is not None
            else "-"
        )
        print(
            f"    {date}  {record.status.value:6s}  {record.event_count:>10,}   "
            f"{_fmt_bytes(record.byte_count):>8s}   {ts_range}"
        )


def _handle_hl_raw(args: Namespace) -> int:
    from ._session import open_session

    bundle = open_session()
    start, end = _resolve_window(args)
    market_filter = _parse_hl_market(args.instrument)

    if market_filter is None:
        print("(specify INSTRUMENT/MARKET_TYPE to inspect raw_hl_l2; raw_hl_fills is firehose)")
        fills_cells = bundle.coverage_repo.list_raw_hl_fills_window(start_date=start, end_date=end)
        _print_firehose_table("raw_hl_fills", fills_cells)
        return 0

    l2_cells = bundle.coverage_repo.list_raw_hl_l2_window(
        market=market_filter, start_date=start, end_date=end
    )
    _print_per_instrument_table(
        f"raw_hl_l2  market={market_filter.market_type.value}/{market_filter.instrument}",
        l2_cells,
    )

    fills_cells = bundle.coverage_repo.list_raw_hl_fills_window(start_date=start, end_date=end)
    _print_firehose_table("raw_hl_fills (firehose)", fills_cells)
    return 0


def _handle_hl_slice(args: Namespace) -> int:
    from ._session import open_session

    bundle = open_session()
    start, end = _resolve_window(args)
    market_filter = _parse_hl_market(args.instrument)

    print("canonical/hyperliquid")
    if market_filter is None:
        print("(specify INSTRUMENT/MARKET_TYPE to inspect a specific market)")
        return 0
    records = bundle.shard_repo.list_hl_window(
        market=market_filter, start_date=start, end_date=end, depth=args.depth
    )
    print()
    print(
        f"  market={market_filter.market_type.value}/{market_filter.instrument}  depth={args.depth}"
    )
    print("    date        status   events       bytes      ts range (UTC)")
    for date in sorted(records):
        record = records[date]
        if record is None:
            print(f"    {date}  MISSING       -            -        -")
            continue
        ts_range = (
            f"{_fmt_ts(record.start_ts_ms)}..{_fmt_ts(record.end_ts_ms)}"
            if record.start_ts_ms is not None and record.end_ts_ms is not None
            else "-"
        )
        print(
            f"    {date}  {record.status.value:6s}  {record.event_count:>10,}   "
            f"{_fmt_bytes(record.byte_count):>8s}   {ts_range}"
        )
    return 0


def _print_firehose_table(label: str, cells: dict) -> None:
    print()
    print(label)
    print("  date        hours        missing")
    rows: dict[str, dict[str, str]] = defaultdict(dict)
    for (date, hour), record in cells.items():
        rows[date][hour] = record.status.value if record else "MISSING"
    for date in sorted(rows):
        hours = rows[date]
        present_count = sum(1 for s in hours.values() if s == CoverageStatus.READY.value)
        missing = sorted(h for h, s in hours.items() if s != CoverageStatus.READY.value)
        missing_str = ", ".join(missing) if missing else "-"
        print(f"  {date}  {present_count:2d}/{len(hours):2d}        {missing_str}")


def _print_per_instrument_table(label: str, cells: dict) -> None:
    print()
    print(label)
    print("  date        hours        missing")
    rows: dict[str, dict[str, str]] = defaultdict(dict)
    for (date, hour), record in cells.items():
        rows[date][hour] = record.status.value if record else "MISSING"
    for date in sorted(rows):
        hours = rows[date]
        present_count = sum(1 for s in hours.values() if s == CoverageStatus.READY.value)
        missing = sorted(h for h, s in hours.items() if s != CoverageStatus.READY.value)
        missing_str = ", ".join(missing) if missing else "-"
        print(f"  {date}  {present_count:2d}/{len(hours):2d}        {missing_str}")


def _fmt_ts(ts_ms: int | None) -> str:
    if ts_ms is None:
        return "-"
    from datetime import UTC, datetime

    return datetime.fromtimestamp(ts_ms / 1000, tz=UTC).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3]


def _fmt_bytes(byte_count: int) -> str:
    if byte_count <= 0:
        return "-"
    if byte_count >= 1_073_741_824:
        return f"{byte_count / 1_073_741_824:.2f} GB"
    if byte_count >= 1_048_576:
        return f"{byte_count / 1_048_576:.2f} MB"
    if byte_count >= 1024:
        return f"{byte_count / 1024:.2f} KB"
    return f"{byte_count} B"
