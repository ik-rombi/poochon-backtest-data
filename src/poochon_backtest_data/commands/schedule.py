"""`schedule list|next` — inspect EventBridge schedules driving the cron jobs."""

from __future__ import annotations

from argparse import Namespace
from datetime import UTC, datetime, timedelta
import sys

from ..settings import get_settings
from ..storage import boto3_session

name = "schedule"


def register(subparsers) -> None:
    parser = subparsers.add_parser(name, help="Inspect EventBridge schedules")
    sub = parser.add_subparsers(dest="action", required=True)

    sub.add_parser("list", help="List all configured schedules")

    nxt = sub.add_parser("next", help="Show upcoming fire times")
    nxt.add_argument("--window", default="6h", help="Lookahead window (e.g. 1h, 6h, 24h)")


def handle(args: Namespace) -> int:
    settings = get_settings()
    session = boto3_session(settings.aws_region)
    client = session.client("scheduler")

    schedules = _list_schedules(client)
    if args.action == "list":
        return _print_list(schedules)
    if args.action == "next":
        window = _parse_window(args.window)
        return _print_next(schedules, window=window)
    raise RuntimeError(f"unsupported schedule action: {args.action}")


def _list_schedules(client) -> list[dict]:
    schedules: list[dict] = []
    paginator = client.get_paginator("list_schedules")
    for page in paginator.paginate():
        for summary in page.get("Schedules", []):
            try:
                detail = client.get_schedule(Name=summary["Name"], GroupName=summary.get("GroupName", "default"))
            except Exception as error:  # noqa: BLE001
                print(f"warning: get_schedule failed for {summary['Name']}: {error}", file=sys.stderr)
                continue
            schedules.append(detail)
    return schedules


def _print_list(schedules: list[dict]) -> int:
    if not schedules:
        print("No EventBridge schedules found.")
        return 0
    print(f"{'name':<40s}  {'expression':<28s}  {'state':<10s}  target")
    enabled_count = 0
    for schedule in sorted(schedules, key=lambda s: s["Name"]):
        name_field = schedule["Name"]
        expression = schedule.get("ScheduleExpression", "?")
        state = schedule.get("State", "?")
        target = schedule.get("Target", {}).get("Arn", "")
        target_short = target.rsplit(":", 1)[-1] if target else "-"
        print(f"{name_field:<40s}  {expression:<28s}  {state:<10s}  {target_short}")
        if state == "ENABLED":
            enabled_count += 1
    print()
    print(f"{len(schedules)} schedules, {enabled_count} ENABLED, {len(schedules) - enabled_count} disabled")
    return 0


def _print_next(schedules: list[dict], *, window: timedelta) -> int:
    if not schedules:
        print("No EventBridge schedules found.")
        return 0
    now = datetime.now(tz=UTC).replace(microsecond=0)
    horizon = now + window

    fires: list[tuple[datetime, str, str]] = []
    for schedule in schedules:
        if schedule.get("State") != "ENABLED":
            continue
        for fire in _next_fires_for_cron(
            schedule.get("ScheduleExpression", ""),
            schedule.get("ScheduleExpressionTimezone", "UTC"),
            now,
            horizon,
        ):
            fires.append((fire, schedule["Name"], schedule.get("Target", {}).get("Input") or ""))

    fires.sort(key=lambda item: item[0])
    print(f"Next {window} of scheduled fires (UTC = now {now.strftime('%H:%M')}):")
    print()
    for fire_time, schedule_name, input_payload in fires:
        delta = fire_time - now
        print(f"  in {_fmt_delta(delta):<10s}  {fire_time.strftime('%H:%M')}   {schedule_name}")
        if input_payload:
            print(f"                       input: {input_payload}")
    if not fires:
        print("  (no fires within the requested window)")
    return 0


def _next_fires_for_cron(
    expression: str, timezone: str, start: datetime, end: datetime
) -> list[datetime]:
    if not expression.startswith("cron("):
        return []
    body = expression[len("cron(") : -1]
    parts = body.split()
    if len(parts) != 6:
        return []
    minute, hour, day_of_month, month, day_of_week, year = parts

    fires: list[datetime] = []
    cursor = start.replace(second=0, microsecond=0)
    end_safe = end.replace(second=0, microsecond=0) + timedelta(minutes=1)
    while cursor <= end_safe and len(fires) < 100:
        if (
            _matches_cron_field(cursor.minute, minute)
            and _matches_cron_field(cursor.hour, hour)
            and _matches_cron_field(cursor.day, day_of_month)
            and _matches_cron_field(cursor.month, month)
            and _matches_cron_field_dow(cursor.weekday(), day_of_week)
            and _matches_cron_field(cursor.year, year)
        ):
            fires.append(cursor)
        cursor = cursor + timedelta(minutes=1)
    return fires


def _matches_cron_field(value: int, expr: str) -> bool:
    if expr in ("*", "?"):
        return True
    if expr.startswith("*/"):
        step = int(expr[2:])
        return value % step == 0
    if "," in expr:
        return any(_matches_cron_field(value, part) for part in expr.split(","))
    if "-" in expr:
        lo, hi = expr.split("-", 1)
        return int(lo) <= value <= int(hi)
    return value == int(expr)


def _matches_cron_field_dow(weekday_idx: int, expr: str) -> bool:
    # AWS cron day-of-week is 1-7 (Sun=1) but we only need wildcards for our schedules.
    if expr in ("*", "?"):
        return True
    return False


def _fmt_delta(delta: timedelta) -> str:
    seconds = int(delta.total_seconds())
    if seconds < 0:
        return "now"
    hours, remainder = divmod(seconds, 3600)
    minutes = remainder // 60
    if hours == 0:
        return f"{minutes}m"
    return f"{hours}h {minutes}m"


def _parse_window(raw: str) -> timedelta:
    if raw.endswith("h") and raw[:-1].isdigit():
        return timedelta(hours=int(raw[:-1]))
    if raw.endswith("m") and raw[:-1].isdigit():
        return timedelta(minutes=int(raw[:-1]))
    raise SystemExit(f"unsupported --window value: {raw}")
