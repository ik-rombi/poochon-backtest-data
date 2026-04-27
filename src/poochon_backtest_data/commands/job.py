from __future__ import annotations

from argparse import Namespace
from datetime import datetime, UTC
import json
import os
import sys
import time

from ..settings import get_settings
from ..storage import boto3_session
from .submit import resolve_state_machine_arn

name = "job"

_VALID_STATUSES = (
    "RUNNING",
    "SUCCEEDED",
    "FAILED",
    "TIMED_OUT",
    "ABORTED",
    "PENDING_REDRIVE",
)


_KIND_TO_OUTPUT_KEY = {
    "pm-mirror": "pm_mirror_state_machine_arn",
    "pm-slice": "pm_slice_state_machine_arn",
    "hl-mirror": "hl_mirror_state_machine_arn",
    "hl-slice": "hl_slice_state_machine_arn",
}


def register(subparsers) -> None:
    parser = subparsers.add_parser(name, help="Inspect Step Functions executions")
    job_subparsers = parser.add_subparsers(dest="job_command", required=True)

    list_parser = job_subparsers.add_parser("list", help="List recent executions")
    list_parser.add_argument("--limit", type=int, default=10)
    list_parser.add_argument("--status", choices=list(_VALID_STATUSES))
    list_parser.add_argument("--stack", default=os.environ.get("POOCHON_PULUMI_STACK", "dev"))
    list_parser.add_argument(
        "--kind",
        choices=list(_KIND_TO_OUTPUT_KEY) + ["all"],
        default="all",
        help="Which state machine to query; defaults to all",
    )
    list_parser.add_argument("--state-machine-arn", default=None)
    list_parser.add_argument("--json", action="store_true", dest="as_json")

    status = job_subparsers.add_parser("status", help="Describe a specific execution")
    status.add_argument("execution_arn")
    status.add_argument("--json", action="store_true", dest="as_json")

    logs = job_subparsers.add_parser("logs", help="Tail CloudWatch logs for an execution's task")
    logs.add_argument("execution_arn")
    logs.add_argument("--follow", action="store_true")
    logs.add_argument("--stack", default=os.environ.get("POOCHON_PULUMI_STACK", "dev"))
    logs.add_argument("--log-group", default=os.environ.get("POOCHON_INGESTION_LOG_GROUP"),
                      help="CloudWatch log group; default discovered from Pulumi shared stack output")


def handle(args: Namespace) -> int:
    if args.job_command == "list":
        return _handle_list(args)
    if args.job_command == "status":
        return _handle_status(args)
    if args.job_command == "logs":
        return _handle_logs(args)
    raise RuntimeError(f"unsupported job command: {args.job_command}")


def _sfn_client():
    settings = get_settings()
    session = boto3_session(settings.aws_region)
    return session.client("stepfunctions")


def _ecs_client():
    settings = get_settings()
    session = boto3_session(settings.aws_region)
    return session.client("ecs")


def _logs_client():
    settings = get_settings()
    session = boto3_session(settings.aws_region)
    return session.client("logs")


def _iso(ts):
    if ts is None:
        return None
    if isinstance(ts, datetime):
        return ts.astimezone(UTC).isoformat()
    return str(ts)


def _handle_list(args: Namespace) -> int:
    if args.state_machine_arn:
        groups = [(args.kind if args.kind != "all" else "explicit", args.state_machine_arn)]
    elif args.kind == "all":
        groups = []
        for kind, output_key in _KIND_TO_OUTPUT_KEY.items():
            try:
                arn = resolve_state_machine_arn(args.stack, output_key)
                groups.append((kind, arn))
            except RuntimeError as error:
                print(f"warning: skipping {kind}: {error}", file=sys.stderr)
    else:
        try:
            arn = resolve_state_machine_arn(args.stack, _KIND_TO_OUTPUT_KEY[args.kind])
        except RuntimeError as error:
            print(f"error: {error}", file=sys.stderr)
            return 1
        groups = [(args.kind, arn)]

    if not groups:
        print("no state machines found", file=sys.stderr)
        return 1

    sfn = _sfn_client()
    output_groups: list[tuple[str, list[dict]]] = []
    for kind, arn in groups:
        kwargs = {"stateMachineArn": arn, "maxResults": args.limit}
        if args.status:
            kwargs["statusFilter"] = args.status
        response = sfn.list_executions(**kwargs)
        output_groups.append((kind, response.get("executions", [])))

    if args.as_json:
        payload = []
        for kind, executions in output_groups:
            for e in executions:
                payload.append(
                    {
                        "kind": kind,
                        "execution_arn": e["executionArn"],
                        "name": e["name"],
                        "status": e["status"],
                        "start_date": _iso(e.get("startDate")),
                        "stop_date": _iso(e.get("stopDate")),
                    }
                )
        print(json.dumps(payload, indent=2, default=str))
        return 0

    any_executions = False
    for kind, executions in output_groups:
        if not executions:
            continue
        any_executions = True
        print(f"\n[{kind}]")
        for e in executions:
            start = _iso(e.get("startDate")) or "-"
            stop = _iso(e.get("stopDate")) or "-"
            print(f"  {e['status']:10}  {start:30}  {stop:30}  {e['name']}")
    if not any_executions:
        print("no executions found")
    return 0


def _handle_status(args: Namespace) -> int:
    response = _sfn_client().describe_execution(executionArn=args.execution_arn)
    summary = {
        "execution_arn": response["executionArn"],
        "state_machine_arn": response["stateMachineArn"],
        "name": response["name"],
        "status": response["status"],
        "start_date": _iso(response.get("startDate")),
        "stop_date": _iso(response.get("stopDate")),
        "input": response.get("input"),
        "output": response.get("output"),
        "cause": response.get("cause"),
        "error": response.get("error"),
    }

    # For running executions, try to attach the ECS task status.
    ecs_task = None
    if response["status"] == "RUNNING":
        ecs_task = _find_ecs_task_status(args.execution_arn)
    summary["ecs_task"] = ecs_task

    if args.as_json:
        print(json.dumps(summary, indent=2, default=str))
        return 0

    print(f"name:        {summary['name']}")
    print(f"status:      {summary['status']}")
    print(f"start:       {summary['start_date']}")
    print(f"stop:        {summary['stop_date'] or '-'}")
    if summary["input"]:
        print(f"input:       {summary['input']}")
    if summary["output"]:
        print(f"output:      {summary['output']}")
    if summary["error"]:
        print(f"error:       {summary['error']}")
        print(f"cause:       {summary.get('cause')}")
    if ecs_task:
        print(f"ecs task:    {ecs_task['task_arn']}")
        print(f"last status: {ecs_task.get('last_status')}")
        print(f"desired:     {ecs_task.get('desired_status')}")
    return 0


def _find_ecs_task_status(execution_arn: str) -> dict | None:
    sfn = _sfn_client()
    history = sfn.get_execution_history(
        executionArn=execution_arn,
        reverseOrder=False,
        maxResults=500,
    )
    task_arn = None
    cluster_arn = None
    for event in history.get("events", []):
        details = event.get("taskSubmittedEventDetails")
        if not details:
            continue
        output = details.get("output")
        if not output:
            continue
        try:
            payload = json.loads(output)
        except (TypeError, json.JSONDecodeError):
            continue
        tasks = payload.get("Tasks") or payload.get("tasks")
        if tasks:
            task_arn = tasks[0].get("TaskArn") or tasks[0].get("taskArn")
            cluster_arn = tasks[0].get("ClusterArn") or tasks[0].get("clusterArn")
            break
    if not task_arn:
        return None

    ecs = _ecs_client()
    described = ecs.describe_tasks(cluster=cluster_arn, tasks=[task_arn])
    tasks = described.get("tasks", [])
    if not tasks:
        return {"task_arn": task_arn}
    task = tasks[0]
    return {
        "task_arn": task_arn,
        "cluster_arn": cluster_arn,
        "last_status": task.get("lastStatus"),
        "desired_status": task.get("desiredStatus"),
        "stopped_reason": task.get("stoppedReason"),
    }


def _handle_logs(args: Namespace) -> int:
    task_info = _find_ecs_task_status(args.execution_arn)
    if not task_info or "task_arn" not in task_info:
        print("no ECS task found for this execution yet", file=sys.stderr)
        return 1

    task_id = task_info["task_arn"].split("/")[-1]
    log_group = args.log_group or os.environ.get("POOCHON_LOG_GROUP") or _discover_log_group(args.stack)
    if not log_group:
        print(
            "could not determine log group; pass --log-group or set POOCHON_INGESTION_LOG_GROUP",
            file=sys.stderr,
        )
        return 1
    # Stream name matches the `awslogs-stream-prefix` set in the task definition.
    log_stream = f"poochon/app/{task_id}"

    logs = _logs_client()

    def tail_once(next_token: str | None) -> tuple[list[dict], str | None]:
        kwargs = {
            "logGroupName": log_group,
            "logStreamName": log_stream,
            "startFromHead": True,
        }
        if next_token:
            kwargs["nextToken"] = next_token
        response = logs.get_log_events(**kwargs)
        return response.get("events", []), response.get("nextForwardToken")

    next_token = None
    printed_any = False
    while True:
        try:
            events, new_token = tail_once(next_token)
        except logs.exceptions.ResourceNotFoundException:
            if not args.follow:
                print(f"log stream {log_stream} not yet created", file=sys.stderr)
                return 1
            time.sleep(2)
            continue
        for event in events:
            printed_any = True
            ts = datetime.fromtimestamp(event["timestamp"] / 1000, tz=UTC).isoformat()
            print(f"{ts}  {event['message']}")
        if new_token == next_token:
            if not args.follow:
                if not printed_any:
                    print("(no log events yet)", file=sys.stderr)
                return 0
            time.sleep(2)
            continue
        next_token = new_token


def _discover_log_group(stack: str) -> str | None:
    from . import infra as infra_cmd
    repo_root = infra_cmd._resolve_repo_root(None)
    for candidate in ("shared", "runtime"):
        stack_dir = repo_root / "infra" / candidate
        if not stack_dir.exists():
            continue
        status = infra_cmd._pulumi_output(stack, stack_dir)
        if status.up and "log_group_name" in status.outputs:
            return status.outputs["log_group_name"]
    return None
