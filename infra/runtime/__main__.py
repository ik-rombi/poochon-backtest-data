"""Step Functions + EventBridge schedules for the slim raw → slice pipeline.

Defines four state machines (pm-mirror, pm-slice, hl-mirror, hl-slice), each
backed by the shared ECS task definition with command overrides. Schedules are
configured via Pulumi config:

    poochon-backtest-data-runtime:
      pmMirrorCron:        cron(15 * * * ? *)
      hlMirrorCron:        cron(10 * * * ? *)
      pmSliceTargets:      ["series:btc-updown-5m", "slug:will-...-2026"]
      pmSliceCron:         cron(30 1 * * ? *)
      hlSliceMarkets:      ["perp:BTC", "perp:ETH"]
      hlSliceCron:         cron(0 1 * * ? *)
      hlSliceDepth:        20
      mirrorBackfillDays:  2

Manual one-off invocations: `submit polymarket slice --target series:KEY ...`
"""

from __future__ import annotations

import json

import pulumi
import pulumi_aws as aws


stack = pulumi.get_stack()
config = pulumi.Config()
prefix = config.get("namePrefix") or "poochon-backtest-data"
core_stack_ref = config.require("coreStackRef")
shared_stack_ref = config.require("sharedStackRef")
expected_aws_account_id = config.require("expectedAwsAccountId")

caller = aws.get_caller_identity_output()


def _require_expected_account(account_id: str) -> str:
    if account_id != expected_aws_account_id:
        raise ValueError(
            f"refusing to deploy to AWS account {account_id}; "
            f"expected {expected_aws_account_id}"
        )
    return account_id


aws_account_id = caller.account_id.apply(_require_expected_account)

core = pulumi.StackReference(core_stack_ref)
shared = pulumi.StackReference(shared_stack_ref)

bucket_name = core.require_output("data_bucket_name")
coverage_table_name = core.require_output("coverage_table_name")
shard_table_name = core.require_output("shard_table_name")

cluster_arn = shared.require_output("cluster_arn")
log_group_name = shared.require_output("log_group_name")
image_uri = shared.require_output("image_uri")
execution_role_arn = shared.require_output("execution_role_arn")
task_role_arn = shared.require_output("task_role_arn")
task_sg_id = shared.require_output("task_sg_id")
subnet_a_id = shared.require_output("subnet_a_id")
subnet_b_id = shared.require_output("subnet_b_id")

region = aws.get_region_output()


def _assume_role_policy(service: str) -> str:
    return json.dumps(
        {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": {"Service": service},
                    "Action": "sts:AssumeRole",
                }
            ],
        }
    )


# ---- shared task definition (one image, command override per state machine) --


_BASE_ENV = {
    "POOCHON_AWS_REGION": aws.config.region,
    "POOCHON_DATA_BUCKET": bucket_name,
    "POOCHON_COVERAGE_TABLE_NAME": coverage_table_name,
    "POOCHON_SHARD_TABLE_NAME": shard_table_name,
}


def _container_definitions(
    *,
    name: str,
    default_command: list[str],
) -> pulumi.Output[str]:
    env_outputs = {f"env__{k}": v for k, v in _BASE_ENV.items()}
    return pulumi.Output.all(image=image_uri, log_group=log_group_name, **env_outputs).apply(
        lambda values: json.dumps(
            [
                {
                    "name": "app",
                    "image": values["image"],
                    "essential": True,
                    "command": default_command,
                    "environment": [
                        {"name": k, "value": str(values[f"env__{k}"])}
                        for k in _BASE_ENV
                    ],
                    "logConfiguration": {
                        "logDriver": "awslogs",
                        "options": {
                            "awslogs-group": values["log_group"],
                            "awslogs-region": aws.config.region,
                            "awslogs-stream-prefix": name,
                        },
                    },
                }
            ]
        )
    )


task_definition = aws.ecs.TaskDefinition(
    "runtime-task-definition",
    family=f"{prefix}-{stack}",
    cpu="4096",
    memory="30720",
    network_mode="awsvpc",
    requires_compatibilities=["FARGATE"],
    execution_role_arn=execution_role_arn,
    task_role_arn=task_role_arn,
    runtime_platform=aws.ecs.TaskDefinitionRuntimePlatformArgs(
        cpu_architecture="ARM64",
        operating_system_family="LINUX",
    ),
    container_definitions=_container_definitions(
        name="runtime",
        default_command=["python", "-m", "poochon_backtest_data.cli", "--help"],
    ),
)


task_network = pulumi.Output.all(subnet_a_id, subnet_b_id, task_sg_id).apply(
    lambda args: {
        "AwsvpcConfiguration": {
            "Subnets": [args[0], args[1]],
            "SecurityGroups": [args[2]],
            "AssignPublicIp": "ENABLED",
        }
    }
)


# ---- IAM for Step Functions ------------------------------------------------


step_role = aws.iam.Role(
    "runtime-step-role",
    assume_role_policy=_assume_role_policy("states.amazonaws.com"),
)

aws.iam.RolePolicy(
    "runtime-step-policy",
    role=step_role.id,
    policy=pulumi.Output.all(
        cluster_arn,
        task_definition.arn,
        execution_role_arn,
        task_role_arn,
    ).apply(
        lambda args: json.dumps(
            {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Action": [
                            "ecs:RunTask",
                            "ecs:StopTask",
                            "ecs:DescribeTasks",
                        ],
                        "Resource": [args[1]],
                    },
                    {
                        "Effect": "Allow",
                        "Action": ["ecs:DescribeClusters"],
                        "Resource": [args[0]],
                    },
                    {
                        "Effect": "Allow",
                        "Action": ["iam:PassRole"],
                        "Resource": [args[2], args[3]],
                    },
                    {
                        "Effect": "Allow",
                        "Action": [
                            "events:PutTargets",
                            "events:PutRule",
                            "events:DescribeRule",
                        ],
                        "Resource": "*",
                    },
                ],
            }
        )
    ),
)


# ---- helpers to build state-machine definitions ----------------------------


_CLI = ["python", "-m", "poochon_backtest_data.cli"]


def _state_machine_definition(*, command_template: list, jsonpath_keys: list[str]) -> pulumi.Output[dict]:
    """Build a state machine definition that wraps ECS RunTask.

    `command_template` is a list of either:
      - str literals
      - dict {"path": "$.fieldname"}  (treated as JSONPath input refs; injected via States.Format)
    """
    arg_exprs: list[str] = []
    for piece in command_template:
        if isinstance(piece, dict) and "path" in piece:
            # Use States.Format('{}', $.field) to coerce non-string ints to strings.
            arg_exprs.append(f"States.Format('{{}}', {piece['path']})")
        elif isinstance(piece, str):
            arg_exprs.append(repr(piece))
        else:
            raise TypeError(f"unsupported command template piece: {piece!r}")
    command_intrinsic = "States.Array(" + ", ".join(arg_exprs) + ")"

    return pulumi.Output.all(
        cluster=cluster_arn,
        task_definition=task_definition.arn,
        network=task_network,
    ).apply(
        lambda values: {
            "StartAt": "RunTask",
            "States": {
                "RunTask": {
                    "Type": "Task",
                    "Resource": "arn:aws:states:::ecs:runTask.sync",
                    "Parameters": {
                        "LaunchType": "FARGATE",
                        "Cluster": values["cluster"],
                        "TaskDefinition": values["task_definition"],
                        "NetworkConfiguration": values["network"],
                        "Overrides": {
                            "ContainerOverrides": [
                                {
                                    "Name": "app",
                                    "Command.$": command_intrinsic,
                                }
                            ]
                        },
                    },
                    "End": True,
                }
            },
        }
    )


def _make_state_machine(*, slug: str, command_template: list) -> aws.sfn.StateMachine:
    return aws.sfn.StateMachine(
        f"sm-{slug}",
        name=f"{prefix}-{slug}-{stack}",
        role_arn=step_role.arn,
        definition=_state_machine_definition(
            command_template=command_template, jsonpath_keys=[]
        ).apply(json.dumps),
    )


# pm-mirror — input: { start_offset_days, end_offset_days }
pm_mirror_sm = _make_state_machine(
    slug="pm-mirror",
    command_template=[
        *_CLI,
        "run",
        "polymarket",
        "mirror",
        "--start-offset-days",
        {"path": "$.start_offset_days"},
        "--end-offset-days",
        {"path": "$.end_offset_days"},
    ],
)

# pm-slice — input: { target_kind, target_key, start_offset_days, end_offset_days }
pm_slice_sm = _make_state_machine(
    slug="pm-slice",
    command_template=[
        *_CLI,
        "run",
        "polymarket",
        "slice",
        "--target",
        {"path": "$.target"},
        "--start-offset-days",
        {"path": "$.start_offset_days"},
        "--end-offset-days",
        {"path": "$.end_offset_days"},
    ],
)

# hl-mirror — input: { instrument, market_type, start_offset_days, end_offset_days }
hl_mirror_sm = _make_state_machine(
    slug="hl-mirror",
    command_template=[
        *_CLI,
        "run",
        "hyperliquid",
        "mirror",
        "--instrument",
        {"path": "$.instrument"},
        "--market-type",
        {"path": "$.market_type"},
        "--start-offset-days",
        {"path": "$.start_offset_days"},
        "--end-offset-days",
        {"path": "$.end_offset_days"},
    ],
)

# hl-slice — input: { instrument, market_type, depth, start_offset_days, end_offset_days }
hl_slice_sm = _make_state_machine(
    slug="hl-slice",
    command_template=[
        *_CLI,
        "run",
        "hyperliquid",
        "slice",
        "--instrument",
        {"path": "$.instrument"},
        "--market-type",
        {"path": "$.market_type"},
        "--depth",
        {"path": "$.depth"},
        "--start-offset-days",
        {"path": "$.start_offset_days"},
        "--end-offset-days",
        {"path": "$.end_offset_days"},
    ],
)


# ---- EventBridge schedules -------------------------------------------------


scheduler_role = aws.iam.Role(
    "runtime-scheduler-role",
    assume_role_policy=_assume_role_policy("scheduler.amazonaws.com"),
)

aws.iam.RolePolicy(
    "runtime-scheduler-policy",
    role=scheduler_role.id,
    policy=pulumi.Output.all(
        pm_mirror_sm.arn,
        pm_slice_sm.arn,
        hl_mirror_sm.arn,
        hl_slice_sm.arn,
    ).apply(
        lambda arns: json.dumps(
            {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Action": ["states:StartExecution"],
                        "Resource": list(arns),
                    }
                ],
            }
        )
    ),
)


def _schedule(
    name: str,
    *,
    cron: str,
    target_arn: pulumi.Input[str],
    payload: dict,
) -> aws.scheduler.Schedule:
    return aws.scheduler.Schedule(
        name,
        name=f"{prefix}-{name}-{stack}",
        schedule_expression=cron,
        flexible_time_window=aws.scheduler.ScheduleFlexibleTimeWindowArgs(mode="OFF"),
        schedule_expression_timezone="UTC",
        target=aws.scheduler.ScheduleTargetArgs(
            arn=target_arn,
            role_arn=scheduler_role.arn,
            input=json.dumps(payload),
        ),
    )


pm_mirror_cron = config.get("pmMirrorCron") or "cron(15 * * * ? *)"
hl_mirror_cron = config.get("hlMirrorCron") or "cron(10 * * * ? *)"
pm_slice_cron = config.get("pmSliceCron") or "cron(30 1 * * ? *)"
hl_slice_cron = config.get("hlSliceCron") or "cron(0 1 * * ? *)"
mirror_backfill_days = config.get_int("mirrorBackfillDays") or 2
hl_slice_depth = config.get_int("hlSliceDepth") or 20

pm_slice_targets = config.get_object("pmSliceTargets") or []
hl_slice_markets = config.get_object("hlSliceMarkets") or []


_pm_mirror_schedule = _schedule(
    "pm-mirror-hourly",
    cron=pm_mirror_cron,
    target_arn=pm_mirror_sm.arn,
    payload={"start_offset_days": -mirror_backfill_days, "end_offset_days": 0},
)


def _hl_mirror_payload(market: str) -> dict:
    if ":" not in market:
        raise ValueError(f"hlSliceMarkets entries must be 'market_type:INSTRUMENT', got '{market}'")
    market_type, instrument = market.split(":", 1)
    return {
        "instrument": instrument,
        "market_type": market_type,
        "start_offset_days": -mirror_backfill_days,
        "end_offset_days": 0,
    }


for raw in hl_slice_markets:
    payload = _hl_mirror_payload(raw)
    safe = raw.replace(":", "-").replace("/", "-")
    _schedule(
        f"hl-mirror-{safe}",
        cron=hl_mirror_cron,
        target_arn=hl_mirror_sm.arn,
        payload=payload,
    )


for raw in pm_slice_targets:
    if ":" not in raw:
        raise ValueError(f"pmSliceTargets entries must be 'series:KEY' or 'slug:KEY', got '{raw}'")
    target_kind, target_key = raw.split(":", 1)
    safe_kind = target_kind
    safe_key = target_key.replace(":", "-").replace("/", "-")
    _schedule(
        f"pm-slice-{safe_kind}-{safe_key}",
        cron=pm_slice_cron,
        target_arn=pm_slice_sm.arn,
        payload={
            "target": raw,
            "start_offset_days": -1,
            "end_offset_days": -1,
        },
    )


for raw in hl_slice_markets:
    if ":" not in raw:
        raise ValueError(f"hlSliceMarkets entries must be 'market_type:INSTRUMENT', got '{raw}'")
    market_type, instrument = raw.split(":", 1)
    safe = raw.replace(":", "-").replace("/", "-")
    _schedule(
        f"hl-slice-{safe}",
        cron=hl_slice_cron,
        target_arn=hl_slice_sm.arn,
        payload={
            "instrument": instrument,
            "market_type": market_type,
            "depth": hl_slice_depth,
            "start_offset_days": -1,
            "end_offset_days": -1,
        },
    )


# ---- exports ---------------------------------------------------------------


pulumi.export("pm_mirror_state_machine_arn", pm_mirror_sm.arn)
pulumi.export("pm_slice_state_machine_arn", pm_slice_sm.arn)
pulumi.export("hl_mirror_state_machine_arn", hl_mirror_sm.arn)
pulumi.export("hl_slice_state_machine_arn", hl_slice_sm.arn)
pulumi.export("scheduler_role_arn", scheduler_role.arn)
