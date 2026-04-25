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


def require_expected_account(account_id: str) -> str:
    if account_id != expected_aws_account_id:
        raise ValueError(
            f"refusing to deploy to AWS account {account_id}; "
            f"expected {expected_aws_account_id}"
        )
    return account_id


aws_account_id = caller.account_id.apply(require_expected_account)

ingestion_mode = (config.get("ingestionMode") or "disabled").lower()
ingestion_venue = (config.get("ingestionVenue") or "hyperliquid").lower()
ingestion_market_type = config.get("ingestionMarketType") or "perp"
ingestion_instrument = config.get("ingestionInstrument") or "BTC"
ingestion_series = config.get("ingestionSeries") or "btc-updown-5m"
ingestion_outcomes = config.get("ingestionOutcomes") or "both"
ingestion_start_date = config.get("ingestionStartDate")
ingestion_end_date = config.get("ingestionEndDate")
ingestion_start_offset_days = config.get_int("ingestionStartOffsetDays")
ingestion_end_offset_days = config.get_int("ingestionEndOffsetDays")
ingestion_depth = config.get_int("ingestionDepth")
ingestion_start_at = config.get("ingestionStartAt")
cron_expression = config.get("cronExpression")
telonex_api_key = config.get_secret("telonexApiKey")

core = pulumi.StackReference(core_stack_ref)
shared = pulumi.StackReference(shared_stack_ref)

bucket_name = core.require_output("data_bucket_name")
coverage_table_name = core.require_output("coverage_table_name")
replay_shard_table_name = core.require_output("replay_shard_table_name")

cluster_arn = shared.require_output("cluster_arn")
log_group_name = shared.require_output("log_group_name")
image_uri = shared.require_output("image_uri")
execution_role_arn = shared.require_output("execution_role_arn")
execution_role_name = shared.require_output("execution_role_name")
task_role_arn = shared.require_output("task_role_arn")
task_sg_id = shared.require_output("task_sg_id")
subnet_a_id = shared.require_output("subnet_a_id")
subnet_b_id = shared.require_output("subnet_b_id")


def build_ingestion_input() -> dict[str, int | str]:
    if ingestion_mode == "disabled":
        return {}
    payload: dict[str, int | str] = {"venue": ingestion_venue}
    if ingestion_venue == "polymarket":
        if ingestion_mode == "cron":
            raise ValueError("cron ingestion is not supported for polymarket")
        if not ingestion_start_date or not ingestion_end_date:
            raise ValueError("ingestionStartDate and ingestionEndDate are required for polymarket")
        payload["series"] = ingestion_series
        payload["outcomes"] = ingestion_outcomes
        payload["start_date"] = ingestion_start_date
        payload["end_date"] = ingestion_end_date
        payload["depth"] = ingestion_depth or 5
        return payload
    if ingestion_venue != "hyperliquid":
        raise ValueError("ingestionVenue must be hyperliquid or polymarket")
    explicit = ingestion_start_date is not None or ingestion_end_date is not None
    relative = (
        ingestion_start_offset_days is not None or ingestion_end_offset_days is not None
    )
    if explicit == relative:
        raise ValueError(
            "configure either ingestionStartDate/ingestionEndDate or "
            "ingestionStartOffsetDays/ingestionEndOffsetDays"
        )
    payload["market_type"] = ingestion_market_type
    payload["instrument"] = ingestion_instrument
    payload["depth"] = ingestion_depth or 20
    if explicit:
        if ingestion_start_date is None or ingestion_end_date is None:
            raise ValueError("ingestionStartDate and ingestionEndDate are both required")
        payload["start_date"] = ingestion_start_date
        payload["end_date"] = ingestion_end_date
    else:
        if ingestion_start_offset_days is None or ingestion_end_offset_days is None:
            raise ValueError(
                "ingestionStartOffsetDays and ingestionEndOffsetDays are both required"
            )
        payload["start_offset_days"] = ingestion_start_offset_days
        payload["end_offset_days"] = ingestion_end_offset_days
    return payload


ingestion_input = build_ingestion_input()
if ingestion_mode == "once" and not ingestion_start_at:
    raise ValueError("ingestionStartAt is required when ingestionMode=once")
if ingestion_mode == "cron" and not cron_expression:
    raise ValueError("cronExpression is required when ingestionMode=cron")
if ingestion_mode not in {"disabled", "once", "cron"}:
    raise ValueError("ingestionMode must be one of disabled, once, or cron")
if ingestion_venue == "polymarket" and ingestion_mode != "disabled" and telonex_api_key is None:
    raise ValueError("telonexApiKey is required for polymarket ingestion")


def assume_role_policy(service: str) -> str:
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


telonex_secret = None
if telonex_api_key is not None:
    telonex_secret = aws.secretsmanager.Secret(
        "telonex-api-key-secret",
        name=f"{prefix}-{stack}-telonex-api-key",
        recovery_window_in_days=0,
    )
    aws.secretsmanager.SecretVersion(
        "telonex-api-key-secret-version",
        secret_id=telonex_secret.id,
        secret_string=telonex_api_key,
    )
    aws.iam.RolePolicy(
        "ecs-execution-secrets-policy",
        role=execution_role_name,
        policy=telonex_secret.arn.apply(
            lambda arn: json.dumps(
                {
                    "Version": "2012-10-17",
                    "Statement": [
                        {
                            "Effect": "Allow",
                            "Action": ["secretsmanager:GetSecretValue"],
                            "Resource": [arn],
                        }
                    ],
                }
            )
        ),
    )


runtime_platform = aws.ecs.TaskDefinitionRuntimePlatformArgs(
    cpu_architecture="ARM64",
    operating_system_family="LINUX",
)

base_env = {
    "POOCHON_AWS_REGION": aws.config.region,
    "POOCHON_DATA_BUCKET": bucket_name,
    "POOCHON_COVERAGE_TABLE_NAME": coverage_table_name,
    "POOCHON_SHARD_TABLE_NAME": replay_shard_table_name,
}
sync_container_secrets = (
    {"POOCHON_TELONEX_API_KEY": telonex_secret.arn}
    if telonex_secret is not None
    else {}
)


def container_definitions(
    *,
    image_name: pulumi.Input[str],
    command: list[str],
    env: dict[str, pulumi.Input[str]],
    secrets: dict[str, pulumi.Input[str]] | None = None,
    log_group: pulumi.Input[str],
    port: int | None = None,
) -> pulumi.Output[str]:
    secrets = secrets or {}
    env_outputs = {f"env__{name}": value for name, value in env.items()}
    secret_outputs = {f"secret__{name}": value for name, value in secrets.items()}
    return pulumi.Output.all(
        image=image_name,
        log_group=log_group,
        **env_outputs,
        **secret_outputs,
    ).apply(
        lambda values: json.dumps(
            [
                {
                    "name": "app",
                    "image": values["image"],
                    "essential": True,
                    "command": command,
                    "environment": [
                        {"name": name, "value": str(values[f"env__{name}"])}
                        for name in env
                    ],
                    "secrets": [
                        {"name": name, "valueFrom": str(values[f"secret__{name}"])}
                        for name in secrets
                    ],
                    "portMappings": (
                        [{"containerPort": port, "hostPort": port, "protocol": "tcp"}]
                        if port is not None
                        else []
                    ),
                    "logConfiguration": {
                        "logDriver": "awslogs",
                        "options": {
                            "awslogs-group": values["log_group"],
                            "awslogs-region": aws.config.region,
                            "awslogs-stream-prefix": "poochon",
                        },
                    },
                }
            ]
        )
    )


sync_task_definition = aws.ecs.TaskDefinition(
    "sync-task-definition",
    family=f"{prefix}-sync-{stack}",
    cpu="4096",
    memory="16384",
    network_mode="awsvpc",
    requires_compatibilities=["FARGATE"],
    execution_role_arn=execution_role_arn,
    task_role_arn=task_role_arn,
    runtime_platform=runtime_platform,
    container_definitions=container_definitions(
        image_name=image_uri,
        command=[
            "python",
            "-m",
            "poochon_backtest_data.cli",
            "hyperliquid-sync-window",
            "--market-type",
            ingestion_market_type,
            "--instrument",
            ingestion_instrument,
            "--start-date",
            ingestion_start_date or "1970-01-01",
            "--end-date",
            ingestion_end_date or "1970-01-01",
            "--depth",
            str(ingestion_depth or 20),
        ],
        env=base_env,
        secrets=sync_container_secrets,
        log_group=log_group_name,
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

step_role = aws.iam.Role(
    "step-functions-role",
    assume_role_policy=assume_role_policy("states.amazonaws.com"),
)

aws.iam.RolePolicy(
    "step-functions-policy",
    role=step_role.id,
    policy=pulumi.Output.all(
        cluster_arn,
        sync_task_definition.arn,
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


def ecs_run_task_state(command_state: str) -> pulumi.Output[dict]:
    return pulumi.Output.all(
        cluster=cluster_arn,
        task_definition=sync_task_definition.arn,
        network=task_network,
    ).apply(
        lambda values: {
            "Type": "Task",
            "Resource": "arn:aws:states:::ecs:runTask.sync",
            "ResultPath": None,
            "Parameters": {
                "LaunchType": "FARGATE",
                "Cluster": values["cluster"],
                "TaskDefinition": values["task_definition"],
                "NetworkConfiguration": values["network"],
                "Overrides": {
                    "ContainerOverrides": [
                        {
                            "Name": "app",
                            "Command.$": command_state,
                        }
                    ]
                },
            },
        }
    )


ingestion_definition = pulumi.Output.all(
    hyperliquid_explicit=ecs_run_task_state(
        "States.Array('python','-m','poochon_backtest_data.cli','hyperliquid-sync-window','--market-type',$.market_type,'--instrument',$.instrument,'--start-date',$.start_date,'--end-date',$.end_date,'--depth',States.Format('{}',$.depth))"
    ),
    hyperliquid_relative=ecs_run_task_state(
        "States.Array('python','-m','poochon_backtest_data.cli','hyperliquid-sync-window','--market-type',$.market_type,'--instrument',$.instrument,'--start-offset-days',States.Format('{}',$.start_offset_days),'--end-offset-days',States.Format('{}',$.end_offset_days),'--depth',States.Format('{}',$.depth))"
    ),
    polymarket=ecs_run_task_state(
        "States.Array('python','-m','poochon_backtest_data.cli','polymarket-sync-series','--series',$.series,'--start-date',$.start_date,'--end-date',$.end_date,'--outcomes',$.outcomes,'--depth',States.Format('{}',$.depth))"
    ),
).apply(
    lambda states: json.dumps(
        {
            "StartAt": "ResolveVenue",
            "States": {
                "ResolveVenue": {
                    "Type": "Choice",
                    "Choices": [
                        {
                            "Variable": "$.venue",
                            "StringEquals": "polymarket",
                            "Next": "SyncPolymarketSeries",
                        },
                        {
                            "Variable": "$.venue",
                            "StringEquals": "hyperliquid",
                            "Next": "ResolveWindowMode",
                        },
                    ],
                    "Default": "MissingVenue",
                },
                "ResolveWindowMode": {
                    "Type": "Choice",
                    "Choices": [
                        {
                            "Variable": "$.start_date",
                            "IsPresent": True,
                            "Next": "SyncHyperliquidExplicit",
                        },
                        {
                            "Variable": "$.start_offset_days",
                            "IsPresent": True,
                            "Next": "SyncHyperliquidRelative",
                        },
                    ],
                    "Default": "MissingWindow",
                },
                "SyncHyperliquidExplicit": {**states["hyperliquid_explicit"], "End": True},
                "SyncHyperliquidRelative": {**states["hyperliquid_relative"], "End": True},
                "SyncPolymarketSeries": {**states["polymarket"], "End": True},
                "MissingVenue": {
                    "Type": "Fail",
                    "Error": "MissingVenue",
                    "Cause": "A supported venue is required",
                },
                "MissingWindow": {
                    "Type": "Fail",
                    "Error": "MissingWindow",
                    "Cause": "Either start_date/end_date or start_offset_days/end_offset_days is required",
                },
            },
        }
    )
)

ingestion_state_machine = aws.sfn.StateMachine(
    "ingestion-state-machine",
    role_arn=step_role.arn,
    definition=ingestion_definition,
)

scheduler_role = aws.iam.Role(
    "scheduler-role",
    assume_role_policy=assume_role_policy("scheduler.amazonaws.com"),
)

aws.iam.RolePolicy(
    "scheduler-policy",
    role=scheduler_role.id,
    policy=ingestion_state_machine.arn.apply(
        lambda arn: json.dumps(
            {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Action": ["states:StartExecution"],
                        "Resource": [arn],
                    }
                ],
            }
        )
    ),
)

ingestion_schedule = None
if ingestion_mode == "once":
    ingestion_schedule = aws.scheduler.Schedule(
        "ingestion-schedule",
        schedule_expression=f"at({ingestion_start_at})",
        flexible_time_window=aws.scheduler.ScheduleFlexibleTimeWindowArgs(mode="OFF"),
        target=aws.scheduler.ScheduleTargetArgs(
            arn=ingestion_state_machine.arn,
            role_arn=scheduler_role.arn,
            input=json.dumps(ingestion_input),
        ),
        schedule_expression_timezone="UTC",
    )
elif ingestion_mode == "cron":
    ingestion_schedule = aws.scheduler.Schedule(
        "ingestion-schedule",
        schedule_expression=cron_expression,
        flexible_time_window=aws.scheduler.ScheduleFlexibleTimeWindowArgs(mode="OFF"),
        target=aws.scheduler.ScheduleTargetArgs(
            arn=ingestion_state_machine.arn,
            role_arn=scheduler_role.arn,
            input=json.dumps(ingestion_input),
        ),
        schedule_expression_timezone="UTC",
    )


pulumi.export("sync_task_definition_arn", sync_task_definition.arn)
pulumi.export("ingestion_state_machine_arn", ingestion_state_machine.arn)
pulumi.export(
    "ingestion_schedule_name",
    ingestion_schedule.name if ingestion_schedule is not None else pulumi.Output.from_input(""),
)
pulumi.export("aws_account_id", aws_account_id)
if telonex_secret is not None:
    pulumi.export("telonex_secret_arn", telonex_secret.arn)
