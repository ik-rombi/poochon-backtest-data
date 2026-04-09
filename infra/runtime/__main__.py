from __future__ import annotations

import json
from pathlib import Path

import pulumi
import pulumi_aws as aws
import pulumi_docker as docker


stack = pulumi.get_stack()
config = pulumi.Config()
prefix = config.get("namePrefix") or "poochon-backtest-data"
core_stack_ref = config.require("coreStackRef")

ingestion_mode = (config.get("ingestionMode") or "disabled").lower()
ingestion_venue = (config.get("ingestionVenue") or "hyperliquid").lower()
ingestion_market_type = config.get("ingestionMarketType") or "perp"
ingestion_instrument = config.get("ingestionInstrument") or "BTC"
ingestion_start_date = config.get("ingestionStartDate")
ingestion_end_date = config.get("ingestionEndDate")
ingestion_start_offset_days = config.get_int("ingestionStartOffsetDays")
ingestion_end_offset_days = config.get_int("ingestionEndOffsetDays")
ingestion_start_at = config.get("ingestionStartAt")
cron_expression = config.get("cronExpression")
ingestion_slug = config.get("ingestionSlug")
ingestion_outcome = config.get("ingestionOutcome")
telonex_api_key = config.get_secret("telonexApiKey")

core = pulumi.StackReference(core_stack_ref)
bucket_name = core.require_output("data_bucket_name")
coverage_table_name = core.require_output("coverage_table_name")
replay_table_name = core.require_output("replay_table_name")

region = aws.get_region_output()
caller = aws.get_caller_identity_output()
availability_zones = aws.get_availability_zones(state="available")


def build_ingestion_input() -> dict[str, int | str]:
    if ingestion_mode == "disabled":
        return {}
    payload: dict[str, int | str] = {"venue": ingestion_venue}
    if ingestion_venue == "polymarket":
        if ingestion_mode == "cron":
            raise ValueError("cron ingestion is not supported for polymarket")
        if not ingestion_slug or not ingestion_outcome:
            raise ValueError("ingestionSlug and ingestionOutcome are required for polymarket ingestion")
        payload["slug"] = ingestion_slug
        payload["outcome"] = ingestion_outcome
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


vpc = aws.ec2.Vpc(
    "runtime-vpc",
    cidr_block="10.42.0.0/16",
    enable_dns_support=True,
    enable_dns_hostnames=True,
    tags={"Name": f"{prefix}-{stack}"},
)

internet_gateway = aws.ec2.InternetGateway(
    "runtime-igw",
    vpc_id=vpc.id,
    tags={"Name": f"{prefix}-{stack}-igw"},
)

route_table = aws.ec2.RouteTable(
    "runtime-public-route-table",
    vpc_id=vpc.id,
    routes=[
        aws.ec2.RouteTableRouteArgs(
            cidr_block="0.0.0.0/0",
            gateway_id=internet_gateway.id,
        )
    ],
)

subnet_a = aws.ec2.Subnet(
    "runtime-subnet-a",
    vpc_id=vpc.id,
    cidr_block="10.42.0.0/24",
    availability_zone=availability_zones.names[0],
    map_public_ip_on_launch=True,
)
subnet_b = aws.ec2.Subnet(
    "runtime-subnet-b",
    vpc_id=vpc.id,
    cidr_block="10.42.1.0/24",
    availability_zone=availability_zones.names[1],
    map_public_ip_on_launch=True,
)

aws.ec2.RouteTableAssociation(
    "runtime-rta-a",
    subnet_id=subnet_a.id,
    route_table_id=route_table.id,
)
aws.ec2.RouteTableAssociation(
    "runtime-rta-b",
    subnet_id=subnet_b.id,
    route_table_id=route_table.id,
)

alb_sg = aws.ec2.SecurityGroup(
    "alb-sg",
    vpc_id=vpc.id,
    description="Public ALB security group",
    ingress=[
        aws.ec2.SecurityGroupIngressArgs(
            protocol="tcp",
            from_port=80,
            to_port=80,
            cidr_blocks=["0.0.0.0/0"],
        )
    ],
    egress=[
        aws.ec2.SecurityGroupEgressArgs(
            protocol="-1",
            from_port=0,
            to_port=0,
            cidr_blocks=["0.0.0.0/0"],
        )
    ],
)

task_sg = aws.ec2.SecurityGroup(
    "task-sg",
    vpc_id=vpc.id,
    description="ECS task security group",
    ingress=[
        aws.ec2.SecurityGroupIngressArgs(
            protocol="tcp",
            from_port=8080,
            to_port=8080,
            security_groups=[alb_sg.id],
        )
    ],
    egress=[
        aws.ec2.SecurityGroupEgressArgs(
            protocol="-1",
            from_port=0,
            to_port=0,
            cidr_blocks=["0.0.0.0/0"],
        )
    ],
)

cluster = aws.ecs.Cluster("runtime-cluster", name=f"{prefix}-{stack}")

log_group = aws.cloudwatch.LogGroup(
    "runtime-log-group",
    retention_in_days=7,
)

ecr_repo = aws.ecr.Repository(
    "app-repository",
    image_scanning_configuration=aws.ecr.RepositoryImageScanningConfigurationArgs(
        scan_on_push=True
    ),
    force_delete=True,
)

ecr_auth = aws.ecr.get_authorization_token_output()

image = docker.Image(
    "app-image",
    image_name=ecr_repo.repository_url.apply(lambda url: f"{url}:latest"),
    build=docker.DockerBuildArgs(
        context=str((Path(__file__).parent / "../..").resolve()),
        dockerfile=str((Path(__file__).parent / "../../Dockerfile").resolve()),
        platform="linux/arm64",
    ),
    registry=docker.RegistryArgs(
        server=ecr_repo.repository_url.apply(lambda url: url.split("/")[0]),
        username=ecr_auth.user_name,
        password=ecr_auth.password,
    ),
)

execution_role = aws.iam.Role(
    "ecs-execution-role",
    assume_role_policy=assume_role_policy("ecs-tasks.amazonaws.com"),
)

aws.iam.RolePolicyAttachment(
    "ecs-execution-policy",
    role=execution_role.name,
    policy_arn="arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy",
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
        role=execution_role.id,
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

task_role = aws.iam.Role(
    "ecs-task-role",
    assume_role_policy=assume_role_policy("ecs-tasks.amazonaws.com"),
)

bucket_arn = bucket_name.apply(lambda name: f"arn:aws:s3:::{name}")
bucket_objects_arn = bucket_name.apply(lambda name: f"arn:aws:s3:::{name}/*")
hyperliquid_archive_bucket_arn = "arn:aws:s3:::hyperliquid-archive"
hyperliquid_archive_objects_arn = "arn:aws:s3:::hyperliquid-archive/*"
hyperliquid_trades_bucket_arn = "arn:aws:s3:::hl-mainnet-node-data"
hyperliquid_trades_objects_arn = "arn:aws:s3:::hl-mainnet-node-data/*"

aws.iam.RolePolicy(
    "ecs-task-data-policy",
    role=task_role.id,
    policy=pulumi.Output.all(
        bucket_arn,
        bucket_objects_arn,
        coverage_table_name,
        replay_table_name,
        region.name,
        caller.account_id,
    ).apply(
        lambda args: json.dumps(
            {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Action": ["s3:GetObject", "s3:PutObject", "s3:HeadObject"],
                        "Resource": [args[1]],
                    },
                    {
                        "Effect": "Allow",
                        "Action": [
                            "s3:GetObject",
                            "s3:GetObjectVersion",
                            "s3:GetBucketLocation",
                            "s3:GetBucketRequestPayment",
                            "s3:ListBucket",
                        ],
                        "Resource": [
                            hyperliquid_archive_bucket_arn,
                            hyperliquid_archive_objects_arn,
                            hyperliquid_trades_bucket_arn,
                            hyperliquid_trades_objects_arn,
                        ],
                    },
                    {
                        "Effect": "Allow",
                        "Action": ["s3:ListBucket"],
                        "Resource": [args[0]],
                    },
                    {
                        "Effect": "Allow",
                        "Action": ["dynamodb:GetItem", "dynamodb:PutItem", "dynamodb:UpdateItem"],
                        "Resource": [
                            f"arn:aws:dynamodb:{args[4]}:{args[5]}:table/{args[2]}",
                            f"arn:aws:dynamodb:{args[4]}:{args[5]}:table/{args[3]}",
                        ],
                    },
                ],
            }
        )
    ),
)


def container_definitions(
    *,
    image_name: pulumi.Input[str],
    command: list[str],
    env: dict[str, pulumi.Input[str]],
    secrets: dict[str, pulumi.Input[str]] | None = None,
    log_group_name: pulumi.Input[str],
    port: int | None = None,
) -> pulumi.Output[str]:
    secrets = secrets or {}
    env_outputs = {f"env__{name}": value for name, value in env.items()}
    secret_outputs = {f"secret__{name}": value for name, value in secrets.items()}
    return pulumi.Output.all(
        image=image_name,
        log_group=log_group_name,
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


base_env = {
    "POOCHON_AWS_REGION": aws.config.region,
    "POOCHON_DATA_BUCKET": bucket_name,
    "POOCHON_COVERAGE_TABLE_NAME": coverage_table_name,
    "POOCHON_REPLAY_TABLE_NAME": replay_table_name,
}
ingest_container_secrets = (
    {"POOCHON_TELONEX_API_KEY": telonex_secret.arn}
    if telonex_secret is not None
    else {}
)

runtime_platform = aws.ecs.TaskDefinitionRuntimePlatformArgs(
    cpu_architecture="ARM64",
    operating_system_family="LINUX",
)

ingest_task_definition = aws.ecs.TaskDefinition(
    "ingest-task-definition",
    family=f"{prefix}-ingest-{stack}",
    cpu="1024",
    memory="2048",
    network_mode="awsvpc",
    requires_compatibilities=["FARGATE"],
    execution_role_arn=execution_role.arn,
    task_role_arn=task_role.arn,
    runtime_platform=runtime_platform,
    container_definitions=container_definitions(
        image_name=image.image_name,
        command=[
            "python",
            "-m",
            "poochon_backtest_data.cli",
            "ingest-range",
            "--market-type",
            ingestion_market_type,
            "--instrument",
            ingestion_instrument,
            "--start-date",
            ingestion_start_date or "1970-01-01",
            "--end-date",
            ingestion_end_date or "1970-01-01",
        ],
        env=base_env,
        secrets=ingest_container_secrets,
        log_group_name=log_group.name,
    ),
)

materialize_task_definition = aws.ecs.TaskDefinition(
    "materialize-task-definition",
    family=f"{prefix}-materialize-{stack}",
    cpu="1024",
    memory="2048",
    network_mode="awsvpc",
    requires_compatibilities=["FARGATE"],
    execution_role_arn=execution_role.arn,
    task_role_arn=task_role.arn,
    runtime_platform=runtime_platform,
    container_definitions=container_definitions(
        image_name=image.image_name,
        command=[
            "python",
            "-m",
            "poochon_backtest_data.cli",
            "materialize-replay",
            "--market-type",
            ingestion_market_type,
            "--instrument",
            ingestion_instrument,
            "--date",
            ingestion_start_date or "1970-01-01",
            "--depth",
            "20",
        ],
        env=base_env,
        log_group_name=log_group.name,
    ),
)

task_network = pulumi.Output.all(subnet_a.id, subnet_b.id, task_sg.id).apply(
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
        cluster.arn,
        ingest_task_definition.arn,
        materialize_task_definition.arn,
        execution_role.arn,
        task_role.arn,
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
                        "Resource": [args[1], args[2]],
                    },
                    {
                        "Effect": "Allow",
                        "Action": ["ecs:DescribeClusters"],
                        "Resource": [args[0]],
                    },
                    {
                        "Effect": "Allow",
                        "Action": ["iam:PassRole"],
                        "Resource": [args[3], args[4]],
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


def ecs_run_task_state(task_definition_arn: pulumi.Input[str], command_state: str) -> pulumi.Output[dict]:
    return pulumi.Output.all(
        cluster=cluster.arn,
        task_definition=task_definition_arn,
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
    explicit=ecs_run_task_state(
        ingest_task_definition.arn,
        "States.Array('python','-m','poochon_backtest_data.cli','ingest-range','--market-type',$.market_type,'--instrument',$.instrument,'--start-date',$.start_date,'--end-date',$.end_date)",
    ),
    relative=ecs_run_task_state(
        ingest_task_definition.arn,
        "States.Array('python','-m','poochon_backtest_data.cli','ingest-range','--market-type',$.market_type,'--instrument',$.instrument,'--start-offset-days',States.Format('{}',$.start_offset_days),'--end-offset-days',States.Format('{}',$.end_offset_days))",
    ),
    polymarket=ecs_run_task_state(
        ingest_task_definition.arn,
        "States.Array('python','-m','poochon_backtest_data.cli','polymarket-ingest-market','--slug',$.slug,'--outcome',$.outcome)",
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
                            "Next": "IngestPolymarket",
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
                            "Next": "IngestRangeExplicit",
                        },
                        {
                            "Variable": "$.start_offset_days",
                            "IsPresent": True,
                            "Next": "IngestRangeRelative",
                        },
                    ],
                    "Default": "MissingWindow",
                },
                "IngestRangeExplicit": {**states["explicit"], "End": True},
                "IngestRangeRelative": {**states["relative"], "End": True},
                "IngestPolymarket": {**states["polymarket"], "End": True},
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

materialize_definition = ecs_run_task_state(
    materialize_task_definition.arn,
    "States.Array('python','-m','poochon_backtest_data.cli','materialize-replay','--market-type',$.market_type,'--instrument',$.instrument,'--date',$.date,'--depth',States.Format('{}',$.depth))",
)

materialize_definition = pulumi.Output.all(
    hyperliquid=materialize_definition,
    polymarket=ecs_run_task_state(
        materialize_task_definition.arn,
        "States.Array('python','-m','poochon_backtest_data.cli','polymarket-materialize-replay','--slug',$.slug,'--outcome',$.outcome,'--depth',States.Format('{}',$.depth))",
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
                            "Next": "MaterializePolymarketReplay",
                        },
                        {
                            "Variable": "$.venue",
                            "StringEquals": "hyperliquid",
                            "Next": "MaterializeHyperliquidReplay",
                        },
                    ],
                    "Default": "MaterializeHyperliquidReplay",
                },
                "MaterializeHyperliquidReplay": {**states["hyperliquid"], "End": True},
                "MaterializePolymarketReplay": {**states["polymarket"], "End": True},
            },
        }
    )
)

materialize_state_machine = aws.sfn.StateMachine(
    "materialize-state-machine",
    role_arn=step_role.arn,
    definition=materialize_definition,
)

api_task_definition = aws.ecs.TaskDefinition(
    "api-task-definition",
    family=f"{prefix}-api-{stack}",
    cpu="512",
    memory="1024",
    network_mode="awsvpc",
    requires_compatibilities=["FARGATE"],
    execution_role_arn=execution_role.arn,
    task_role_arn=task_role.arn,
    runtime_platform=runtime_platform,
    container_definitions=container_definitions(
        image_name=image.image_name,
        command=["python", "-m", "poochon_backtest_data.cli", "api"],
        env={**base_env, "POOCHON_REPLAY_STATE_MACHINE_ARN": materialize_state_machine.arn},
        log_group_name=log_group.name,
        port=8080,
    ),
)

aws.iam.RolePolicy(
    "ecs-task-stepfunctions-policy",
    role=task_role.id,
    policy=materialize_state_machine.arn.apply(
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

alb = aws.lb.LoadBalancer(
    "runtime-alb",
    load_balancer_type="application",
    security_groups=[alb_sg.id],
    subnets=[subnet_a.id, subnet_b.id],
)

target_group = aws.lb.TargetGroup(
    "api-target-group",
    port=8080,
    protocol="HTTP",
    target_type="ip",
    vpc_id=vpc.id,
    health_check=aws.lb.TargetGroupHealthCheckArgs(
        path="/api/v1/health",
        protocol="HTTP",
        matcher="200",
    ),
)

listener = aws.lb.Listener(
    "api-listener",
    load_balancer_arn=alb.arn,
    port=80,
    protocol="HTTP",
    default_actions=[
        aws.lb.ListenerDefaultActionArgs(
            type="forward",
            target_group_arn=target_group.arn,
        )
    ],
)

api_service = aws.ecs.Service(
    "api-service",
    cluster=cluster.arn,
    desired_count=1,
    launch_type="FARGATE",
    task_definition=api_task_definition.arn,
    deployment_minimum_healthy_percent=0,
    deployment_maximum_percent=100,
    network_configuration=aws.ecs.ServiceNetworkConfigurationArgs(
        subnets=[subnet_a.id, subnet_b.id],
        security_groups=[task_sg.id],
        assign_public_ip=True,
    ),
    load_balancers=[
        aws.ecs.ServiceLoadBalancerArgs(
            target_group_arn=target_group.arn,
            container_name="app",
            container_port=8080,
        )
    ],
    wait_for_steady_state=True,
    opts=pulumi.ResourceOptions(depends_on=[listener]),
)

pulumi.export("cluster_arn", cluster.arn)
pulumi.export("api_url", alb.dns_name.apply(lambda dns: f"http://{dns}"))
pulumi.export("ingestion_state_machine_arn", ingestion_state_machine.arn)
pulumi.export("materialize_state_machine_arn", materialize_state_machine.arn)
pulumi.export(
    "ingestion_schedule_name",
    pulumi.Output.from_input(None)
    if ingestion_schedule is None
    else ingestion_schedule.name,
)
