from __future__ import annotations

from datetime import UTC, datetime, timedelta
import json
from pathlib import Path

import pulumi
import pulumi_aws as aws
import pulumi_docker as docker


stack = pulumi.get_stack()
config = pulumi.Config()
prefix = config.get("namePrefix") or "poochon-backtest-data"
proof_symbol = config.get("proofSymbol") or "BTC"
proof_date = config.get("proofDate") or "2025-05-24"
bootstrap_at = config.get("bootstrapAt") or (
    datetime.now(tz=UTC) + timedelta(minutes=10)
).strftime("%Y-%m-%dT%H:%M:%S")
core_stack_ref = config.require("coreStackRef")

core = pulumi.StackReference(core_stack_ref)
bucket_name = core.require_output("data_bucket_name")
coverage_table_name = core.require_output("coverage_table_name")
replay_table_name = core.require_output("replay_table_name")

region = aws.get_region_output()
caller = aws.get_caller_identity_output()
availability_zones = aws.get_availability_zones(state="available")


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

task_role = aws.iam.Role(
    "ecs-task-role",
    assume_role_policy=assume_role_policy("ecs-tasks.amazonaws.com"),
)

bucket_arn = bucket_name.apply(lambda name: f"arn:aws:s3:::{name}")
bucket_objects_arn = bucket_name.apply(lambda name: f"arn:aws:s3:::{name}/*")
hyperliquid_archive_objects_arn = "arn:aws:s3:::hyperliquid-archive/*"
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
                        "Action": ["s3:GetObject"],
                        "Resource": [
                            hyperliquid_archive_objects_arn,
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
    log_group_name: pulumi.Input[str],
    port: int | None = None,
) -> pulumi.Output[str]:
    return pulumi.Output.all(image=image_name, log_group=log_group_name, **env).apply(
        lambda values: json.dumps(
            [
                {
                    "name": "app",
                    "image": values["image"],
                    "essential": True,
                    "command": command,
                    "environment": [
                        {"name": name, "value": str(values[name])}
                        for name in env
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

runtime_platform = aws.ecs.TaskDefinitionRuntimePlatformArgs(
    cpu_architecture="ARM64",
    operating_system_family="LINUX",
)

backfill_task_definition = aws.ecs.TaskDefinition(
    "backfill-task-definition",
    family=f"{prefix}-backfill-{stack}",
    cpu="512",
    memory="1024",
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
            "backfill-day",
            "--symbol",
            proof_symbol,
            "--date",
            proof_date,
        ],
        env=base_env,
        log_group_name=log_group.name,
    ),
)

normalize_task_definition = aws.ecs.TaskDefinition(
    "normalize-task-definition",
    family=f"{prefix}-normalize-{stack}",
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
            "normalize-day",
            "--symbol",
            proof_symbol,
            "--date",
            proof_date,
        ],
        env=base_env,
        log_group_name=log_group.name,
    ),
)

materialize_task_definition_placeholder = aws.ecs.TaskDefinition(
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
            "--symbol",
            proof_symbol,
            "--date",
            proof_date,
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
        backfill_task_definition.arn,
        normalize_task_definition.arn,
        materialize_task_definition_placeholder.arn,
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
                        "Resource": [args[1], args[2], args[3]],
                    },
                    {
                        "Effect": "Allow",
                        "Action": ["ecs:DescribeClusters"],
                        "Resource": [args[0]],
                    },
                    {
                        "Effect": "Allow",
                        "Action": ["iam:PassRole"],
                        "Resource": [args[4], args[5]],
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
    backfill=ecs_run_task_state(
        backfill_task_definition.arn,
        "States.Array('python','-m','poochon_backtest_data.cli','backfill-day','--symbol',$.symbol,'--date',$.date)",
    ),
    normalize=ecs_run_task_state(
        normalize_task_definition.arn,
        "States.Array('python','-m','poochon_backtest_data.cli','normalize-day','--symbol',$.symbol,'--date',$.date)",
    ),
).apply(
    lambda states: json.dumps(
        {
            "StartAt": "BackfillDay",
            "States": {
                "BackfillDay": {**states["backfill"], "Next": "NormalizeDay"},
                "NormalizeDay": {**states["normalize"], "End": True},
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
    materialize_task_definition_placeholder.arn,
    "States.Array('python','-m','poochon_backtest_data.cli','materialize-replay','--symbol',$.symbol,'--date',$.date)",
).apply(
    lambda state: json.dumps({"StartAt": "MaterializeReplay", "States": {"MaterializeReplay": {**state, "End": True}}})
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

bootstrap_schedule = aws.scheduler.Schedule(
    "bootstrap-ingestion-schedule",
    schedule_expression=f"at({bootstrap_at})",
    flexible_time_window=aws.scheduler.ScheduleFlexibleTimeWindowArgs(mode="OFF"),
    target=aws.scheduler.ScheduleTargetArgs(
        arn=ingestion_state_machine.arn,
        role_arn=scheduler_role.arn,
        input=json.dumps({"symbol": proof_symbol, "date": proof_date}),
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
pulumi.export("bootstrap_schedule_name", bootstrap_schedule.name)
