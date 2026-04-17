from __future__ import annotations

import json

import pulumi
import pulumi_aws as aws


stack = pulumi.get_stack()
config = pulumi.Config()
prefix = config.get("namePrefix") or "poochon-backtest-data"
core_stack_ref = config.require("coreStackRef")
shared_stack_ref = config.require("sharedStackRef")

core = pulumi.StackReference(core_stack_ref)
shared = pulumi.StackReference(shared_stack_ref)

bucket_name = core.require_output("data_bucket_name")
coverage_table_name = core.require_output("coverage_table_name")
replay_shard_table_name = core.require_output("replay_shard_table_name")

vpc_id = shared.require_output("vpc_id")
subnet_a_id = shared.require_output("subnet_a_id")
subnet_b_id = shared.require_output("subnet_b_id")
task_sg_id = shared.require_output("task_sg_id")
cluster_arn = shared.require_output("cluster_arn")
log_group_name = shared.require_output("log_group_name")
image_uri = shared.require_output("image_uri")
execution_role_arn = shared.require_output("execution_role_arn")
task_role_arn = shared.require_output("task_role_arn")


alb_sg = aws.ec2.SecurityGroup(
    "alb-sg",
    vpc_id=vpc_id,
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


def container_definitions(
    *,
    image_name: pulumi.Input[str],
    command: list[str],
    env: dict[str, pulumi.Input[str]],
    log_group: pulumi.Input[str],
    port: int | None = None,
) -> pulumi.Output[str]:
    env_outputs = {f"env__{name}": value for name, value in env.items()}
    return pulumi.Output.all(
        image=image_name,
        log_group=log_group,
        **env_outputs,
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


api_task_definition = aws.ecs.TaskDefinition(
    "api-task-definition",
    family=f"{prefix}-api-{stack}",
    cpu="1024",
    memory="2048",
    network_mode="awsvpc",
    requires_compatibilities=["FARGATE"],
    execution_role_arn=execution_role_arn,
    task_role_arn=task_role_arn,
    runtime_platform=runtime_platform,
    container_definitions=container_definitions(
        image_name=image_uri,
        command=["python", "-m", "poochon_backtest_data.cli", "api"],
        env=base_env,
        log_group=log_group_name,
        port=8080,
    ),
)

alb = aws.lb.LoadBalancer(
    "api-alb",
    load_balancer_type="application",
    security_groups=[alb_sg.id],
    subnets=[subnet_a_id, subnet_b_id],
)

target_group = aws.lb.TargetGroup(
    "api-target-group",
    port=8080,
    protocol="HTTP",
    target_type="ip",
    vpc_id=vpc_id,
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
    cluster=cluster_arn,
    desired_count=1,
    launch_type="FARGATE",
    task_definition=api_task_definition.arn,
    deployment_minimum_healthy_percent=0,
    deployment_maximum_percent=100,
    network_configuration=aws.ecs.ServiceNetworkConfigurationArgs(
        subnets=[subnet_a_id, subnet_b_id],
        security_groups=[task_sg_id],
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


pulumi.export("api_url", alb.dns_name.apply(lambda dns: f"http://{dns}"))
pulumi.export("api_service_name", api_service.name)
pulumi.export("api_task_definition_arn", api_task_definition.arn)
pulumi.export("alb_arn", alb.arn)
