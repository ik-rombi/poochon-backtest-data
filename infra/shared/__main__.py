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
expected_aws_account_id = config.require("expectedAwsAccountId")

core = pulumi.StackReference(core_stack_ref)
bucket_name = core.require_output("data_bucket_name")
coverage_table_name = core.require_output("coverage_table_name")
shard_table_name = core.require_output("shard_table_name")

region = aws.get_region_output()
caller = aws.get_caller_identity_output()
availability_zones = aws.get_availability_zones(state="available")


def require_expected_account(account_id: str) -> str:
    if account_id != expected_aws_account_id:
        raise ValueError(
            f"refusing to deploy to AWS account {account_id}; "
            f"expected {expected_aws_account_id}"
        )
    return account_id


aws_account_id = caller.account_id.apply(require_expected_account)


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
    "shared-vpc",
    cidr_block="10.42.0.0/16",
    enable_dns_support=True,
    enable_dns_hostnames=True,
    tags={"Name": f"{prefix}-{stack}"},
)

internet_gateway = aws.ec2.InternetGateway(
    "shared-igw",
    vpc_id=vpc.id,
    tags={"Name": f"{prefix}-{stack}-igw"},
)

route_table = aws.ec2.RouteTable(
    "shared-public-route-table",
    vpc_id=vpc.id,
    routes=[
        aws.ec2.RouteTableRouteArgs(
            cidr_block="0.0.0.0/0",
            gateway_id=internet_gateway.id,
        )
    ],
)

subnet_a = aws.ec2.Subnet(
    "shared-subnet-a",
    vpc_id=vpc.id,
    cidr_block="10.42.0.0/24",
    availability_zone=availability_zones.names[0],
    map_public_ip_on_launch=True,
)
subnet_b = aws.ec2.Subnet(
    "shared-subnet-b",
    vpc_id=vpc.id,
    cidr_block="10.42.1.0/24",
    availability_zone=availability_zones.names[1],
    map_public_ip_on_launch=True,
)

aws.ec2.RouteTableAssociation(
    "shared-rta-a",
    subnet_id=subnet_a.id,
    route_table_id=route_table.id,
)
aws.ec2.RouteTableAssociation(
    "shared-rta-b",
    subnet_id=subnet_b.id,
    route_table_id=route_table.id,
)

# Task SG allows ingress within the VPC on 8080 for runtime/API tasks and
# unrestricted egress.
task_sg = aws.ec2.SecurityGroup(
    "shared-task-sg",
    vpc_id=vpc.id,
    description="ECS task security group (shared by runtime tasks)",
    ingress=[
        aws.ec2.SecurityGroupIngressArgs(
            protocol="tcp",
            from_port=8080,
            to_port=8080,
            cidr_blocks=["10.42.0.0/16"],
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

cluster = aws.ecs.Cluster("shared-cluster", name=f"{prefix}-{stack}")

log_group = aws.cloudwatch.LogGroup("shared-log-group", retention_in_days=7)

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
        shard_table_name,
        region.name,
        aws_account_id,
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
                        "Action": [
                            "dynamodb:GetItem",
                            "dynamodb:PutItem",
                            "dynamodb:UpdateItem",
                            "dynamodb:BatchGetItem",
                            "dynamodb:Scan",
                        ],
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


pulumi.export("vpc_id", vpc.id)
pulumi.export("subnet_a_id", subnet_a.id)
pulumi.export("subnet_b_id", subnet_b.id)
pulumi.export("task_sg_id", task_sg.id)
pulumi.export("cluster_arn", cluster.arn)
pulumi.export("cluster_name", cluster.name)
pulumi.export("log_group_name", log_group.name)
pulumi.export("ecr_repo_url", ecr_repo.repository_url)
pulumi.export("image_uri", image.image_name)
pulumi.export("execution_role_arn", execution_role.arn)
pulumi.export("execution_role_name", execution_role.name)
pulumi.export("execution_role_id", execution_role.id)
pulumi.export("task_role_arn", task_role.arn)
pulumi.export("aws_account_id", aws_account_id)
