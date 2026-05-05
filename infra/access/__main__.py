from __future__ import annotations

import json
from typing import Any

import pulumi
import pulumi_aws as aws


stack = pulumi.get_stack()
config = pulumi.Config()
prefix = config.get("namePrefix") or "poochon-backtest-data"
core_stack_ref = config.require("coreStackRef")
expected_aws_account_id = config.require("expectedAwsAccountId")
external_id = config.require("externalId")
trusted_principal_arns = config.require_object("trustedPrincipalArns")

if not isinstance(trusted_principal_arns, list) or not all(
    isinstance(item, str) and item.strip() for item in trusted_principal_arns
):
    raise ValueError("trustedPrincipalArns must be a non-empty list of IAM role ARNs")

caller = aws.get_caller_identity_output()
region = aws.get_region_output()
core = pulumi.StackReference(core_stack_ref)


def require_expected_account(account_id: str) -> str:
    if account_id != expected_aws_account_id:
        raise ValueError(
            f"refusing to deploy to AWS account {account_id}; "
            f"expected {expected_aws_account_id}"
        )
    return account_id


aws_account_id = caller.account_id.apply(require_expected_account)

data_bucket_name = core.require_output("data_bucket_name")
coverage_table_name = core.require_output("coverage_table_name")
shard_table_name = core.require_output("shard_table_name")


def table_arn(args: list[str]) -> str:
    table_name, account_id, region_name = args
    return f"arn:aws:dynamodb:{region_name}:{account_id}:table/{table_name}"


coverage_table_arn = pulumi.Output.all(
    coverage_table_name, aws_account_id, region.name
).apply(table_arn)
shard_table_arn = pulumi.Output.all(
    shard_table_name, aws_account_id, region.name
).apply(table_arn)
bucket_arn = data_bucket_name.apply(lambda name: f"arn:aws:s3:::{name}")
canonical_objects_arn = data_bucket_name.apply(lambda name: f"arn:aws:s3:::{name}/canonical/*")


def read_broker_trust_policy() -> str:
    return json.dumps(
        {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": {"AWS": [arn.strip() for arn in trusted_principal_arns]},
                    "Action": "sts:AssumeRole",
                    "Condition": {"StringEquals": {"sts:ExternalId": external_id}},
                }
            ],
        }
    )


read_broker_role = aws.iam.Role(
    "readBrokerRole",
    name=f"{prefix}-{stack}-read-broker",
    assume_role_policy=read_broker_trust_policy(),
    tags={
        "Project": "poochon-backtest-data",
        "Stack": stack,
        "Purpose": "poochon-control-plane-read-broker",
    },
)


def read_broker_policy(args: list[Any]) -> str:
    bucket_resource, canonical_resource, coverage_resource, shard_resource = args
    return json.dumps(
        {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Sid": "ReadCatalogTables",
                    "Effect": "Allow",
                    "Action": [
                        "dynamodb:BatchGetItem",
                        "dynamodb:GetItem",
                        "dynamodb:Query",
                        "dynamodb:Scan",
                    ],
                    "Resource": [coverage_resource, shard_resource],
                },
                {
                    "Sid": "ListCanonicalObjects",
                    "Effect": "Allow",
                    "Action": ["s3:ListBucket"],
                    "Resource": [bucket_resource],
                    "Condition": {"StringLike": {"s3:prefix": ["canonical/*"]}},
                },
                {
                    "Sid": "ReadCanonicalObjects",
                    "Effect": "Allow",
                    "Action": ["s3:GetObject"],
                    "Resource": [canonical_resource],
                },
            ],
        }
    )


aws.iam.RolePolicy(
    "readBrokerPolicy",
    name=f"{prefix}-{stack}-read-broker",
    role=read_broker_role.id,
    policy=pulumi.Output.all(
        bucket_arn,
        canonical_objects_arn,
        coverage_table_arn,
        shard_table_arn,
    ).apply(read_broker_policy),
)

pulumi.export("readBrokerRoleArn", read_broker_role.arn)
pulumi.export("externalId", external_id)
pulumi.export("region", region.name)
pulumi.export("dataBucketName", data_bucket_name)
pulumi.export("coverageTableName", coverage_table_name)
pulumi.export("shardTableName", shard_table_name)
pulumi.export("trustedPrincipalArns", trusted_principal_arns)
