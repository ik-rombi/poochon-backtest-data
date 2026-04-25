from __future__ import annotations

import pulumi
import pulumi_aws as aws


stack = pulumi.get_stack()
config = pulumi.Config()
prefix = config.get("namePrefix") or "poochon-backtest-data"
expected_aws_account_id = config.require("expectedAwsAccountId")

caller = aws.get_caller_identity_output()
region = aws.get_region_output()

def require_expected_account(account_id: str) -> str:
    if account_id != expected_aws_account_id:
        raise ValueError(
            f"refusing to deploy to AWS account {account_id}; "
            f"expected {expected_aws_account_id}"
        )
    return account_id


aws_account_id = caller.account_id.apply(require_expected_account)

bucket_name = pulumi.Output.all(aws_account_id, region.name).apply(
    lambda args: f"{prefix}-{args[0]}-{args[1]}-{stack}"
)

data_bucket = aws.s3.BucketV2(
    "data-bucket",
    bucket=bucket_name,
    force_destroy=False,
    tags={"Project": "poochon-backtest-data", "Stack": stack},
)

aws.s3.BucketPublicAccessBlock(
    "data-bucket-public-access",
    bucket=data_bucket.id,
    block_public_acls=True,
    block_public_policy=True,
    ignore_public_acls=True,
    restrict_public_buckets=True,
)

coverage_table = aws.dynamodb.Table(
    "coverage-table",
    name=f"{prefix}-coverage-{stack}",
    billing_mode="PAY_PER_REQUEST",
    hash_key="pk",
    attributes=[aws.dynamodb.TableAttributeArgs(name="pk", type="S")],
    tags={"Project": "poochon-backtest-data", "Stack": stack},
)

replay_table = aws.dynamodb.Table(
    "replay-table",
    name=f"{prefix}-replays-{stack}",
    billing_mode="PAY_PER_REQUEST",
    hash_key="replay_id",
    attributes=[aws.dynamodb.TableAttributeArgs(name="replay_id", type="S")],
    tags={"Project": "poochon-backtest-data", "Stack": stack},
)

replay_shard_table = aws.dynamodb.Table(
    "replay-shard-table",
    name=f"{prefix}-replay-shards-{stack}",
    billing_mode="PAY_PER_REQUEST",
    hash_key="shard_id",
    attributes=[aws.dynamodb.TableAttributeArgs(name="shard_id", type="S")],
    tags={"Project": "poochon-backtest-data", "Stack": stack},
)

pulumi.export("data_bucket_name", data_bucket.bucket)
pulumi.export("coverage_table_name", coverage_table.name)
pulumi.export("replay_table_name", replay_table.name)
pulumi.export("replay_shard_table_name", replay_shard_table.name)
pulumi.export("aws_account_id", aws_account_id)
