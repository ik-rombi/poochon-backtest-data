from __future__ import annotations

from dataclasses import asdict
import io
import json
from typing import Any, Iterator

import boto3
from botocore.exceptions import ClientError
import orjson
import zstandard

from .models import CanonicalShardRecord, CoverageRecord, ReplayRecord


def boto3_session(region: str):
    return boto3.session.Session(region_name=region)


class S3Store:
    def __init__(self, session: boto3.session.Session, bucket: str):
        self.region = session.region_name or "eu-west-1"
        self.bucket = bucket
        self.client = session.client("s3")

    def clone(self) -> "S3Store":
        return S3Store(boto3_session(self.region), self.bucket)

    def put_bytes(
        self,
        key: str,
        data: bytes,
        *,
        content_type: str | None = None,
        content_encoding: str | None = None,
    ) -> None:
        extra: dict[str, Any] = {}
        if content_type:
            extra["ContentType"] = content_type
        if content_encoding:
            extra["ContentEncoding"] = content_encoding
        self.client.put_object(Bucket=self.bucket, Key=key, Body=data, **extra)

    def put_json(self, key: str, payload: dict[str, Any]) -> None:
        self.put_bytes(
            key,
            orjson.dumps(payload, option=orjson.OPT_INDENT_2),
            content_type="application/json",
        )

    def put_file(self, key: str, path: str, *, content_type: str | None = None) -> None:
        extra: dict[str, Any] = {}
        if content_type:
            extra["ExtraArgs"] = {"ContentType": content_type}
        self.client.upload_file(path, self.bucket, key, **extra)

    def get_bytes(self, key: str) -> bytes:
        response = self.client.get_object(Bucket=self.bucket, Key=key)
        return response["Body"].read()

    def exists(self, key: str) -> bool:
        return self.object_size(key) is not None

    def object_size(self, key: str) -> int | None:
        try:
            response = self.client.head_object(Bucket=self.bucket, Key=key)
            return int(response["ContentLength"])
        except ClientError as error:
            if error.response["ResponseMetadata"]["HTTPStatusCode"] == 404:
                return None
            raise

    def stream_zstd(self, key: str, chunk_size: int = 64 * 1024) -> Iterator[bytes]:
        response = self.client.get_object(Bucket=self.bucket, Key=key)
        body = response["Body"]
        reader = zstandard.ZstdDecompressor().stream_reader(body)
        try:
            while True:
                chunk = reader.read(chunk_size)
                if not chunk:
                    break
                yield chunk
        finally:
            reader.close()
            body.close()


class CoverageRepository:
    def __init__(self, session: boto3.session.Session, table_name: str):
        self.region = session.region_name or "eu-west-1"
        self.table_name = table_name
        self.table = session.resource("dynamodb").Table(table_name)

    def clone(self) -> "CoverageRepository":
        return CoverageRepository(boto3_session(self.region), self.table_name)

    def get(self, pk: str) -> CoverageRecord | None:
        response = self.table.get_item(Key={"pk": pk})
        item = response.get("Item")
        if not item:
            return None
        return CoverageRecord.model_validate(item)

    def put(self, record: CoverageRecord) -> None:
        self.table.put_item(Item=record.model_dump(mode="json"))


class ReplayRepository:
    def __init__(self, session: boto3.session.Session, table_name: str):
        self.table = session.resource("dynamodb").Table(table_name)

    def get(self, replay_id: str) -> ReplayRecord | None:
        response = self.table.get_item(Key={"replay_id": replay_id})
        item = response.get("Item")
        if not item:
            return None
        return ReplayRecord.model_validate(item)

    def create_if_absent(self, record: ReplayRecord) -> ReplayRecord:
        try:
            self.table.put_item(
                Item=record.model_dump(mode="json"),
                ConditionExpression="attribute_not_exists(replay_id)",
            )
            return record
        except ClientError as error:
            if error.response["Error"]["Code"] != "ConditionalCheckFailedException":
                raise
            existing = self.get(record.replay_id)
            if existing is None:
                raise
            return existing

    def put(self, record: ReplayRecord) -> None:
        self.table.put_item(Item=record.model_dump(mode="json"))


class CanonicalShardRepository:
    def __init__(self, session: boto3.session.Session, table_name: str):
        self.region = session.region_name or "eu-west-1"
        self.table_name = table_name
        self.table = session.resource("dynamodb").Table(table_name)

    def clone(self) -> "CanonicalShardRepository":
        return CanonicalShardRepository(boto3_session(self.region), self.table_name)

    def get(self, shard_id: str) -> CanonicalShardRecord | None:
        response = self.table.get_item(Key={"shard_id": shard_id})
        item = response.get("Item")
        if not item:
            return None
        return CanonicalShardRecord.model_validate(item)

    def put(self, record: CanonicalShardRecord) -> None:
        self.table.put_item(Item=record.model_dump(mode="json"))
