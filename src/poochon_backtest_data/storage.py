from __future__ import annotations

import io
import json
from typing import Any, Iterator

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError
import orjson
import zstandard

from .models import (
    CanonicalShardRecord,
    CoverageRecord,
    DatasetKind,
    MarketRef,
    PolymarketTarget,
    Venue,
    canonical_hl_shard_id,
    canonical_pm_shard_id,
    coverage_pk_canonical_hl,
    coverage_pk_canonical_pm,
    coverage_pk_raw_hl_fills,
    coverage_pk_raw_hl_l2,
    coverage_pk_raw_pmxt,
    iter_dates_inclusive,
)

S3_CLIENT_CONFIG = Config(max_pool_connections=64)
DYNAMODB_BATCH_GET_LIMIT = 100


def boto3_session(region: str):
    return boto3.session.Session(region_name=region)


class S3Store:
    def __init__(self, session: boto3.session.Session, bucket: str):
        self.region = session.region_name or "us-east-1"
        self.bucket = bucket
        self.client = session.client("s3", config=S3_CLIENT_CONFIG)

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

    def get_bytes(self, key: str, *, max_retries: int = 5) -> bytes:
        """Download an S3 object as bytes, with retries on mid-stream connection drops."""
        from botocore.exceptions import ClientError, ConnectionClosedError, ResponseStreamingError
        from http.client import IncompleteRead
        import time

        last_error: Exception | None = None
        for attempt in range(max_retries):
            try:
                response = self.client.get_object(Bucket=self.bucket, Key=key)
                return response["Body"].read()
            except (ResponseStreamingError, ConnectionClosedError, IncompleteRead, ConnectionResetError) as error:
                last_error = error
                if attempt + 1 < max_retries:
                    backoff = 2 ** attempt
                    time.sleep(backoff)
            except ClientError:
                raise
        assert last_error is not None
        raise last_error

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

    def list_prefix(self, prefix: str) -> Iterator[str]:
        paginator = self.client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                yield obj["Key"]

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
        self.region = session.region_name or "us-east-1"
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

    def batch_get(self, pks: list[str]) -> dict[str, CoverageRecord | None]:
        result: dict[str, CoverageRecord | None] = {pk: None for pk in pks}
        if not pks:
            return result
        client = self.table.meta.client
        unique = list(dict.fromkeys(pks))
        for start in range(0, len(unique), DYNAMODB_BATCH_GET_LIMIT):
            chunk = unique[start : start + DYNAMODB_BATCH_GET_LIMIT]
            request = {self.table_name: {"Keys": [{"pk": pk} for pk in chunk]}}
            while request:
                response = client.batch_get_item(RequestItems=request)
                for item in response.get("Responses", {}).get(self.table_name, []):
                    record = CoverageRecord.model_validate(item)
                    result[record.pk] = record
                request = response.get("UnprocessedKeys") or {}
        return result

    def list_raw_pmxt_window(
        self,
        *,
        start_date: str,
        end_date: str,
    ) -> dict[tuple[str, str], CoverageRecord | None]:
        dates = iter_dates_inclusive(start_date, end_date)
        pk_to_cell: dict[str, tuple[str, str]] = {}
        for date in dates:
            for hour in range(24):
                pk = coverage_pk_raw_pmxt(date, hour)
                pk_to_cell[pk] = (date, f"{hour:02d}")
        records = self.batch_get(list(pk_to_cell))
        return {cell: records[pk] for pk, cell in pk_to_cell.items()}

    def list_raw_hl_l2_window(
        self,
        *,
        market: MarketRef,
        start_date: str,
        end_date: str,
    ) -> dict[tuple[str, str], CoverageRecord | None]:
        dates = iter_dates_inclusive(start_date, end_date)
        pk_to_cell: dict[str, tuple[str, str]] = {}
        for date in dates:
            for hour in range(24):
                pk = coverage_pk_raw_hl_l2(market, date, hour)
                pk_to_cell[pk] = (date, f"{hour:02d}")
        records = self.batch_get(list(pk_to_cell))
        return {cell: records[pk] for pk, cell in pk_to_cell.items()}

    def list_raw_hl_fills_window(
        self,
        *,
        start_date: str,
        end_date: str,
    ) -> dict[tuple[str, str], CoverageRecord | None]:
        dates = iter_dates_inclusive(start_date, end_date)
        pk_to_cell: dict[str, tuple[str, str]] = {}
        for date in dates:
            for hour in range(24):
                pk = coverage_pk_raw_hl_fills(date, hour)
                pk_to_cell[pk] = (date, f"{hour:02d}")
        records = self.batch_get(list(pk_to_cell))
        return {cell: records[pk] for pk, cell in pk_to_cell.items()}


class CanonicalShardRepository:
    def __init__(self, session: boto3.session.Session, table_name: str):
        self.region = session.region_name or "us-east-1"
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

    def batch_get(self, shard_ids: list[str]) -> dict[str, CanonicalShardRecord | None]:
        result: dict[str, CanonicalShardRecord | None] = {sid: None for sid in shard_ids}
        if not shard_ids:
            return result
        client = self.table.meta.client
        unique = list(dict.fromkeys(shard_ids))
        for start in range(0, len(unique), DYNAMODB_BATCH_GET_LIMIT):
            chunk = unique[start : start + DYNAMODB_BATCH_GET_LIMIT]
            request = {self.table_name: {"Keys": [{"shard_id": sid} for sid in chunk]}}
            while request:
                response = client.batch_get_item(RequestItems=request)
                for item in response.get("Responses", {}).get(self.table_name, []):
                    record = CanonicalShardRecord.model_validate(item)
                    result[record.shard_id] = record
                request = response.get("UnprocessedKeys") or {}
        return result

    def list_hl_window(
        self,
        *,
        market: MarketRef,
        start_date: str,
        end_date: str,
        depth: int,
    ) -> dict[str, CanonicalShardRecord | None]:
        shard_ids = {
            date: canonical_hl_shard_id(market, date, depth)
            for date in iter_dates_inclusive(start_date, end_date)
        }
        records = self.batch_get(list(shard_ids.values()))
        return {date: records[sid] for date, sid in shard_ids.items()}

    def list_pm_window(
        self,
        *,
        target: PolymarketTarget,
        start_date: str,
        end_date: str,
        depth: int,
    ) -> dict[str, CanonicalShardRecord | None]:
        shard_ids = {
            date: canonical_pm_shard_id(target=target, date=date, depth=depth)
            for date in iter_dates_inclusive(start_date, end_date)
        }
        records = self.batch_get(list(shard_ids.values()))
        return {date: records[sid] for date, sid in shard_ids.items()}
