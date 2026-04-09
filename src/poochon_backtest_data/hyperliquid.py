from __future__ import annotations

from datetime import UTC, datetime
import io
import os
from pathlib import Path
from typing import Iterator

import lz4.frame
import orjson
import pyarrow as pa
import pyarrow.parquet as pq

from .models import (
    CoverageRecord,
    CoverageStatus,
    DatasetKind,
    NormalizedL2Snapshot,
    NormalizedTrade,
    coverage_pk,
    normalized_l2_s3_key,
    normalized_trade_s3_key,
    raw_l2_s3_key,
    raw_trade_s3_key,
    utc_now_iso,
)
from .storage import CoverageRepository, S3Store

L2_SOURCE_BUCKET = "hyperliquid-archive"
TRADE_SOURCE_BUCKET = "hl-mainnet-node-data"


def source_date(date: str) -> str:
    return date.replace("-", "")


def l2_source_key(symbol: str, date: str, hour: int) -> str:
    return f"market_data/{source_date(date)}/{hour}/l2Book/{symbol}.lz4"


def trade_source_key(date: str, hour: int) -> str:
    return f"node_trades/hourly/{source_date(date)}/{hour}.lz4"


def requester_pays_copy(
    destination: S3Store,
    *,
    source_bucket: str,
    source_key: str,
    destination_key: str,
    request_payer: str = "requester",
) -> int:
    if destination.exists(destination_key):
        head = destination.client.head_object(Bucket=destination.bucket, Key=destination_key)
        return int(head["ContentLength"])
    response = destination.client.get_object(
        Bucket=source_bucket,
        Key=source_key,
        RequestPayer=request_payer,
    )
    payload = response["Body"].read()
    destination.put_bytes(destination_key, payload, content_type="application/octet-stream")
    return len(payload)


def iter_lz4_json_lines(payload: bytes) -> Iterator[tuple[int, dict]]:
    with lz4.frame.open(io.BytesIO(payload), mode="rb") as reader:
        for line_number, raw_line in enumerate(reader, start=1):
            stripped = raw_line.strip()
            if not stripped:
                continue
            yield line_number, orjson.loads(stripped)


def iso_to_epoch_ms(value: str) -> int:
    if value.endswith("Z"):
        value = value[:-1]
    if "." in value:
        base, fraction = value.split(".", 1)
        fraction_digits = "".join(ch for ch in fraction if ch.isdigit())
    else:
        base, fraction_digits = value, ""
    dt = datetime.strptime(base, "%Y-%m-%dT%H:%M:%S").replace(tzinfo=UTC)
    microseconds = int((fraction_digits + "000000")[:6] or "0")
    dt = dt.replace(microsecond=microseconds)
    return int(dt.timestamp() * 1000)


def parse_l2_snapshot(line: dict, *, source_hour: int, source_line_number: int) -> NormalizedL2Snapshot:
    data = line["raw"]["data"]
    return NormalizedL2Snapshot(
        ts_ms=int(data["time"]),
        symbol=data["coin"],
        bids_json=orjson.dumps(data["levels"][0]).decode("utf-8"),
        asks_json=orjson.dumps(data["levels"][1]).decode("utf-8"),
        source_hour=source_hour,
        source_line_number=source_line_number,
    )


def parse_trade(
    line: dict,
    *,
    symbol: str,
    source_hour: int,
    source_line_number: int,
) -> NormalizedTrade | None:
    if line["coin"] != symbol:
        return None
    return NormalizedTrade(
        ts_ms=iso_to_epoch_ms(line["time"]),
        symbol=line["coin"],
        side="Buy" if line["side"] == "B" else "Sell",
        px=float(line["px"]),
        sz=float(line["sz"]),
        hash=line["hash"],
        source_hour=source_hour,
        source_line_number=source_line_number,
    )


def backfill_day(
    destination: S3Store,
    coverage: CoverageRepository,
    *,
    symbol: str,
    date: str,
    request_payer: str = "requester",
) -> None:
    l2_bytes = 0
    trade_bytes = 0
    for hour in range(24):
        copied_l2 = requester_pays_copy(
            destination,
            source_bucket=L2_SOURCE_BUCKET,
            source_key=l2_source_key(symbol, date, hour),
            destination_key=raw_l2_s3_key(symbol, date, hour),
            request_payer=request_payer,
        )
        copied_trades = requester_pays_copy(
            destination,
            source_bucket=TRADE_SOURCE_BUCKET,
            source_key=trade_source_key(date, hour),
            destination_key=raw_trade_s3_key(symbol, date, hour),
            request_payer=request_payer,
        )
        l2_bytes += copied_l2
        trade_bytes += copied_trades
        coverage.put(
            CoverageRecord(
                pk=coverage_pk(DatasetKind.RAW_L2, symbol, date, f"{hour:02d}"),
                dataset_kind=DatasetKind.RAW_L2,
                symbol=symbol,
                date=date,
                hour=f"{hour:02d}",
                status=CoverageStatus.READY,
                object_count=1,
                byte_count=copied_l2,
                row_count=0,
                updated_at=utc_now_iso(),
                source=f"s3://{L2_SOURCE_BUCKET}/{l2_source_key(symbol, date, hour)}",
            )
        )
        coverage.put(
            CoverageRecord(
                pk=coverage_pk(DatasetKind.RAW_TRADES, symbol, date, f"{hour:02d}"),
                dataset_kind=DatasetKind.RAW_TRADES,
                symbol=symbol,
                date=date,
                hour=f"{hour:02d}",
                status=CoverageStatus.READY,
                object_count=1,
                byte_count=copied_trades,
                row_count=0,
                updated_at=utc_now_iso(),
                source=f"s3://{TRADE_SOURCE_BUCKET}/{trade_source_key(date, hour)}",
            )
        )

    coverage.put(
        CoverageRecord(
            pk=coverage_pk(DatasetKind.RAW_L2, symbol, date, "daily"),
            dataset_kind=DatasetKind.RAW_L2,
            symbol=symbol,
            date=date,
            hour="daily",
            status=CoverageStatus.READY,
            object_count=24,
            byte_count=l2_bytes,
            row_count=0,
            updated_at=utc_now_iso(),
            source=L2_SOURCE_BUCKET,
        )
    )
    coverage.put(
        CoverageRecord(
            pk=coverage_pk(DatasetKind.RAW_TRADES, symbol, date, "daily"),
            dataset_kind=DatasetKind.RAW_TRADES,
            symbol=symbol,
            date=date,
            hour="daily",
            status=CoverageStatus.READY,
            object_count=24,
            byte_count=trade_bytes,
            row_count=0,
            updated_at=utc_now_iso(),
            source=TRADE_SOURCE_BUCKET,
        )
    )


def _write_parquet(rows: list[dict], schema: pa.Schema, path: Path) -> None:
    table = pa.Table.from_pylist(rows, schema=schema)
    pq.write_table(table, path, compression="zstd")


def normalize_day(destination: S3Store, coverage: CoverageRepository, *, symbol: str, date: str) -> None:
    l2_schema = pa.schema(
        [
            ("ts_ms", pa.int64()),
            ("symbol", pa.string()),
            ("bids_json", pa.large_string()),
            ("asks_json", pa.large_string()),
            ("source_hour", pa.int8()),
            ("source_line_number", pa.int64()),
        ]
    )
    trade_schema = pa.schema(
        [
            ("ts_ms", pa.int64()),
            ("symbol", pa.string()),
            ("side", pa.string()),
            ("px", pa.float64()),
            ("sz", pa.float64()),
            ("hash", pa.string()),
            ("source_hour", pa.int8()),
            ("source_line_number", pa.int64()),
        ]
    )
    total_l2_rows = 0
    total_trade_rows = 0
    for hour in range(24):
        l2_rows: list[dict] = []
        trade_rows: list[dict] = []
        l2_payload = destination.get_bytes(raw_l2_s3_key(symbol, date, hour))
        trade_payload = destination.get_bytes(raw_trade_s3_key(symbol, date, hour))
        for line_number, raw_line in iter_lz4_json_lines(l2_payload):
            snapshot = parse_l2_snapshot(
                raw_line,
                source_hour=hour,
                source_line_number=line_number,
            )
            l2_rows.append(snapshot.__dict__)
        for line_number, raw_line in iter_lz4_json_lines(trade_payload):
            trade = parse_trade(
                raw_line,
                symbol=symbol,
                source_hour=hour,
                source_line_number=line_number,
            )
            if trade is None:
                continue
            trade_rows.append(trade.__dict__)

        total_l2_rows += len(l2_rows)
        total_trade_rows += len(trade_rows)
        tmp_dir = Path("/tmp") / "poochon-backtest-data"
        tmp_dir.mkdir(parents=True, exist_ok=True)
        l2_path = tmp_dir / f"{symbol}-{date}-{hour:02d}-l2.parquet"
        trade_path = tmp_dir / f"{symbol}-{date}-{hour:02d}-trade.parquet"
        _write_parquet(l2_rows, l2_schema, l2_path)
        _write_parquet(trade_rows, trade_schema, trade_path)
        destination.put_file(normalized_l2_s3_key(symbol, date, hour), str(l2_path), content_type="application/octet-stream")
        destination.put_file(normalized_trade_s3_key(symbol, date, hour), str(trade_path), content_type="application/octet-stream")
        coverage.put(
            CoverageRecord(
                pk=coverage_pk(DatasetKind.NORMALIZED_L2, symbol, date, f"{hour:02d}"),
                dataset_kind=DatasetKind.NORMALIZED_L2,
                symbol=symbol,
                date=date,
                hour=f"{hour:02d}",
                status=CoverageStatus.READY,
                object_count=1,
                byte_count=l2_path.stat().st_size,
                row_count=len(l2_rows),
                updated_at=utc_now_iso(),
                source=normalized_l2_s3_key(symbol, date, hour),
            )
        )
        coverage.put(
            CoverageRecord(
                pk=coverage_pk(DatasetKind.NORMALIZED_TRADES, symbol, date, f"{hour:02d}"),
                dataset_kind=DatasetKind.NORMALIZED_TRADES,
                symbol=symbol,
                date=date,
                hour=f"{hour:02d}",
                status=CoverageStatus.READY,
                object_count=1,
                byte_count=trade_path.stat().st_size,
                row_count=len(trade_rows),
                updated_at=utc_now_iso(),
                source=normalized_trade_s3_key(symbol, date, hour),
            )
        )
    coverage.put(
        CoverageRecord(
            pk=coverage_pk(DatasetKind.NORMALIZED_L2, symbol, date, "daily"),
            dataset_kind=DatasetKind.NORMALIZED_L2,
            symbol=symbol,
            date=date,
            hour="daily",
            status=CoverageStatus.READY,
            object_count=24,
            byte_count=0,
            row_count=total_l2_rows,
            updated_at=utc_now_iso(),
            source="s3",
        )
    )
    coverage.put(
        CoverageRecord(
            pk=coverage_pk(DatasetKind.NORMALIZED_TRADES, symbol, date, "daily"),
            dataset_kind=DatasetKind.NORMALIZED_TRADES,
            symbol=symbol,
            date=date,
            hour="daily",
            status=CoverageStatus.READY,
            object_count=24,
            byte_count=0,
            row_count=total_trade_rows,
            updated_at=utc_now_iso(),
            source="s3",
        )
    )
