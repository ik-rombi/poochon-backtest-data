from __future__ import annotations

from datetime import UTC, datetime
import io
from pathlib import Path
from typing import Any, Iterator

import lz4.frame
import orjson
import pyarrow as pa
import pyarrow.parquet as pq

from .canonical import build_hyperliquid_canonical_day
from .models import (
    CoverageRecord,
    CoverageStatus,
    DatasetKind,
    IngestionRequest,
    MarketRef,
    NormalizedL2Snapshot,
    NormalizedTrade,
    coverage_pk,
    normalized_l2_s3_key,
    normalized_trade_s3_key,
    raw_fill_s3_key,
    raw_l2_s3_key,
    raw_trade_s3_key,
    utc_now_iso,
)
from .storage import CanonicalShardRepository, CoverageRepository, S3Store

L2_SOURCE_BUCKET = "hyperliquid-archive"
TRADE_SOURCE_BUCKET = "hl-mainnet-node-data"


def source_date(date: str) -> str:
    return date.replace("-", "")


def l2_source_key(market: MarketRef, date: str, hour: int) -> str:
    return f"market_data/{source_date(date)}/{hour}/l2Book/{market.instrument}.lz4"


def trade_source_key(date: str, hour: int) -> str:
    return f"node_fills_by_block/hourly/{source_date(date)}/{hour}.lz4"


def requester_pays_copy(
    destination: S3Store,
    *,
    source_bucket: str,
    source_key: str,
    destination_key: str,
    request_payer: str = "requester",
) -> int | None:
    """Copy an object from a requester-pays bucket into our data bucket.

    Returns the byte count written, or None if the upstream object does
    not exist yet (so the caller can skip this hour instead of dying
    mid-window). Upstream archives often lag by days/hours — treating a
    missing key as "not yet available" matches Polymarket's Telonex path,
    which uses empty-parquet sentinels for 404s.
    """
    existing_size = destination.object_size(destination_key)
    if existing_size is not None:
        return existing_size
    try:
        response = destination.client.get_object(
            Bucket=source_bucket,
            Key=source_key,
            RequestPayer=request_payer,
        )
    except destination.client.exceptions.NoSuchKey:
        return None
    except ClientError as error:
        if error.response.get("Error", {}).get("Code") in {"NoSuchKey", "404"}:
            return None
        raise
    payload = response["Body"].read()
    destination.put_bytes(destination_key, payload, content_type="application/octet-stream")
    return len(payload)


def iter_lz4_json_lines(payload: bytes) -> Iterator[tuple[int, dict[str, Any]]]:
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


def parse_l2_snapshot(line: dict[str, Any], *, source_hour: int, source_line_number: int) -> NormalizedL2Snapshot:
    data = line["raw"]["data"]
    return NormalizedL2Snapshot(
        ts_ms=int(data["time"]),
        instrument=data["coin"],
        bids_json=orjson.dumps(data["levels"][0]).decode("utf-8"),
        asks_json=orjson.dumps(data["levels"][1]).decode("utf-8"),
        source_hour=source_hour,
        source_line_number=source_line_number,
    )


def _fill_group_key(fill: dict[str, Any]) -> tuple[str, int, int, str, str, str]:
    return (
        str(fill["coin"]),
        int(fill["time"]),
        int(fill["tid"]),
        str(fill.get("hash", "")),
        str(fill["px"]),
        str(fill["sz"]),
    )


def _select_canonical_fill(group: list[dict[str, Any]]) -> dict[str, Any]:
    crossed = [candidate for candidate in group if candidate["crossed"]]
    candidates = crossed or group
    return min(candidates, key=lambda item: (item["addr"], int(item["oid"])))


def parse_trade(
    line: dict[str, Any],
    *,
    instrument: str,
    source_hour: int,
    source_line_number: int,
) -> NormalizedTrade | None:
    fill = line.get("fill", line)
    if fill.get("coin") != instrument:
        return None
    if int(fill.get("tid", 0)) == 0:
        return None
    if fill.get("dir") == "Spot Dust Conversion":
        return None
    side = str(fill["side"])
    if side not in {"A", "B"}:
        return None
    return NormalizedTrade(
        ts_ms=int(fill["time"]),
        instrument=instrument,
        side="Buy" if side == "B" else "Sell",
        px=float(fill["px"]),
        sz=float(fill["sz"]),
        hash=str(fill.get("hash", "")),
        source_hour=source_hour,
        source_line_number=source_line_number,
    )


def collapse_fill_trades(
    payload: bytes,
    *,
    instrument: str,
    source_hour: int,
) -> list[NormalizedTrade]:
    grouped: dict[tuple[str, int, int, str, str, str], list[dict[str, Any]]] = {}
    for line_number, raw_line in iter_lz4_json_lines(payload):
        for event_index, event in enumerate(raw_line.get("events") or [], start=1):
            if not isinstance(event, list | tuple) or len(event) != 2:
                continue
            addr, fill = event
            candidate = {
                "addr": str(addr),
                "oid": int(fill.get("oid", 0)),
                "crossed": bool(fill.get("crossed")),
                "fill": fill,
                "source_line_number": line_number * 10000 + event_index,
            }
            if fill.get("coin") != instrument:
                continue
            if int(fill.get("tid", 0)) == 0:
                continue
            if fill.get("dir") == "Spot Dust Conversion":
                continue
            grouped.setdefault(_fill_group_key(fill), []).append(candidate)

    rows: list[NormalizedTrade] = []
    for group in grouped.values():
        canonical = _select_canonical_fill(group)
        parsed = parse_trade(
            canonical["fill"],
            instrument=instrument,
            source_hour=source_hour,
            source_line_number=min(item["source_line_number"] for item in group),
        )
        if parsed is not None:
            rows.append(parsed)
    rows.sort(key=lambda item: (item.ts_ms, item.source_line_number))
    return rows


def _coverage_ready(
    coverage: CoverageRepository,
    dataset_kind: DatasetKind,
    market: MarketRef,
    date: str,
    hour: str,
) -> CoverageRecord | None:
    record = coverage.get(coverage_pk(dataset_kind, market, date, hour))
    if record is None or record.status != CoverageStatus.READY:
        return None
    return record


def _day_objects_exist(destination: S3Store, keys: list[str]) -> bool:
    return all(destination.exists(key) for key in keys)


def _put_coverage(
    coverage: CoverageRepository,
    *,
    dataset_kind: DatasetKind,
    market: MarketRef,
    date: str,
    hour: str,
    object_count: int,
    byte_count: int,
    row_count: int,
    source: str,
    status: CoverageStatus = CoverageStatus.READY,
) -> CoverageRecord:
    record = CoverageRecord(
        pk=coverage_pk(dataset_kind, market, date, hour),
        dataset_kind=dataset_kind,
        venue=market.venue,
        market_type=market.market_type,
        instrument=market.instrument,
        date=date,
        hour=hour,
        status=status,
        object_count=object_count,
        byte_count=byte_count,
        row_count=row_count,
        updated_at=utc_now_iso(),
        source=source,
    )
    coverage.put(record)
    return record


def backfill_day(
    destination: S3Store,
    coverage: CoverageRepository,
    *,
    market: MarketRef,
    date: str,
    request_payer: str = "requester",
) -> None:
    l2_keys = [raw_l2_s3_key(market, date, hour) for hour in range(24)]
    trade_keys = [raw_fill_s3_key(market, date, hour) for hour in range(24)]
    if (
        _coverage_ready(coverage, DatasetKind.RAW_L2, market, date, "daily")
        and _coverage_ready(coverage, DatasetKind.RAW_TRADES, market, date, "daily")
        and _day_objects_exist(destination, l2_keys + trade_keys)
    ):
        return

    l2_bytes = 0
    trade_bytes = 0
    l2_hours_ok = 0
    trade_hours_ok = 0
    for hour in range(24):
        l2_hour = _coverage_ready(coverage, DatasetKind.RAW_L2, market, date, f"{hour:02d}")
        trade_hour = _coverage_ready(coverage, DatasetKind.RAW_TRADES, market, date, f"{hour:02d}")
        l2_key = raw_l2_s3_key(market, date, hour)
        trade_key = raw_fill_s3_key(market, date, hour)

        if l2_hour and destination.exists(l2_key):
            copied_l2 = l2_hour.byte_count
        else:
            copied_l2 = requester_pays_copy(
                destination,
                source_bucket=L2_SOURCE_BUCKET,
                source_key=l2_source_key(market, date, hour),
                destination_key=l2_key,
                request_payer=request_payer,
            )
            if copied_l2 is not None:
                _put_coverage(
                    coverage,
                    dataset_kind=DatasetKind.RAW_L2,
                    market=market,
                    date=date,
                    hour=f"{hour:02d}",
                    object_count=1,
                    byte_count=copied_l2,
                    row_count=0,
                    source=f"s3://{L2_SOURCE_BUCKET}/{l2_source_key(market, date, hour)}",
                )

        if trade_hour and destination.exists(trade_key):
            copied_trades = trade_hour.byte_count
        else:
            copied_trades = requester_pays_copy(
                destination,
                source_bucket=TRADE_SOURCE_BUCKET,
                source_key=trade_source_key(date, hour),
                destination_key=trade_key,
                request_payer=request_payer,
            )
            if copied_trades is not None:
                _put_coverage(
                    coverage,
                    dataset_kind=DatasetKind.RAW_TRADES,
                    market=market,
                    date=date,
                    hour=f"{hour:02d}",
                    object_count=1,
                    byte_count=copied_trades,
                    row_count=0,
                    source=f"s3://{TRADE_SOURCE_BUCKET}/{trade_source_key(date, hour)}",
                )

        if copied_l2 is not None:
            l2_hours_ok += 1
        if copied_trades is not None:
            trade_hours_ok += 1
        l2_bytes += copied_l2 or 0
        trade_bytes += copied_trades or 0

    # Daily rollup must reflect reality: object_count = hours that actually
    # landed, status = READY only when the full 24h is present. Partial days
    # (upstream lag, transient 404s) are marked FAILED so downstream stages
    # and `data inventory` can tell apart "complete day" from "we tried but
    # upstream had nothing yet."
    _put_coverage(
        coverage,
        dataset_kind=DatasetKind.RAW_L2,
        market=market,
        date=date,
        hour="daily",
        object_count=l2_hours_ok,
        byte_count=l2_bytes,
        row_count=0,
        source=L2_SOURCE_BUCKET,
        status=CoverageStatus.READY if l2_hours_ok == 24 else CoverageStatus.FAILED,
    )
    _put_coverage(
        coverage,
        dataset_kind=DatasetKind.RAW_TRADES,
        market=market,
        date=date,
        hour="daily",
        object_count=trade_hours_ok,
        byte_count=trade_bytes,
        row_count=0,
        source=TRADE_SOURCE_BUCKET,
        status=CoverageStatus.READY if trade_hours_ok == 24 else CoverageStatus.FAILED,
    )


def _write_parquet(rows: list[dict[str, Any]], schema: pa.Schema, path: Path) -> None:
    table = pa.Table.from_pylist(rows, schema=schema)
    pq.write_table(table, path, compression="zstd")


def normalize_day(destination: S3Store, coverage: CoverageRepository, *, market: MarketRef, date: str) -> None:
    l2_keys = [normalized_l2_s3_key(market, date, hour) for hour in range(24)]
    trade_keys = [normalized_trade_s3_key(market, date, hour) for hour in range(24)]
    if (
        _coverage_ready(coverage, DatasetKind.NORMALIZED_L2, market, date, "daily")
        and _coverage_ready(coverage, DatasetKind.NORMALIZED_TRADES, market, date, "daily")
        and _day_objects_exist(destination, l2_keys + trade_keys)
    ):
        return

    l2_schema = pa.schema(
        [
            ("ts_ms", pa.int64()),
            ("instrument", pa.string()),
            ("bids_json", pa.large_string()),
            ("asks_json", pa.large_string()),
            ("source_hour", pa.int8()),
            ("source_line_number", pa.int64()),
        ]
    )
    trade_schema = pa.schema(
        [
            ("ts_ms", pa.int64()),
            ("instrument", pa.string()),
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
    total_l2_bytes = 0
    total_trade_bytes = 0

    tmp_dir = Path("/tmp") / "poochon-backtest-data"
    tmp_dir.mkdir(parents=True, exist_ok=True)
    instrument_slug = market.encoded_instrument()

    for hour in range(24):
        l2_hour = _coverage_ready(coverage, DatasetKind.NORMALIZED_L2, market, date, f"{hour:02d}")
        trade_hour = _coverage_ready(coverage, DatasetKind.NORMALIZED_TRADES, market, date, f"{hour:02d}")
        l2_key = normalized_l2_s3_key(market, date, hour)
        trade_key = normalized_trade_s3_key(market, date, hour)
        if l2_hour and trade_hour and destination.exists(l2_key) and destination.exists(trade_key):
            total_l2_rows += l2_hour.row_count
            total_trade_rows += trade_hour.row_count
            total_l2_bytes += l2_hour.byte_count
            total_trade_bytes += trade_hour.byte_count
            continue

        l2_rows: list[dict[str, Any]] = []
        l2_payload = destination.get_bytes(raw_l2_s3_key(market, date, hour))
        fill_payload = destination.get_bytes(raw_trade_s3_key(market, date, hour))
        for line_number, raw_line in iter_lz4_json_lines(l2_payload):
            snapshot = parse_l2_snapshot(
                raw_line,
                source_hour=hour,
                source_line_number=line_number,
            )
            l2_rows.append(snapshot.__dict__)
        l2_rows.sort(key=lambda item: (int(item["ts_ms"]), int(item["source_line_number"])))
        trade_rows = [row.__dict__ for row in collapse_fill_trades(fill_payload, instrument=market.instrument, source_hour=hour)]

        l2_path = tmp_dir / f"{instrument_slug}-{date}-{hour:02d}-l2.parquet"
        trade_path = tmp_dir / f"{instrument_slug}-{date}-{hour:02d}-trade.parquet"
        _write_parquet(l2_rows, l2_schema, l2_path)
        _write_parquet(trade_rows, trade_schema, trade_path)
        destination.put_file(l2_key, str(l2_path), content_type="application/octet-stream")
        destination.put_file(trade_key, str(trade_path), content_type="application/octet-stream")

        l2_record = _put_coverage(
            coverage,
            dataset_kind=DatasetKind.NORMALIZED_L2,
            market=market,
            date=date,
            hour=f"{hour:02d}",
            object_count=1,
            byte_count=l2_path.stat().st_size,
            row_count=len(l2_rows),
            source=l2_key,
        )
        trade_record = _put_coverage(
            coverage,
            dataset_kind=DatasetKind.NORMALIZED_TRADES,
            market=market,
            date=date,
            hour=f"{hour:02d}",
            object_count=1,
            byte_count=trade_path.stat().st_size,
            row_count=len(trade_rows),
            source=trade_key,
        )
        total_l2_rows += l2_record.row_count
        total_trade_rows += trade_record.row_count
        total_l2_bytes += l2_record.byte_count
        total_trade_bytes += trade_record.byte_count

    _put_coverage(
        coverage,
        dataset_kind=DatasetKind.NORMALIZED_L2,
        market=market,
        date=date,
        hour="daily",
        object_count=24,
        byte_count=total_l2_bytes,
        row_count=total_l2_rows,
        source="s3",
    )
    _put_coverage(
        coverage,
        dataset_kind=DatasetKind.NORMALIZED_TRADES,
        market=market,
        date=date,
        hour="daily",
        object_count=24,
        byte_count=total_trade_bytes,
        row_count=total_trade_rows,
        source="s3",
    )


def sync_window(
    destination: S3Store,
    coverage: CoverageRepository,
    shard_repo: CanonicalShardRepository,
    *,
    request: IngestionRequest,
    request_payer: str = "requester",
    depth: int = 20,
) -> None:
    market = request.day_request(request.resolve_window()[0])
    for date in request.iter_dates():
        backfill_day(
            destination,
            coverage,
            market=market,
            date=date,
            request_payer=request_payer,
        )
        normalize_day(
            destination,
            coverage,
            market=market,
            date=date,
        )
        build_hyperliquid_canonical_day(
            market=market,
            date=date,
            depth=depth,
            s3_store=destination,
            coverage_repo=coverage,
            shard_repo=shard_repo,
        )


def ingest_range(
    destination: S3Store,
    coverage: CoverageRepository,
    *,
    request: IngestionRequest,
    request_payer: str = "requester",
) -> None:
    raise RuntimeError("ingest-range is deprecated; use hyperliquid-sync-window instead")
