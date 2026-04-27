"""Parsers + raw mirror for Hyperliquid archives.

L2 snapshots come from `hyperliquid-archive` per (instrument, hour). Fills come
from the `hl-mainnet-node-data` firehose per hour (covers all coins).

The slice builder consumes parsed rows inline; there is no separate normalize stage.
"""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import UTC, datetime
import io
import logging
from typing import Any, Iterator

import lz4.frame
import orjson

from .models import (
    CoverageRecord,
    CoverageStatus,
    DatasetKind,
    MarketRef,
    NormalizedL2Snapshot,
    NormalizedTrade,
    coverage_pk_raw_hl_fills,
    coverage_pk_raw_hl_l2,
    raw_hl_fills_s3_key,
    raw_hl_l2_s3_key,
    utc_now_iso,
)
from .storage import CoverageRepository, S3Store

logger = logging.getLogger(__name__)

L2_SOURCE_BUCKET = "hyperliquid-archive"
FILLS_SOURCE_BUCKET = "hl-mainnet-node-data"
HL_MIRROR_WORKERS = 16


def hl_l2_source_key(instrument: str, date: str, hour: int) -> str:
    """Object key for one hour of L2 in the hyperliquid-archive bucket."""
    return f"market_data/{date.replace('-', '')}/{hour}/l2Book/{instrument}.lz4"


def hl_fills_source_key(date: str, hour: int) -> str:
    """Object key for one hour of fills in the hl-mainnet-node-data bucket."""
    return f"node_fills_by_block/hourly/{date.replace('-', '')}/{hour}.lz4"


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


def parse_l2_snapshot(
    line: dict[str, Any], *, source_hour: int, source_line_number: int
) -> NormalizedL2Snapshot:
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
    """Parse one hourly fills lz4 firehose, filter to `instrument`, dedupe, return ordered trades."""
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


def parse_l2_lz4_payload(
    payload: bytes, *, source_hour: int
) -> list[NormalizedL2Snapshot]:
    """Parse one hourly L2 lz4 file into ordered snapshots."""
    rows: list[NormalizedL2Snapshot] = []
    for line_number, raw_line in iter_lz4_json_lines(payload):
        rows.append(
            parse_l2_snapshot(
                raw_line, source_hour=source_hour, source_line_number=line_number
            )
        )
    rows.sort(key=lambda item: (item.ts_ms, item.source_line_number))
    return rows


# --- raw mirror --------------------------------------------------------------


@dataclass
class HLMirrorSummary:
    l2_mirrored: int = 0
    l2_skipped: int = 0
    l2_failed: int = 0
    fills_mirrored: int = 0
    fills_skipped: int = 0
    fills_failed: int = 0
    bytes_total: int = 0


def mirror_hl_window(
    *,
    market: MarketRef,
    start_date: str,
    end_date: str,
    s3_store: S3Store,
    coverage_repo: CoverageRepository,
    request_payer: str = "requester",
    workers: int = HL_MIRROR_WORKERS,
) -> HLMirrorSummary:
    """Mirror Hyperliquid L2 (per-instrument) and fills (firehose) for the window.

    Idempotent: hours already in S3 with READY coverage are skipped.
    """
    from .models import iter_dates_inclusive

    summary = HLMirrorSummary()
    targets: list[tuple[str, str, int]] = []
    for date in iter_dates_inclusive(start_date, end_date):
        for hour in range(24):
            targets.append(("l2", date, hour))
            targets.append(("fills", date, hour))

    with ThreadPoolExecutor(max_workers=max(1, workers)) as executor:
        futures = {}
        for kind, date, hour in targets:
            if kind == "l2":
                fut = executor.submit(
                    _mirror_one_l2_hour,
                    s3_store=s3_store,
                    coverage_repo=coverage_repo,
                    market=market,
                    date=date,
                    hour=hour,
                    request_payer=request_payer,
                )
            else:
                fut = executor.submit(
                    _mirror_one_fills_hour,
                    s3_store=s3_store,
                    coverage_repo=coverage_repo,
                    date=date,
                    hour=hour,
                    request_payer=request_payer,
                )
            futures[fut] = (kind, date, hour)

        for future in as_completed(futures):
            kind, date, hour = futures[future]
            try:
                result, byte_count = future.result()
            except Exception as error:  # noqa: BLE001
                logger.exception(
                    "hl mirror failed kind=%s date=%s hour=%02d: %s",
                    kind,
                    date,
                    hour,
                    error,
                )
                if kind == "l2":
                    summary.l2_failed += 1
                else:
                    summary.fills_failed += 1
                continue
            if kind == "l2":
                if result == "mirrored":
                    summary.l2_mirrored += 1
                elif result == "skipped":
                    summary.l2_skipped += 1
                else:
                    summary.l2_failed += 1
            else:
                if result == "mirrored":
                    summary.fills_mirrored += 1
                elif result == "skipped":
                    summary.fills_skipped += 1
                else:
                    summary.fills_failed += 1
            summary.bytes_total += byte_count

    return summary


def _mirror_one_l2_hour(
    *,
    s3_store: S3Store,
    coverage_repo: CoverageRepository,
    market: MarketRef,
    date: str,
    hour: int,
    request_payer: str,
) -> tuple[str, int]:
    pk = coverage_pk_raw_hl_l2(market, date, hour)
    s3_key = raw_hl_l2_s3_key(market, date, hour)
    existing = coverage_repo.get(pk)
    if (
        existing is not None
        and existing.status == CoverageStatus.READY
        and s3_store.exists(s3_key)
    ):
        return "skipped", 0

    source_key = hl_l2_source_key(market.instrument, date, hour)
    byte_count = _requester_pays_copy(
        s3_store,
        source_bucket=L2_SOURCE_BUCKET,
        source_key=source_key,
        destination_key=s3_key,
        request_payer=request_payer,
    )
    if byte_count is None:
        return "failed", 0

    coverage_repo.put(
        CoverageRecord(
            pk=pk,
            dataset_kind=DatasetKind.RAW_HL_L2,
            status=CoverageStatus.READY,
            object_count=1,
            byte_count=byte_count,
            row_count=0,
            updated_at=utc_now_iso(),
            source=f"s3://{L2_SOURCE_BUCKET}/{source_key}",
            venue=market.venue,
            market_type=market.market_type,
            instrument=market.instrument,
            date=date,
            hour=f"{hour:02d}",
        )
    )
    logger.info(
        "hl mirror PUT l2 market=%s/%s date=%s hour=%02d bytes=%d",
        market.market_type.value,
        market.instrument,
        date,
        hour,
        byte_count,
    )
    return "mirrored", byte_count


def _mirror_one_fills_hour(
    *,
    s3_store: S3Store,
    coverage_repo: CoverageRepository,
    date: str,
    hour: int,
    request_payer: str,
) -> tuple[str, int]:
    pk = coverage_pk_raw_hl_fills(date, hour)
    s3_key = raw_hl_fills_s3_key(date, hour)
    existing = coverage_repo.get(pk)
    if (
        existing is not None
        and existing.status == CoverageStatus.READY
        and s3_store.exists(s3_key)
    ):
        return "skipped", 0

    source_key = hl_fills_source_key(date, hour)
    byte_count = _requester_pays_copy(
        s3_store,
        source_bucket=FILLS_SOURCE_BUCKET,
        source_key=source_key,
        destination_key=s3_key,
        request_payer=request_payer,
    )
    if byte_count is None:
        return "failed", 0

    coverage_repo.put(
        CoverageRecord(
            pk=pk,
            dataset_kind=DatasetKind.RAW_HL_FILLS,
            status=CoverageStatus.READY,
            object_count=1,
            byte_count=byte_count,
            row_count=0,
            updated_at=utc_now_iso(),
            source=f"s3://{FILLS_SOURCE_BUCKET}/{source_key}",
            date=date,
            hour=f"{hour:02d}",
        )
    )
    logger.info(
        "hl mirror PUT fills date=%s hour=%02d bytes=%d", date, hour, byte_count
    )
    return "mirrored", byte_count


def _requester_pays_copy(
    s3_store: S3Store,
    *,
    source_bucket: str,
    source_key: str,
    destination_key: str,
    request_payer: str,
) -> int | None:
    """Download from a requester-pays bucket; upload to our destination."""
    try:
        response = s3_store.client.get_object(
            Bucket=source_bucket,
            Key=source_key,
            RequestPayer=request_payer,
        )
    except Exception as error:  # noqa: BLE001
        logger.warning(
            "hl mirror upstream get failed bucket=%s key=%s: %s",
            source_bucket,
            source_key,
            error,
        )
        return None
    payload = response["Body"].read()
    s3_store.put_bytes(destination_key, payload, content_type="application/octet-stream")
    return len(payload)
