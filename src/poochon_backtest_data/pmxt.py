"""PMXT raw firehose mirror.

Downloads hourly Polymarket orderbook snapshots from r2v2.pmxt.dev and uploads
them to the S3 raw bucket. Idempotent per (date, hour); already-mirrored hours
are skipped based on the CoverageRecord + a HEAD against S3.
"""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
import logging

import httpx

from .models import (
    CoverageRecord,
    CoverageStatus,
    DatasetKind,
    coverage_pk_raw_pmxt,
    iter_dates_inclusive,
    raw_pmxt_filename,
    raw_pmxt_s3_key,
    raw_pmxt_upstream_url,
    utc_now_iso,
)
from .storage import CoverageRepository, S3Store

logger = logging.getLogger(__name__)

PMXT_BUFFER_MINUTES = 30
PMXT_MIRROR_WORKERS = 4
PMXT_HTTP_TIMEOUT = 600.0


@dataclass
class MirrorSummary:
    mirrored: int = 0
    skipped: int = 0
    failed: int = 0
    bytes_total: int = 0


def mirror_pmxt_window(
    *,
    s3_store: S3Store,
    coverage_repo: CoverageRepository,
    start_date: str,
    end_date: str,
    pmxt_base_url: str,
    http_client: httpx.Client | None = None,
    workers: int = PMXT_MIRROR_WORKERS,
    now: datetime | None = None,
) -> MirrorSummary:
    """Mirror PMXT hourly files into S3 for the inclusive window.

    Skips hours that are already mirrored (READY coverage record + S3 object
    present) and the current hour until `PMXT_BUFFER_MINUTES` past it (to allow
    upstream publish lag).
    """
    summary = MirrorSummary()
    own_client = http_client is None
    client = http_client or httpx.Client(
        timeout=PMXT_HTTP_TIMEOUT, follow_redirects=True
    )
    try:
        targets = _planned_targets(start_date, end_date, now=now or datetime.now(tz=UTC))
        if not targets:
            return summary

        with ThreadPoolExecutor(max_workers=max(1, workers)) as executor:
            futures = {
                executor.submit(
                    _mirror_one_hour,
                    s3_store=s3_store,
                    coverage_repo=coverage_repo,
                    client=client,
                    pmxt_base_url=pmxt_base_url,
                    date=date,
                    hour=hour,
                ): (date, hour)
                for date, hour in targets
            }
            for future in as_completed(futures):
                date, hour = futures[future]
                try:
                    result = future.result()
                except Exception as error:  # noqa: BLE001
                    logger.exception("mirror failed date=%s hour=%02d: %s", date, hour, error)
                    summary.failed += 1
                    continue
                if result == "mirrored":
                    summary.mirrored += 1
                elif result == "skipped":
                    summary.skipped += 1
                else:
                    summary.failed += 1
        return summary
    finally:
        if own_client:
            client.close()


def _planned_targets(
    start_date: str, end_date: str, *, now: datetime
) -> list[tuple[str, int]]:
    cutoff = now - timedelta(minutes=PMXT_BUFFER_MINUTES)
    targets: list[tuple[str, int]] = []
    for date in iter_dates_inclusive(start_date, end_date):
        for hour in range(24):
            hour_start = datetime.fromisoformat(f"{date}T{hour:02d}:00:00+00:00")
            if hour_start > cutoff:
                continue
            targets.append((date, hour))
    return targets


def _mirror_one_hour(
    *,
    s3_store: S3Store,
    coverage_repo: CoverageRepository,
    client: httpx.Client,
    pmxt_base_url: str,
    date: str,
    hour: int,
) -> str:
    pk = coverage_pk_raw_pmxt(date, hour)
    s3_key = raw_pmxt_s3_key(date, hour)
    existing = coverage_repo.get(pk)
    if (
        existing is not None
        and existing.status == CoverageStatus.READY
        and s3_store.exists(s3_key)
    ):
        logger.debug("pmxt mirror skip date=%s hour=%02d (already READY)", date, hour)
        return "skipped"

    upstream = raw_pmxt_upstream_url(pmxt_base_url, date, hour)
    logger.info("pmxt mirror GET  %s", upstream)
    # Stream download into a temp file rather than buffering in memory; for
    # ~400 MB hourly PMXT files this prevents Fargate OOM with multiple workers.
    import os
    import tempfile

    with tempfile.NamedTemporaryFile(prefix=f"pmxt-{date}-{hour:02d}-", delete=False) as tmp:
        tmp_path = tmp.name
    try:
        with client.stream("GET", upstream) as response:
            if response.status_code == 404:
                logger.warning(
                    "pmxt mirror upstream 404 date=%s hour=%02d (file not yet published?)",
                    date,
                    hour,
                )
                return "failed"
            response.raise_for_status()
            with open(tmp_path, "wb") as fh:
                for chunk in response.iter_bytes(chunk_size=8 * 1024 * 1024):
                    fh.write(chunk)
        byte_count = os.path.getsize(tmp_path)
        s3_store.put_file(
            s3_key, tmp_path, content_type="application/vnd.apache.parquet"
        )
    finally:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
    record = CoverageRecord(
        pk=pk,
        dataset_kind=DatasetKind.RAW_PMXT,
        status=CoverageStatus.READY,
        object_count=1,
        byte_count=byte_count,
        row_count=0,
        updated_at=utc_now_iso(),
        source=upstream,
        date=date,
        hour=f"{hour:02d}",
    )
    coverage_repo.put(record)
    logger.info(
        "pmxt mirror PUT  date=%s hour=%02d s3_key=%s bytes=%d",
        date,
        hour,
        s3_key,
        byte_count,
    )
    return "mirrored"
