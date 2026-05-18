from __future__ import annotations

from contextlib import contextmanager
import io
import json
from pathlib import Path
import sys
from typing import Any, Iterator

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
import zstandard

from . import canonical as canonical_module
from .canonical import PMSliceStats, _slice_pmxt_for_date


DEFAULT_PM_GOLDEN_THRESHOLDS = {
    "missing": 0,
    "crossed_at_sample": 0,
    "exact_top1_price_rate": 0.999,
    "within_1_tick_top1_rate": 0.9999,
    "exact_top5_price_rate": 0.99,
}

LevelBook = dict[float, float]
BookState = dict[str, dict[str, LevelBook]]


def run_polymarket_golden_fixture(
    *,
    fixture_prefix: str,
    work_dir: Path,
    canonical_out: Path | None = None,
    report_out: Path | None = None,
) -> dict[str, Any]:
    """Download a golden fixture prefix and validate it end to end."""
    work_dir.mkdir(parents=True, exist_ok=True)
    manifest = _load_fixture_manifest(fixture_prefix=fixture_prefix, work_dir=work_dir)
    live_path, pmxt_hours = _download_fixture_inputs(
        fixture_prefix=fixture_prefix,
        manifest=manifest,
        work_dir=work_dir,
    )
    return run_polymarket_golden_validation(
        date=manifest["date"],
        live_path=live_path,
        pmxt_hours=pmxt_hours,
        depth=int(manifest.get("depth", 5)),
        canonical_out=canonical_out or work_dir / "canonical-data.parquet",
        report_out=report_out or work_dir / "compare-report.json",
        thresholds=manifest.get("thresholds") or DEFAULT_PM_GOLDEN_THRESHOLDS,
    )


def run_polymarket_golden_validation(
    *,
    date: str,
    live_path: Path,
    pmxt_hours: dict[int, Path],
    depth: int = 5,
    canonical_out: Path,
    report_out: Path | None = None,
    thresholds: dict[str, float | int] | None = None,
) -> dict[str, Any]:
    """Rebuild canonical data from PMXT hour files and compare it to WS/live."""
    canonical_out.parent.mkdir(parents=True, exist_ok=True)
    asset_to_instrument = _asset_instruments_from_live(live_path)
    if not asset_to_instrument:
        raise ValueError(f"no token_id -> instrument mappings found in {live_path}")
    rows_out = _build_canonical_from_local_pmxt(
        date=date,
        pmxt_hours=pmxt_hours,
        asset_to_instrument=asset_to_instrument,
        depth=depth,
        canonical_out=canonical_out,
    )
    summary = compare_canonical_to_live(
        canonical_path=canonical_out,
        live_path=live_path,
        depth=depth,
    )
    summary["build"] = {
        "date": date,
        "pmxt_hours": {f"{hour:02d}": str(path) for hour, path in sorted(pmxt_hours.items())},
        "asset_ids": len(asset_to_instrument),
        "rows_out": rows_out,
    }
    errors = check_polymarket_golden_thresholds(
        summary,
        thresholds=thresholds or DEFAULT_PM_GOLDEN_THRESHOLDS,
    )
    summary["thresholds"] = thresholds or DEFAULT_PM_GOLDEN_THRESHOLDS
    summary["threshold_errors"] = errors
    payload = json.dumps(summary, indent=2, sort_keys=True)
    if report_out is not None:
        report_out.parent.mkdir(parents=True, exist_ok=True)
        report_out.write_text(payload + "\n")
    return summary


def compare_canonical_to_live(
    *,
    canonical_path: Path,
    live_path: Path,
    depth: int = 5,
) -> dict[str, Any]:
    canonical_min_ts, canonical_max_ts = _canonical_min_max(canonical_path)
    events, live_counts, instruments = _load_live_events(live_path, max_ts_ms=canonical_max_ts)
    live_state: BookState = {}
    canonical_state: BookState = {}
    canonical = _CanonicalCursor(canonical_path, instruments)

    compared = 0
    missing = 0
    crossed = 0
    exact_top1_price = 0
    exact_top1_price_size = 0
    within_1_tick_top1 = 0
    exact_top5_price = 0
    exact_top5_price_size = 0
    snapshots = 0
    mismatches: list[dict[str, Any]] = []

    idx = 0
    while idx < len(events):
        ts_ms = events[idx][0]
        touched: set[str] = set()
        while idx < len(events) and events[idx][0] == ts_ms:
            touched.add(_apply_live_event(events[idx], live_state))
            idx += 1
        if ts_ms < canonical_min_ts:
            continue
        canonical.advance_until(ts_ms, canonical_state)
        for instrument in sorted(touched):
            live_top = _top_state(live_state, instrument, depth=depth)
            if live_top is None:
                continue
            snapshots += 1
            canonical_top = _top_state(canonical_state, instrument, depth=depth)
            if canonical_top is None:
                missing += 1
                if len(mismatches) < 25:
                    mismatches.append(
                        {
                            "reason": "missing_canonical_state",
                            "ts_ms": ts_ms,
                            "instrument": instrument,
                            "live": _jsonable_top(live_top),
                            "canonical": None,
                        }
                    )
                continue
            compared += 1
            if canonical_top["bid_px"] >= canonical_top["ask_px"]:
                crossed += 1
            bid_diff = abs(canonical_top["bid_px"] - live_top["bid_px"])
            ask_diff = abs(canonical_top["ask_px"] - live_top["ask_px"])
            top1_price_ok = bid_diff <= 1e-9 and ask_diff <= 1e-9
            top1_size_ok = (
                abs(canonical_top["bid_sz"] - live_top["bid_sz"]) <= 1e-6
                and abs(canonical_top["ask_sz"] - live_top["ask_sz"]) <= 1e-6
            )
            top5_price_ok = _same_prices(canonical_top["bids"], live_top["bids"]) and _same_prices(
                canonical_top["asks"], live_top["asks"]
            )
            top5_size_ok = _same_prices_sizes(
                canonical_top["bids"], live_top["bids"]
            ) and _same_prices_sizes(canonical_top["asks"], live_top["asks"])
            if top1_price_ok:
                exact_top1_price += 1
            if top1_price_ok and top1_size_ok:
                exact_top1_price_size += 1
            if bid_diff <= 0.010000001 and ask_diff <= 0.010000001:
                within_1_tick_top1 += 1
            if top5_price_ok:
                exact_top5_price += 1
            if top5_price_ok and top5_size_ok:
                exact_top5_price_size += 1
            if (not top1_price_ok or not top1_size_ok) and len(mismatches) < 25:
                mismatches.append(
                    {
                        "reason": "mismatch",
                        "ts_ms": ts_ms,
                        "instrument": instrument,
                        "live": _jsonable_top(live_top),
                        "canonical": _jsonable_top(canonical_top),
                        "bid_diff": bid_diff,
                        "ask_diff": ask_diff,
                    }
                )

    return {
        "canonical": {
            "path": str(canonical_path),
            "min_ts_ms": canonical_min_ts,
            "max_ts_ms": canonical_max_ts,
            "rows_seen": canonical.rows_seen,
            "rows_applied": canonical.rows_applied,
        },
        "live": {
            "path": str(live_path),
            "counts": live_counts,
            "events_loaded": len(events),
            "instruments": len(instruments),
        },
        "depth": depth,
        "snapshots": snapshots,
        "missing": missing,
        "compared": compared,
        "crossed_at_sample": crossed,
        "exact_top1_price": exact_top1_price,
        "exact_top1_price_rate": exact_top1_price / compared if compared else None,
        "exact_top1_price_size": exact_top1_price_size,
        "exact_top1_price_size_rate": exact_top1_price_size / compared if compared else None,
        "within_1_tick_top1": within_1_tick_top1,
        "within_1_tick_top1_rate": within_1_tick_top1 / compared if compared else None,
        "exact_top5_price": exact_top5_price,
        "exact_top5_price_rate": exact_top5_price / compared if compared else None,
        "exact_top5_price_size": exact_top5_price_size,
        "exact_top5_price_size_rate": exact_top5_price_size / compared if compared else None,
        "first_mismatches": mismatches,
    }


def check_polymarket_golden_thresholds(
    summary: dict[str, Any],
    *,
    thresholds: dict[str, float | int] | None = None,
) -> list[str]:
    thresholds = thresholds or DEFAULT_PM_GOLDEN_THRESHOLDS
    errors: list[str] = []
    for key in ("missing", "crossed_at_sample"):
        expected = int(thresholds[key])
        actual = int(summary.get(key, -1))
        if actual != expected:
            errors.append(f"{key}={actual} expected {expected}")
    for key in ("exact_top1_price_rate", "within_1_tick_top1_rate", "exact_top5_price_rate"):
        expected = float(thresholds[key])
        actual_raw = summary.get(key)
        actual = float(actual_raw) if actual_raw is not None else -1.0
        if actual < expected:
            errors.append(f"{key}={actual:.12f} below {expected:.12f}")
    return errors


def _build_canonical_from_local_pmxt(
    *,
    date: str,
    pmxt_hours: dict[int, Path],
    asset_to_instrument: dict[str, str],
    depth: int,
    canonical_out: Path,
) -> int:
    empty_payload = _empty_parquet_like(next(iter(pmxt_hours.values())))
    payloads = {hour: path.read_bytes() for hour, path in pmxt_hours.items()}

    def fetch_pmxt_payload(_s3_store: object, _date: str, hour: int) -> bytes:
        return payloads.get(hour, empty_payload)

    stats = PMSliceStats()
    original_fetch = canonical_module._fetch_pmxt_payload
    canonical_module._fetch_pmxt_payload = fetch_pmxt_payload  # type: ignore[method-assign]
    try:
        return _slice_pmxt_for_date(
            s3_store=object(),  # type: ignore[arg-type]
            date=date,
            asset_to_instrument=asset_to_instrument,
            asset_ids=list(asset_to_instrument),
            depth=depth,
            stats=stats,
            data_parquet_path=canonical_out,
        )
    finally:
        canonical_module._fetch_pmxt_payload = original_fetch  # type: ignore[method-assign]


def _empty_parquet_like(path: Path) -> bytes:
    schema = pq.ParquetFile(path).schema_arrow
    arrays = [pa.array([], type=field.type) for field in schema]
    table = pa.Table.from_arrays(arrays, schema=schema)
    buffer = io.BytesIO()
    pq.write_table(table, buffer)
    return buffer.getvalue()


def _asset_instruments_from_live(path: Path) -> dict[str, str]:
    mapping: dict[str, str] = {}
    with _open_text(path) as handle:
        for line in handle:
            row = json.loads(line)
            token_id = row.get("token_id") or row.get("asset_id")
            slug = row.get("slug")
            outcome = row.get("outcome")
            if token_id and slug and outcome:
                mapping[str(token_id)] = f"{slug}:{outcome}"
    return mapping


def _load_fixture_manifest(*, fixture_prefix: str, work_dir: Path) -> dict[str, Any]:
    manifest_path = work_dir / "manifest.json"
    if fixture_prefix.startswith("s3://"):
        bucket, prefix = _parse_s3_uri(fixture_prefix)
        _download_s3_file(bucket=bucket, key=f"{prefix}/manifest.json", dest=manifest_path)
    else:
        manifest_path = Path(fixture_prefix) / "manifest.json"
    manifest = json.loads(manifest_path.read_text())
    if "date" not in manifest or "live_capture" not in manifest or "pmxt_hours" not in manifest:
        raise ValueError("fixture manifest must contain date, live_capture, and pmxt_hours")
    return manifest


def _download_fixture_inputs(
    *,
    fixture_prefix: str,
    manifest: dict[str, Any],
    work_dir: Path,
) -> tuple[Path, dict[int, Path]]:
    if not fixture_prefix.startswith("s3://"):
        base = Path(fixture_prefix)
        live_path = base / manifest["live_capture"]
        return live_path, {
            int(hour): base / rel_path for hour, rel_path in manifest["pmxt_hours"].items()
        }

    bucket, prefix = _parse_s3_uri(fixture_prefix)
    live_path = work_dir / Path(manifest["live_capture"]).name
    _download_s3_file(bucket=bucket, key=f"{prefix}/{manifest['live_capture']}", dest=live_path)
    pmxt_hours: dict[int, Path] = {}
    for hour, rel_path in manifest["pmxt_hours"].items():
        dest = work_dir / Path(rel_path).name
        _download_s3_file(bucket=bucket, key=f"{prefix}/{rel_path}", dest=dest)
        pmxt_hours[int(hour)] = dest
    return live_path, pmxt_hours


def _parse_s3_uri(uri: str) -> tuple[str, str]:
    if not uri.startswith("s3://"):
        raise ValueError(f"not an s3 uri: {uri}")
    bucket_and_key = uri.removeprefix("s3://")
    bucket, _, key = bucket_and_key.partition("/")
    return bucket, key.rstrip("/")


def _download_s3_file(*, bucket: str, key: str, dest: Path) -> None:
    import boto3

    dest.parent.mkdir(parents=True, exist_ok=True)
    boto3.client("s3").download_file(bucket, key, str(dest))


@contextmanager
def _open_text(path: Path) -> Iterator[io.TextIOBase]:
    if path.suffix == ".zst":
        with path.open("rb") as raw:
            reader = zstandard.ZstdDecompressor().stream_reader(raw)
            text = io.TextIOWrapper(reader, encoding="utf-8")
            try:
                yield text
            finally:
                text.close()
    else:
        with path.open() as handle:
            yield handle


def _to_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _price_key(value: float) -> float:
    return round(float(value), 9)


def _parse_levels(raw: Any) -> tuple[tuple[float, float], ...]:
    if raw is None:
        return ()
    if isinstance(raw, str):
        if not raw:
            return ()
        raw = json.loads(raw)
    levels: list[tuple[float, float]] = []
    for level in raw or []:
        if isinstance(level, dict):
            px = _to_float(level.get("px", level.get("price")))
            sz = _to_float(level.get("sz", level.get("size")))
        else:
            px = _to_float(level[0]) if len(level) > 0 else None
            sz = _to_float(level[1]) if len(level) > 1 else None
        if px is None or sz is None or sz <= 0.0:
            continue
        levels.append((_price_key(px), float(sz)))
    return tuple(levels)


def _replace(levels: tuple[tuple[float, float], ...]) -> LevelBook:
    return {px: sz for px, sz in levels if sz > 0.0}


def _set_level(book: LevelBook, px: float, sz: float) -> None:
    px = _price_key(px)
    if sz <= 0.0:
        book.pop(px, None)
    else:
        book[px] = float(sz)


def _top(book: LevelBook, *, bid: bool, depth: int) -> tuple[tuple[float, float], ...]:
    prices = sorted(book, reverse=bid)[:depth]
    return tuple((px, book[px]) for px in prices)


def _top_state(state: BookState, instrument: str, *, depth: int) -> dict[str, Any] | None:
    book = state.get(instrument)
    if not book:
        return None
    bids = _top(book["bids"], bid=True, depth=depth)
    asks = _top(book["asks"], bid=False, depth=depth)
    if not bids or not asks:
        return None
    return {
        "bids": bids,
        "asks": asks,
        "bid_px": bids[0][0],
        "bid_sz": bids[0][1],
        "ask_px": asks[0][0],
        "ask_sz": asks[0][1],
    }


def _canonical_min_max(path: Path) -> tuple[int, int]:
    parquet = pq.ParquetFile(path)
    mins: list[int] = []
    maxs: list[int] = []
    for batch in parquet.iter_batches(columns=["ts_ms"], batch_size=262_144):
        column = batch.column(0)
        mins.append(int(pc.min(column).as_py()))
        maxs.append(int(pc.max(column).as_py()))
    if not mins:
        raise ValueError(f"empty canonical parquet: {path}")
    return min(mins), max(maxs)


def _load_live_events(path: Path, *, max_ts_ms: int) -> tuple[list[tuple], dict[str, int], set[str]]:
    events: list[tuple] = []
    counts = {
        "lines": 0,
        "book": 0,
        "price_change": 0,
        "last_trade_price": 0,
        "skipped_after_max_ts": 0,
        "skipped_other": 0,
    }
    instruments: set[str] = set()
    with _open_text(path) as handle:
        for line in handle:
            counts["lines"] += 1
            row = json.loads(line)
            ts_ms = int(row["timestamp"])
            if ts_ms > max_ts_ms:
                counts["skipped_after_max_ts"] += 1
                continue
            event_type = row.get("event_type")
            if event_type == "book":
                if "slug" not in row or "outcome" not in row:
                    counts["skipped_other"] += 1
                    continue
                instrument = sys.intern(f"{row['slug']}:{row['outcome']}")
                bids = _parse_levels(row.get("bids"))
                asks = _parse_levels(row.get("asks"))
                seq = int(row.get("capture_seq", counts["lines"]))
                events.append((ts_ms, seq, instrument, "book", bids, asks))
                instruments.add(instrument)
                counts["book"] += 1
            elif event_type == "price_change":
                if "slug" not in row or "outcome" not in row:
                    counts["skipped_other"] += 1
                    continue
                side = row.get("side")
                px = _to_float(row.get("price"))
                sz = _to_float(row.get("size"))
                if side not in {"BUY", "SELL", "Buy", "Sell"} or px is None or sz is None:
                    counts["skipped_other"] += 1
                    continue
                instrument = sys.intern(f"{row['slug']}:{row['outcome']}")
                side_norm = "Buy" if str(side).upper() == "BUY" else "Sell"
                seq = int(row.get("capture_seq", counts["lines"]))
                events.append((ts_ms, seq, instrument, "delta", side_norm, _price_key(px), float(sz)))
                instruments.add(instrument)
                counts["price_change"] += 1
            elif event_type == "last_trade_price":
                counts["last_trade_price"] += 1
            else:
                counts["skipped_other"] += 1
    events.sort(key=lambda item: (item[0], item[1]))
    return events, counts, instruments


class _CanonicalCursor:
    def __init__(self, path: Path, instruments: set[str]) -> None:
        self._parquet = pq.ParquetFile(path)
        self._batches = self._parquet.iter_batches(
            columns=["ts_ms", "instrument", "kind", "bids", "asks", "delta_levels"],
            batch_size=65_536,
        )
        self._instruments = instruments
        self._columns: dict[str, list] | None = None
        self._idx = 0
        self._rows = 0
        self.current: dict[str, Any] | None = None
        self.rows_seen = 0
        self.rows_applied = 0
        self._load_next_row()

    def _load_next_batch(self) -> bool:
        try:
            batch = next(self._batches)
        except StopIteration:
            self._columns = None
            self._idx = 0
            self._rows = 0
            return False
        self._columns = {name: batch.column(name).to_pylist() for name in batch.schema.names}
        self._idx = 0
        self._rows = batch.num_rows
        return True

    def _load_next_row(self) -> None:
        while True:
            if self._columns is None or self._idx >= self._rows:
                if not self._load_next_batch():
                    self.current = None
                    return
            assert self._columns is not None
            idx = self._idx
            self._idx += 1
            self.rows_seen += 1
            instrument = self._columns["instrument"][idx]
            if instrument not in self._instruments:
                continue
            self.current = {
                "ts_ms": self._columns["ts_ms"][idx],
                "instrument": instrument,
                "kind": self._columns["kind"][idx],
                "bids": self._columns["bids"][idx],
                "asks": self._columns["asks"][idx],
                "delta_levels": self._columns["delta_levels"][idx],
            }
            return

    def advance_until(self, ts_ms: int, state: BookState) -> None:
        while self.current is not None and self.current["ts_ms"] <= ts_ms:
            _apply_canonical_row(self.current, state)
            self.rows_applied += 1
            self._load_next_row()


def _apply_canonical_row(row: dict[str, Any], state: BookState) -> None:
    book = state.setdefault(row["instrument"], {"bids": {}, "asks": {}})
    if row["kind"] == "l2_snapshot":
        book["bids"] = _replace(_parse_levels(row["bids"]))
        book["asks"] = _replace(_parse_levels(row["asks"]))
        return
    if row["kind"] != "delta_batch":
        return
    for level in row["delta_levels"] or []:
        side = level.get("side")
        px = _to_float(level.get("px"))
        sz = _to_float(level.get("sz"))
        if side not in {"Buy", "Sell"} or px is None or sz is None:
            continue
        target = book["bids"] if side == "Buy" else book["asks"]
        _set_level(target, px, sz)


def _apply_live_event(event: tuple, state: BookState) -> str:
    instrument = event[2]
    book = state.setdefault(instrument, {"bids": {}, "asks": {}})
    if event[3] == "book":
        book["bids"] = _replace(event[4])
        book["asks"] = _replace(event[5])
    else:
        side = event[4]
        target = book["bids"] if side == "Buy" else book["asks"]
        _set_level(target, event[5], event[6])
    return instrument


def _same_prices(left: tuple[tuple[float, float], ...], right: tuple[tuple[float, float], ...]) -> bool:
    return tuple(px for px, _ in left) == tuple(px for px, _ in right)


def _same_prices_sizes(
    left: tuple[tuple[float, float], ...], right: tuple[tuple[float, float], ...]
) -> bool:
    if len(left) != len(right):
        return False
    for (left_px, left_sz), (right_px, right_sz) in zip(left, right, strict=True):
        if abs(left_px - right_px) > 1e-9 or abs(left_sz - right_sz) > 1e-6:
            return False
    return True


def _jsonable_top(top: dict[str, Any] | None) -> dict[str, Any] | None:
    if top is None:
        return None
    return {
        "bid_px": top["bid_px"],
        "bid_sz": top["bid_sz"],
        "ask_px": top["ask_px"],
        "ask_sz": top["ask_sz"],
        "bids": [list(level) for level in top["bids"]],
        "asks": [list(level) for level in top["asks"]],
    }
