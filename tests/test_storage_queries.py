from __future__ import annotations

from unittest.mock import MagicMock
from datetime import UTC, datetime

import pytest

from poochon_backtest_data.models import (
    CanonicalFileFamily,
    CanonicalShardRecord,
    CanonicalShardStatus,
    CoverageRecord,
    CoverageStatus,
    DatasetKind,
    MarketRef,
    MarketType,
    OutcomesMode,
    Venue,
    canonical_hyperliquid_shard_id,
    coverage_pk,
    utc_now_iso,
)
from poochon_backtest_data.storage import (
    CanonicalShardRepository,
    CoverageRepository,
    S3Store,
)


def _make_repo(cls, table_name: str, items_by_key: dict[str, dict]):
    """Build a repo with a mocked table.meta.client that serves items_by_key
    through batch_get_item, splitting large requests across responses like
    real DynamoDB."""
    client = MagicMock()

    # Configure batch_get_item to return matching items + UnprocessedKeys loop handling.
    def batch_get_item(RequestItems):
        # Only one table per call in our usage.
        (tbl, spec), = RequestItems.items()
        keys = spec["Keys"]
        # Return at most 2 items per call to exercise UnprocessedKeys path.
        first_batch = keys[:2]
        remainder = keys[2:]
        responses = []
        for key in first_batch:
            pk_field = "pk" if "pk" in key else "shard_id"
            key_value = key[pk_field]
            if key_value in items_by_key:
                responses.append(items_by_key[key_value])
        result = {
            "Responses": {tbl: responses},
            "UnprocessedKeys": {tbl: {"Keys": remainder}} if remainder else {},
        }
        return result

    def scan_paginator_side_effect(**kwargs):
        items = [v for k, v in items_by_key.items()
                 if k.startswith(kwargs.get("ExpressionAttributeValues", {}).get(":prefix", ""))]
        # Return a single page of results
        return iter([{"Items": items}])

    paginator = MagicMock()
    paginator.paginate = MagicMock(side_effect=lambda **kwargs: scan_paginator_side_effect(**kwargs))
    client.get_paginator.return_value = paginator
    client.batch_get_item.side_effect = batch_get_item

    table = MagicMock()
    table.meta.client = client

    repo = cls.__new__(cls)
    repo.region = "eu-west-1"
    repo.table_name = table_name
    repo.table = table
    return repo


def _coverage_item(pk: str) -> dict:
    # Parse pk to fill required fields; pk format:
    # {dataset_kind}#{venue}#{market_type}#{instrument}#{date}#{hour}
    parts = pk.split("#")
    if len(parts) == 6:
        dataset_kind, venue, market_type, instrument, date, hour = parts
    else:
        dataset_kind, venue, market_type, instrument, date, hour = (
            "raw_l2", "hyperliquid", "perp", "BTC", "2026-02-19", "00",
        )
    return {
        "pk": pk,
        "dataset_kind": dataset_kind,
        "venue": venue,
        "market_type": market_type,
        "instrument": instrument,
        "date": date,
        "hour": hour,
        "status": "READY",
        "object_count": 1,
        "byte_count": 1024,
        "row_count": 0,
        "source": "test",
        "updated_at": utc_now_iso(),
    }


def _shard_item(shard_id: str) -> dict:
    return {
        "shard_id": shard_id,
        "venue": "hyperliquid",
        "market_type": "perp",
        "instrument": "BTC",
        "date": "2026-02-19",
        "depth": 20,
        "status": "READY",
        "shard_prefix": "canonical/hyperliquid/.../",
        "manifest_s3_key": "canonical/hyperliquid/.../manifest.json",
        "event_count": 0,
        "created_at": utc_now_iso(),
        "updated_at": utc_now_iso(),
        "source_refs": [],
        "files": [],
    }


class TestCoverageBatchGet:
    def test_empty_input_returns_empty_dict(self):
        repo = _make_repo(CoverageRepository, "coverage", {})
        result = repo.batch_get([])
        assert result == {}

    def test_returns_map_with_present_and_missing(self):
        items = {"pk_a": _coverage_item("pk_a"), "pk_b": _coverage_item("pk_b")}
        repo = _make_repo(CoverageRepository, "coverage", items)
        result = repo.batch_get(["pk_a", "pk_missing", "pk_b"])
        assert result["pk_a"] is not None and result["pk_a"].pk == "pk_a"
        assert result["pk_b"] is not None
        assert result["pk_missing"] is None

    def test_handles_unprocessed_keys_retry(self):
        # 5 pks, mock returns 2 per call, so we need 3 calls via UnprocessedKeys.
        items = {f"pk_{i}": _coverage_item(f"pk_{i}") for i in range(5)}
        repo = _make_repo(CoverageRepository, "coverage", items)
        result = repo.batch_get([f"pk_{i}" for i in range(5)])
        assert all(result[f"pk_{i}"] is not None for i in range(5))

    def test_deduplicates_input(self):
        items = {"pk_a": _coverage_item("pk_a")}
        repo = _make_repo(CoverageRepository, "coverage", items)
        result = repo.batch_get(["pk_a", "pk_a", "pk_a"])
        assert len(result) == 1
        assert result["pk_a"] is not None


class TestCoverageListWindow:
    def test_enumerates_expected_cells(self):
        market = MarketRef(
            venue=Venue.HYPERLIQUID,
            market_type=MarketType.PERP,
            instrument="BTC",
        )
        # Seed a few pks as READY; leave others missing.
        hours = [f"{h:02d}" for h in range(3)]  # hours 00, 01, 02 for this test
        seeded = {}
        for hour in hours:
            pk = coverage_pk(DatasetKind.RAW_L2, market, "2026-02-19", hour)
            seeded[pk] = _coverage_item(pk)

        repo = _make_repo(CoverageRepository, "coverage", seeded)
        cells = repo.list_window(
            dataset_kind=DatasetKind.RAW_L2,
            market=market,
            start_date="2026-02-19",
            end_date="2026-02-20",
            hours=hours,
        )
        assert len(cells) == 6  # 2 dates * 3 hours
        # First day has records, second day doesn't.
        for hour in hours:
            assert cells[("2026-02-19", hour)] is not None
            assert cells[("2026-02-20", hour)] is None


class TestCanonicalShardListWindow:
    def test_hyperliquid_window_returns_per_date_map(self):
        market = MarketRef(
            venue=Venue.HYPERLIQUID,
            market_type=MarketType.PERP,
            instrument="BTC",
        )
        sid1 = canonical_hyperliquid_shard_id(market, "2026-02-19", 20)
        sid2 = canonical_hyperliquid_shard_id(market, "2026-02-20", 20)
        seeded = {sid1: _shard_item(sid1)}  # second date missing

        repo = _make_repo(CanonicalShardRepository, "shards", seeded)
        result = repo.list_hyperliquid_window(
            market=market,
            start_date="2026-02-19",
            end_date="2026-02-20",
            depth=20,
        )
        assert result["2026-02-19"] is not None
        assert result["2026-02-20"] is None


class TestS3StoreListPrefix:
    def test_paginates_through_contents(self):
        client = MagicMock()
        paginator = MagicMock()
        paginator.paginate.return_value = iter([
            {"Contents": [{"Key": "a/1"}, {"Key": "a/2"}]},
            {"Contents": [{"Key": "a/3"}]},
            {"Contents": []},
        ])
        client.get_paginator.return_value = paginator

        store = S3Store.__new__(S3Store)
        store.region = "eu-west-1"
        store.bucket = "test-bucket"
        store.client = client

        keys = list(store.list_prefix("a/"))
        assert keys == ["a/1", "a/2", "a/3"]
        paginator.paginate.assert_called_once_with(Bucket="test-bucket", Prefix="a/")
