from __future__ import annotations

import io
from pathlib import Path

import httpx
import pyarrow as pa
import pyarrow.parquet as pq
import orjson
import zstandard

import poochon_backtest_data.polymarket_telonex as polymarket_telonex
from poochon_backtest_data.canonical import (
    build_polymarket_canonical_day,
    build_polymarket_canonical_day_from_storage,
)
from poochon_backtest_data.models import (
    CanonicalShardRecord,
    CoverageRecord,
    CoverageStatus,
    DatasetKind,
    OutcomesMode,
    PolymarketMarketResolution,
    PolymarketReplayCreateRequest,
    PolymarketSeriesSyncRequest,
    coverage_pk,
    polymarket_metadata_s3_key,
    polymarket_normalized_l2_s3_key,
    polymarket_normalized_trade_s3_key,
    polymarket_raw_l2_s3_key,
    polymarket_raw_trade_s3_key,
    utc_now_iso,
)
from poochon_backtest_data.polymarket_telonex import (
    backfill_market,
    discover_series_markets,
    normalize_market,
    resolve_market,
    sync_series,
)


class FakeS3Store:
    def __init__(self):
        self.objects: dict[str, bytes] = {}

    def put_bytes(self, key: str, data: bytes, *, content_type: str | None = None, content_encoding: str | None = None) -> None:
        self.objects[key] = data

    def put_json(self, key: str, payload: dict) -> None:
        self.objects[key] = orjson.dumps(payload)

    def put_file(self, key: str, path: str, *, content_type: str | None = None) -> None:
        self.objects[key] = Path(path).read_bytes()

    def get_bytes(self, key: str) -> bytes:
        return self.objects[key]

    def exists(self, key: str) -> bool:
        return key in self.objects


class FakeCoverageRepository:
    def __init__(self):
        self.items: dict[str, CoverageRecord] = {}

    def get(self, pk: str) -> CoverageRecord | None:
        return self.items.get(pk)

    def put(self, record: CoverageRecord) -> None:
        self.items[record.pk] = record


class FakeShardRepository:
    def __init__(self):
        self.items: dict[str, CanonicalShardRecord] = {}

    def get(self, shard_id: str):
        return self.items.get(shard_id)

    def put(self, record: CanonicalShardRecord) -> None:
        self.items[record.shard_id] = record


def parquet_bytes(rows: list[dict], schema: pa.Schema) -> bytes:
    table = pa.Table.from_pylist(rows, schema=schema)
    buffer = io.BytesIO()
    pq.write_table(table, buffer, compression="zstd")
    return buffer.getvalue()


def market_payload(
    slug: str,
    *,
    start_date: str = "2026-02-19T00:00:00Z",
    end_date: str = "2026-02-20T00:00:00Z",
) -> dict:
    return {
        "slug": slug,
        "question": "Bitcoin Up or Down - test",
        "conditionId": f"0x{slug}",
        "outcomes": '["Up","Down"]',
        "clobTokenIds": '["asset-up","asset-down"]',
        "startDate": start_date,
        "endDate": end_date,
    }


def mock_client(
    payloads: dict[str, dict],
    *,
    vatic: dict[tuple[str, int], dict] | None = None,
    binance: dict[tuple[str, int], list] | None = None,
) -> httpx.Client:
    vatic = vatic or {}
    binance = binance or {}

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.startswith("/markets/slug/"):
            slug = request.url.path.rsplit("/", 1)[-1]
            payload = payloads.get(slug)
            if payload is None:
                return httpx.Response(404, json={"detail": "not found"})
            return httpx.Response(200, json=payload)
        if request.url.path == "/api/v1/targets/timestamp":
            asset = str(request.url.params.get("asset"))
            timestamp = int(str(request.url.params.get("timestamp")))
            payload = vatic.get((asset, timestamp))
            if payload is None:
                return httpx.Response(404, json={"detail": "not found"})
            return httpx.Response(200, json=payload)
        if request.url.path.startswith("/api/v1/targets/"):
            timestamp = int(request.url.path.rsplit("/", 1)[-1])
            asset = str(request.url.params.get("asset"))
            payload = vatic.get((asset, timestamp))
            if payload is None:
                return httpx.Response(404, json={"detail": "not found"})
            return httpx.Response(200, json=payload)
        if request.url.path == "/api/v3/klines":
            symbol = str(request.url.params.get("symbol"))
            start_time = int(str(request.url.params.get("startTime")))
            payload = binance.get((symbol, start_time))
            if payload is None:
                return httpx.Response(404, json={"detail": "not found"})
            return httpx.Response(200, json=payload)
        return httpx.Response(404, json={"detail": "not found"})

    return httpx.Client(transport=httpx.MockTransport(handler), base_url="https://test.local")


def ready_coverage(dataset_kind: DatasetKind, resolution: PolymarketMarketResolution, date: str) -> CoverageRecord:
    market = resolution.market_ref()
    return CoverageRecord(
        pk=coverage_pk(dataset_kind, market, date, "daily"),
        dataset_kind=dataset_kind,
        venue=market.venue,
        market_type=market.market_type,
        instrument=market.instrument,
        date=date,
        hour="daily",
        status=CoverageStatus.READY,
        object_count=1,
        byte_count=1,
        row_count=1,
        updated_at=utc_now_iso(),
        source="test",
    )


def test_discover_series_markets_returns_both_outcomes(monkeypatch) -> None:
    monkeypatch.setattr(
        polymarket_telonex,
        "_iter_series_candidate_slugs",
        lambda series, start_date, end_date: [
            "btc-updown-5m-1771459200",
            "btc-updown-5m-1771459500",
        ],
    )
    client = mock_client(
        {
            "btc-updown-5m-1771459200": market_payload(
                "btc-updown-5m-1771459200",
                start_date="2026-02-18T00:08:38.273Z",
                end_date="2026-02-20T00:00:00Z",
            )
        },
        vatic={("btc", 1771459200): {"price": 66000.0}},
    )

    resolutions = discover_series_markets(
        PolymarketSeriesSyncRequest(
            series="btc-updown-5m",
            start_date="2026-02-19",
            end_date="2026-02-19",
        ),
        client=client,
    )

    assert {(item.slug, item.outcome) for item in resolutions} == {
        ("btc-updown-5m-1771459200", "Up"),
        ("btc-updown-5m-1771459200", "Down"),
    }
    assert all(item.price_to_beat == 66000.0 for item in resolutions)
    assert all(item.price_to_beat_source == "vatic" for item in resolutions)
    assert all(item.start_ts_ms == 1771459200000 for item in resolutions)
    assert all(item.end_ts_ms == 1771459500000 for item in resolutions)
    client.close()


def test_resolve_market_falls_back_to_binance_for_price_to_beat() -> None:
    client = mock_client(
        {"btc-updown-5m-1775181000": market_payload("btc-updown-5m-1775181000")},
        binance={("BTCUSDT", 1775181000000): [[1775181000000, "65999.5"]]},
    )

    resolution = resolve_market(
        PolymarketReplayCreateRequest(slug="btc-updown-5m-1775181000", outcome="Up"),
        client=client,
    )

    assert resolution.price_to_beat == 65999.5
    assert resolution.price_to_beat_source == "binance_open_1m"
    assert resolution.price_to_beat_quality == "proxy"
    client.close()


def test_resolve_market_uses_slug_window_for_contract_bounds() -> None:
    client = mock_client(
        {
            "btc-updown-5m-1771459200": market_payload(
                "btc-updown-5m-1771459200",
                start_date="2026-02-18T00:08:38.273Z",
                end_date="2026-02-20T00:00:00Z",
            )
        },
        vatic={("btc", 1771459200): {"price": 66000.0}},
    )

    resolution = resolve_market(
        PolymarketReplayCreateRequest(slug="btc-updown-5m-1771459200", outcome="Up"),
        client=client,
    )

    assert resolution.start_time == "2026-02-19T00:00:00+00:00"
    assert resolution.end_time == "2026-02-19T00:05:00+00:00"
    assert resolution.start_ts_ms == 1771459200000
    assert resolution.end_ts_ms == 1771459500000
    assert resolution.dates == ("2026-02-19",)
    assert resolution.price_to_beat == 66000.0
    assert resolution.price_to_beat_source == "vatic"
    client.close()


def test_normalize_market_converts_telonex_rows() -> None:
    store = FakeS3Store()
    coverage = FakeCoverageRepository()
    resolution = resolve_market(
        PolymarketReplayCreateRequest(slug="btc-updown-5m-1775181000", outcome="Up"),
        client=mock_client({"btc-updown-5m-1775181000": market_payload("btc-updown-5m-1775181000")}),
    )
    market = resolution.market_ref()
    date = resolution.dates[0]

    book_schema = pa.schema(
        [
            ("timestamp_us", pa.int64()),
            ("local_timestamp_us", pa.int64()),
            ("exchange", pa.string()),
            ("market_id", pa.string()),
            ("slug", pa.string()),
            ("asset_id", pa.string()),
            ("outcome", pa.string()),
            ("bid_price_0", pa.string()),
            ("bid_size_0", pa.string()),
            ("bid_price_1", pa.string()),
            ("bid_size_1", pa.string()),
            ("bid_price_2", pa.string()),
            ("bid_size_2", pa.string()),
            ("bid_price_3", pa.string()),
            ("bid_size_3", pa.string()),
            ("bid_price_4", pa.string()),
            ("bid_size_4", pa.string()),
            ("ask_price_0", pa.string()),
            ("ask_size_0", pa.string()),
            ("ask_price_1", pa.string()),
            ("ask_size_1", pa.string()),
            ("ask_price_2", pa.string()),
            ("ask_size_2", pa.string()),
            ("ask_price_3", pa.string()),
            ("ask_size_3", pa.string()),
            ("ask_price_4", pa.string()),
            ("ask_size_4", pa.string()),
        ]
    )
    trade_schema = pa.schema(
        [
            ("timestamp_us", pa.int64()),
            ("local_timestamp_us", pa.int64()),
            ("exchange", pa.string()),
            ("market_id", pa.string()),
            ("slug", pa.string()),
            ("asset_id", pa.string()),
            ("outcome", pa.string()),
            ("price", pa.string()),
            ("size", pa.string()),
            ("side", pa.string()),
            ("trade_id", pa.string()),
            ("origin_asset_id", pa.string()),
        ]
    )

    empty_book = parquet_bytes([], book_schema)
    empty_trades = parquet_bytes([], trade_schema)
    for day in resolution.dates:
        store.put_bytes(polymarket_raw_l2_s3_key(market, resolution.market_id, day), empty_book)
        store.put_bytes(polymarket_raw_trade_s3_key(market, resolution.market_id, day), empty_trades)

    store.put_bytes(
        polymarket_raw_l2_s3_key(market, resolution.market_id, date),
        parquet_bytes(
            [
                {
                    "timestamp_us": 1771459201000000,
                    "local_timestamp_us": 1771459201000001,
                    "exchange": "polymarket",
                    "market_id": resolution.market_id,
                    "slug": resolution.slug,
                    "asset_id": resolution.asset_id,
                    "outcome": resolution.outcome,
                    "bid_price_0": "0.49",
                    "bid_size_0": "10",
                    "bid_price_1": None,
                    "bid_size_1": None,
                    "bid_price_2": None,
                    "bid_size_2": None,
                    "bid_price_3": None,
                    "bid_size_3": None,
                    "bid_price_4": None,
                    "bid_size_4": None,
                    "ask_price_0": "0.51",
                    "ask_size_0": "11",
                    "ask_price_1": None,
                    "ask_size_1": None,
                    "ask_price_2": None,
                    "ask_size_2": None,
                    "ask_price_3": None,
                    "ask_size_3": None,
                    "ask_price_4": None,
                    "ask_size_4": None,
                }
            ],
            book_schema,
        ),
    )
    store.put_bytes(
        polymarket_raw_trade_s3_key(market, resolution.market_id, date),
        parquet_bytes(
            [
                {
                    "timestamp_us": 1771459201000000,
                    "local_timestamp_us": 1771459201000002,
                    "exchange": "polymarket",
                    "market_id": resolution.market_id,
                    "slug": resolution.slug,
                    "asset_id": resolution.asset_id,
                    "outcome": resolution.outcome,
                    "price": "0.5",
                    "size": "5",
                    "side": "buy",
                    "trade_id": "trade-1",
                    "origin_asset_id": "asset-down",
                }
            ],
            trade_schema,
        ),
    )

    normalize_market(store, coverage, resolution=resolution)

    l2_rows = pq.read_table(
        io.BytesIO(store.objects[polymarket_normalized_l2_s3_key(market, resolution.market_id, date)])
    ).to_pylist()
    trade_rows = pq.read_table(
        io.BytesIO(store.objects[polymarket_normalized_trade_s3_key(market, resolution.market_id, date)])
    ).to_pylist()
    assert l2_rows[0]["instrument"] == resolution.instrument
    assert '"px":"0.49"' in l2_rows[0]["bids_json"]
    assert trade_rows[0]["side"] == "Buy"


def test_backfill_market_writes_empty_parquet_when_telonex_day_is_missing() -> None:
    store = FakeS3Store()
    coverage = FakeCoverageRepository()
    resolution = resolve_market(
        PolymarketReplayCreateRequest(slug="btc-updown-5m-1775181000", outcome="Up"),
        client=mock_client({"btc-updown-5m-1775181000": market_payload("btc-updown-5m-1775181000")}),
    )
    market = resolution.market_ref()
    date = resolution.dates[0]

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(404, json={"detail": "not found"})

    client = httpx.Client(transport=httpx.MockTransport(handler), base_url="https://test.local")
    try:
        backfill_market(
            store,
            coverage,
            resolution=resolution,
            telonex_api_key="test-key",
            client=client,
        )
    finally:
        client.close()

    raw_l2_key = polymarket_raw_l2_s3_key(market, resolution.market_id, date)
    raw_trade_key = polymarket_raw_trade_s3_key(market, resolution.market_id, date)
    assert raw_l2_key in store.objects
    assert raw_trade_key in store.objects
    assert pq.read_table(io.BytesIO(store.objects[raw_l2_key])).num_rows == 0
    assert pq.read_table(io.BytesIO(store.objects[raw_trade_key])).num_rows == 0

    normalize_market(store, coverage, resolution=resolution)

    normalized_l2_key = polymarket_normalized_l2_s3_key(market, resolution.market_id, date)
    normalized_trade_key = polymarket_normalized_trade_s3_key(market, resolution.market_id, date)
    assert pq.read_table(io.BytesIO(store.objects[normalized_l2_key])).num_rows == 0
    assert pq.read_table(io.BytesIO(store.objects[normalized_trade_key])).num_rows == 0
    assert coverage.items[coverage_pk(DatasetKind.RAW_L2, market, date, "daily")].source.endswith(":empty")
    assert coverage.items[coverage_pk(DatasetKind.RAW_TRADES, market, date, "daily")].source.endswith(":empty")


def test_build_polymarket_canonical_day_merges_both_outcomes() -> None:
    up = PolymarketMarketResolution(
        slug="btc-updown-5m-1",
        question="q",
        outcome="Up",
        market_id="0xmarket",
        asset_id="asset-up",
        instrument="btc-updown-5m-1:Up",
        start_time="2026-02-19T00:00:00+00:00",
        end_time="2026-02-20T00:00:00+00:00",
        start_ts_ms=1771459200000,
        end_ts_ms=1771545600000,
        dates=("2026-02-19", "2026-02-20"),
        price_to_beat=66000.0,
        price_to_beat_source="vatic",
        price_to_beat_quality="exact",
    )
    down = up.model_copy(
        update={"outcome": "Down", "asset_id": "asset-down", "instrument": "btc-updown-5m-1:Down"}
    )
    date = "2026-02-19"
    store = FakeS3Store()
    coverage = FakeCoverageRepository()
    shard_repo = FakeShardRepository()

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

    for resolution in (up, down):
        market = resolution.market_ref()
        coverage.put(ready_coverage(DatasetKind.NORMALIZED_L2, resolution, date))
        coverage.put(ready_coverage(DatasetKind.NORMALIZED_TRADES, resolution, date))
        store.objects[polymarket_normalized_trade_s3_key(market, resolution.market_id, date)] = parquet_bytes(
            [
                {
                    "ts_ms": 1771459201000,
                    "instrument": resolution.instrument,
                    "side": "Buy",
                    "px": 0.5,
                    "sz": 10.0,
                    "hash": f"hash-{resolution.outcome}",
                    "source_hour": 0,
                    "source_line_number": 1,
                }
            ],
            trade_schema,
        )
        store.objects[polymarket_normalized_l2_s3_key(market, resolution.market_id, date)] = parquet_bytes(
            [
                {
                    "ts_ms": 1771459201000,
                    "instrument": resolution.instrument,
                    "bids_json": '[{"px":"0.49","sz":"10","n":0}]',
                    "asks_json": '[{"px":"0.51","sz":"11","n":0}]',
                    "source_hour": 0,
                    "source_line_number": 2,
                }
            ],
            l2_schema,
        )

    record = build_polymarket_canonical_day(
        date=date,
        series_key="btc-updown-5m",
        outcomes=OutcomesMode.BOTH,
        depth=5,
        resolutions=[up, down],
        s3_store=store,
        coverage_repo=coverage,
        shard_repo=shard_repo,
    )

    with zstandard.ZstdDecompressor().stream_reader(io.BytesIO(store.objects[record.shard_s3_key])) as reader:
        payload = reader.read().decode("utf-8").strip().splitlines()
    assert len(payload) == 5
    assert '"Contract"' in payload[0]
    assert '"ListedCurrent"' in payload[0]
    assert '"Trade"' in payload[1]
    assert any('"btc-updown-5m-1:Up"' in line for line in payload)
    assert any('"btc-updown-5m-1:Down"' in line for line in payload)


def test_build_polymarket_canonical_day_from_storage_scans_normalized_objects() -> None:
    up = PolymarketMarketResolution(
        slug="btc-updown-5m-1",
        question="q",
        outcome="Up",
        market_id="0xmarket",
        asset_id="asset-up",
        instrument="btc-updown-5m-1:Up",
        start_time="2026-02-19T00:00:00+00:00",
        end_time="2026-02-20T00:00:00+00:00",
        start_ts_ms=1771459200000,
        end_ts_ms=1771545600000,
        dates=("2026-02-19",),
        price_to_beat=66000.0,
        price_to_beat_source="vatic",
        price_to_beat_quality="exact",
    )
    down = up.model_copy(
        update={"outcome": "Down", "asset_id": "asset-down", "instrument": "btc-updown-5m-1:Down"}
    )
    date = "2026-02-19"
    store = FakeS3Store()
    shard_repo = FakeShardRepository()

    class FakePaginator:
        def __init__(self, objects):
            self.objects = objects

        def paginate(self, **kwargs):
            yield {"Contents": [{"Key": key} for key in self.objects if key.startswith(kwargs["Prefix"])]}

    class FakeS3Client:
        def __init__(self, objects):
            self.objects = objects

        def get_paginator(self, name: str):
            assert name == "list_objects_v2"
            return FakePaginator(self.objects)

    store.bucket = "test-bucket"
    store.client = FakeS3Client(store.objects)

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

    for resolution in (up, down):
        market = resolution.market_ref()
        store.put_json(polymarket_metadata_s3_key(resolution), resolution.model_dump(mode="json"))
        store.objects[polymarket_normalized_trade_s3_key(market, resolution.market_id, date)] = parquet_bytes(
            [
                {
                    "ts_ms": 1771459201000,
                    "instrument": resolution.instrument,
                    "side": "Buy",
                    "px": 0.5,
                    "sz": 10.0,
                    "hash": f"hash-{resolution.outcome}",
                    "source_hour": 0,
                    "source_line_number": 1,
                }
            ],
            trade_schema,
        )
        store.objects[polymarket_normalized_l2_s3_key(market, resolution.market_id, date)] = parquet_bytes(
            [
                {
                    "ts_ms": 1771459201000,
                    "instrument": resolution.instrument,
                    "bids_json": '[{"px":"0.49","sz":"10","n":0}]',
                    "asks_json": '[{"px":"0.51","sz":"11","n":0}]',
                    "source_hour": 0,
                    "source_line_number": 2,
                }
            ],
            l2_schema,
        )

    record = build_polymarket_canonical_day_from_storage(
        date=date,
        series_key="btc-updown-5m",
        outcomes=OutcomesMode.BOTH,
        depth=5,
        s3_store=store,
        shard_repo=shard_repo,
    )

    with zstandard.ZstdDecompressor().stream_reader(io.BytesIO(store.objects[record.shard_s3_key])) as reader:
        payload = reader.read().decode("utf-8").strip().splitlines()
    assert len(payload) == 5
    assert '"Contract"' in payload[0]
    assert any('"btc-updown-5m-1:Up"' in line for line in payload)
    assert any('"btc-updown-5m-1:Down"' in line for line in payload)


def test_build_polymarket_canonical_day_filters_to_current_and_next_contracts() -> None:
    date = "2026-02-19"
    store = FakeS3Store()
    coverage = FakeCoverageRepository()
    shard_repo = FakeShardRepository()

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

    resolutions = [
        PolymarketMarketResolution(
            slug=f"btc-updown-5m-{start}",
            question="q",
            outcome=outcome,
            market_id=f"0x{start}",
            asset_id=f"asset-{start}-{outcome.lower()}",
            instrument=f"btc-updown-5m-{start}:{outcome}",
            start_time="2026-02-19T00:00:00+00:00",
            end_time="2026-02-19T00:05:00+00:00",
            start_ts_ms=start * 1000,
            end_ts_ms=(start + 300) * 1000,
            dates=(date,),
            price_to_beat=66000.0,
            price_to_beat_source="vatic",
            price_to_beat_quality="exact",
        )
        for start in (1771459200, 1771459500, 1771459800)
        for outcome in ("Up", "Down")
    ]

    for index, resolution in enumerate(resolutions, start=1):
        market = resolution.market_ref()
        coverage.put(ready_coverage(DatasetKind.NORMALIZED_L2, resolution, date))
        coverage.put(ready_coverage(DatasetKind.NORMALIZED_TRADES, resolution, date))
        store.objects[polymarket_normalized_trade_s3_key(market, resolution.market_id, date)] = parquet_bytes(
            [
                {
                    "ts_ms": 1771459210000,
                    "instrument": resolution.instrument,
                    "side": "Buy",
                    "px": 0.5,
                    "sz": 10.0,
                    "hash": f"hash-{index}",
                    "source_hour": 0,
                    "source_line_number": 1,
                },
                *(
                    [
                        {
                            "ts_ms": 1771459801000,
                            "instrument": resolution.instrument,
                            "side": "Buy",
                            "px": 0.99,
                            "sz": 1.0,
                            "hash": f"late-{index}",
                            "source_hour": 0,
                            "source_line_number": 3,
                        }
                    ]
                    if resolution.slug == "btc-updown-5m-1771459200"
                    else []
                ),
            ],
            trade_schema,
        )
        store.objects[polymarket_normalized_l2_s3_key(market, resolution.market_id, date)] = parquet_bytes(
            [
                {
                    "ts_ms": 1771459210000,
                    "instrument": resolution.instrument,
                    "bids_json": '[{"px":"0.49","sz":"10","n":0}]',
                    "asks_json": '[{"px":"0.51","sz":"11","n":0}]',
                    "source_hour": 0,
                    "source_line_number": 2,
                }
            ],
            l2_schema,
        )

    record = build_polymarket_canonical_day(
        date=date,
        series_key="btc-updown-5m",
        outcomes=OutcomesMode.BOTH,
        depth=5,
        resolutions=resolutions,
        s3_store=store,
        coverage_repo=coverage,
        shard_repo=shard_repo,
    )

    with zstandard.ZstdDecompressor().stream_reader(io.BytesIO(store.objects[record.shard_s3_key])) as reader:
        payload = reader.read().decode("utf-8")

    assert "btc-updown-5m-1771459200:Up" in payload
    assert "btc-updown-5m-1771459500:Up" in payload
    assert "btc-updown-5m-1771459800:Up" not in payload
    assert "\"px\":0.99" not in payload


def test_build_polymarket_canonical_day_from_storage_falls_back_without_metadata() -> None:
    date = "2026-02-19"
    up = PolymarketMarketResolution(
        slug="btc-updown-5m-1771459200",
        question="q",
        outcome="Up",
        market_id="0xmarket",
        asset_id="asset-up",
        instrument="btc-updown-5m-1771459200:Up",
        start_time="2026-02-19T00:00:00+00:00",
        end_time="2026-02-19T00:05:00+00:00",
        start_ts_ms=1771459200000,
        end_ts_ms=1771459500000,
        dates=(date,),
    )
    down = up.model_copy(
        update={
            "outcome": "Down",
            "asset_id": "asset-down",
            "instrument": "btc-updown-5m-1771459200:Down",
        }
    )
    store = FakeS3Store()
    shard_repo = FakeShardRepository()

    class FakePaginator:
        def __init__(self, objects):
            self.objects = objects

        def paginate(self, **kwargs):
            yield {"Contents": [{"Key": key} for key in self.objects if key.startswith(kwargs["Prefix"])]}

    class FakeS3Client:
        def __init__(self, objects):
            self.objects = objects

        def get_paginator(self, name: str):
            assert name == "list_objects_v2"
            return FakePaginator(self.objects)

    store.bucket = "test-bucket"
    store.client = FakeS3Client(store.objects)

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

    for resolution in (up, down):
        market = resolution.market_ref()
        store.objects[polymarket_normalized_trade_s3_key(market, resolution.market_id, date)] = parquet_bytes(
            [
                {
                    "ts_ms": 1771459201000,
                    "instrument": resolution.instrument,
                    "side": "Buy",
                    "px": 0.5,
                    "sz": 10.0,
                    "hash": f"hash-{resolution.outcome}",
                    "source_hour": 0,
                    "source_line_number": 1,
                }
            ],
            trade_schema,
        )
        store.objects[polymarket_normalized_l2_s3_key(market, resolution.market_id, date)] = parquet_bytes(
            [
                {
                    "ts_ms": 1771459201000,
                    "instrument": resolution.instrument,
                    "bids_json": '[{"px":"0.49","sz":"10","n":0}]',
                    "asks_json": '[{"px":"0.51","sz":"11","n":0}]',
                    "source_hour": 0,
                    "source_line_number": 2,
                }
            ],
            l2_schema,
        )

    record = build_polymarket_canonical_day_from_storage(
        date=date,
        series_key="btc-updown-5m",
        outcomes=OutcomesMode.BOTH,
        depth=5,
        s3_store=store,
        shard_repo=shard_repo,
    )

    with zstandard.ZstdDecompressor().stream_reader(io.BytesIO(store.objects[record.shard_s3_key])) as reader:
        payload = reader.read().decode("utf-8").strip().splitlines()
    assert payload
    assert '"Contract"' in payload[0]


def test_sync_series_clips_market_dates_to_requested_window(monkeypatch) -> None:
    resolution = PolymarketMarketResolution(
        slug="btc-updown-5m-1",
        question="q",
        outcome="Up",
        market_id="0xmarket",
        asset_id="asset-up",
        instrument="btc-updown-5m-1:Up",
        start_time="2026-02-18T10:00:00+00:00",
        end_time="2026-02-19T10:00:00+00:00",
        start_ts_ms=1771418400000,
        end_ts_ms=1771504800000,
        dates=("2026-02-18", "2026-02-19"),
    )
    seen_backfill_dates: list[tuple[str, ...]] = []
    seen_normalize_dates: list[tuple[str, ...]] = []
    built_dates: list[str] = []

    monkeypatch.setattr(
        polymarket_telonex,
        "discover_series_markets",
        lambda request, client=None: [resolution],
    )

    def fake_backfill(*args, **kwargs):
        seen_backfill_dates.append(kwargs["resolution"].dates)

    def fake_normalize(*args, **kwargs):
        seen_normalize_dates.append(kwargs["resolution"].dates)

    def fake_build(*, date, **kwargs):
        built_dates.append(date)

    monkeypatch.setattr(polymarket_telonex, "backfill_market", fake_backfill)
    monkeypatch.setattr(polymarket_telonex, "normalize_market", fake_normalize)
    monkeypatch.setattr(polymarket_telonex, "build_polymarket_canonical_day_from_storage", fake_build)

    returned = sync_series(
        FakeS3Store(),
        FakeCoverageRepository(),
        FakeShardRepository(),
        request=PolymarketSeriesSyncRequest(
            series="btc-updown-5m",
            start_date="2026-02-19",
            end_date="2026-02-19",
        ),
        telonex_api_key="test-key",
        client=httpx.Client(transport=httpx.MockTransport(lambda request: httpx.Response(500))),
    )

    assert seen_backfill_dates == [("2026-02-19",)]
    assert seen_normalize_dates == [("2026-02-19",)]
    assert built_dates == ["2026-02-19"]
    assert returned[0].dates == ("2026-02-19",)
