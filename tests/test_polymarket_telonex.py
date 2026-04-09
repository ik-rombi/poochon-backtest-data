from __future__ import annotations

import io
from pathlib import Path

import httpx
import orjson
import pyarrow as pa
import pyarrow.parquet as pq
import zstandard

from poochon_backtest_data.models import (
    CoverageRecord,
    CoverageStatus,
    DatasetKind,
    MarketType,
    PolymarketReplayCreateRequest,
    ReplayRequest,
    ReplayStatus,
    Venue,
    coverage_pk,
    new_pending_replay,
    polymarket_normalized_l2_s3_key,
    polymarket_normalized_trade_s3_key,
    polymarket_raw_l2_s3_key,
    polymarket_raw_trade_s3_key,
    replay_s3_key,
    utc_now_iso,
)
from poochon_backtest_data.polymarket_telonex import (
    backfill_market,
    ingest_market,
    normalize_market,
    resolve_market,
)
from poochon_backtest_data.service import materialize_replay


class FakeS3Store:
    def __init__(self):
        self.objects: dict[str, bytes] = {}

    def put_bytes(
        self,
        key: str,
        data: bytes,
        *,
        content_type: str | None = None,
        content_encoding: str | None = None,
    ) -> None:
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


class FakeReplayRepository:
    def __init__(self):
        self.items = {}

    def get(self, replay_id: str):
        return self.items.get(replay_id)

    def put(self, record):
        self.items[record.replay_id] = record


def parquet_bytes(rows: list[dict], schema: pa.Schema) -> bytes:
    table = pa.Table.from_pylist(rows, schema=schema)
    buffer = io.BytesIO()
    pq.write_table(table, buffer, compression="zstd")
    return buffer.getvalue()


def mock_client(book_by_date: dict[str, bytes], trades_by_date: dict[str, bytes]) -> httpx.Client:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/markets/slug/btc-updown-5m-1775181000":
            return httpx.Response(
                200,
                json={
                    "slug": "btc-updown-5m-1775181000",
                    "question": "Bitcoin Up or Down - April 2, 9:50PM-9:55PM ET",
                    "conditionId": "0xmarket",
                    "outcomes": '["Up","Down"]',
                    "clobTokenIds": '["asset-up","asset-down"]',
                    "startDate": "2026-04-02T01:58:12.365266Z",
                    "endDate": "2026-04-03T01:55:00Z",
                },
            )
        parts = request.url.path.strip("/").split("/")
        if parts[:3] == ["v1", "downloads", "polymarket"]:
            channel = parts[3]
            date = parts[4]
            params = dict(request.url.params)
            if params.get("market_id") != "0xmarket" or params.get("outcome") != "Up":
                return httpx.Response(400, json={"detail": "bad selector"})
            if channel == "book_snapshot_5":
                payload = book_by_date.get(date)
            elif channel == "trades":
                payload = trades_by_date.get(date)
            else:
                payload = None
            if payload is None:
                return httpx.Response(404, json={"detail": "missing"})
            return httpx.Response(200, content=payload)
        return httpx.Response(404, json={"detail": "not found"})

    return httpx.Client(transport=httpx.MockTransport(handler), base_url="https://test.local")


def test_resolve_market_maps_outcome_and_utc_dates() -> None:
    client = mock_client({}, {})
    resolution = resolve_market(
        PolymarketReplayCreateRequest(slug="btc-updown-5m-1775181000", outcome="Up"),
        client=client,
    )
    assert resolution.market_id == "0xmarket"
    assert resolution.asset_id == "asset-up"
    assert resolution.instrument == "btc-updown-5m-1775181000:Up"
    assert resolution.dates == ("2026-04-02", "2026-04-03")
    client.close()


def test_ingest_market_skips_existing_ready_raw_objects() -> None:
    store = FakeS3Store()
    coverage = FakeCoverageRepository()
    client = mock_client({}, {})
    resolution = resolve_market(
        PolymarketReplayCreateRequest(slug="btc-updown-5m-1775181000", outcome="Up"),
        client=client,
    )
    market = resolution.market_ref()

    for date in resolution.dates:
        l2_key = polymarket_raw_l2_s3_key(market, resolution.market_id, date)
        trade_key = polymarket_raw_trade_s3_key(market, resolution.market_id, date)
        store.put_bytes(l2_key, b"book")
        store.put_bytes(trade_key, b"trades")
        coverage.put(
            CoverageRecord(
                pk=coverage_pk(DatasetKind.RAW_L2, market, date, "daily"),
                dataset_kind=DatasetKind.RAW_L2,
                venue=market.venue,
                market_type=market.market_type,
                instrument=market.instrument,
                date=date,
                hour="daily",
                status=CoverageStatus.READY,
                object_count=1,
                byte_count=4,
                row_count=0,
                updated_at=utc_now_iso(),
                source="test",
            )
        )
        coverage.put(
            CoverageRecord(
                pk=coverage_pk(DatasetKind.RAW_TRADES, market, date, "daily"),
                dataset_kind=DatasetKind.RAW_TRADES,
                venue=market.venue,
                market_type=market.market_type,
                instrument=market.instrument,
                date=date,
                hour="daily",
                status=CoverageStatus.READY,
                object_count=1,
                byte_count=6,
                row_count=0,
                updated_at=utc_now_iso(),
                source="test",
            )
        )

    backfill_market(store, coverage, resolution=resolution, telonex_api_key="token", client=client)

    assert store.objects[polymarket_raw_l2_s3_key(market, resolution.market_id, "2026-04-02")] == b"book"
    assert store.objects[
        polymarket_raw_trade_s3_key(market, resolution.market_id, "2026-04-03")
    ] == b"trades"
    client.close()


def test_normalize_market_converts_book_and_trade_rows() -> None:
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
    store = FakeS3Store()
    coverage = FakeCoverageRepository()
    client = mock_client({}, {})
    resolution = resolve_market(
        PolymarketReplayCreateRequest(slug="btc-updown-5m-1775181000", outcome="Up"),
        client=client,
    )
    market = resolution.market_ref()
    date = "2026-04-03"
    empty_book = parquet_bytes([], book_schema)
    empty_trades = parquet_bytes([], trade_schema)
    store.put_bytes(
        polymarket_raw_l2_s3_key(market, resolution.market_id, "2026-04-02"),
        empty_book,
    )
    store.put_bytes(
        polymarket_raw_trade_s3_key(market, resolution.market_id, "2026-04-02"),
        empty_trades,
    )
    store.put_bytes(
        polymarket_raw_l2_s3_key(market, resolution.market_id, date),
        parquet_bytes(
            [
                {
                    "timestamp_us": 1775174406549000,
                    "local_timestamp_us": 1775174406557748,
                    "exchange": "polymarket",
                    "market_id": "0xmarket",
                    "slug": resolution.slug,
                    "asset_id": resolution.asset_id,
                    "outcome": resolution.outcome,
                    "bid_price_0": "0.5",
                    "bid_size_0": "78.06",
                    "bid_price_1": None,
                    "bid_size_1": None,
                    "bid_price_2": None,
                    "bid_size_2": None,
                    "bid_price_3": None,
                    "bid_size_3": None,
                    "bid_price_4": None,
                    "bid_size_4": None,
                    "ask_price_0": "0.51",
                    "ask_size_0": "175.14",
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
                    "timestamp_us": 1775180221877000,
                    "local_timestamp_us": 1775180221892230,
                    "exchange": "polymarket",
                    "market_id": "0xmarket",
                    "slug": resolution.slug,
                    "asset_id": resolution.asset_id,
                    "outcome": resolution.outcome,
                    "price": "0.5",
                    "size": "5",
                    "side": "sell",
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
    assert '"px":"0.5"' in l2_rows[0]["bids_json"]
    assert trade_rows[0]["side"] == "Sell"
    client.close()


def test_materialize_replay_merges_polymarket_dates_and_emits_venue_label() -> None:
    request = ReplayRequest(
        venue=Venue.POLYMARKET,
        market_type=MarketType.BINARY,
        instrument="btc-updown-5m-1775181000:Up",
        depth=3,
        slug="btc-updown-5m-1775181000",
        outcome="Up",
        market_id="0xmarket",
        asset_id="asset-up",
        dates=("2026-04-02", "2026-04-03"),
        start_ts_ms=1775174400000,
        end_ts_ms=1775181414000,
    )
    market = request.market_ref()
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
    s3 = FakeS3Store()
    coverage = FakeCoverageRepository()
    replay_repo = FakeReplayRepository()

    for date in request.dates:
        coverage.put(
            CoverageRecord(
                pk=coverage_pk(DatasetKind.NORMALIZED_L2, market, date, "daily"),
                dataset_kind=DatasetKind.NORMALIZED_L2,
                venue=market.venue,
                market_type=market.market_type,
                instrument=market.instrument,
                date=date,
                hour="daily",
                status=CoverageStatus.READY,
                object_count=1,
                byte_count=0,
                row_count=1,
                updated_at=utc_now_iso(),
                source="test",
            )
        )
        coverage.put(
            CoverageRecord(
                pk=coverage_pk(DatasetKind.NORMALIZED_TRADES, market, date, "daily"),
                dataset_kind=DatasetKind.NORMALIZED_TRADES,
                venue=market.venue,
                market_type=market.market_type,
                instrument=market.instrument,
                date=date,
                hour="daily",
                status=CoverageStatus.READY,
                object_count=1,
                byte_count=0,
                row_count=1,
                updated_at=utc_now_iso(),
                source="test",
            )
        )

    s3.objects[polymarket_normalized_l2_s3_key(market, "0xmarket", "2026-04-02")] = parquet_bytes(
        [
            {
                "ts_ms": 1775174406549,
                "instrument": request.instrument,
                "bids_json": '[{"px":"0.5","sz":"10","n":0}]',
                "asks_json": '[{"px":"0.51","sz":"11","n":0}]',
                "source_hour": 0,
                "source_line_number": 2,
            }
        ],
        l2_schema,
    )
    s3.objects[
        polymarket_normalized_trade_s3_key(market, "0xmarket", "2026-04-02")
    ] = parquet_bytes([], trade_schema)
    s3.objects[polymarket_normalized_l2_s3_key(market, "0xmarket", "2026-04-03")] = parquet_bytes(
        [
            {
                "ts_ms": 1775180221877,
                "instrument": request.instrument,
                "bids_json": '[{"px":"0.52","sz":"12","n":0},{"px":"0.51","sz":"13","n":0}]',
                "asks_json": '[{"px":"0.53","sz":"9","n":0},{"px":"0.54","sz":"8","n":0}]',
                "source_hour": 0,
                "source_line_number": 2,
            }
        ],
        l2_schema,
    )
    s3.objects[
        polymarket_normalized_trade_s3_key(market, "0xmarket", "2026-04-03")
    ] = parquet_bytes(
        [
            {
                "ts_ms": 1775180221877,
                "instrument": request.instrument,
                "side": "Buy",
                "px": 0.52,
                "sz": 5.0,
                "hash": "trade-2",
                "source_hour": 0,
                "source_line_number": 1,
            }
        ],
        trade_schema,
    )

    record = materialize_replay(
        request=request,
        s3_store=s3,
        coverage_repo=coverage,
        replay_repo=replay_repo,
    )

    with zstandard.ZstdDecompressor().stream_reader(io.BytesIO(s3.objects[replay_s3_key(request)])) as reader:
        payload = reader.read().decode("utf-8")
    lines = payload.strip().splitlines()
    first = orjson.loads(lines[0])
    second = orjson.loads(lines[1])
    assert record.status == ReplayStatus.READY
    assert record.event_count == 3
    assert first["Market"]["L2Snapshot"]["instrument"]["venue"] == "Polymarket"
    assert second["Market"]["Trade"]["instrument"]["symbol"] == request.instrument
    assert len(orjson.loads(lines[2])["Market"]["L2Snapshot"]["bids"]) == 2


def test_materialize_replay_reuses_existing_ready_polymarket_artifact() -> None:
    request = ReplayRequest(
        venue=Venue.POLYMARKET,
        market_type=MarketType.BINARY,
        instrument="btc-updown-5m-1775181000:Up",
        depth=5,
        slug="btc-updown-5m-1775181000",
        outcome="Up",
        market_id="0xmarket",
        asset_id="asset-up",
        dates=("2026-04-02", "2026-04-03"),
        start_ts_ms=1,
        end_ts_ms=2,
    )
    s3 = FakeS3Store()
    coverage = FakeCoverageRepository()
    replay_repo = FakeReplayRepository()
    market = request.market_ref()
    for date in request.dates:
        coverage.put(
            CoverageRecord(
                pk=coverage_pk(DatasetKind.NORMALIZED_L2, market, date, "daily"),
                dataset_kind=DatasetKind.NORMALIZED_L2,
                venue=market.venue,
                market_type=market.market_type,
                instrument=market.instrument,
                date=date,
                hour="daily",
                status=CoverageStatus.READY,
                object_count=1,
                byte_count=0,
                row_count=0,
                updated_at=utc_now_iso(),
                source="test",
            )
        )
        coverage.put(
            CoverageRecord(
                pk=coverage_pk(DatasetKind.NORMALIZED_TRADES, market, date, "daily"),
                dataset_kind=DatasetKind.NORMALIZED_TRADES,
                venue=market.venue,
                market_type=market.market_type,
                instrument=market.instrument,
                date=date,
                hour="daily",
                status=CoverageStatus.READY,
                object_count=1,
                byte_count=0,
                row_count=0,
                updated_at=utc_now_iso(),
                source="test",
            )
        )
    existing = new_pending_replay(request).model_copy(
        update={"status": ReplayStatus.READY, "event_count": 99, "updated_at": utc_now_iso()}
    )
    replay_repo.put(existing)
    s3.objects[replay_s3_key(request)] = b"compressed"

    result = materialize_replay(
        request=request,
        s3_store=s3,
        coverage_repo=coverage,
        replay_repo=replay_repo,
    )

    assert result.event_count == 99
