# poochon-backtest-data

AWS-backed historical ingestion and canonical replay materialization for `../bitchon/bot`.

This repo owns the data plane that turns venue/provider data into deterministic replay streams:

1. discover markets/contracts
2. copy or download raw venue data into S3
3. normalize raw data into a stable parquet schema
4. materialize canonical replay shards as typed Parquet families plus shard manifests
5. serve replay window manifests and shard file downloads to consumers

## Stack Layout

- `infra/core`
  - persistent storage
  - S3 bucket for raw, normalized, metadata, replay, and canonical artifacts
  - DynamoDB tables for coverage, replay records, and canonical shard records
- `infra/runtime`
  - ephemeral compute/networking
  - ECS, Step Functions, ALB, networking, log groups, secrets, and task definitions
  - safe to destroy after refresh jobs finish
- `src/poochon_backtest_data`
  - ingestion, normalization, canonical build, API, and CLI entrypoints

Current dev stack defaults:

- region: `eu-west-1`
- core stack: persistent
- runtime stack: ephemeral

## End-To-End Flow

```text
Provider / Venue data
  -> raw S3 objects
  -> normalized parquet
  -> canonical shard (manifest.json + *.parquet families)
  -> canonical window manifest
  -> Parquet family download consumed by bitchon ReplaySource
```

There are two venue families today:

- Hyperliquid
  - raw source: Hyperliquid public archive buckets
  - normalized units: hourly L2 and trade parquet
  - canonical units: daily shard per instrument/date/depth
- Polymarket
  - market discovery: Gamma
  - historical raw source: Telonex parquet downloads
  - price-to-beat enrichment: Vatic first, Binance 1-minute open fallback
  - normalized units: per-market parquet keyed by `market_id`
  - canonical units: daily shard per `series_key`/date/outcomes/depth

## Persistent State

### DynamoDB

- Coverage table
  - keyed by `pk`
  - tracks whether raw or normalized inputs are ready for a given venue / market / date / hour
  - canonical key format:
    - `dataset_kind#venue#market_type#instrument#date#hour`
- Replay table
  - keyed by `replay_id`
  - tracks ad hoc replay builds
- Replay shard table
  - keyed by `shard_id`
  - tracks canonical shard manifests already materialized in S3

### S3 Families

- Hyperliquid raw
  - `raw/hyperliquid/l2book/market_type=<...>/date=<YYYY-MM-DD>/hour=<HH>/instrument=<instrument>/<instrument>.lz4`
  - `raw/hyperliquid/node_fills_by_block/market_type=<...>/date=<YYYY-MM-DD>/hour=<HH>/instrument=<instrument>/part-<HH>.lz4`
- Polymarket raw
  - `raw/telonex/polymarket/channel=book_snapshot_5/market_id=<market_id>/instrument=<instrument>/date=<YYYY-MM-DD>/part-000.parquet`
  - `raw/telonex/polymarket/channel=trades/market_id=<market_id>/instrument=<instrument>/date=<YYYY-MM-DD>/part-000.parquet`
- Polymarket metadata
  - `metadata/polymarket/market_id=<market_id>/instrument=<instrument>/manifest.json`
- Hyperliquid normalized
  - `normalized/hyperliquid/l2_snapshot/market_type=<...>/date=<YYYY-MM-DD>/hour=<HH>/instrument=<instrument>/part-000.parquet`
  - `normalized/hyperliquid/trade/market_type=<...>/date=<YYYY-MM-DD>/hour=<HH>/instrument=<instrument>/part-000.parquet`
- Polymarket normalized
  - `normalized/polymarket/kind=l2_snapshot/market_id=<market_id>/instrument=<instrument>/date=<YYYY-MM-DD>/part-000.parquet`
  - `normalized/polymarket/kind=trade/market_id=<market_id>/instrument=<instrument>/date=<YYYY-MM-DD>/part-000.parquet`
- Canonical replay shards
  - Hyperliquid:
    - `canonical/hyperliquid/market_type=<...>/instrument=<instrument>/date=<YYYY-MM-DD>/depth=<N>/trades.parquet`
    - `canonical/hyperliquid/market_type=<...>/instrument=<instrument>/date=<YYYY-MM-DD>/depth=<N>/books.parquet`
    - `canonical/hyperliquid/market_type=<...>/instrument=<instrument>/date=<YYYY-MM-DD>/depth=<N>/manifest.json`
  - Polymarket:
    - `canonical/polymarket/series=<series_key>/outcomes=<mode>/date=<YYYY-MM-DD>/depth=<N>/contracts.parquet`
    - `canonical/polymarket/series=<series_key>/outcomes=<mode>/date=<YYYY-MM-DD>/depth=<N>/trades.parquet`
    - `canonical/polymarket/series=<series_key>/outcomes=<mode>/date=<YYYY-MM-DD>/depth=<N>/books.parquet`
    - `canonical/polymarket/series=<series_key>/outcomes=<mode>/date=<YYYY-MM-DD>/depth=<N>/manifest.json`
- Replay artifacts
  - `replays/.../events.jsonl.zst`
  - `replays/.../manifest.json`

## Data Structures

### Raw Stage

Raw objects are provider-native payloads. This stage preserves source fidelity and keeps provider-specific parsing isolated from the normalized schema.

#### Hyperliquid Raw

- L2 raw
  - compressed NDJSON copied from the Hyperliquid archive bucket
  - parser reads `raw.data.time`, `raw.data.coin`, and `raw.data.levels`
- Fill/trade raw
  - compressed NDJSON copied from `node_fills_by_block`
  - parser groups duplicate fills by `(coin, time, tid, hash, px, sz)` and picks a canonical fill row

Hyperliquid raw is intentionally not restated as a stable schema in this repo because it mirrors the upstream archive format directly.

#### Polymarket Raw

Polymarket raw data is parquet from Telonex with fixed schemas.

`book_snapshot_5` columns:

```text
timestamp_us
local_timestamp_us
exchange
market_id
slug
asset_id
outcome
bid_price_0..4
bid_size_0..4
ask_price_0..4
ask_size_0..4
```

`trades` columns:

```text
timestamp_us
local_timestamp_us
exchange
market_id
slug
asset_id
outcome
price
size
side
trade_id
origin_asset_id
```

#### Polymarket Metadata Manifest

Each discovered outcome contract is also stored as a JSON manifest. The persisted fields are:

```json
{
  "venue": "polymarket",
  "market_type": "binary",
  "slug": "btc-updown-5m-1771459200",
  "question": "Bitcoin Up or Down - ...",
  "outcome": "Up",
  "market_id": "0x...",
  "asset_id": "9936...",
  "instrument": "btc-updown-5m-1771459200:Up",
  "start_time": "2026-02-19T00:00:00+00:00",
  "end_time": "2026-02-19T00:05:00+00:00",
  "start_ts_ms": 1771459200000,
  "end_ts_ms": 1771459500000,
  "dates": ["2026-02-19"],
  "price_to_beat": 66461.0,
  "price_to_beat_source": "binance_us_open_1m",
  "price_to_beat_quality": "proxy"
}
```

Notes:

- `series_key` is derived from `slug` by trimming the trailing timestamp segment.
- `start_ts_ms` and `end_ts_ms` are contract-window timestamps, not Gamma listing timestamps.
- `price_to_beat_source`
  - `vatic` means exact source
  - `binance_open_1m` means proxy fallback from Binance global
  - `binance_us_open_1m` means proxy fallback from Binance US
- `price_to_beat_quality`
  - `exact`
  - `proxy`

### Normalized Stage

Normalized parquet is the stable intermediate contract used by canonical builders.

There are only two normalized row shapes across venues.

#### Normalized L2 Snapshot

```text
ts_ms
instrument
bids_json
asks_json
source_hour
source_line_number
```

Semantics:

- `ts_ms`
  - event timestamp in milliseconds
- `instrument`
  - venue-specific symbol string
  - examples:
    - Hyperliquid: `BTC`
    - Polymarket: `btc-updown-5m-1771459200:Up`
- `bids_json` / `asks_json`
  - JSON-encoded arrays of levels
  - each level is `{ "px": "...", "sz": "...", "n": <count> }`
- `source_hour`
  - original hourly partition for Hyperliquid
  - `0` for current Polymarket daily objects
- `source_line_number`
  - stable tie-breaker used during canonical merge

#### Normalized Trade

```text
ts_ms
instrument
side
px
sz
hash
source_hour
source_line_number
```

Semantics:

- `side`
  - `Buy` or `Sell`
- `px`
  - numeric price
- `sz`
  - numeric size
- `hash`
  - source trade hash or trade id
- `source_hour` and `source_line_number`
  - deterministic merge ordering metadata

### Canonical Replay Stage

Canonical replay is a manifest-rooted Parquet dataset. This is the only historical format consumed by `../bitchon/bot` replay and backtest flows.

Each shard emits up to three typed family files.

#### `trades.parquet`

```json
{
  "event_seq": 17,
  "ts_ms": 1771459201234,
  "instrument": "btc-updown-5m-1771459200:Up",
  "px": 0.5,
  "sz": 10.0,
  "side": "Buy"
}
```

#### `books.parquet`

```json
{
  "event_seq": 18,
  "ts_ms": 1771459201234,
  "instrument": "btc-updown-5m-1771459200:Up",
  "bid_px_0": 0.49,
  "bid_sz_0": 10.0,
  "bid_level_count_0": 0,
  "ask_px_0": 0.51,
  "ask_sz_0": 11.0,
  "ask_level_count_0": 0
}
```

Bid and ask columns are flattened per depth level and truncated to the requested canonical depth when the shard is built.

#### `contracts.parquet`

```json
{
  "event_seq": 1,
  "ts_ms": 1771459201000,
  "kind": "ListedCurrent",
  "series_key": "btc-updown-5m",
  "slug": "btc-updown-5m-1771459200",
  "market_id": "0x...",
  "start_ts_ms": 1771459200000,
  "end_ts_ms": 1771459500000,
  "price_to_beat": 66461.0,
  "price_to_beat_source": "binance_us_open_1m",
  "price_to_beat_quality": "proxy",
  "outcome_0": "Down",
  "outcome_0_asset_id": "8059...",
  "outcome_0_instrument": "btc-updown-5m-1771459200:Down",
  "outcome_1": "Up",
  "outcome_1_asset_id": "9936...",
  "outcome_1_instrument": "btc-updown-5m-1771459200:Up"
}
```

Contract lifecycle kinds:

- `ListedCurrent`
  - current active contract became visible to the stream
- `ListedNext`
  - next adjacent contract became visible to the stream
- `Activated`
  - previous next contract rolled into current
- `Resolved`
  - previous current contract rolled out of the stream

### Polymarket Replay Contract Rules

- replay exposes only the current contract and the next contract for a `series_key`
- `series_key` is the slug family, for example `btc-updown-5m`
- live and canonical replay are expected to follow the same lifecycle model
- canonical builders derive the next contract by looking for the contract whose `start_ts_ms` is exactly one interval after the current contract

### Ordering Rules

Canonical merge order is deterministic:

1. `ts_ms` ascending
2. trade rows before L2 rows when timestamps are equal
3. source stream order
4. `source_line_number`

That is why normalized rows carry `source_hour` and `source_line_number`.

## CLI And Operational Flow

The local CLI is enough to operate against the AWS-backed storage layer. Runtime infrastructure is only needed when you want the managed ECS/Step Functions execution path.

Required environment for local AWS-backed runs:

```bash
export POOCHON_AWS_REGION=eu-west-1
export POOCHON_DATA_BUCKET=poochon-backtest-data-778822980471-eu-west-1-dev
export POOCHON_COVERAGE_TABLE_NAME=poochon-backtest-data-coverage-dev
export POOCHON_REPLAY_TABLE_NAME=poochon-backtest-data-replays-dev
export POOCHON_SHARD_TABLE_NAME=poochon-backtest-data-replay-shards-dev
export POOCHON_TELONEX_API_KEY=...
```

### Refresh Polymarket Data And Canonical Replay

```bash
uv run poochon-backtest-data polymarket-sync-series \
  --series btc-updown-5m \
  --start-date 2026-02-19 \
  --end-date 2026-02-21 \
  --outcomes both \
  --depth 5
```

This command:

- discovers the Polymarket contracts for the window
- overwrites metadata manifests for discovered outcomes
- downloads any missing raw Telonex parquet
- normalizes any missing per-market parquet
- force-rebuilds the canonical daily shards for the requested dates

If normalized inputs already exist and only canonical output is stale, rebuild only the canonical stage:

```bash
uv run poochon-backtest-data polymarket-build-canonical-window \
  --series btc-updown-5m \
  --start-date 2026-02-19 \
  --end-date 2026-02-21 \
  --outcomes both \
  --depth 5 \
  --force
```

### Refresh Hyperliquid Data

```bash
uv run poochon-backtest-data hyperliquid-sync-window \
  --market-type perp \
  --instrument BTC \
  --start-date 2026-02-19 \
  --end-date 2026-02-21 \
  --depth 20
```

If normalized Hyperliquid inputs already exist and only canonical output is stale, rebuild only the canonical stage:

```bash
uv run poochon-backtest-data hyperliquid-build-canonical-window \
  --market-type perp \
  --instrument BTC \
  --start-date 2026-02-19 \
  --end-date 2026-02-21 \
  --depth 20 \
  --force
```

### Runtime Stack Lifecycle

Bring runtime up only when you want the AWS-managed execution path:

```bash
cd infra/runtime
PULUMI_PYTHON_CMD=./.venv/bin/python pulumi up --stack dev
```

Destroy runtime when work is done:

```bash
cd infra/runtime
PULUMI_PYTHON_CMD=./.venv/bin/python pulumi destroy --yes --stack dev
```

Destroying `infra/runtime` should remove ephemeral AWS resources only:

- VPC / subnets / route tables / security groups
- ECS cluster / task definitions / services
- ALB / target groups / listeners
- ECR repo
- runtime log groups
- runtime secrets
- Step Functions and schedules

It must not remove `infra/core` storage:

- S3 bucket
- coverage table
- replay table
- replay-shard table

## Consumer Contract

`../bitchon/bot` should treat canonical replay as the stable integration boundary.

- raw provider payloads are not a consumer contract
- normalized parquet is an internal data-plane contract
- canonical replay manifest + Parquet families are the consumer-facing replay contract

If replay behavior changes, update this README first and keep `bitchon` live/replay semantics aligned.
