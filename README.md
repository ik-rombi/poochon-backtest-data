# poochon-backtest-data

AWS-backed historical ingestion and canonical replay materialization for `../bitchon/bot`.

This repo owns the data plane that turns venue/provider data into deterministic replay streams:

1. discover markets/contracts
2. copy or download raw venue data into S3
3. normalize raw data into a stable parquet schema
4. materialize canonical replay shards as typed Parquet families plus shard manifests
5. serve replay window manifests and shard file downloads to consumers

## Stack Layout

Four Pulumi stacks, each with a distinct lifecycle:

- `infra/core`
  - persistent storage; rarely touched
  - S3 bucket for raw, normalized, metadata, replay, and canonical artifacts
  - DynamoDB tables for coverage, replay records, and canonical shard records
- `infra/shared`
  - shared compute/networking plumbing; ~$0 idle
  - VPC + subnets + IGW, ECS cluster, CloudWatch log group, ECR repo and Docker image, IAM execution/task roles
- `infra/write`
  - ingestion control plane; ~$0 idle (Fargate is pay-per-run)
  - Sync task definition, Step Functions state machine, IAM role for Step Functions, optional EventBridge scheduler, optional Telonex secret
- `infra/read`
  - consumer-facing FastAPI; ~$30/mo when up (ALB + 1 Fargate task)
  - API task definition, ECS service, ALB + target group + listener, ALB security group
  - **destroy this stack independently when you don't need the API** — `infra/write` keeps working

Dependency order: `core → shared → (write, read in parallel)`.

`src/poochon_backtest_data/` holds the ingestion, normalization, canonical build, API, and CLI entrypoints. The same CLI binary runs both on your laptop (for local `run`) and inside Fargate (launched by the state machine).

Current dev stack defaults:

- region: `eu-west-1`
- core stack: persistent
- shared stack: persistent (effectively — kept up because destroy cost = ECR image rebuild)
- write stack: persistent (~$0 idle)
- read stack: bring up only when serving consumers

## Separating write and read

The split exists so that turning off the read API doesn't disturb ingestion:

```bash
# bring up ingestion control plane only
cd infra/shared && pulumi up --stack dev
cd infra/write  && pulumi up --stack dev

# ingest stays live; submit jobs via CLI or state machine directly
poochon-backtest-data submit hyperliquid --instrument BTC --market-type perp \
  --start-date 2026-02-19 --end-date 2026-02-19 --depth 20

# later — bring up the read side
cd infra/read && pulumi up --stack dev
# … use it …
cd infra/read && pulumi destroy --stack dev  # stops ALB/API billing
```

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

#### Raw Object Keying

Raw keys are composed from the minimum dimensions needed to make each object uniquely addressable and idempotent to re-copy. The dimensions differ per venue because upstream cadence and fan-out differ.

##### Hyperliquid

Hourly-partitioned, scoped to an instrument:

| Family  | Key dimensions                              | Destination key                                                                                                         |
| ------- | ------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------- |
| L2 book | `market_type`, `date`, `hour`, `instrument` | `raw/hyperliquid/l2book/market_type=<…>/date=<YYYY-MM-DD>/hour=<HH>/instrument=<instrument>/<instrument>.lz4`            |
| Fills   | `market_type`, `date`, `hour`, `instrument` | `raw/hyperliquid/node_fills_by_block/market_type=<…>/date=<YYYY-MM-DD>/hour=<HH>/instrument=<instrument>/part-<HH>.lz4` |

Notes:

- Sources are requester-pays buckets: `s3://hyperliquid-archive/market_data/YYYYMMDD/<hour>/l2Book/<instrument>.lz4` (L2) and `s3://hl-mainnet-node-data/node_fills_by_block/hourly/YYYYMMDD/<hour>.lz4` (fills).
- L2 upstream is already instrument-scoped; one upstream object maps to one destination key.
- Fills upstream is **not** instrument-scoped — it is one `.lz4` per hour covering the whole venue. We still store it under an `instrument=<instrument>` prefix, so the same upstream object is copied once per ingested instrument. Filtering to `coin == <instrument>` happens at normalize time.
- `market_type` comes from `MarketRef.market_type` (e.g. `perp`).
- `instrument` is URL-encoded via `MarketRef.encoded_instrument()`.
- `date` is the UTC calendar day; `hour` is the zero-padded UTC hour.

##### Polymarket

Daily-partitioned, scoped to a single outcome contract:

| Family   | Key dimensions                       | Destination key                                                                                                                     |
| -------- | ------------------------------------ | ----------------------------------------------------------------------------------------------------------------------------------- |
| Book     | `market_id`, `instrument`, `date`    | `raw/telonex/polymarket/channel=book_snapshot_5/market_id=<market_id>/instrument=<instrument>/date=<YYYY-MM-DD>/part-000.parquet`   |
| Trades   | `market_id`, `instrument`, `date`    | `raw/telonex/polymarket/channel=trades/market_id=<market_id>/instrument=<instrument>/date=<YYYY-MM-DD>/part-000.parquet`            |
| Metadata | `market_id`, `instrument`            | `metadata/polymarket/market_id=<market_id>/instrument=<instrument>/manifest.json`                                                   |

Notes:

- Source: Telonex REST `GET /<channel>/<date>?market_id=<market_id>&outcome=<outcome>`, Bearer-authed with `POOCHON_TELONEX_API_KEY`.
- Partitioning is daily because Telonex returns one parquet per `(market_id, outcome, date)`.
- `market_id` is the on-chain hex address; `instrument` is `<slug>:<outcome>` (e.g. `btc-updown-5m-1771459200:Up`). Both are URL-encoded.
- Both `market_id` and `instrument` appear in the key even though the former implies the latter — listings stay scoped either way, and raw/normalized key shapes stay symmetric.
- A 404 from Telonex is written as an **empty parquet** at the canonical key so downstream stages always see a uniform input shape.

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

The CLI is an operator console for the pipeline — it executes stages locally, submits jobs to AWS, inspects what data exists, and reports stack status. Everything talks directly to S3 / DynamoDB / Step Functions; no local server required.

Required environment:

```bash
export POOCHON_AWS_REGION=eu-west-1
export POOCHON_DATA_BUCKET=poochon-backtest-data-778822980471-eu-west-1-dev
export POOCHON_COVERAGE_TABLE_NAME=poochon-backtest-data-coverage-dev
export POOCHON_SHARD_TABLE_NAME=poochon-backtest-data-replay-shards-dev
export POOCHON_TELONEX_API_KEY=...  # only for polymarket
```

### Command tree

```
poochon-backtest-data
├── api                             # serve FastAPI (consumer read API)
├── infra
│   └── status                      # UP/DOWN per Pulumi stack + key outputs
├── data
│   ├── inventory                   # expected/ready/failed/missing per stage
│   └── coverage                    # dump DDB coverage rows by pk prefix
├── run                             # execute a stage LOCALLY (blocks)
│   ├── hyperliquid {raw|normalize|canonical|all}
│   └── polymarket  {discover|raw|normalize|canonical|all}
├── submit                          # start a Step Functions execution on AWS
│   ├── hyperliquid
│   └── polymarket
└── job                             # track AWS executions
    ├── list
    ├── status <execution-arn>
    └── logs <execution-arn> [--follow]
```

The legacy flat commands (`hyperliquid-sync-window`, `polymarket-sync-series`, etc.) still work as deprecated aliases so existing Step Functions state machines keep running during the transition.

### Canonical walkthrough

```bash
# 1. Where are we?
poochon-backtest-data infra status

# 2. Run a single stage locally — idempotent, so re-running is a no-op.
poochon-backtest-data run hyperliquid raw \
  --market-type perp --instrument BTC \
  --start-date 2026-02-19 --end-date 2026-02-19

# 3. Check what landed.
poochon-backtest-data data inventory \
  --venue hyperliquid --market-type perp --instrument BTC \
  --start-date 2026-02-19 --end-date 2026-02-19 --depth 20

# 4. Full stream, same window.
poochon-backtest-data run hyperliquid all \
  --market-type perp --instrument BTC \
  --start-date 2026-02-19 --end-date 2026-02-19 --depth 20

# 5. Same thing on AWS instead of your laptop.
poochon-backtest-data submit hyperliquid \
  --market-type perp --instrument BTC \
  --start-date 2026-02-19 --end-date 2026-02-19 --depth 20
# → prints execution ARN

# 6. Track it.
poochon-backtest-data job status <execution-arn>
poochon-backtest-data job logs <execution-arn> --follow
```

### Refreshing Polymarket

```bash
poochon-backtest-data run polymarket all \
  --series btc-updown-5m \
  --start-date 2026-02-19 --end-date 2026-02-21 \
  --outcomes both --depth 5
```

This discovers contracts, writes metadata manifests, downloads missing raw Telonex parquet, normalizes per-market parquet, and force-rebuilds the canonical daily shards. Every sub-stage is individually callable (`run polymarket discover`, `run polymarket raw`, etc.) if you want to isolate a step.

### Stack Lifecycle

Bring the ingestion path up (one-time):

```bash
cd infra/shared && pulumi up --stack dev
cd infra/write  && pulumi up --stack dev
```

Bring the read API up (only when needed):

```bash
cd infra/read && pulumi up --stack dev
```

Take the read API down without touching ingestion:

```bash
cd infra/read && pulumi destroy --yes --stack dev
```

Full teardown of the ephemeral side (keeps `infra/core` data):

```bash
cd infra/read   && pulumi destroy --yes --stack dev
cd infra/write  && pulumi destroy --yes --stack dev
cd infra/shared && pulumi destroy --yes --stack dev
```

Destroying any of `shared`, `write`, or `read` must not touch `infra/core`:

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
