# poochon-backtest-data

AWS-backed historical ingestion and canonical replay materialization for `../bitchon/bot`.

This repo owns the data plane that turns venue/provider data into deterministic replay streams:

1. mirror raw venue/provider data into S3
2. discover market schedules and settlement metadata when a slice needs it
3. materialize canonical daily replay shards
4. persist shard catalog records in DynamoDB
5. broker shard discovery and direct S3 downloads to consumers

## Stack Layout

Pulumi stacks are split by lifecycle:

- `infra/core`
  - persistent storage; rarely touched
  - S3 bucket for raw and canonical artifacts
  - DynamoDB tables for coverage records and canonical shard records
- `infra/shared`
  - shared compute/networking plumbing; ~$0 idle
  - VPC + subnets + IGW, ECS cluster, CloudWatch log group, ECR repo and Docker image, IAM execution/task roles
- `infra/runtime`
  - ingestion/materialization jobs; ~$0 idle (Fargate is pay-per-run)
  - Step Functions state machines and EventBridge schedules for raw mirroring and canonical slice builds
- `infra/access`
  - cross-account read broker for Poochon control-plane access
  - IAM role trusted by the Poochon control-plane role with an ExternalId
  - read-only DynamoDB catalog access and S3 `GetObject` access to canonical prefixes

Dependency order: `core → shared → runtime`; `access` depends on `core` and can
be deployed independently after the Poochon control-plane role ARN is known.

`src/poochon_backtest_data/` holds the raw mirror, canonical slice builders, storage/catalog helpers, and CLI entrypoints. The same CLI binary runs both on your laptop (for local `run`) and inside Fargate (launched by the state machine).

Current dev stack defaults:

- region: `us-east-1`
- core stack: persistent
- shared stack: persistent (effectively — kept up because destroy cost = ECR image rebuild)
- runtime stack: persistent scheduler/state-machine definitions with pay-per-run ECS tasks
- access stack: persistent cross-account read role used by Poochon

## Poochon Control-Plane Broker

End-user download access is brokered by the sibling `../poochon` control plane.
This repo remains the source of truth for S3 and DynamoDB. The `infra/access`
stack exposes a narrow read-broker role; Poochon assumes it, reads the shard
catalog, and returns short-lived S3 URLs for canonical shard files. Poochon does
not proxy Parquet bytes.

```bash
cd infra/access
pulumi up --stack dev-east
```

The consumer-facing CLI surface lives in `../poochon`:

```bash
poochon backtest-data list --venue polymarket --series btc-updown-5m
poochon backtest-data inspect <shard-id>
poochon backtest-data download <shard-id> --out ./data/backtest
```

## End-To-End Flow

```text
Provider / Venue data
  -> raw S3 objects
  -> canonical shard (manifest.json + data.parquet, plus schedule.parquet for Polymarket)
  -> DynamoDB shard catalog
  -> direct S3 file download consumed by bitchon ReplaySource
```

There are two venue families today:

- Hyperliquid
  - raw source: Hyperliquid public archive buckets
  - raw units: hourly L2 and fills archives
  - canonical units: daily shard per instrument/date/depth
- Polymarket
  - raw source: PMXT hourly Polymarket orderbook files
  - schedule discovery: Gamma
  - price-to-beat enrichment: Vatic first, Binance 1-minute open fallback
  - canonical units: daily shard per `series_key`/date/depth

## Persistent State

### DynamoDB

- Coverage table
  - keyed by `pk`
  - tracks whether raw mirror cells and canonical shard builds are ready
  - key examples:
    - `raw_pmxt#<YYYY-MM-DD>#<HH>`
    - `raw_hl_l2#<market_type>#<instrument>#<YYYY-MM-DD>#<HH>`
    - `raw_hl_fills#<YYYY-MM-DD>#<HH>`
    - `canonical_pm#<target_kind>#<target_key>#<YYYY-MM-DD>`
    - `canonical_hl#<market_type>#<instrument>#<YYYY-MM-DD>`
- Shard table
  - keyed by `shard_id`
  - tracks canonical shard manifests already materialized in S3

### S3 Families

- Hyperliquid raw
  - `raw/hyperliquid/l2book/market_type=<...>/date=<YYYY-MM-DD>/hour=<HH>/instrument=<instrument>/<instrument>.lz4`
  - `raw/hyperliquid/node_fills_by_block/date=<YYYY-MM-DD>/hour=<HH>/fills.lz4`
- Polymarket raw
  - `raw/pmxt/orderbook/date=<YYYY-MM-DD>/hour=<HH>/polymarket_orderbook_<YYYY-MM-DD>T<HH>.parquet`
- Canonical replay shards
  - Hyperliquid:
    - `canonical/hyperliquid/market_type=<...>/instrument=<instrument>/date=<YYYY-MM-DD>/depth=<N>/data.parquet`
    - `canonical/hyperliquid/market_type=<...>/instrument=<instrument>/date=<YYYY-MM-DD>/depth=<N>/manifest.json`
  - Polymarket:
    - `canonical/polymarket/<series|slug>/<target_key>/date=<YYYY-MM-DD>/depth=<N>/data.parquet`
    - `canonical/polymarket/<series|slug>/<target_key>/date=<YYYY-MM-DD>/depth=<N>/schedule.parquet`
    - `canonical/polymarket/<series|slug>/<target_key>/date=<YYYY-MM-DD>/depth=<N>/manifest.json`

## Data Structures

### Raw Stage

Raw objects are provider-native payloads. This stage preserves source fidelity and keeps provider-specific parsing isolated from the canonical schema.

#### Raw Object Keying

Raw keys are composed from the minimum dimensions needed to make each object uniquely addressable and idempotent to re-copy. The dimensions differ per venue because upstream cadence and fan-out differ.

##### Hyperliquid

Hourly-partitioned, scoped to an instrument:

| Family  | Key dimensions                              | Destination key                                                                                              |
| ------- | ------------------------------------------- | ------------------------------------------------------------------------------------------------------------ |
| L2 book | `market_type`, `date`, `hour`, `instrument` | `raw/hyperliquid/l2book/market_type=<...>/date=<YYYY-MM-DD>/hour=<HH>/instrument=<instrument>/<instrument>.lz4` |
| Fills   | `date`, `hour`                              | `raw/hyperliquid/node_fills_by_block/date=<YYYY-MM-DD>/hour=<HH>/fills.lz4`                                  |

Notes:

- Sources are requester-pays buckets: `s3://hyperliquid-archive/market_data/YYYYMMDD/<hour>/l2Book/<instrument>.lz4` (L2) and `s3://hl-mainnet-node-data/node_fills_by_block/hourly/YYYYMMDD/<hour>.lz4` (fills).
- L2 upstream is already instrument-scoped; one upstream object maps to one destination key.
- Fills upstream is **not** instrument-scoped — it is one `.lz4` per hour covering the whole venue. It is mirrored once per hour and filtered to `coin == <instrument>` at slice-build time.
- `market_type` comes from `MarketRef.market_type` (e.g. `perp`).
- `instrument` is URL-encoded via `MarketRef.encoded_instrument()`.
- `date` is the UTC calendar day; `hour` is the zero-padded UTC hour.

##### Polymarket

Hourly-partitioned PMXT firehose data, not scoped to one market:

| Family    | Key dimensions | Destination key                                                                                              |
| --------- | -------------- | ------------------------------------------------------------------------------------------------------------ |
| Orderbook | `date`, `hour` | `raw/pmxt/orderbook/date=<YYYY-MM-DD>/hour=<HH>/polymarket_orderbook_<YYYY-MM-DD>T<HH>.parquet`              |

Notes:

- Source: PMXT `GET /polymarket_orderbook_<YYYY-MM-DD>T<HH>.parquet`.
- The mirror stage writes one raw object per hour. Canonical slice builders filter those hourly files by target asset ids after discovering the target schedule.
- The current hour is skipped until a publish-lag buffer has passed, so scheduled mirrors do not mark not-yet-published files as permanent failures.

#### Hyperliquid Raw

- L2 raw
  - compressed NDJSON copied from the Hyperliquid archive bucket
  - parser reads `raw.data.time`, `raw.data.coin`, and `raw.data.levels`
- Fill/trade raw
  - compressed NDJSON copied from `node_fills_by_block`
  - parser groups duplicate fills by `(coin, time, tid, hash, px, sz)` and picks a canonical fill row

Hyperliquid raw is intentionally not restated as a stable schema in this repo because it mirrors the upstream archive format directly.

#### Polymarket Raw

Polymarket raw data is PMXT parquet. The canonical builder currently consumes
these fields:

```text
timestamp
asset_id
event_type
bids
asks
price
size
side
```

Supported `event_type` values are translated into canonical data rows:

- `book` -> `l2_snapshot`
- `price_change` -> `delta_batch`
- `last_trade_price` -> `trade`

#### Polymarket Schedule Data

Polymarket slices discover market schedule rows at build time using Gamma, Vatic,
and Binance fallbacks. `series_key` is derived from `slug` by trimming the
trailing timestamp segment, and `start_ts_ms` / `end_ts_ms` are contract-window
timestamps, not Gamma listing timestamps.

### Canonical Slice Stage

Canonical replay is a manifest-rooted Parquet dataset. This is the stable
historical format consumed by `../bitchon/bot` replay and backtest flows.

Each Hyperliquid shard emits `data.parquet` and `manifest.json`. Each
Polymarket shard emits `data.parquet`, `schedule.parquet`, and `manifest.json`.

#### `data.parquet`

```text
ts_ms
instrument
kind
bids
asks
delta_levels
px
sz
side
```

Semantics:

- `kind` is `l2_snapshot`, `delta_batch`, or `trade`
- `bids` / `asks` are depth-limited price levels for full snapshots
- `delta_levels` contains side/price/size updates for Polymarket price changes
- `px`, `sz`, and `side` are populated for trade rows

#### `schedule.parquet`

```text
target_kind
target_key
slug
market_id
start_ts_ms
end_ts_ms
price_to_beat
price_to_beat_source
price_to_beat_quality
outcomes
```

`schedule.parquet` is Polymarket-only. The `outcomes` field carries outcome,
asset id, replay instrument, and settlement payout for each side of the binary
market.

Contract lifecycle events are emitted at replay time from `schedule.parquet`:

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

### Consumer Ordering

Canonical rows carry `ts_ms` and `kind`; consumers should use those fields when
they need to merge or order events. The current raw-to-slice pipeline no longer
writes a normalized intermediate dataset or `source_line_number` columns.

## CLI And Operational Flow

The CLI is an operator console for the pipeline — it executes stages locally, submits jobs to AWS, inspects what data exists, and reports stack status. Everything talks directly to S3 / DynamoDB / Step Functions; no local server required.

Required environment:

```bash
export POOCHON_AWS_REGION=us-east-1
export POOCHON_PULUMI_STACK=dev-east
export POOCHON_DATA_BUCKET="$(cd infra/core && pulumi stack output data_bucket_name --stack dev-east)"
export POOCHON_COVERAGE_TABLE_NAME="$(cd infra/core && pulumi stack output coverage_table_name --stack dev-east)"
export POOCHON_SHARD_TABLE_NAME="$(cd infra/core && pulumi stack output shard_table_name --stack dev-east)"
```

### Command tree

```
poochon-backtest-data
├── infra
│   └── status                      # UP/DOWN per Pulumi stack + key outputs
├── data
│   ├── hyperliquid {raw|slice}      # inspect mirrored raw data and canonical slices
│   └── polymarket  {raw|slice}
├── run                             # execute mirror/slice work locally (blocks)
│   ├── hyperliquid {mirror|slice|all}
│   └── polymarket  {mirror|slice|all}
├── submit                          # start runtime Step Functions executions on AWS
│   ├── hyperliquid {mirror|slice}
│   └── polymarket  {mirror|slice}
├── schedule                        # inspect EventBridge schedules
│   ├── list
│   └── next
└── job                             # track AWS executions
    ├── list
    ├── status <execution-arn>
    └── logs <execution-arn> [--follow]
```

### Canonical walkthrough

```bash
# 1. Where are we?
poochon-backtest-data infra status

# 2. Run a single stage locally — idempotent, so re-running is a no-op.
poochon-backtest-data run hyperliquid mirror \
  --market-type perp --instrument BTC \
  --start-date 2026-02-19 --end-date 2026-02-19

# 3. Check what landed.
poochon-backtest-data data hyperliquid raw BTC/perp \
  --start-date 2026-02-19 --end-date 2026-02-19

# 4. Full stream, same window.
poochon-backtest-data run hyperliquid all \
  --market-type perp --instrument BTC \
  --start-date 2026-02-19 --end-date 2026-02-19 --depth 20

# 5. Same thing on AWS instead of your laptop.
poochon-backtest-data submit hyperliquid mirror \
  --market-type perp --instrument BTC \
  --start-date 2026-02-19 --end-date 2026-02-19
poochon-backtest-data submit hyperliquid slice \
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
  --target series:btc-updown-5m \
  --start-date 2026-02-19 --end-date 2026-02-21 \
  --force
```

This mirrors PMXT raw firehose data for the requested window, then builds
canonical daily slices for the target. Each sub-stage is individually callable
(`run polymarket mirror`, `run polymarket slice`) if you want to isolate a step.

### Stack Lifecycle

Bring the persistent runtime path up:

```bash
(cd infra/core && pulumi up --stack dev-east)
(cd infra/shared && pulumi up --stack dev-east)
(cd infra/runtime && pulumi up --stack dev-east)
```

Expose read-only catalog and canonical shard access to the Poochon control
plane after the control-plane role ARN is known:

```bash
(cd infra/access && pulumi up --stack dev-east)
```

Take down the broker without touching ingestion or stored data:

```bash
(cd infra/access && pulumi destroy --yes --stack dev-east)
```

Full teardown of the non-data side (keeps `infra/core` data):

```bash
(cd infra/access && pulumi destroy --yes --stack dev-east)
(cd infra/runtime && pulumi destroy --yes --stack dev-east)
(cd infra/shared && pulumi destroy --yes --stack dev-east)
```

Destroying any of `shared`, `runtime`, or `access` must not touch `infra/core`:

- S3 bucket
- coverage table
- shard table

## Consumer Contract

`../bitchon/bot` should treat canonical replay as the stable integration boundary.

- raw provider payloads are not a consumer contract
- canonical `data.parquet`, optional `schedule.parquet`, and `manifest.json` are the replay contract
- Poochon brokers catalog discovery and short-lived S3 downloads; this repo remains the data source of truth

If replay behavior changes, update this README first and keep `bitchon` live/replay semantics aligned.
