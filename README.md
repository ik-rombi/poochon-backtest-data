# poochon-backtest-data

AWS-backed Hyperliquid historical ingestion and replay materialization for `../bitchon/bot`.

This repo contains:

- Pulumi stacks for persistent data-plane resources and ephemeral runtime resources
- ECS worker entrypoints for raw copy, normalization, and replay materialization
- FastAPI service for replay creation, status inspection, and NDJSON streaming
