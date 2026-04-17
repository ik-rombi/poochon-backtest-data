#!/usr/bin/env bash
#
# Migrate from the single infra/runtime stack to the new four-stack layout
# (core + shared + write + read). Idempotent — safe to re-run.
#
# Phases:
#   --init      pulumi stack init dev for shared/write/read (local-only, safe)
#   --preview   pulumi preview for shared/write/read (read-only, safe, shows diffs)
#   --up        pulumi up for shared -> write -> read (CREATES real AWS resources)
#   --verify    run the validation checklist (infra status, submit, job, curl)
#   --retire    pulumi destroy infra/runtime + rm -rf infra/runtime/ (IRREVERSIBLE)
#   --all       init -> preview -> up -> verify (stops before retire)
#
# Each phase only does its own work; phases are independent so you can mix.
#
# Required env:
#   POOCHON_PULUMI_STACK       (default: dev)
#   POOCHON_DATA_BUCKET, POOCHON_COVERAGE_TABLE_NAME, POOCHON_SHARD_TABLE_NAME
#   POOCHON_AWS_REGION
#   (loaded from .env.local if present)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$REPO_ROOT"

if [[ -f .env.local ]]; then
  set -a
  # shellcheck disable=SC1091
  source .env.local
  set +a
fi

STACK="${POOCHON_PULUMI_STACK:-dev}"
NEW_STACKS=(shared write read)

log() { printf "\033[1;34m==>\033[0m %s\n" "$*"; }
warn() { printf "\033[1;33m[warn]\033[0m %s\n" "$*" >&2; }
die() { printf "\033[1;31m[error]\033[0m %s\n" "$*" >&2; exit 1; }

require_cli() {
  command -v pulumi >/dev/null || die "pulumi CLI not on PATH"
  command -v uv >/dev/null || die "uv not on PATH"
  command -v docker >/dev/null || warn "docker not on PATH — infra/shared will fail to build the image"
}

ensure_venv() {
  local dir="$1"
  if [[ ! -x "$dir/.venv/bin/python" ]]; then
    log "  bootstrapping venv in $dir"
    ( cd "$dir" && uv venv --seed >/dev/null )
    ( cd "$dir" && uv pip install --quiet "pulumi>=3.0.0,<4.0.0" "pulumi-aws>=6.0.0,<7.0.0" )
    if [[ "$(basename "$dir")" == "shared" ]]; then
      ( cd "$dir" && uv pip install --quiet "pulumi-docker>=4.0.0,<5.0.0" )
    fi
  fi
}

pulumi_run() {
  local dir="$1"; shift
  ensure_venv "$dir"
  PULUMI_PYTHON_CMD="$dir/.venv/bin/python" pulumi --cwd "$dir" "$@"
}

cmd_init() {
  log "Phase: init"
  for name in "${NEW_STACKS[@]}"; do
    local dir="$REPO_ROOT/infra/$name"
    if [[ ! -d "$dir" ]]; then
      warn "skipping $name — directory missing"
      continue
    fi
    ensure_venv "$dir"
    if pulumi_run "$dir" stack ls 2>/dev/null | grep -q "^$STACK "; then
      log "  infra/$name stack '$STACK' already initialized"
    else
      log "  infra/$name: pulumi stack init $STACK"
      pulumi_run "$dir" stack init "$STACK"
    fi
  done
}

cmd_preview() {
  log "Phase: preview"
  for name in "${NEW_STACKS[@]}"; do
    local dir="$REPO_ROOT/infra/$name"
    if [[ ! -d "$dir" ]]; then
      warn "skipping $name — directory missing"
      continue
    fi
    log "  infra/$name: pulumi preview --stack $STACK"
    pulumi_run "$dir" preview --stack "$STACK" || warn "preview for $name had errors (expected before shared is deployed)"
  done
}

cmd_up() {
  log "Phase: up (this CREATES real AWS resources)"
  for name in "${NEW_STACKS[@]}"; do
    local dir="$REPO_ROOT/infra/$name"
    if [[ ! -d "$dir" ]]; then
      warn "skipping $name — directory missing"
      continue
    fi
    log "  infra/$name: pulumi up --stack $STACK --yes"
    pulumi_run "$dir" up --stack "$STACK" --yes || die "up failed for $name"
  done
}

cmd_verify() {
  log "Phase: verify"

  log "  infra status"
  uv run poochon-backtest-data infra status --stack "$STACK" || warn "infra status returned non-zero"

  local arn
  arn=$(cd "$REPO_ROOT/infra/write" && pulumi stack output ingestion_state_machine_arn --stack "$STACK" 2>/dev/null || echo "")
  if [[ -z "$arn" ]]; then
    warn "no ingestion_state_machine_arn yet — skipping submit smoke test"
  else
    log "  state machine arn: $arn"
    local today
    today=$(date -u +%Y-%m-%d)
    log "  submit smoke test (hyperliquid BTC, UTC today=$today)"
    local exec_arn
    exec_arn=$(uv run poochon-backtest-data submit hyperliquid \
      --market-type perp --instrument BTC \
      --start-date "$today" --end-date "$today" --depth 20 \
      --stack "$STACK" 2>&1 | grep -E "^arn:aws:states:" | head -1 || echo "")
    if [[ -n "$exec_arn" ]]; then
      log "  execution arn: $exec_arn"
      log "  job status:"
      uv run poochon-backtest-data job status "$exec_arn" || true
    else
      warn "submit did not return an execution ARN — check output manually"
    fi
  fi

  local api_url
  api_url=$(cd "$REPO_ROOT/infra/read" && pulumi stack output api_url --stack "$STACK" 2>/dev/null || echo "")
  if [[ -n "$api_url" ]]; then
    log "  api url: $api_url"
    log "  curl $api_url/api/v1/health"
    curl -sf "$api_url/api/v1/health" || warn "health check failed (ALB warm-up can take ~60s)"
    echo
  else
    warn "no api_url — is infra/read up?"
  fi
}

cmd_retire() {
  log "Phase: retire infra/runtime (IRREVERSIBLE)"
  local runtime_dir="$REPO_ROOT/infra/runtime"
  if [[ ! -d "$runtime_dir" ]]; then
    log "  infra/runtime already removed"
    return 0
  fi

  printf "\033[1;33mThis will pulumi destroy infra/runtime and delete the directory. Continue? (yes/no) \033[0m"
  read -r reply
  if [[ "$reply" != "yes" ]]; then
    die "aborted"
  fi

  log "  pulumi destroy --stack $STACK"
  ( cd "$runtime_dir" && pulumi destroy --stack "$STACK" --yes )
  log "  rm -rf infra/runtime"
  rm -rf "$runtime_dir"
  log "  done — commit the deletion with git"
}

cmd_all() {
  cmd_init
  cmd_preview
  printf "\n\033[1;33mReady to 'pulumi up' on shared/write/read. This creates real AWS resources (~\$30/mo for read-side ALB). Continue? (yes/no) \033[0m"
  read -r reply
  if [[ "$reply" != "yes" ]]; then
    die "aborted before pulumi up"
  fi
  cmd_up
  cmd_verify
}

main() {
  require_cli
  if [[ $# -eq 0 ]]; then
    echo "usage: $0 [--init|--preview|--up|--verify|--retire|--all]"
    exit 1
  fi
  case "$1" in
    --init) cmd_init ;;
    --preview) cmd_preview ;;
    --up) cmd_up ;;
    --verify) cmd_verify ;;
    --retire) cmd_retire ;;
    --all) cmd_all ;;
    -h|--help)
      grep -E "^#( |$)" "$0" | sed 's/^# \{0,1\}//'
      ;;
    *) die "unknown phase: $1 (use --help)" ;;
  esac
}

main "$@"
