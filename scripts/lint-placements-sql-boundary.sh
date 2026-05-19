#!/usr/bin/env bash
set -euo pipefail

hits="$(rg -n --glob '*.go' --glob '!server/placements/**' --glob '!server/store/**' 'channel_placements' server || true)"
if [[ -n "$hits" ]]; then
  echo "[lint-placements-sql-boundary] channel_placements SQL belongs in server/placements; server/store is allowed for migrations/schema tests" >&2
  printf '%s\n' "$hits" >&2
  exit 1
fi
