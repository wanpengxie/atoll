#!/usr/bin/env bash
# M1.1-T3 e2e smoke entry. Runs the in-process node smoke that exercises
# device.command.send → ws push → mock device callback → dispatch.completed
# without requiring a real chrome extension or xhs.com login.
#
# Prereq: pnpm install at repo root (so daemon's `ws` dep is available).
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
SMOKE_SCRIPT="${REPO_ROOT}/lightcone/daemon/scripts/smoke-m1.1-xhs-real.mjs"

if [[ ! -f "${SMOKE_SCRIPT}" ]]; then
  echo "smoke script missing: ${SMOKE_SCRIPT}" >&2
  exit 1
fi

cd "${REPO_ROOT}/lightcone/daemon"
exec node "${SMOKE_SCRIPT}" "$@"
