#!/usr/bin/env bash
# check-banned-mcp.sh — guard against MCP brand residue in xhs-extension.
# fork lineage exception (lightcone/) does NOT cover devices/xhs-extension/;
# spec: .dalek/agent-user.md <banned> + .dalek/pm/reviews/convergent-8/round-3/fix-spec.md §R4-T4.
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TARGET_DIR="${ROOT_DIR}/devices/xhs-extension"

if [ ! -d "${TARGET_DIR}" ]; then
  echo "FAIL: target dir not found: ${TARGET_DIR}" >&2
  exit 1
fi

if ! command -v rg >/dev/null 2>&1; then
  echo "FAIL: ripgrep (rg) is required for check-banned-mcp.sh" >&2
  exit 1
fi

# Known self-reference: the `check:banned-mcp` npm script entry in
# devices/xhs-extension/package.json — that string is the guard's own
# name, not a brand reference. Filtered out post-hoc to keep the broad
# `mcp` grep simple and forward-compatible.
matches=$(rg -n -i 'mcp|@modelcontextprotocol' "${TARGET_DIR}" \
  --glob '!node_modules/**' \
  --glob '!dist/**' \
  --glob '!pnpm-lock.yaml' \
  | grep -v 'check:banned-mcp' \
  || true)

if [ -n "${matches}" ]; then
  echo 'FAIL: banned MCP wording in devices/xhs-extension/' >&2
  echo "${matches}" >&2
  exit 1
fi

echo 'OK'
