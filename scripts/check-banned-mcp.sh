#!/usr/bin/env bash
# check-banned-mcp.sh — repo-wide guard against MCP brand residue.
#
# Scans the entire coagent repo for the banned tokens `mcp` (case-insensitive)
# and `@modelcontextprotocol`. Excludes:
#   - lightcone/**            upstream fork lineage exception
#   - .dalek/**               PM workspace (review notes, specs, etc.)
#   - .git/**                 git internals
#   - **/node_modules/**      installed dependencies
#   - **/dist/**              build output
#   - pnpm-lock.yaml          lockfile (transitive package names)
#
# Self-references (expected, not real residue):
#   - scripts/check-banned-mcp.sh             this guard's own source.
#   - devices/xhs-extension/package.json line `"check:banned-mcp": ...`
#                                              npm script entry that calls this guard.
#   - Makefile lines naming the `check-banned-mcp` make target / its bash
#     scripts/check-banned-mcp.sh invocation.
#
# spec: .dalek/agent-user.md <banned> + .dalek/pm/reviews/convergent-8/round-4/fix-spec.md §R5-T3
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

if ! command -v rg >/dev/null 2>&1; then
  echo "FAIL: ripgrep (rg) is required for check-banned-mcp.sh" >&2
  exit 1
fi

# rg prints `path:line:content`. We strip the guard's own source by glob and
# then strip the self-referential `check[-:]banned-mcp` token (matches both
# the npm-script colon form `check:banned-mcp` and the make-target hyphen
# form `check-banned-mcp`, which also covers `scripts/check-banned-mcp.sh`
# path references). Any other `mcp` hit (e.g. a new build filter sneaking
# back into the Makefile) survives the filter and trips the guard.
matches=$(rg -n -i 'mcp|@modelcontextprotocol' "${ROOT_DIR}" \
  --glob '!lightcone/**' \
  --glob '!.dalek/**' \
  --glob '!.git/**' \
  --glob '!**/node_modules/**' \
  --glob '!**/dist/**' \
  --glob '!pnpm-lock.yaml' \
  --glob '!scripts/check-banned-mcp.sh' \
  | grep -Ev 'check[-:]banned-mcp' \
  || true)

if [ -n "${matches}" ]; then
  echo 'FAIL: banned MCP wording detected (path:line:content):' >&2
  echo "${matches}" >&2
  exit 1
fi

echo 'OK'
