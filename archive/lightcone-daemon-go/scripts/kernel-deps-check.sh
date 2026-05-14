#!/usr/bin/env bash
# scripts/kernel-deps-check.sh
#
# Defense-in-depth complement to .go-arch-lint.yaml:
#
#   go-arch-lint auto-allows stdlib imports (database/sql, net/http) and
#   only enforces third-party vendor whitelists. The m1.5-tickets §T2
#   spec however bans these stdlib packages from `kernel/` outright:
#
#     mustNotDependOn:
#       - database/sql
#       - github.com/mattn/go-sqlite3
#       - net/http
#       - github.com/gorilla/**
#       - github.com/gin-gonic/**
#       - github.com/wanpengxie/go-kimi/**
#
# This script enforces the full ban by walking `go list -deps
# ./kernel/...` and failing fast on any forbidden import path.
#
# Authoritative spec: .dalek/pm/m1.5-tickets.md §T2 acceptance:
#   "kernel/ 编译时无 sqlite/HTTP/LLM 依赖（用 `go list -deps ./kernel/...`
#    自检）"
#
# Run from the daemon-go module root (where `go.mod` lives).

set -euo pipefail

# Forbidden import-path prefixes / patterns for kernel/.
FORBIDDEN_PATTERNS=(
  '^database/sql$'
  '^database/sql/.*'
  '^net/http$'
  '^net/http/.*'
  '^github.com/mattn/go-sqlite3'
  '^modernc.org/sqlite'
  '^github.com/gorilla/'
  '^github.com/gin-gonic/'
  '^github.com/wanpengxie/go-kimi'
)

# Collect deps; `go list -deps` outputs one import path per line.
DEPS=$(go list -deps ./kernel/... 2>/dev/null)

violations=()
while IFS= read -r dep; do
  for pat in "${FORBIDDEN_PATTERNS[@]}"; do
    if [[ "$dep" =~ $pat ]]; then
      violations+=("$dep")
      break
    fi
  done
done <<< "$DEPS"

if [[ ${#violations[@]} -gt 0 ]]; then
  echo "[FAIL] kernel/ has forbidden imports (per m1.5-tickets §T2):" >&2
  for v in "${violations[@]}"; do
    echo "  - $v" >&2
  done
  echo "" >&2
  echo "kernel/ MUST stay free of sqlite drivers, HTTP packages, and" >&2
  echo "LLM-runtime libraries. Move concrete backends to runtime/ (T3)" >&2
  echo "or adapters/ (T4)." >&2
  exit 1
fi

echo "[ok] kernel/ deps clean — no sqlite / HTTP / LLM imports"
