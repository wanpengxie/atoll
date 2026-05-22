#!/usr/bin/env bash
# lint-protocol-refs.sh — reject stale protocol-doc references in code.

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

CODE_DIRS=(kernel runtime adapters server cmd pkg ui scripts)
EXCLUDES=(
  --exclude-dir=archive
  --exclude-dir=node_modules
  --exclude-dir=dist
  --exclude-dir=.git
  --exclude=lint-protocol-refs.sh
)

dirs=()
for d in "${CODE_DIRS[@]}"; do
  [ -e "$d" ] && dirs+=("$d")
done
if [ ${#dirs[@]} -eq 0 ]; then
  printf '[protocol-refs] [skip] no code dirs present\n'
  exit 0
fi

bad='v4-layer|v4-message-definition|[Mm]1[.]5'
hits=$(grep -rnE "$bad" "${dirs[@]}" "${EXCLUDES[@]}" 2>/dev/null || true)
if [ -n "$hits" ]; then
  printf '❌ protocol-refs: stale protocol references found:\n' >&2
  printf '%s\n' "$hits" | sed 's/^/    /' >&2
  exit 1
fi

printf '✅ protocol refs lint passed\n'
