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

bad='(^|[^A-Za-z0-9_-])(v4-[A-Za-z0-9_.-]*[A-Za-z0-9_]|[Mm]1[.]5)([^A-Za-z0-9_-]|$)'

filter_allowed_hits() {
  grep -vE '(^|/)\.dalek/pm/archive/' || true
}

run_self_test() {
  tmp="$(mktemp -d)"
  trap 'rm -rf "$tmp"' RETURN
  mkdir -p "$tmp/src" "$tmp/.dalek/pm/archive"
  printf 'stale ref: v4-foundation\n' > "$tmp/src/bad.txt"
  printf 'model id deepseek-v4-pro is not a protocol ref\n' > "$tmp/src/allowed-model.txt"
  printf 'archived spec ref v4-foundation\n' > "$tmp/.dalek/pm/archive/m1.3-v4-foundation-spec.md"

  if ! grep -rnE "$bad" "$tmp/src/bad.txt" >/dev/null; then
    printf '[protocol-refs] self-test failed: v4-foundation was not rejected\n' >&2
    exit 1
  fi
  if grep -rnE "$bad" "$tmp/src/allowed-model.txt" >/dev/null; then
    printf '[protocol-refs] self-test failed: deepseek-v4-pro false-positive\n' >&2
    exit 1
  fi
  archived_hits=$(grep -rnE "$bad" "$tmp/.dalek/pm/archive" 2>/dev/null | filter_allowed_hits)
  if [ -n "$archived_hits" ]; then
    printf '[protocol-refs] self-test failed: archived PM refs were not exempted\n' >&2
    printf '%s\n' "$archived_hits" >&2
    exit 1
  fi
  printf '[protocol-refs] self-test passed\n'
}

if [ "${1:-}" = "--self-test" ]; then
  run_self_test
  exit 0
fi

dirs=()
for d in "${CODE_DIRS[@]}"; do
  [ -e "$d" ] && dirs+=("$d")
done
if [ ${#dirs[@]} -eq 0 ]; then
  printf '[protocol-refs] [skip] no code dirs present\n'
  exit 0
fi

hits=$(grep -rnE "$bad" "${dirs[@]}" "${EXCLUDES[@]}" 2>/dev/null | filter_allowed_hits || true)
if [ -n "$hits" ]; then
  printf '❌ protocol-refs: stale protocol references found:\n' >&2
  printf '%s\n' "$hits" | sed 's/^/    /' >&2
  exit 1
fi

printf '✅ protocol refs lint passed\n'
