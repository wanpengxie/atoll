#!/usr/bin/env bash
set -euo pipefail

# Event-landing gate. Invariant: every agent.turn.* / agent.tool.* type on the wire is
# registered in registry/activity.go (the one vocabulary source) — an
# unregistered type is a phantom event no consumer knows about. Not
# compiler-provable because the wire value is a string: producers can
# hand-write a literal, so the gate greps for exactly that. Allowlist
# entries require reason/owner/expires and fail when expired.

root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$root"

allowlist=scripts/activity-types-allowlist.tsv
today=$(date -u +%F)
failures=0

valid_expiry() {
  local value=$1 normalized
  [[ "$value" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]] || return 1
  normalized=$(date -u -d "$value" +%F 2>/dev/null) || return 1
  [[ "$normalized" == "$value" ]]
}

declare -A known=()
while IFS= read -r value; do known["$value"]=1; done < <(
  rg -o '"agent\.(turn|tool)\.[a-z.]+"' registry/activity.go | tr -d '"' | sort -u
)

declare -A allowed=()
while IFS=$'\t' read -r typ reason owner expires extra; do
  [[ -z "${typ:-}" || "$typ" == "type" ]] && continue
  if [[ -z "${reason:-}" || -z "${owner:-}" || -z "${expires:-}" || -n "${extra:-}" ]]; then
    echo "[activity-types] malformed allowlist row for $typ: require type/reason/owner/expires" >&2
    failures=$((failures + 1))
    continue
  fi
  if ! valid_expiry "$expires"; then
    echo "[activity-types] malformed allowlist expiry for $typ: $expires (want YYYY-MM-DD)" >&2
    failures=$((failures + 1))
    continue
  fi
  if [[ "$expires" < "$today" ]]; then
    echo "[activity-types] expired allowlist row: $typ expired $expires" >&2
    failures=$((failures + 1))
    continue
  fi
  allowed["$typ"]=1
done < "$allowlist"

is_registered_or_allowed() {
  local typ=$1
  [[ -n "${known[$typ]:-}" || -n "${allowed[$typ]:-}" ]]
}

if [[ "${1:-}" == "--self-test" ]]; then
  if valid_expiry "not-a-date" || valid_expiry "2026-99-99"; then
    echo "[activity-types] strict self-test failed: malformed expiry was accepted" >&2
    exit 1
  fi
  if ! valid_expiry "2099-12-31"; then
    echo "[activity-types] strict self-test failed: valid expiry was rejected" >&2
    exit 1
  fi
  if is_registered_or_allowed "agent.turn.unregistered.negative_fixture"; then
    echo "[activity-types] strict self-test failed: unknown type was accepted" >&2
    exit 1
  fi
  if ! is_registered_or_allowed "agent.turn.started"; then
    echo "[activity-types] strict self-test failed: registered type was rejected" >&2
    exit 1
  fi
  echo "[activity-types] strict self-test PASS"
  exit 0
fi

while IFS= read -r typ; do
  is_registered_or_allowed "$typ" && continue
  echo "[activity-types] unregistered activity type: $typ" >&2
  failures=$((failures + 1))
done < <(rg -o '"agent\.(turn|tool)\.[a-z.]+"' --glob '*.go' | sed -E 's/.*"(agent\.(turn|tool)\.[a-z.]+)"/\1/' | sort -u)

# Production code must consume registry constants. Literal spellings outside
# the single registry are accepted only in tests, where wire assertions need
# to pin the actual public values.
while IFS= read -r hit; do
  [[ -z "$hit" ]] && continue
  echo "[activity-types] producer uses a literal instead of registry constant: $hit" >&2
  failures=$((failures + 1))
done < <(rg -n '"activity\.[a-z.]+"' --glob '*.go' --glob '!*_test.go' --glob '!registry/activity.go' || true)

if (( failures > 0 )); then
  echo "[activity-types] FAIL: $failures violation(s)" >&2
  exit 1
fi
echo "[activity-types] PASS"
