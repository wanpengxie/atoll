#!/usr/bin/env bash
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$ROOT"

failures=0

strict_pattern='closeEntered|beforePresenceWait|entryFor|beforeDeliver|feedStarted|seedAdmitFailHook|revokeFailHook|homeCloseHook|SetSeedAdmitFailForTest|SetRevokeFailForTest|SetHomeCloseHookForTest|PokeHub|NewPokeHub|LeakedPumps|leakedPumps|LaneWriteTimeoutMs|LaneCapacity|ErrGatewayClosed|CheckedAt|ChannelFailure|EntitlementFailure|errChannelHomeNotOpen|submitInput|closeDetachedHome|presenceOn|DroppedCount\b|droppedCount\b|lane_dropped'

scan_strict() {
	local hits status
	set +e
	hits=$(rg -n --type go "$strict_pattern" "$@" 2>&1)
	status=$?
	set -e
	if (( status > 1 )); then
		echo "[gateway-retired] strict scan failed" >&2
		echo "$hits" >&2
		return "$status"
	fi
	if [[ -n "$hits" ]]; then
		echo "$hits"
		return 1
	fi
}

# Fixture mode is the same strict scanner used by CI below. Keeping it in this script
# lets the self-test prove the executable checker, not a copied strings.Contains table.
if [[ ${1:-} == "--fixture-root" ]]; then
	shift
	if ! scan_strict "$@"; then
		exit 1
	fi
	exit 0
fi

self_test_strict() {
	local tmp
	tmp=$(mktemp -d)
	trap 'rm -rf "$tmp"' RETURN
	mkdir -p "$tmp/positive" "$tmp/negative"
	printf 'package fixture\ntype PokeHub struct{}\nfunc DroppedCount() int64 { return 0 }\n' >"$tmp/positive/fixture.go"
	printf 'package fixture\nconst laneCapacity = 64\nfunc DroppedCounts() int64 { return 0 }\nfunc keep() { home.Subscribe() }\n' >"$tmp/negative/fixture.go"
	if "$0" --fixture-root "$tmp/positive" >/dev/null 2>&1; then
		echo "[gateway-retired] strict self-test failed: retired fixture passed" >&2
		exit 1
	fi
	if ! "$0" --fixture-root "$tmp/negative" >/dev/null 2>&1; then
		echo "[gateway-retired] strict self-test failed: valid fixture failed" >&2
		exit 1
	fi
}

self_test_strict

# check_live runs the exact rg surface but treats full-line retirement comments as the
# documented allowlist. Any code/string/identifier hit is live residue and fails.
check_live() {
	local mode=$1
	local label=$2
	local pattern=$3
	shift 3

	local hits status
	set +e
	if [[ "$mode" == "ci" ]]; then
		hits=$(rg -ni --type go "$pattern" "$@" 2>&1)
	else
		hits=$(rg -n --type go "$pattern" "$@" 2>&1)
	fi
  status=$?
  set -e
  if (( status > 1 )); then
    echo "[gateway-retired] ${label}: rg failed" >&2
    echo "$hits" >&2
    exit "$status"
  fi

  while IFS= read -r hit; do
    [[ -z "$hit" ]] && continue
    local source=${hit#*:*:}
    if [[ ! "$source" =~ ^[[:space:]]*// ]]; then
      echo "[gateway-retired] ${label}: live retired-symbol hit: ${hit}" >&2
      failures=$((failures + 1))
    fi
  done <<< "$hits"
}

check_live cs "wire/API symbols" \
  'FrameDetach|DetachPayload|StaleBinding|stale_binding|BindingGen|bindingGen|binding_gen|genGuard|WithBindingGuard|SetBinding|DeliverAnyGen|CodeNotMember' \
  drivers/ platform/ app/ e2e/ lib/

check_live ci "gateway binding semantics" \
  'binding slot|binding registry|binding_gen|bindingGen|绑定世代|stale.binding|FrameDetach' \
  drivers/gateway/ platform/subjectgate/ platform/internal/humancell/ platform/home/

check_live cs "revocation-source migration" \
  'RevocationSource|SubscribeRevoked|onRevoked|SetRevokeSink|onRevoke' \
  drivers/gateway/ platform/ app/ e2e/ lib/

check_live cs "channel-bound websocket URL" \
  'ws\?channel=|/ws\?channel' \
  drivers/ app/ cmd/ e2e/

strict_hits=""
if ! strict_hits=$(scan_strict drivers/gateway/ app/ cmd/server/ platform/home/); then
	while IFS= read -r hit; do
		[[ -z "$hit" ]] && continue
		echo "[gateway-retired] demolition residue: ${hit}" >&2
		failures=$((failures + 1))
	done <<< "$strict_hits"
fi

if (( failures > 0 )); then
  echo "[gateway-retired] FAIL: ${failures} live retired-symbol hit(s)" >&2
  exit 1
fi

echo "[gateway-retired] PASS: zero live retired-symbol hits"
