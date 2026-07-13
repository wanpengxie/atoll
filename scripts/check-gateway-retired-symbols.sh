#!/usr/bin/env bash
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$ROOT"

failures=0

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

if (( failures > 0 )); then
  echo "[gateway-retired] FAIL: ${failures} live retired-symbol hit(s)" >&2
  exit 1
fi

echo "[gateway-retired] PASS: zero live retired-symbol hits"
