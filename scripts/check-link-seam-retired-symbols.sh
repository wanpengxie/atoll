#!/usr/bin/env bash
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$ROOT"

failures=0

labels=(
	"retired app composition table"
	"retired composition migration marker"
	"retired default SQL column"
	"retired HTTP plan endpoint"
	"retired actor factory branch"
	"retired handshake resolver interface"
	"client-declared daemon identity"
	"alternate Home desired/builder source"
	"optional composition resolver"
	"optional attach authorities"
	"link-local port ownership"
	"alternate compute plan sink"
	"second actor factory representation"
	"one-step runtime attach API"
	"unlocked public app DB opener"
	"pre-release adoption/repair semantics"
)

patterns=(
	'channel_actors'
	'composition_migrated'
	'(channels|c)\.default_agent|ADD COLUMN default_agent|DROP COLUMN default_agent'
	'/compute/plan'
	'\bLegacy\b'
	'HandshakeResolveFunc|AttachHandshake'
	'\bComputeID\b|\bBoundID\b|json:"(compute_id|bound_id)"'
	'cfg\.Desired|cfg\.Builder'
	'CompositionResolver[[:space:]]*!=[[:space:]]*nil'
	'a\.(declarations|composition|registry|daemonAuthority|actorLock|portIndex)[[:space:]]*(!=|==)[[:space:]]*nil'
	'ports[[:space:]]*=[[:space:]]*map\[actor\.ActorID\]actorrt\.Incarnation|link-local quiet-stop fallback'
	'\bPlanSink\b|\bplanSink\b'
	'\bfullCaps\b|func[[:space:]]+CapsFactory[[:space:]]*\('
	'func[[:space:]]*\(r[[:space:]]+\*Runtime\)[[:space:]]+Attach\b|PrepareHandshakeObserved|CommitWhile'
	'func[[:space:]]+OpenDB[[:space:]]*\('
	'migration generation|pre-epoch|repairs inactive|half-written.*repaired|old daemon build|historical.*migration'
)

samples=(
	'var table = "channel_actors"'
	'var marker = "composition_migrated"'
	'query := "ALTER TABLE channels ADD COLUMN default_agent TEXT"'
	'const endpoint = "/compute/plan"'
	'type Legacy struct{}'
	'type HandshakeResolveFunc func()'
	'type request struct { ComputeID string }'
	'use(cfg.Desired)'
	'if cfg.CompositionResolver != nil {}'
	'if a.declarations == nil {}'
	'ports = map[actor.ActorID]actorrt.Incarnation{}'
	'var sink PlanSink'
	'func CapsFactory() {}'
	'func (r *Runtime) Attach() {}'
	'func OpenDB(path string) {}'
	'const mode = "pre-epoch"'
)

if (( ${#labels[@]} != ${#patterns[@]} || ${#patterns[@]} != ${#samples[@]} )); then
	echo "[link-seam-retired] internal rule table length mismatch" >&2
	exit 2
fi

for i in "${!patterns[@]}"; do
	if ! printf '%s\n' "${samples[$i]}" | rg -q "${patterns[$i]}"; then
		echo "[link-seam-retired] self-test failed: ${labels[$i]}" >&2
		exit 2
	fi
done

check_live() {
	local label=$1
	local pattern=$2
	local hits status
	set +e
	hits=$(rg -n --type go \
		--glob '!**/*_test.go' \
		"$pattern" app/ cmd/ platform/ runtime/ 2>&1)
	status=$?
	set -e
	if (( status > 1 )); then
		echo "[link-seam-retired] ${label}: rg failed" >&2
		echo "$hits" >&2
		exit "$status"
	fi
	while IFS= read -r hit; do
		[[ -z "$hit" ]] && continue
		local source=${hit#*:*:}
		# Documentation may name what was retired. Executable identifiers, string
		# literals and executable identifiers may not.
		if [[ "$source" =~ ^[[:space:]]*// ]] || [[ "$source" =~ ^[[:space:]]*-- ]]; then
			continue
		fi
		echo "[link-seam-retired] ${label}: live retired-symbol hit: ${hit}" >&2
		failures=$((failures + 1))
	done <<< "$hits"
}

for i in "${!patterns[@]}"; do
	check_live "${labels[$i]}" "${patterns[$i]}"
done

if (( failures > 0 )); then
	echo "[link-seam-retired] FAIL: ${failures} live retired-symbol hit(s)" >&2
	exit 1
fi

echo "[link-seam-retired] PASS: zero live retired-symbol hits"
