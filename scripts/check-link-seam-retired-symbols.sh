#!/usr/bin/env bash
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$ROOT"

failures=0

check_live() {
	local label=$1
	local pattern=$2
	local hits status
	set +e
	hits=$(rg -n --type go \
		--glob '!**/*_test.go' \
		--glob '!**/*migrator.go' \
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
		# literals, and SQL outside the explicit migrator allowlist may not.
		if [[ "$source" =~ ^[[:space:]]*// ]] || [[ "$source" =~ ^[[:space:]]*-- ]]; then
			continue
		fi
		echo "[link-seam-retired] ${label}: live retired-symbol hit: ${hit}" >&2
		failures=$((failures + 1))
	done <<< "$hits"
}

check_live "app composition source" 'channel_actors'
check_live "retired default SQL column" '(channels|c)\.default_agent|ADD COLUMN default_agent|DROP COLUMN default_agent'
check_live "retired HTTP plan endpoint" '/compute/plan'

if (( failures > 0 )); then
	echo "[link-seam-retired] FAIL: ${failures} live retired-symbol hit(s)" >&2
	exit 1
fi

echo "[link-seam-retired] PASS: zero live retired-symbol hits"
