#!/usr/bin/env bash
set -euo pipefail

# Data-plane v0 scope gate. Inspect every changed line relative to HEAD plus
# every untracked file, so newly-created code cannot escape the review set.
repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$repo_root"

added=$(mktemp)
trap 'rm -f "$added"' EXIT

git diff --no-ext-diff --unified=0 HEAD -- . \
  | awk '/^\+\+\+/{next} /^\+/{print substr($0, 2)}' >"$added"

while IFS= read -r path; do
  [[ -z "$path" || "$path" == "scripts/check-data-plane-scope.sh" ]] && continue
  [[ -f "$path" ]] && sed 's/^/[untracked] /' "$path" >>"$added"
done < <(git status --porcelain --untracked-files=all | awk '$1 == "??" {sub(/^\?\? /, ""); print}')

# Match prohibited feature shapes rather than Go's ordinary `for ... range`.
forbidden='(^|[^[:alnum:]_])(s3|oss)([^[:alnum:]_]|$)|presign(ed|ing)?|space://|(^|[^[:alnum:]_])p2p([^[:alnum:]_]|$)|Content-Range|Accept-Ranges|Range:[[:space:]]*bytes|Range(Request|Response|Spec|Header|Start|End)|range[_ -]?(read|request|start|end)|byte[_ -]?range|断点|并行分块|缓存预热|cache[_ -]?warm|replica(tion)?|迁移|同步副本|quota|配额|计量|single[_ -]?use|核销|短名|short[_ -]?name|默认宿主|default[_ -]?host|目录归档|directory[_ -]?archive'
if rg -ni "$forbidden" "$added"; then
  echo "[data-plane-scope] prohibited v0 feature found in changed content" >&2
  exit 1
fi

# KV behavior is outside this construction's scope. New or rewritten KindKV
# call sites fail mechanically. Deletion is permitted only because section H
# explicitly removes the obsolete outer read facade that happened to expose
# KV; the existing simple-create assertions below still pin KV behavior.
if git diff --no-ext-diff --unified=0 HEAD -- . \
  | awk '/^\+\+\+/{next} /^\+/{print}' \
  | rg -n 'KindKV'; then
  echo "[data-plane-scope] KindKV call point changed" >&2
  exit 1
fi

go test ./runtime/accessdoor -run 'TestDoorCreate/member_creates_with_KindKV_and_initial_bytes|TestMintedHandleRunsFullPath' -count=1
echo "[data-plane-scope] PASS"
