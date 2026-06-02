#!/usr/bin/env bash
# v2 architecture-boundary checks (complements go-arch-lint component graph).
# Enforces the §1.2 禁止清单 of coagent-v2-arch-constraints-and-checks.md:
set -uo pipefail

fail() { echo "[lint-arch-boundaries] FAIL: $1" >&2; exit 1; }
command -v rg >/dev/null 2>&1 || { echo "[lint-arch-boundaries] ripgrep required" >&2; exit 1; }

# 1. v1 god-object trees must be gone.
[ -d framework ] && fail "framework/ 顶层 god-object 残留 (multiuser/devicetransit)"
[ -d adapters/framework ] && fail "adapters/framework Manager god-object 残留"
[ -d adapters/proxy ] && fail "adapters/proxy 残留 (应溶解 daemon)"

# 2. kernel 杂质包必须消解。
for p in closure actorreg adapter fencing; do
  [ -d "kernel/$p" ] && fail "kernel/$p 残留 (应消解)"
done

# 3. 没有产品代码 import 已删的 v1 包。
if rg -q "ActOS/(framework/|adapters/framework|adapters/proxy/|internal/proxy|pkg/|kernel/adapter\b)" --glob '*.go' --glob '!*_test.go' kernel runtime lib adapters wire server daemon cmd sdk obs 2>/dev/null; then
  fail "产品代码 import 已删 v1 包 (framework/adapters-framework/pkg/kernel-adapter)"
fi

# 4. Manager god-object 运行期面不复活。
if rg -q "func \([a-z ]*\*?Manager\) (Dispatch|OnExternalCallback|OnRuntimeEvent|RunGC)\b" --glob '*.go' lib server daemon 2>/dev/null; then
  fail "Manager god-object 运行期面复活"
fi

# 5. viewsync / channel-placement-saga / reclaim 已消失 (import 级)。
if rg -q "ActOS/.*\b(viewsync|channelplacement|devicetransit)\b" --glob '*.go' --glob '!*_test.go' . 2>/dev/null; then
  fail "viewsync/channelplacement/devicetransit import 残留"
fi


echo "[lint-arch-boundaries] v2 boundary checks passed ✓"
