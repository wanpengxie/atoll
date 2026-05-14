#!/usr/bin/env bash
# lint-banned-words.sh — banned-word lint for coagent (covers codex 必修 #14)
#
# spec: .dalek/pm/m1.5-tickets.md §T8 #5
#       .dalek/pm/m1.5-server-rewrite-and-restructure.md §15.2.2
#
# 分层扫描：
#   CODE_DIRS   (kernel runtime adapters server cmd pkg ui)      — 严格禁止
#   ACTIVE_SPEC (.dalek/pm/v4-* + .dalek/pm/m1.5-*)               — 严格禁止
#   历史/讨论文档 grandfather（不在 ACTIVE_SPEC 列表里，不扫）
#   archive/ 默认 grep 排除（lightcone/* 等已归档到 archive/）
#
# 禁词：
#   1) MCP / @modelcontextprotocol / MCP server / capability_set.mcp
#   2) dogfood
#   3) 1studio
#   4) lightcone（仅 CODE_DIRS 严格；ACTIVE_SPEC 允许历史/迁移引用）
#
# 允许的"元引用"（policy / archive / lint-self-ref / 反面 lint 例子 / M1.3 grandfather）
# 通过 ALLOW 正则放行；任何一条 grep -E 命中即视为合法，不计违规。

set -euo pipefail

# ----------------------------------------------------------------------------
# 目标集合
# ----------------------------------------------------------------------------
CODE_DIRS=(kernel runtime adapters server cmd pkg ui)

ACTIVE_SPEC=(
  .dalek/pm/v4-message-definition.md
  .dalek/pm/v4-layer0-spec.md
  .dalek/pm/v4-layer1-spec.md
  .dalek/pm/v4-layer2-spec.md
  .dalek/pm/v4-layer3-spec.md
  .dalek/pm/v4-layer4-spec.md
  .dalek/pm/m1.5-server-rewrite-and-restructure.md
  .dalek/pm/m1.5-tickets.md
)

# 公共目录排除（grep -r 用）
EXCLUDES=(
  --exclude-dir=archive
  --exclude-dir=node_modules
  --exclude-dir=dist
  --exclude-dir=.git
  --exclude=lint-banned-words.sh
  # M1.5-T5: pnpm 锁文件的 integrity hash (base64) 会偶发包含 "mcp"
  # 子串，纯属字符级 false positive；锁文件本身由 pnpm 生成，无人写。
  --exclude=pnpm-lock.yaml
)

# T9 之前 xhs Chrome extension 的 TS 代码沿用了 lightcone 历史引用，曾用
# EXT_PATH_FILTER 把整个 extension 子树临时移出严格扫描。T9 phase-refs-extension
# 完成 lightcone → coagent 重写后，extension 已纳入正常扫描；过滤器移除。

# ----------------------------------------------------------------------------
# Allowlist —— 元引用放行
# ----------------------------------------------------------------------------
# 任一 pattern 命中 → 该行允许。允许 pattern 按"线索"组织：
#   policy   零 MCP / MCP 原则 / 永久禁止 / banned: ... / 故意引入违规 / lint 拒
#   archive  mcp-servers / lightcone-mcp-servers / archive/...
#   self     scripts/lint-banned-words / check[-:]banned-mcp / lint-banned-words.sh / fail "<word>" / grep "<word>"
#   meta     反面例子 / 反面 lint 例子 / "<word>"  作为引文/标记
#   legacy   mcp_loading / mcp 子系统 / MCP 工具 / go-kimi MCP（M1.3 grandfather context）
#   thesis   "MCP / Slack / Lotus" v5 thesis 历史名词列举
MCP_ALLOW='零\s*MCP|MCP\s*原则|永久禁止|mcp[ /-]?servers?|MCP[ /-]?servers?|lightcone[/-]mcp-servers|archive/[A-Za-z0-9_./-]*mcp|lint-banned-words|check[-:]banned-mcp|不引入\s*mcp|不引入.*MCP|MCP / Slack|MCP /Slack|❌.*[Mm][Cc][Pp]|grep.*[Mm][Cc][Pp]|fail.*[Mm][Cc][Pp]|MCP reference|lint 拒|故意引入违规|反面.*lint|mcp_loading|[Mm][Cc][Pp]\s+子系统|[Mm][Cc][Pp]\s+工具|go-kimi.*[Mm][Cc][Pp]|[Mm][Cc][Pp].*go-kimi|capability_set\.mcp|\(mcp\||@modelcontextprotocol\||@modelcontextprotocol /|@modelcontextprotocol\)|对零\s*MCP|no\s+MCP|banned:\s*MCP|banned:\s*mcp|禁.*MCP|禁.*mcp'

DOGFOOD_ALLOW='禁.*dogfood|dogfood.*禁|no[- ]?dogfood|grep.*dogfood|fail.*dogfood|lint-banned-words|user feedback memory|lint 拒|故意引入违规|反面.*lint|feedback_no_dogfood'

ONESTUDIO_ALLOW='禁.*1studio|1studio.*禁|grep.*1studio|fail.*1studio|lint-banned-words|1studio.*coagent|coagent.*1studio|lint 拒|故意引入违规|反面.*lint|user feedback memory'

# lightcone scan 只跑在 CODE_DIRS；ACTIVE_SPEC 允许引用（archive/migration spec）
LIGHTCONE_ALLOW='archive/.*lightcone|lightcone.*archive|lint-banned-words|fail.*lightcone|grep.*lightcone'

# ----------------------------------------------------------------------------
# 工具函数
# ----------------------------------------------------------------------------
errors=0
fail() {
  errors=$((errors + 1))
  printf '❌ banned-words: %s\n' "$*" >&2
}

# scan_dirs <label> <bad_regex> <allow_regex>
#   扫 CODE_DIRS 中实际存在的目录；命中且未被 allow 放行 → fail
scan_dirs() {
  local label="$1" bad="$2" allow="$3"
  local dirs=()
  for d in "${CODE_DIRS[@]}"; do
    [ -e "$d" ] && dirs+=("$d")
  done
  if [ ${#dirs[@]} -eq 0 ]; then
    printf '[banned-words] [skip] %s: no CODE_DIRS present yet\n' "$label"
    return 0
  fi
  local hits
  hits=$(grep -rnEi "$bad" "${dirs[@]}" "${EXCLUDES[@]}" 2>/dev/null \
           | grep -vE "$allow" || true)
  if [ -n "$hits" ]; then
    fail "$label found in CODE_DIRS:"
    printf '%s\n' "$hits" | sed 's/^/    /' >&2
  else
    printf '[banned-words] [ok]   %s clean in CODE_DIRS\n' "$label"
  fi
}

# scan_spec <label> <bad_regex> <allow_regex>
#   扫 ACTIVE_SPEC 中存在的文件；命中且未被 allow 放行 → fail
scan_spec() {
  local label="$1" bad="$2" allow="$3"
  local files=()
  for f in "${ACTIVE_SPEC[@]}"; do
    [ -f "$f" ] && files+=("$f")
  done
  if [ ${#files[@]} -eq 0 ]; then
    printf '[banned-words] [skip] %s: ACTIVE_SPEC files absent\n' "$label"
    return 0
  fi
  local hits
  hits=$(grep -nEi "$bad" "${files[@]}" 2>/dev/null \
           | grep -vE "$allow" || true)
  if [ -n "$hits" ]; then
    fail "$label found in ACTIVE_SPEC (only policy / archive / meta refs are grandfathered):"
    printf '%s\n' "$hits" | sed 's/^/    /' >&2
  else
    printf '[banned-words] [ok]   %s clean in ACTIVE_SPEC\n' "$label"
  fi
}

# ----------------------------------------------------------------------------
# 1) MCP — code 严格 + active spec 严格
# ----------------------------------------------------------------------------
MCP_BAD='(mcp|@modelcontextprotocol|MCP server|capability_set\.mcp)'
scan_dirs "MCP"  "$MCP_BAD" "$MCP_ALLOW"
scan_spec "MCP"  "$MCP_BAD" "$MCP_ALLOW"

# ----------------------------------------------------------------------------
# 2) dogfood — code + active spec
# ----------------------------------------------------------------------------
scan_dirs "dogfood" 'dogfood' "$DOGFOOD_ALLOW"
scan_spec "dogfood" 'dogfood' "$DOGFOOD_ALLOW"

# ----------------------------------------------------------------------------
# 3) 1studio — code + active spec
# ----------------------------------------------------------------------------
scan_dirs "1studio" '1studio' "$ONESTUDIO_ALLOW"
scan_spec "1studio" '1studio' "$ONESTUDIO_ALLOW"

# ----------------------------------------------------------------------------
# 4) lightcone — code only (active spec / archive 例外)
# ----------------------------------------------------------------------------
scan_dirs "lightcone" 'lightcone' "$LIGHTCONE_ALLOW"

# ----------------------------------------------------------------------------
# 收口
# ----------------------------------------------------------------------------
if [ "$errors" -gt 0 ]; then
  printf '❌ banned-words: %d category/categories failed\n' "$errors" >&2
  exit 1
fi
printf '✅ banned-words check passed\n'
