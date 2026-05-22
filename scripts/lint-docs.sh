#!/usr/bin/env bash
# lint-docs.sh — documentation cross-reference checker
#
# spec: launch-ticket notes §T8 #5 (5) 文档 lint
#
# 校验范围：当前现役文档（.dalek/pm/*.md 顶层，不含 archive/ 和 reviews/）
#   1) 任意 ".dalek/pm/<file>.md" 路径引用必须真实存在
#   2) markdown link  [text](path.md) 的相对 path 必须存在（外链 / 锚点 / 非 .md 不查）
#
# 历史文档（archive/ + reviews/）grandfather 不扫，理由：
#   - archive 内是 M1.0-M1.2 历史 spec，路径已物理迁移，逐条修补成本高
#   - reviews 内是 round-N 评审记录，与现役文档无交叉风险
#
# 不做：§X.Y 章节存在性校验（spec 内含大量历史 §X.Y 跨文档引用，工程量大；M2+ 再加）

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

errors=0
fail() {
  errors=$((errors + 1))
  printf '❌ docs: %s\n' "$*" >&2
}

if [ ! -d .dalek/pm ]; then
  printf '[docs] [skip] .dalek/pm/ not present\n'
  exit 0
fi

# 列出现役 .dalek/pm/*.md（顶层，不含 archive/ reviews/）
mapfile -t LIVE_DOCS < <(find .dalek/pm -maxdepth 1 -type f -name '*.md' | sort)
if [ ${#LIVE_DOCS[@]} -eq 0 ]; then
  printf '[docs] [skip] no live .dalek/pm/*.md found\n'
  exit 0
fi

# ----------------------------------------------------------------------------
# 1) ".dalek/pm/<file>.md" 路径引用必须存在
# ----------------------------------------------------------------------------
while IFS= read -r match; do
  src="${match%%:*}"
  rest="${match#*:}"
  lineno="${rest%%:*}"
  ref="${rest#*:}"
  # grep -o 已经把匹配限制在字符集内，无需再修剪
  if [ ! -f "$ref" ]; then
    fail "$src:$lineno → missing .dalek/pm reference: '$ref'"
  fi
done < <(
  grep -Eno '\.dalek/pm/[A-Za-z0-9_/-]+(\.[A-Za-z0-9_/-]+)*\.md' "${LIVE_DOCS[@]}" 2>/dev/null \
    | sort -u
)

# ----------------------------------------------------------------------------
# 2) markdown link [text](path.md) 的相对路径必须存在
# ----------------------------------------------------------------------------
while IFS= read -r match; do
  src="${match%%:*}"
  rest="${match#*:}"
  lineno="${rest%%:*}"
  payload="${rest#*:}"
  # 抽 path：$payload 形如 "[label](path.md#anchor)"
  path=$(printf '%s' "$payload" | sed -E 's#.*\]\(([^)]+)\).*#\1#')
  # 跳过外链
  case "$path" in
    http://*|https://*|mailto:*|//*|'#'*) continue ;;
  esac
  # 砍 anchor
  path="${path%%#*}"
  [ -z "$path" ] && continue
  case "$path" in
    *.md) ;;
    *)    continue ;;
  esac
  dir="$(dirname "$src")"
  # 绝对路径（以 / 开头）→ 取仓库根
  case "$path" in
    /*) candidate="${path#/}" ;;
    *)  candidate="$dir/$path" ;;
  esac
  # 简单规范化：消除 ./ 与 a/../
  candidate="$(printf '%s' "$candidate" | sed -E 's#/\./#/#g; s#^\./##')"
  # 处理 ../
  while [[ "$candidate" == */*/../* ]] || [[ "$candidate" == */../* ]]; do
    new="$(printf '%s' "$candidate" | sed -E 's#[^/]+/\.\./##')"
    [ "$new" = "$candidate" ] && break
    candidate="$new"
  done
  if [ ! -f "$candidate" ]; then
    fail "$src:$lineno → broken markdown link: '$path' (resolved to '$candidate')"
  fi
done < <(
  grep -Eno '\[[^]]+\]\([^)]+\.md(#[^)]*)?\)' "${LIVE_DOCS[@]}" 2>/dev/null \
    | sort -u
)

if [ "$errors" -gt 0 ]; then
  printf '❌ docs: %d cross-reference error(s)\n' "$errors" >&2
  exit 1
fi
printf '✅ docs lint passed\n'
