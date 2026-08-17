#!/usr/bin/env bash
# atoll interactive installer.
#
# This is the layer above boot: it asks, checks, and writes; the binary
# (`atoll up`) carves c0 and runs the node. Nothing here touches the ledgers.
#
#   scripts/install.sh            # interactive
#   scripts/install.sh --yes      # accept every default (CI / scripted)
#
# What it does, in order:
#   1. preflight   — binary, previous install, agent CLIs (codex/claude), port, home
#   2. steward     — pick which detected agent becomes c0's steward
#   3. password    — root password (typed twice, or generated)
#   4. home / addr / web reminder
#   5. install     — writes <home>/atoll.env + <home>/server/root-password, runs `atoll up`
set -euo pipefail

YES=0
for a in "$@"; do case "$a" in --yes|-y) YES=1 ;; -h|--help) sed -n '2,16p' "$0"; exit 0 ;; esac; done

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
ATOLL_BIN="${ATOLL_BIN:-$REPO_ROOT/bin/atoll}"
DEFAULT_HOME="${ATOLL_HOME:-$HOME/.atoll}"
DEFAULT_ADDR="${ATOLL_ADDR:-127.0.0.1:8832}"

bold() { printf '\033[1m%s\033[0m\n' "$*"; }
ok()   { printf '  \033[32m✔\033[0m %s\n' "$*"; }
warn() { printf '  \033[33m!\033[0m %s\n' "$*"; }
bad()  { printf '  \033[31m✘\033[0m %s\n' "$*"; }
ask()  { # ask VAR "prompt" "default"
  local __var=$1 __prompt=$2 __def=${3:-}
  if [ "$YES" = 1 ]; then printf -v "$__var" '%s' "$__def"; return; fi
  local __in
  read -r -p "$__prompt${__def:+ [$__def]}: " __in
  printf -v "$__var" '%s' "${__in:-$__def}"
}
confirm() { # confirm "prompt" default(y|n)
  local def=${2:-y} in
  if [ "$YES" = 1 ]; then [ "$def" = y ]; return; fi
  read -r -p "$1 [$([ "$def" = y ] && echo Y/n || echo y/N)]: " in
  in=${in:-$def}; [[ "$in" =~ ^[Yy] ]]
}

echo
bold "atoll installer"
echo "  boot 只刻 c0；这个脚本负责问、检、写，然后交给 \`atoll up\`。"
echo

# ---------------------------------------------------------------- 1. preflight
bold "1/5 预检"

if [ ! -x "$ATOLL_BIN" ]; then
  warn "找不到 $ATOLL_BIN"
  if [ -f "$REPO_ROOT/Makefile" ] && command -v go >/dev/null; then
    if confirm "  现在 make build 编一个？" y; then (cd "$REPO_ROOT" && make build) ; fi
  fi
  [ -x "$ATOLL_BIN" ] || { bad "没有可用的 atoll 二进制（设 ATOLL_BIN 或先 make build）"; exit 1; }
fi
ok "atoll 二进制: $ATOLL_BIN"

ask HOME_DIR "  安装目录（node home）" "$DEFAULT_HOME"
HOME_DIR="${HOME_DIR/#\~/$HOME}"
PREV_MARKER=""
if [ -f "$HOME_DIR/server/registry.db" ] && command -v sqlite3 >/dev/null; then
  PREV_MARKER=$(sqlite3 "$HOME_DIR/server/registry.db" "select installed_at from atoll_install where id=1" 2>/dev/null || true)
elif [ -f "$HOME_DIR/server/registry.db" ]; then
  PREV_MARKER="unknown"
fi
if [ -n "$PREV_MARKER" ]; then
  when="$PREV_MARKER"; [ "$PREV_MARKER" != unknown ] && when=$(date -d @$((PREV_MARKER/1000)) '+%F %T' 2>/dev/null || echo "$PREV_MARKER")
  warn "检测到已安装的 atoll：$HOME_DIR（装于 $when）"
  echo "    1.0 之前没有升级/兼容路径：库形变了就重装。两种处理："
  echo "      [o] 直接打开这个实例（不重装，密码/agent 保持原样）"
  echo "      [r] 把它挪到 ${HOME_DIR}.bak-<时间> 后全新安装"
  ask PREV_ACTION "  怎么处理" "o"
  case "$PREV_ACTION" in
    r|R) BAK="${HOME_DIR}.bak-$(date +%Y%m%d-%H%M%S)"; mv "$HOME_DIR" "$BAK"; ok "已挪到 $BAK";;
    *)   ok "将直接打开既有实例（跳过密码/steward 设置）"; OPEN_EXISTING=1;;
  esac
fi
# an existing atoll.env is the memory of a previous install: its values are the defaults now.
EXISTING_ADDR=""; EXISTING_STEWARD=""
if [ -f "$HOME_DIR/atoll.env" ]; then
  EXISTING_ADDR=$(sed -n 's/^ATOLL_ADDR=//p' "$HOME_DIR/atoll.env" | tail -1)
  EXISTING_STEWARD=$(sed -n 's/^ATOLL_STEWARD=//p' "$HOME_DIR/atoll.env" | tail -1)
fi
mkdir -p "$HOME_DIR"
[ -w "$HOME_DIR" ] || { bad "$HOME_DIR 不可写"; exit 1; }
ok "home 可写: $HOME_DIR"

# agent CLIs — deliberately blunt: binary present? version? credential present?
declare -A AGENT_OK AGENT_NOTE
detect_codex() {
  if ! command -v codex >/dev/null; then AGENT_OK[codex]=0; AGENT_NOTE[codex]="未安装（npm i -g @openai/codex）"; return; fi
  local v; v=$(codex --version 2>/dev/null | head -1)
  if [ -s "$HOME/.codex/auth.json" ] || codex login status >/dev/null 2>&1; then
    AGENT_OK[codex]=1; AGENT_NOTE[codex]="$v · 已登录"
  else
    AGENT_OK[codex]=0; AGENT_NOTE[codex]="$v · 未登录（codex login）"
  fi
}
detect_claude() {
  if ! command -v claude >/dev/null; then AGENT_OK[claude]=0; AGENT_NOTE[claude]="未安装（npm i -g @anthropic-ai/claude-code）"; return; fi
  local v; v=$(claude --version 2>/dev/null | head -1)
  if [ -s "$HOME/.claude/.credentials.json" ] || claude auth status 2>/dev/null | grep -q '"loggedIn": *true'; then
    AGENT_OK[claude]=1; AGENT_NOTE[claude]="$v · 已登录"
  else
    AGENT_OK[claude]=0; AGENT_NOTE[claude]="$v · 未登录（claude auth login）"
  fi
}
detect_codex; detect_claude
for a in codex claude; do
  if [ "${AGENT_OK[$a]}" = 1 ]; then ok "$a: ${AGENT_NOTE[$a]}"; else warn "$a: ${AGENT_NOTE[$a]}"; fi
done

ask ADDR "  监听地址" "${EXISTING_ADDR:-$DEFAULT_ADDR}"
PORT="${ADDR##*:}"
if command -v ss >/dev/null && ss -ltn 2>/dev/null | awk '{print $4}' | grep -q ":${PORT}\$"; then
  warn "端口 $PORT 已被占用；换一个或先停掉占用者"
  ask ADDR "  监听地址" "$ADDR"
fi
ok "监听地址: $ADDR"
echo

if [ "${OPEN_EXISTING:-0}" = 1 ]; then
  STEWARD="$EXISTING_STEWARD"; PASSWORD=""
else
# ---------------------------------------------------------------- 2. steward
bold "2/5 c0 的 steward（默认 agent）"
CANDIDATES=(); for a in codex claude; do [ "${AGENT_OK[$a]}" = 1 ] && CANDIDATES+=("$a"); done
if [ ${#CANDIDATES[@]} -eq 0 ]; then
  warn "没有检测到可用且已登录的 agent。可以先装完再补：装好 codex/claude 后重跑本脚本选 [r] 重装，或用 actor.template.register 加。"
  ask STEWARD "  仍要写哪个 class 作 steward（codex/claude，留空=codex）" "codex"
else
  ask STEWARD "  选一个作 steward（可选：${CANDIDATES[*]}）" "${CANDIDATES[0]}"
fi
case "$STEWARD" in codex|claude) ;; *) bad "steward 只能是 codex 或 claude"; exit 1;; esac
ok "steward = $STEWARD"
echo

# ---------------------------------------------------------------- 3. password
bold "3/5 root 密码（账号 root@atoll.local）"
PASSWORD=""
if [ "$YES" = 1 ]; then
  PASSWORD="${ATOLL_ROOT_PASSWORD:-}"
else
  while :; do
    read -r -s -p "  输入密码（回车=随机生成）: " p1; echo
    if [ -z "$p1" ]; then break; fi
    read -r -s -p "  再输一遍: " p2; echo
    [ "$p1" = "$p2" ] && { PASSWORD="$p1"; break; }
    warn "两次不一致，再来"
  done
fi
if [ -z "$PASSWORD" ]; then
  PASSWORD=$(head -c 24 /dev/urandom | base64 | tr -dc 'A-Za-z0-9' | head -c 20)
  ok "已生成随机密码"
else
  ok "密码已设置"
fi
echo
fi

# ---------------------------------------------------------------- 4. reminders
bold "4/5 确认"
echo "  home        : $HOME_DIR"
echo "  addr        : $ADDR"
[ -n "$STEWARD" ] && echo "  steward     : $STEWARD"
echo "  web UI      : 独立仓 atoll-web（对着 http://$ADDR 起：cd ../atoll-web && npm run dev）"
echo "  token       : $HOME_DIR/server/atoll-token（本地自动化用 Bearer）"
[ -n "$PASSWORD" ] && echo "  密码文件    : $HOME_DIR/server/root-password（0600）"
confirm "  开始安装并启动？" y || { echo "已取消"; exit 0; }
echo

# ---------------------------------------------------------------- 5. install
bold "5/5 安装 + 启动"
mkdir -p "$HOME_DIR/server"
{
  echo "# written by scripts/install.sh $(date '+%F %T'); a bare 'atoll up --dir $HOME_DIR' reads this"
  echo "ATOLL_ADDR=$ADDR"
  [ -n "$STEWARD" ] && echo "ATOLL_STEWARD=$STEWARD"
} > "$HOME_DIR/atoll.env"
if [ -n "$PASSWORD" ]; then
  (umask 077; printf '%s\n' "$PASSWORD" > "$HOME_DIR/server/root-password")
fi
ok "已写 $HOME_DIR/atoll.env"

UP_ARGS=(up --dir "$HOME_DIR" --addr "$ADDR")
[ -n "$STEWARD" ] && [ "${OPEN_EXISTING:-0}" != 1 ] && UP_ARGS+=(--steward "$STEWARD")
[ -n "$PASSWORD" ] && UP_ARGS+=(--root-password "$PASSWORD")
LOG="$HOME_DIR/atoll-up.log"
"$ATOLL_BIN" "${UP_ARGS[@]}" >"$LOG" 2>&1 &
UP_PID=$!
trap 'kill "$UP_PID" 2>/dev/null; wait "$UP_PID" 2>/dev/null; exit 0' INT TERM
for i in $(seq 1 60); do
  if curl -fs "http://$ADDR/healthz" >/dev/null 2>&1; then break; fi
  if ! kill -0 "$UP_PID" 2>/dev/null; then bad "atoll up 退出了，看日志：$LOG"; tail -20 "$LOG"; exit 1; fi
  sleep 0.5
done
if ! curl -fs "http://$ADDR/healthz" >/dev/null 2>&1; then bad "30s 内没起来，看日志：$LOG"; exit 1; fi

echo
bold "atoll 已在跑"
echo "  登录        : POST http://$ADDR/api/identity/login  {\"email\":\"root@atoll.local\",\"password\":<密码>}"
[ -n "$PASSWORD" ] && echo "  密码        : 见 $HOME_DIR/server/root-password"
echo "  token       : $HOME_DIR/server/atoll-token"
echo "  日志        : $LOG"
[ -n "$STEWARD" ] && echo "  c0 里的 steward 是 $STEWARD——登录后在 c0 直接对它说话"
echo "  以后启动    : $ATOLL_BIN up --dir $HOME_DIR   （读 atoll.env，同一实例）"
echo "  Ctrl-C 停止。"
wait "$UP_PID"
