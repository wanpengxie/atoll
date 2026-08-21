#!/usr/bin/env bash
# atoll interactive installer.
#
# This is the layer above boot: it asks, checks, and writes; the binary
# (`atoll up`) carves c0 and runs the node. Nothing here touches the ledgers.
#
#   curl -fsSL https://raw.githubusercontent.com/wanpengxie/atoll/main/scripts/install.sh | bash
#   ./install.sh                  # from inside a release tarball
#   scripts/install.sh            # from a source checkout
#   ... --yes                     # accept every default (CI / scripted)
#
# Where the binary comes from is decided, not asked: one lying next to this
# script is it (a release tarball); otherwise a source checkout builds one;
# otherwise it is downloaded from the matching GitHub release. Every route then
# runs the same wizard — a user installing and a developer testing must not be
# walking different code.
#
#   ATOLL_VERSION=v0.01   pin a release instead of taking the latest
#   ATOLL_BIN=/path/atoll  use a binary you already have
#
# What it does, in order:
#   0. acquire     — find or fetch the binary (see above)
#   1. preflight   — previous install, agent CLIs (codex/claude), port, home
#   2. steward     — pick which detected agent becomes c0's steward
#   3. password    — root password (typed twice, or generated)
#   4. home / addr / confirm
#   5. install     — writes <home>/atoll.env + <home>/server/root-password, runs `atoll up`
set -euo pipefail

REPO_SLUG="${ATOLL_REPO:-wanpengxie/atoll}"

# `curl ... | bash` hands the script a pipe for stdin, and every prompt below
# would read EOF from it — the wizard would silently answer itself. The terminal
# is still there, so take it back. With no terminal at all (CI), there is nobody
# to ask: take the defaults and say so.
YES=0
for a in "$@"; do case "$a" in --yes|-y) YES=1 ;; -h|--help) sed -n '2,28p' "$0"; exit 0 ;; esac; done
if [ ! -t 0 ]; then
  if (exec </dev/tty) 2>/dev/null; then
    exec </dev/tty
  else
    YES=1
    echo "  ! 没有可交互的终端，全部取默认值（等同 --yes）" >&2
  fi
fi

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
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

sha256_of() {
  if command -v sha256sum >/dev/null; then sha256sum "$1" | awk '{print $1}'
  elif command -v shasum >/dev/null; then shasum -a 256 "$1" | awk '{print $1}'
  fi
}

# download_release fetches the build for this machine. mac ships as two builds
# because Intel and Apple Silicon are not interchangeable; nobody is asked which
# one — uname already knows.
download_release() {
  command -v curl >/dev/null || { bad "需要 curl"; exit 1; }
  local os arch ver name base tmp want got dest
  os=$(uname -s | tr '[:upper:]' '[:lower:]')
  arch=$(uname -m)
  case "$arch" in
    x86_64|amd64)  arch=amd64 ;;
    arm64|aarch64) arch=arm64 ;;
    *) bad "没有为 $arch 出包"; exit 1 ;;
  esac
  case "$os" in darwin|linux) ;; *) bad "没有为 $os 出包"; exit 1 ;; esac

  ver="${ATOLL_VERSION:-}"
  if [ -z "$ver" ]; then
    ver=$(curl -fsSL "https://api.github.com/repos/$REPO_SLUG/releases/latest" \
          | sed -n 's/.*"tag_name"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/p' | head -1)
    [ -n "$ver" ] || { bad "查不到最新发行版；用 ATOLL_VERSION=<tag> 指定一个"; exit 1; }
  fi
  name="atoll_${ver}_${os}_${arch}"
  base="https://github.com/$REPO_SLUG/releases/download/$ver"
  tmp=$(mktemp -d)
  trap 'rm -rf "$tmp"' EXIT

  echo "  ↓ $name.tar.gz"
  curl -fL --progress-bar -o "$tmp/$name.tar.gz" "$base/$name.tar.gz" \
    || { bad "下载失败：$base/$name.tar.gz"; exit 1; }

  # 包和清单来自同一个 release，所以这道校验防的是传输截断和坏包，
  # 不是有意投毒——那要靠签名，这个版本还没有。
  if curl -fsSL -o "$tmp/checksums.txt" "$base/checksums.txt" 2>/dev/null; then
    want=$(awk -v f="$name.tar.gz" '$2==f || $2=="*"f {print $1; exit}' "$tmp/checksums.txt")
    got=$(sha256_of "$tmp/$name.tar.gz")
    if [ -n "$want" ] && [ -n "$got" ] && [ "$want" != "$got" ]; then
      bad "sha256 不符——包在路上坏了，装机中止"
      echo "    期望 $want"
      echo "    实际 $got"
      exit 1
    fi
    [ -n "$want" ] && ok "sha256 校验通过"
  else
    warn "拿不到 checksums.txt，跳过校验"
  fi

  tar -xzf "$tmp/$name.tar.gz" -C "$tmp"
  dest="${ATOLL_INSTALL_DIR:-$HOME/.local/bin}"
  mkdir -p "$dest"
  cp "$tmp/$name/atoll" "$dest/atoll"
  chmod 0755 "$dest/atoll"
  ATOLL_BIN="$dest/atoll"
  ok "已装到 $ATOLL_BIN"
  case ":$PATH:" in
    *":$dest:"*) ;;
    *) warn "$dest 不在 PATH 里；想直接敲 atoll 就加一行到 shell 配置： export PATH=\"$dest:\$PATH\"" ;;
  esac
}

# ---------------------------------------------------------------- 0. acquire
bold "0/5 取 atoll 二进制"

if [ -n "${ATOLL_BIN:-}" ]; then
  [ -x "$ATOLL_BIN" ] || { bad "ATOLL_BIN=$ATOLL_BIN 不是可执行文件"; exit 1; }
  ok "用你指定的二进制"
elif [ -x "$SCRIPT_DIR/atoll" ]; then
  ATOLL_BIN="$SCRIPT_DIR/atoll"
  ok "发行包里的二进制"
elif [ -f "$REPO_ROOT/go.mod" ] && command -v go >/dev/null; then
  ATOLL_BIN="$REPO_ROOT/bin/atoll"
  if [ ! -x "$ATOLL_BIN" ]; then
    warn "源码树里还没编过"
    confirm "  现在 make build 编一个？" y && (cd "$REPO_ROOT" && make build)
  fi
  [ -x "$ATOLL_BIN" ] || { bad "没有可用的 atoll 二进制（设 ATOLL_BIN 或先 make build）"; exit 1; }
  ok "源码树编出来的二进制"
else
  download_release
fi
echo "    $ATOLL_BIN"
echo "    $("$ATOLL_BIN" version 2>/dev/null || echo '版本未知（旧二进制）')"
echo

# ---------------------------------------------------------------- 1. preflight
bold "1/5 预检"

ask HOME_DIR "  安装目录（node home）" "$DEFAULT_HOME"
HOME_DIR="${HOME_DIR/#\~/$HOME}"
PREV_MARKER=""
if [ -f "$HOME_DIR/server/registry.db" ] && command -v sqlite3 >/dev/null; then
  PREV_MARKER=$(sqlite3 "$HOME_DIR/server/registry.db" "select installed_at from atoll_install where id=1" 2>/dev/null || true)
elif [ -f "$HOME_DIR/server/registry.db" ]; then
  PREV_MARKER="unknown"
fi
fmt_epoch() { # GNU date takes -d @<epoch>, BSD/macOS takes -r <epoch>
  date -d "@$1" '+%F %T' 2>/dev/null || date -r "$1" '+%F %T' 2>/dev/null || printf '%s' "$1"
}
if [ -n "$PREV_MARKER" ]; then
  when="$PREV_MARKER"; [ "$PREV_MARKER" != unknown ] && when=$(fmt_epoch $((PREV_MARKER/1000)))
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
# Per-agent state lives in AGENT_OK_<name>/AGENT_NOTE_<name>, not an associative
# array: macOS still ships bash 3.2, which has no `declare -A`.
AGENT_OK_codex=0;  AGENT_NOTE_codex=""
AGENT_OK_claude=0; AGENT_NOTE_claude=""
agent_set()  { eval "AGENT_OK_$1=\$2; AGENT_NOTE_$1=\$3"; }
agent_ok()   { local __v="AGENT_OK_$1";   printf '%s' "${!__v}"; }
agent_note() { local __v="AGENT_NOTE_$1"; printf '%s' "${!__v}"; }
detect_codex() {
  if ! command -v codex >/dev/null; then agent_set codex 0 "未安装（npm i -g @openai/codex）"; return; fi
  local v; v=$(codex --version 2>/dev/null | head -1)
  if [ -s "$HOME/.codex/auth.json" ] || codex login status >/dev/null 2>&1; then
    agent_set codex 1 "$v · 已登录"
  else
    agent_set codex 0 "$v · 未登录（codex login）"
  fi
}
detect_claude() {
  if ! command -v claude >/dev/null; then agent_set claude 0 "未安装（npm i -g @anthropic-ai/claude-code）"; return; fi
  local v; v=$(claude --version 2>/dev/null | head -1)
  if [ -s "$HOME/.claude/.credentials.json" ] || claude auth status 2>/dev/null | grep -q '"loggedIn": *true'; then
    agent_set claude 1 "$v · 已登录"
  else
    agent_set claude 0 "$v · 未登录（claude auth login）"
  fi
}
detect_codex; detect_claude
for a in codex claude; do
  if [ "$(agent_ok "$a")" = 1 ]; then ok "$a: $(agent_note "$a")"; else warn "$a: $(agent_note "$a")"; fi
done

ask ADDR "  监听地址" "${EXISTING_ADDR:-$DEFAULT_ADDR}"
PORT="${ADDR##*:}"
port_busy() { # ss on linux, lsof on macOS; no tool = no opinion
  if command -v ss >/dev/null; then
    ss -ltn 2>/dev/null | awk '{print $4}' | grep -q ":$1\$"
  elif command -v lsof >/dev/null; then
    lsof -nP -iTCP:"$1" -sTCP:LISTEN >/dev/null 2>&1
  else
    return 1
  fi
}
if port_busy "$PORT"; then
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
CANDIDATES=(); for a in codex claude; do [ "$(agent_ok "$a")" = 1 ] && CANDIDATES+=("$a"); done
if [ ${#CANDIDATES[@]} -eq 0 ]; then
  warn "没有检测到可用且已登录的 agent。可以先装完再补：装好 codex/claude 后重跑本脚本选 [r] 重装，或用 system.actor.template.create 加。"
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
echo "  web UI      : http://$ADDR （在二进制里，跟 API 同一个端口）"
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
echo "  打开        : http://$ADDR  （用 root@atoll.local 登录）"
echo "  登录 API    : POST http://$ADDR/api/identity/login  {\"email\":\"root@atoll.local\",\"password\":<密码>}"
[ -n "$PASSWORD" ] && echo "  密码        : 见 $HOME_DIR/server/root-password"
echo "  token       : $HOME_DIR/server/atoll-token"
echo "  日志        : $LOG"
[ -n "$STEWARD" ] && echo "  c0 里的 steward 是 $STEWARD——登录后在 c0 直接对它说话"
echo "  以后启动    : $ATOLL_BIN up --dir $HOME_DIR   （读 atoll.env，同一实例）"
echo "  Ctrl-C 停止。"
wait "$UP_PID"
