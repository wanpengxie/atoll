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
# otherwise it is downloaded from the matching release. The public OSS mirror
# is tried first and GitHub remains the fallback. Every route then runs the same
# wizard — a user installing and a developer testing must not be walking
# different code.
#
#   ATOLL_VERSION=v0.01   pin a release instead of taking the latest
#   ATOLL_BIN=/path/atoll  use a binary you already have
#   ATOLL_DOWNLOAD_BASE=https://...  override the preferred release mirror
#
# What it does, in order:
#   0. acquire     — find or fetch the binary (see above)
#   1. preflight   — previous install, agent CLIs (codex/claude), port, home
#   2. steward     — pick which detected agent becomes c0's steward
#   3. password    — root password (typed twice, or generated)
#   4. home / addr / confirm
#   5. install     — writes <home>/atoll.env, runs `atoll up`; the password is
#                    shown once on the terminal and never written to disk
#
# 铁律：第 4 步"开始安装？"之前恒不落任何持久动作——不搬 home、不装二进制、
# 不建目录、不写文件。确认前只收集与只读检查；答 n 时机器与你来之前一样。

# 这是 bash 脚本，但用户十有八九会 `curl … | zsh`（macOS 默认壳）或 `| sh`。
# zsh 的 `read -p` 是"从 coprocess 读"而不是"打提示"，走到第一处提问就报
# `no coprocess` 退出，向导整个消失。装机脚本不能指望用户记得管给哪个壳：
# 不在 bash 里就自己搬进 bash 重跑。管道模式下脚本正从 stdin 流入、没有
# 文件可以 re-exec——把剩余部分落到临时文件再交给 bash（unlink 由 bash 侧
# 做，打开中的文件删掉是安全的）。这段守卫必须是 POSIX/zsh/dash 都能跑的
# 语法，其余部分才允许 bash 专有写法。
if [ -z "${BASH_VERSION:-}" ]; then
  command -v bash >/dev/null 2>&1 || { echo "atoll installer 需要 bash，请先安装 bash" >&2; exit 1; }
  # "$0 是个存在的文件"不等于"$0 是本脚本"：curl … | /bin/zsh 时 $0 就是
  # zsh 二进制本身，exec bash 去解析它必死。只有 $0 的头部带着本脚本的
  # 哨兵标记才算文件模式，其余一律按流模式处理。
  if [ -f "$0" ] && head -c 16384 "$0" 2>/dev/null | grep -q ATOLL_RESUME_MARK; then
    exec bash "$0" "$@"
  fi
  # mktemp 恒带显式模板：无参调用在新旧 BSD/GNU/busybox 之间行为不一。
  _atoll_self=$(mktemp "${TMPDIR:-/tmp}/atoll-install.XXXXXX") || exit 1
  cat > "$_atoll_self" 2>/dev/null || true
  # slurp 只有在壳逐字节读 stdin（bash/zsh 都是）时才拿得到完整的剩余脚本。
  # dash 这类壳按块预读，走到这里时后文已被吞掉一截——用守卫后紧跟的哨兵行
  # 校验临时文件是不是恰好从断点接上；缺损就从发布地址重取完整脚本。
  if [ "$(head -n1 "$_atoll_self" 2>/dev/null)" = "# ATOLL_RESUME_MARK" ]; then
    ATOLL_REEXEC_TMP="$_atoll_self" exec bash "$_atoll_self" "$@"
  fi
  _atoll_url="${ATOLL_INSTALL_URL:-https://raw.githubusercontent.com/wanpengxie/atoll/main/scripts/install.sh}"
  echo "  ! 当前 shell 预读了管道里的脚本，就地接管不完整；从 $_atoll_url 重新取一份交给 bash" >&2
  # 兜底下载 curl/wget 二选一：alpine/容器环境常常没有 curl、恒有 busybox wget。
  if command -v curl >/dev/null 2>&1 && curl -fsSL --connect-timeout 15 --max-time 60 "$_atoll_url" -o "$_atoll_self" 2>/dev/null; then
    ATOLL_REEXEC_TMP="$_atoll_self" exec bash "$_atoll_self" "$@"
  fi
  if command -v wget >/dev/null 2>&1 && wget -q -O "$_atoll_self" "$_atoll_url" 2>/dev/null; then
    ATOLL_REEXEC_TMP="$_atoll_self" exec bash "$_atoll_self" "$@"
  fi
  rm -f "$_atoll_self"
  echo "atoll installer: 取不到脚本；请改用  curl -f#SL $_atoll_url | bash" >&2
  exit 1
fi
# ATOLL_RESUME_MARK
set -euo pipefail

# 全部逻辑收进 main、文件最后一行才调用：curl|bash 下载中途被掐断时，bash
# 手里只有半个 main 的定义——语法错误，一行副作用都不会执行。恒不把有副作
# 用的语句放在 main 之外。
main() {

# 守卫留下的临时脚本副本。只删临时目录里的东西：这是环境变量，恒不拿它
# 当"删任意路径"的授权。
if [ -n "${ATOLL_REEXEC_TMP:-}" ]; then
  case "$ATOLL_REEXEC_TMP" in
    "${TMPDIR:-/tmp}"/*) rm -f "$ATOLL_REEXEC_TMP" ;;
  esac
fi

# set -u 下 $HOME 缺失会在深处报一句没人看得懂的 unbound variable；
# 在门口把话说清楚。
[ -n "${HOME:-}" ] || { echo "atoll installer 需要 HOME 环境变量（当前未设置）" >&2; exit 1; }

REPO_SLUG="${ATOLL_REPO:-wanpengxie/atoll}"
DOWNLOAD_BASE="${ATOLL_DOWNLOAD_BASE:-https://atoll-package.oss-cn-beijing.aliyuncs.com}"
DOWNLOAD_BASE="${DOWNLOAD_BASE%/}"

# 管道模式（curl|bash / 守卫 re-exec）下 $0 不是脚本文件，dirname 只会指到
# 无关目录——这时"旁边的二进制/源码树"判据全部失效，恒走 release 下载或
# 显式 ATOLL_BIN，绝不把 cwd 里恰好叫 atoll 的文件当发行件。
STREAMED=0
if [ -n "${ATOLL_REEXEC_TMP:-}" ] || [ ! -f "$0" ]; then STREAMED=1; fi

YES=0
TTY_FD=0
for a in "$@"; do case "$a" in
  --yes|-y) YES=1 ;;
  -h|--help)
    if [ "$STREAMED" = 0 ]; then sed -n '2,30p' "$0"; else echo "用法见 https://github.com/$REPO_SLUG"; fi
    exit 0 ;;
esac; done
# curl|bash 时 fd0 是脚本流，恒不动它；向导用自己的 fd(3) 跟终端说话。
# exec 的重定向自左向右生效，/dev/tty 打不开的原生报错要用命令组整体捂住。
if [ ! -t 0 ]; then
  if { exec 3</dev/tty; } 2>/dev/null; then
    TTY_FD=3
  else
    YES=1
    echo "  ! 没有可交互的终端，全部取默认值（等同 --yes）" >&2
  fi
fi

# CDPATH= 前缀：用户设了 CDPATH 时 cd 会额外打印目录名，污染命令替换。
REPO_ROOT="$(CDPATH= cd "$(dirname "$0")/.." 2>/dev/null && pwd || echo /nonexistent)"
SCRIPT_DIR="$(CDPATH= cd "$(dirname "$0")" 2>/dev/null && pwd || echo /nonexistent)"
DEFAULT_HOME="${ATOLL_HOME:-$HOME/.atoll}"
DEFAULT_ADDR="${ATOLL_ADDR:-127.0.0.1:8832}"

bold() { printf '\033[1m%s\033[0m\n' "$*"; }
ok()   { printf '  \033[32m✔\033[0m %s\n' "$*"; }
warn() { printf '  \033[33m!\033[0m %s\n' "$*"; }
bad()  { printf '  \033[31m✘\033[0m %s\n' "$*"; }
# 终端输入结束（Ctrl-D / 断开）不是"取默认值"：确认类问题把 EOF 当默认 y
# 会变成"挂断电话等于同意安装"。恒明确报错退出。
eof_abort() { echo; bad "终端输入已结束，未执行安装"; exit 1; }
ask()  { # ask VAR "prompt" "default"
  local __var=$1 __prompt=$2 __def=${3:-}
  if [ "$YES" = 1 ]; then printf -v "$__var" '%s' "$__def"; return; fi
  local __in
  read -r -u "$TTY_FD" -p "$__prompt${__def:+ [$__def]}: " __in || eof_abort
  printf -v "$__var" '%s' "${__in:-$__def}"
}
confirm() { # confirm "prompt" default(y|n)
  local def=${2:-y} in
  if [ "$YES" = 1 ]; then [ "$def" = y ]; return; fi
  read -r -u "$TTY_FD" -p "$1 [$([ "$def" = y ] && echo Y/n || echo y/N)]: " in || eof_abort
  in=${in:-$def}; [[ "$in" =~ ^[Yy] ]]
}

# 生命周期收口：脚本无论怎么退（成功、报错、信号），下载临时目录不留、
# 起了一半的节点不留。DL_TMP/UP_PID 是全局的——trap 引用函数局部变量是
# 上一版的病（返回后 unbound，正常退出反而报错）。
DL_TMP=""
UP_PID=""
cleanup() {
  if [ -n "$UP_PID" ] && kill -0 "$UP_PID" 2>/dev/null; then
    kill "$UP_PID" 2>/dev/null || true
    wait "$UP_PID" 2>/dev/null || true
  fi
  if [ -n "$DL_TMP" ]; then rm -rf "$DL_TMP" || true; fi
}
trap cleanup EXIT

echo
bold "atoll installer"
echo "  boot 只刻 c0；这个脚本负责问、检、写，然后交给 \`atoll up\`。"
echo

sha256_of() {
  if command -v sha256sum >/dev/null; then sha256sum "$1" | awk '{print $1}'
  elif command -v shasum >/dev/null; then shasum -a 256 "$1" | awk '{print $1}'
  fi
}

# Small control files are fetched before the archive so a flaky metadata request
# cannot throw away a package that already finished downloading. curl's retry
# set is deliberately limited to options present in the older curl shipped by
# macOS; each request still has a hard upper bound.
fetch_small() { # fetch_small URL DEST
  curl -fsSL --connect-timeout 15 --max-time 60 \
    --retry 4 --retry-delay 2 --retry-max-time 180 \
    -o "$2" "$1" </dev/null
}

# Keep a partial archive across transient failures within this installer run and
# resume it with Range. If an origin refuses Range (curl 33), discard only that
# partial and retry once from byte zero. The final sha256 remains authoritative.
fetch_archive() { # fetch_archive URL DEST
  local url=$1 dest=$2 attempt rc
  for attempt in 1 2 3 4 5; do
    if [ -t 2 ]; then
      if curl -fL --connect-timeout 15 --speed-limit 1024 --speed-time 60 \
          -C - --progress-bar -o "$dest" "$url" </dev/null; then return 0; else rc=$?; fi
    else
      if curl -fsSL --connect-timeout 15 --speed-limit 1024 --speed-time 60 \
          -C - -o "$dest" "$url" </dev/null; then return 0; else rc=$?; fi
    fi
    [ "$rc" = 33 ] && rm -f "$dest"
    [ "$attempt" = 5 ] && return "$rc"
    warn "下载中断，2 秒后从断点重试（${attempt}/5）"
    sleep 2
  done
}

# download_release fetches the build for this machine. mac ships as two builds
# because Intel and Apple Silicon are not interchangeable; nobody is asked which
# one — uname already knows.
# 只下载到临时目录并校验；装入 ~/.local/bin 发生在最终确认之后（install_binary）。
PENDING_BIN_SRC=""
download_release() {
  command -v curl >/dev/null || { bad "需要 curl"; exit 1; }
  local os arch ver name base want got rc mirror_latest github_latest candidate
  local -a candidates
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
    echo "  … 查询最新发行版（OSS 镜像）"
    mirror_latest=$(mktemp "${TMPDIR:-/tmp}/atoll-latest.XXXXXX")
    if fetch_small "$DOWNLOAD_BASE/releases/latest" "$mirror_latest" 2>/dev/null; then
      ver=$(tr -d '[:space:]' < "$mirror_latest")
    fi
    rm -f "$mirror_latest"
    # 镜像尚未配置或暂时不可达时，仍可从 GitHub 的 latest 重定向取版本。
    case "$ver" in
      v[0-9]*) ;;
      *)
        echo "  … OSS 不可用，回退查询 GitHub"
        github_latest=$(curl -fsSLI --connect-timeout 15 --max-time 60 \
          --retry 3 --retry-delay 2 --retry-max-time 120 \
          -o /dev/null -w '%{url_effective}' \
          "https://github.com/$REPO_SLUG/releases/latest" 2>/dev/null </dev/null || true)
        ver="${github_latest##*/}"
        ;;
    esac
    case "$ver" in ""|latest) bad "查不到最新发行版；用 ATOLL_VERSION=<tag> 指定一个"; exit 1 ;; esac
  fi
  case "$ver" in v[0-9]*) ;; *) bad "发行版号不合法：$ver"; exit 1 ;; esac
  name="atoll_${ver}_${os}_${arch}"
  DL_TMP=$(mktemp -d "${TMPDIR:-/tmp}/atoll-dl.XXXXXX")

  # 先拿同源校验清单，再碰大包。一次安装恒从同一个 origin 取清单和包，
  # 不把两个镜像在不同时间的内容拼起来；sha256 是最后一道硬门。
  candidates=(
    "$DOWNLOAD_BASE/releases/$ver"
    "https://github.com/$REPO_SLUG/releases/download/$ver"
  )
  base=""
  for candidate in "${candidates[@]}"; do
    echo "  … 检查发行源 ${candidate%%/releases/*}"
    if fetch_small "$candidate/checksums.txt" "$DL_TMP/checksums.txt" 2>/dev/null; then
      base="$candidate"
      break
    fi
  done
  [ -n "$base" ] || { bad "OSS 和 GitHub 都拿不到 checksums.txt，装机中止"; exit 1; }
  want=$(awk -v f="$name.tar.gz" '$2==f || $2=="*"f {print $1; exit}' "$DL_TMP/checksums.txt")
  [ -n "$want" ] || { bad "checksums.txt 里没有 $name.tar.gz 的条目，装机中止"; exit 1; }

  # 下载进度：终端上交给 curl 的进度条；没有终端（CI、docker exec、重定向）
  # 时 --progress-bar 会完全静默，就自己每 5 秒报一次已下载量——慢可以，
  # 哑不行，用户必须能看出它活着。
  # 报大小的 HEAD 是纯装饰：探测失败恒不挡下载（total=0 就不报而已）。
  local size_note total watcher
  total=$(curl -fsIL --connect-timeout 15 --max-time 20 "$base/$name.tar.gz" 2>/dev/null </dev/null \
          | tr -d '\r' | awk 'tolower($1)=="content-length:"{n=$2} END{print n+0}') || total=0
  size_note=""; [ "${total:-0}" -gt 0 ] && size_note="（$((total/1024/1024)) MB）"
  echo "  ↓ $name.tar.gz $size_note"
  if [ -t 2 ]; then
    fetch_archive "$base/$name.tar.gz" "$DL_TMP/$name.tar.gz" \
      || { bad "下载失败：$base/$name.tar.gz"; exit 1; }
  else
    ( while [ ! -f "$DL_TMP/.done" ]; do
        sleep 5
        [ -f "$DL_TMP/.done" ] && break
        got=$(wc -c < "$DL_TMP/$name.tar.gz" 2>/dev/null || echo 0)
        if [ "${total:-0}" -gt 0 ]; then echo "    … $((got/1024)) KB / $((total/1024)) KB"; else echo "    … 已下载 $((got/1024)) KB"; fi
      done ) &
    watcher=$!
    if fetch_archive "$base/$name.tar.gz" "$DL_TMP/$name.tar.gz"; then rc=0; else rc=$?; fi
    : > "$DL_TMP/.done"; wait "$watcher" 2>/dev/null || true
    [ "$rc" = 0 ] || { bad "下载失败：$base/$name.tar.gz"; exit 1; }
  fi

  # 校验 fail-closed：README 承诺了 sha256 校验，那它就恒不许被静默跳过。
  # 包和清单来自同一个 release，这道校验防的是传输截断和坏包，不是有意
  # 投毒——那要靠签名，这个版本还没有。
  got=$(sha256_of "$DL_TMP/$name.tar.gz")
  [ -n "$got" ] || { bad "机器上没有 sha256sum/shasum，无法校验，装机中止"; exit 1; }
  if [ "$want" != "$got" ]; then
    bad "sha256 不符——包在路上坏了，装机中止"
    echo "    期望 $want"
    echo "    实际 $got"
    exit 1
  fi
  ok "sha256 校验通过"

  tar -xzf "$DL_TMP/$name.tar.gz" -C "$DL_TMP" </dev/null
  PENDING_BIN_SRC="$DL_TMP/$name/atoll"
  [ -x "$PENDING_BIN_SRC" ] || { bad "包里没有可执行的 atoll"; exit 1; }
  ATOLL_BIN="$PENDING_BIN_SRC"
  # 变量紧贴全角字符时恒用 ${var} 花括号形：macOS bash 3.2 的旧解析器会把
  # 全角字符的首字节咬进变量名（set -u 下直接 unbound variable 崩掉）。
  ok "已取到 ${ver}（确认安装后装入 ${ATOLL_INSTALL_DIR:-$HOME/.local/bin}）"
}

# 确认之后才把下载的二进制装进用户目录；写法是临时文件 + mv，恒不原地
# 覆写——写一半断掉不能留下半个二进制。
install_binary() {
  [ -n "$PENDING_BIN_SRC" ] || return 0
  local dest="${ATOLL_INSTALL_DIR:-$HOME/.local/bin}"
  mkdir -p "$dest"
  cp "$PENDING_BIN_SRC" "$dest/.atoll.new"
  chmod 0755 "$dest/.atoll.new"
  mv -f "$dest/.atoll.new" "$dest/atoll"
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
elif [ "$STREAMED" = 0 ] && [ -x "$SCRIPT_DIR/atoll" ]; then
  ATOLL_BIN="$SCRIPT_DIR/atoll"
  ok "发行包里的二进制"
elif [ "$STREAMED" = 0 ] && [ -f "$REPO_ROOT/go.mod" ] && command -v go >/dev/null; then
  ATOLL_BIN="$REPO_ROOT/bin/atoll"
  if [ ! -x "$ATOLL_BIN" ]; then
    warn "源码树里还没编过"
    confirm "  现在 make build 编一个？" y && (cd "$REPO_ROOT" && make build </dev/null)
  fi
  [ -x "$ATOLL_BIN" ] || { bad "没有可用的 atoll 二进制（设 ATOLL_BIN 或先 make build）"; exit 1; }
  ok "源码树编出来的二进制"
else
  download_release
fi
echo "    $("$ATOLL_BIN" version </dev/null 2>/dev/null || echo '版本未知（旧二进制）')"
echo

# ---------------------------------------------------------------- 1. preflight
bold "1/5 预检"

ask HOME_DIR "  安装目录（node home）" "$DEFAULT_HOME"
# 只展开孤立的 ~ 和 ~/ 前缀。~alice 这种形拼不出正确的家目录（bash 参数
# 展开会造出 /home/你alice），明确拒绝好过安静装错地方。
case "$HOME_DIR" in
  "~")     HOME_DIR="$HOME" ;;
  "~/"*)   HOME_DIR="$HOME/${HOME_DIR#\~/}" ;;
  "~"*)    bad "不支持 ~user 形式的路径，请写绝对路径"; exit 1 ;;
esac
PREV_MARKER=""
if [ -f "$HOME_DIR/server/registry.db" ] && command -v sqlite3 >/dev/null; then
  PREV_MARKER=$(sqlite3 "$HOME_DIR/server/registry.db" "select installed_at from atoll_install where id=1" </dev/null 2>/dev/null || true)
elif [ -f "$HOME_DIR/server/registry.db" ]; then
  PREV_MARKER="unknown"
fi
fmt_epoch() { # GNU date takes -d @<epoch>, BSD/macOS takes -r <epoch>
  date -d "@$1" '+%F %T' 2>/dev/null || date -r "$1" '+%F %T' 2>/dev/null || printf '%s' "$1"
}
RESET_HOME=0
if [ -n "$PREV_MARKER" ]; then
  when="$PREV_MARKER"; [ "$PREV_MARKER" != unknown ] && when=$(fmt_epoch $((PREV_MARKER/1000)))
  warn "检测到已安装的 atoll：${HOME_DIR}（装于 ${when}）"
  echo "    1.0 之前没有升级/兼容路径：库形变了就重装。两种处理："
  echo "      [o] 直接打开这个实例（不重装，密码/agent 保持原样）"
  echo "      [r] 把它挪到 ${HOME_DIR}.bak-<时间> 后全新安装"
  ask PREV_ACTION "  怎么处理" "o"
  case "$PREV_ACTION" in
    r|R) RESET_HOME=1; ok "确认安装后将挪到 ${HOME_DIR}.bak-<时间>";;
    *)   ok "将直接打开既有实例（跳过密码/steward 设置）"; OPEN_EXISTING=1;;
  esac
fi
# an existing atoll.env is the memory of a previous install: its values are the defaults now.
EXISTING_ADDR=""; EXISTING_STEWARD=""
if [ -f "$HOME_DIR/atoll.env" ]; then
  EXISTING_ADDR=$(sed -n 's/^ATOLL_ADDR=//p' "$HOME_DIR/atoll.env" | tail -1)
  EXISTING_STEWARD=$(sed -n 's/^ATOLL_STEWARD=//p' "$HOME_DIR/atoll.env" | tail -1)
fi
# 确认前只做只读检查：目录已存在就查它本身，不存在就查最近的既有父目录。
# mkdir 恒在第 5 步。
_probe="$HOME_DIR"
while [ ! -e "$_probe" ]; do _probe=$(dirname "$_probe"); done
[ -w "$_probe" ] || { bad "$_probe 不可写，装不进 $HOME_DIR"; exit 1; }
ok "home 可写: $HOME_DIR"

# agent CLIs — deliberately blunt: binary present? version? credential present?
# Per-agent state lives in AGENT_OK_<name>/AGENT_NOTE_<name>, not an associative
# array: macOS still ships bash 3.2, which has no `declare -A`.
# 所有外部探测命令恒关 stdin（</dev/null）：curl|bash 下 fd0 是脚本流，
# 子命令读一口 stdin 吃掉的就是脚本接下来的行。探测失败恒不挡安装。
AGENT_OK_codex=0;  AGENT_NOTE_codex=""
AGENT_OK_claude=0; AGENT_NOTE_claude=""
agent_set()  { eval "AGENT_OK_$1=\$2; AGENT_NOTE_$1=\$3"; }
agent_ok()   { local __v="AGENT_OK_$1";   printf '%s' "${!__v}"; }
agent_note() { local __v="AGENT_NOTE_$1"; printf '%s' "${!__v}"; }
detect_codex() {
  if ! command -v codex >/dev/null; then agent_set codex 0 "未安装（npm i -g @openai/codex）"; return; fi
  local v; v=$(codex --version </dev/null 2>/dev/null | head -1) || true
  if [ -s "$HOME/.codex/auth.json" ] || codex login status </dev/null >/dev/null 2>&1; then
    agent_set codex 1 "$v · 已登录"
  else
    agent_set codex 0 "$v · 未登录（codex login）"
  fi
}
detect_claude() {
  if ! command -v claude >/dev/null; then agent_set claude 0 "未安装（npm i -g @anthropic-ai/claude-code）"; return; fi
  local v; v=$(claude --version </dev/null 2>/dev/null | head -1) || true
  if [ -s "$HOME/.claude/.credentials.json" ] || claude auth status </dev/null 2>/dev/null | grep -q '"loggedIn": *true'; then
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
port_busy() { # port_busy PORT [HOST]
  # 恒不解析 ss/lsof：各家方言不同（busybox 的 lsof 不认参数、恒返回成功，
  # 拿它判端口就是恒报被占）。bash 内建 /dev/tcp 直连探测零外部依赖，
  # 3.2 起就有：连得上 = 有人在听。
  local host="${2:-127.0.0.1}"
  case "$host" in 0.0.0.0|::|"") host=127.0.0.1 ;; esac
  (exec 3<>"/dev/tcp/$host/$1") 2>/dev/null
}
# 占用的端口上起不来节点，"警告一声照样装"只是把失败推迟到第 5 步。
# 循环到给出一个空闲端口为止；无人值守模式没得问，直接明确失败。
PORT="${ADDR##*:}"
while port_busy "$PORT" "${ADDR%:*}"; do
  warn "端口 $PORT 已被占用；换一个或先停掉占用者再填同一个"
  [ "$YES" = 1 ] && { bad "默认端口被占用且无人值守，装机中止（设 ATOLL_ADDR 换端口）"; exit 1; }
  ask ADDR "  监听地址" "$ADDR"
  PORT="${ADDR##*:}"
done
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
bold "3/5 root 密码（账号 root，全名 root@atoll.local）"
PASSWORD=""
if [ "$YES" = 1 ]; then
  PASSWORD="${ATOLL_ROOT_PASSWORD:-}"
else
  while :; do
    read -r -s -u "$TTY_FD" -p "  输入密码（回车=随机生成）: " p1 || eof_abort
    echo
    if [ -z "$p1" ]; then break; fi
    read -r -s -u "$TTY_FD" -p "  再输一遍: " p2 || eof_abort
    echo
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
[ "$RESET_HOME" = 1 ] && echo "                （既有实例将先挪到 ${HOME_DIR}.bak-<时间>）"
echo "  addr        : $ADDR"
[ -n "$STEWARD" ] && echo "  steward     : $STEWARD"
echo "  web UI      : http://$ADDR （在二进制里，跟 API 同一个端口）"
echo "  token       : $HOME_DIR/server/atoll-token（本地自动化用 Bearer）"
[ -n "$PASSWORD" ] && echo "  密码        : 安装结束时显示一次，只此一次（本机只留 bcrypt 哈希，不留明码）"
confirm "  开始安装并启动？" y || { echo "已取消（机器上没有落任何东西）"; exit 0; }
echo

# ---------------------------------------------------------------- 5. install
bold "5/5 安装 + 启动"
install_binary
if [ "$RESET_HOME" = 1 ] && [ -e "$HOME_DIR" ]; then
  BAK="${HOME_DIR}.bak-$(date +%Y%m%d-%H%M%S)"
  mv "$HOME_DIR" "$BAK"
  ok "既有实例已挪到 $BAK"
fi
mkdir -p "$HOME_DIR/server"
# 配置文件写临时名再 mv：既有实例的 atoll.env 恒不被写一半的文件顶掉。
{
  echo "# written by scripts/install.sh $(date '+%F %T'); a bare 'atoll up --dir $HOME_DIR' reads this"
  echo "ATOLL_ADDR=$ADDR"
  [ -n "$STEWARD" ] && echo "ATOLL_STEWARD=$STEWARD"
} > "$HOME_DIR/.atoll.env.new"
mv -f "$HOME_DIR/.atoll.env.new" "$HOME_DIR/atoll.env"
# 密码恒不落盘。节点存的是 bcrypt 哈希，明码只在下面向你显示一次。
#
# 早先的版本会把它写进 server/root-password。那份明码只在**这次运行会把密码
# 显示给你**时才删——否则删掉就是把人锁在外面拿走他唯一的副本。直接打开既有
# 实例（不设密码）时只提醒，不动它。
if [ -f "$HOME_DIR/server/root-password" ]; then
  if [ -n "$PASSWORD" ]; then
    rm -f "$HOME_DIR/server/root-password"
    ok "已删除旧版留下的明码文件 server/root-password（新密码在下面显示）"
  else
    warn "$HOME_DIR/server/root-password 是旧版留下的明码副本。"
    warn "记下里面的密码后请自行删除；节点本身只用 bcrypt 哈希，不需要它。"
  fi
fi
ok "已写 $HOME_DIR/atoll.env"

UP_ARGS=(up --dir "$HOME_DIR" --addr "$ADDR")
[ -n "$STEWARD" ] && [ "${OPEN_EXISTING:-0}" != 1 ] && UP_ARGS+=(--steward "$STEWARD")
LOG="$HOME_DIR/atoll-up.log"
# 密码走环境变量（atoll up 的 flag 为空时读 ATOLL_ROOT_PASSWORD），恒不进
# argv——命令行参数在进程表里对同机观察者可见。子进程关掉 stdin 和向导的
# fd3，别让节点攥着终端。
ATOLL_ROOT_PASSWORD="$PASSWORD" "$ATOLL_BIN" "${UP_ARGS[@]}" </dev/null >"$LOG" 2>&1 3<&- &
UP_PID=$!
trap 'kill "$UP_PID" 2>/dev/null || true; wait "$UP_PID" 2>/dev/null || true; exit 0' INT TERM
echo "  … 等待节点起来（http://$ADDR/healthz）"
# 整数计数 + 整数 sleep：seq 和小数 sleep 都是 busybox 可裁剪的特性，
# 这里失败发生在节点已启动之后，误判会把好好的节点杀掉——不赌。
i=0
while [ "$i" -lt 30 ]; do
  i=$((i+1))
  if curl -fs --max-time 2 "http://$ADDR/healthz" >/dev/null 2>&1 </dev/null; then break; fi
  if ! kill -0 "$UP_PID" 2>/dev/null; then bad "atoll up 退出了，看日志：$LOG"; tail -20 "$LOG"; exit 1; fi
  [ $((i % 5)) = 0 ] && echo "    … 还在等（${i}s）"
  sleep 1
done
if ! curl -fs --max-time 2 "http://$ADDR/healthz" >/dev/null 2>&1 </dev/null; then
  bad "30s 内没起来；已停掉半启动的节点，看日志：$LOG"
  tail -20 "$LOG"
  exit 1
fi

echo
bold "atoll 已在跑"
echo "  打开        : http://$ADDR    （账号填 root 即可）"
echo "  登录 API    : POST http://$ADDR/api/identity/login  {\"email\":\"root\",\"password\":<密码>}"
if [ -n "$PASSWORD" ]; then
  echo "  密码        : $PASSWORD"
  echo "                ↑ 现在记下来。本机只存 bcrypt 哈希，没有第二份，之后读不出来。"
  echo "                忘了就用新密码重新引导一次：ATOLL_ROOT_PASSWORD=新密码 atoll up --dir $HOME_DIR"
fi
echo "  token       : $HOME_DIR/server/atoll-token"
echo "  日志        : $LOG"
[ -n "$STEWARD" ] && echo "  c0 里的 steward 是 ${STEWARD}——登录后在 c0 直接对它说话"
echo "  以后启动    : $ATOLL_BIN up --dir $HOME_DIR   （读 atoll.env，同一实例）"
echo "  Ctrl-C 停止。"
wait "$UP_PID" || {
  rc=$?
  UP_PID=""
  bad "节点退出（${rc}）；日志：$LOG"
  tail -5 "$LOG"
  exit 1
}
UP_PID=""

}
main "$@"
