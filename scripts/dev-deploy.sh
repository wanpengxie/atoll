#!/usr/bin/env bash
# dev-deploy.sh — single-command rebuild + rolling restart for local stack.
#
# Steps (fail-fast on each):
#   1. source .env (验证 secrets / origins 都齐)
#   2. rebuild Go binaries (server / daemon / worker / cli)
#   3. rebuild extension zip + ui dist
#   4. graceful stop worker → daemon → server
#   5. start server, wait :8832 listen
#   6. start daemon, wait daemonbus.ws.connected event
#   7. print final pid + binary mtime + commit hash + WS epoch
#
# Usage:
#   bash scripts/dev-deploy.sh              # full rebuild + restart
#   SKIP_BUILD=1 bash scripts/dev-deploy.sh # skip rebuild, only restart
#   SKIP_UI=1    bash scripts/dev-deploy.sh # skip ui/extension rebuild

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

DATA_DIR="${COAGENT_DATA_DIR:-/tmp/coagent-dev}"
LOG_DIR="$DATA_DIR"
DB_PATH="$DATA_DIR/server.db"
DAEMON_DATA="$DATA_DIR/daemon"
SERVER_LOG="$LOG_DIR/server.log"
DAEMON_LOG="$LOG_DIR/daemon.log"
DAEMONBUS_URL="${COAGENT_DAEMONBUS_URL:-ws://127.0.0.1:8832/daemonbus}"

red()   { printf '\033[31m%s\033[0m\n' "$*" >&2; }
green() { printf '\033[32m%s\033[0m\n' "$*"; }
blue()  { printf '\033[34m%s\033[0m\n' "$*"; }
fail()  { red "[deploy] FAILED: $*"; exit 1; }

# -----------------------------------------------------------------------------
# Step 1: env
# -----------------------------------------------------------------------------
blue "[deploy] step 1: source .env"
[ -f .env ] || fail ".env missing in $REPO_ROOT"
set -a; source .env; set +a

for var in COAGENT_DAEMON_SECRET COAGENT_HUMAN_SECRET COAGENT_SESSION_SECRET \
           COAGENT_DEVICEBUS_ALLOWED_ORIGINS \
           COAGENT_PUSHHUB_ALLOWED_ORIGINS COAGENT_DAEMONBUS_ALLOWED_ORIGINS \
           KIMI_API_KEY KIMI_BASE_URL KIMI_MODEL; do
  [ -n "${!var:-}" ] || fail "env var $var empty in .env"
done

# -----------------------------------------------------------------------------
# Step 2: rebuild Go binaries
# -----------------------------------------------------------------------------
if [ "${SKIP_BUILD:-0}" = "1" ]; then
  blue "[deploy] step 2: SKIP_BUILD=1, skipping go build"
else
  blue "[deploy] step 2: go build server / daemon / worker / cli / proxy"
  mkdir -p bin
  for b in server daemon worker cli; do
    go build -o "bin/coagent-$b" "./cmd/$b" || fail "go build cmd/$b"
  done
  # Native proxy binary for direct local use + cross-compiled installer
  # binaries served by /install/coagent-proxy_<os>_<arch>. The installer
  # set is what one-line `curl … | sh` fetches when a remote user installs
  # the daemon on their own machine. We keep coverage to the platforms
  # spec §13 #2 lists as MVP: linux/darwin × amd64/arm64.
  go build -o "bin/coagent-proxy" "./cmd/coagent-proxy" || fail "go build cmd/coagent-proxy"
  mkdir -p bin/installers
  for os_arch in linux_amd64 linux_arm64 darwin_amd64 darwin_arm64; do
    goos="${os_arch%_*}"
    goarch="${os_arch##*_}"
    out="bin/installers/coagent-proxy_${os_arch}"
    GOOS="$goos" GOARCH="$goarch" go build -trimpath -ldflags="-s -w" -o "$out" "./cmd/coagent-proxy" \
      || fail "cross-build coagent-proxy ${os_arch}"
  done
fi

# -----------------------------------------------------------------------------
# Step 3: rebuild UI (extension zip + dist)
# -----------------------------------------------------------------------------
if [ "${SKIP_UI:-0}" = "1" ]; then
  blue "[deploy] step 3: SKIP_UI=1, skipping ui rebuild"
elif [ "${SKIP_BUILD:-0}" = "1" ]; then
  blue "[deploy] step 3: SKIP_BUILD=1, skipping ui rebuild"
else
  blue "[deploy] step 3: rebuild extension zip + ui dist"
  make extension-zip > /tmp/deploy-extension.log 2>&1 || fail "make extension-zip (log: /tmp/deploy-extension.log)"
  (cd ui && pnpm build > /tmp/deploy-ui.log 2>&1) || fail "ui pnpm build (log: /tmp/deploy-ui.log)"
fi

# -----------------------------------------------------------------------------
# Step 4: graceful stop worker → daemon → server
# -----------------------------------------------------------------------------
blue "[deploy] step 4: graceful stop"

stop_proc() {
  local pattern="$1" name="$2" timeout="${3:-8}"
  local pids
  pids=$(pgrep -f "$pattern" || true)
  [ -z "$pids" ] && { green "  $name already stopped"; return 0; }
  echo "  $name pids: $pids → SIGINT"
  for p in $pids; do kill -INT "$p" 2>/dev/null || true; done
  local i=0
  while [ "$i" -lt "$timeout" ]; do
    sleep 1
    pids=$(pgrep -f "$pattern" || true)
    [ -z "$pids" ] && { green "  $name stopped cleanly"; return 0; }
    i=$((i+1))
  done
  red "  $name didn't exit in ${timeout}s → SIGKILL"
  for p in $pids; do kill -9 "$p" 2>/dev/null || true; done
  sleep 1
}

stop_proc "bin/coagent-worker" "worker" 5
stop_proc "bin/coagent-daemon" "daemon" 8
stop_proc "bin/coagent-server" "server" 5

# port 8832 must be free
if ss -tlnp 2>/dev/null | grep -q ":8832 "; then
  fail "port 8832 still bound after stop"
fi

# -----------------------------------------------------------------------------
# Step 5: start server, wait listen
# -----------------------------------------------------------------------------
blue "[deploy] step 5: start server :8832"
mkdir -p "$DATA_DIR"
# Start services in a new session so non-interactive runners that clean up
# their own process group do not tear down the deployed stack on exit.
setsid ./bin/coagent-server \
  -db "$DB_PATH" \
  -addr :8832 \
  -ui-dist ./ui/dist \
  -installer-dir ./bin/installers \
  > "$SERVER_LOG" 2>&1 &
SERVER_PID=$!

for i in $(seq 1 15); do
  sleep 1
  if ss -tlnp 2>/dev/null | grep -q ":8832 .*pid=$SERVER_PID"; then
    green "  server pid=$SERVER_PID listening :8832 (after ${i}s)"
    break
  fi
  if ! kill -0 "$SERVER_PID" 2>/dev/null; then
    red "  server crashed early; last log:"
    tail -10 "$SERVER_LOG" >&2
    fail "server did not start"
  fi
  [ "$i" = "15" ] && {
    tail -10 "$SERVER_LOG" >&2
    fail "server didn't listen in 15s"
  }
done

# -----------------------------------------------------------------------------
# Step 6: start daemon, wait daemonbus.ws.connected
# -----------------------------------------------------------------------------
blue "[deploy] step 6: start daemon → $DAEMONBUS_URL"
setsid ./bin/coagent-daemon \
  -server-url "$DAEMONBUS_URL" \
  -key "$COAGENT_DAEMON_SECRET" \
  -human-caller-secret "$COAGENT_HUMAN_SECRET" \
  -data-dir "$DAEMON_DATA" \
  -daemon-id daemon-dev \
  > "$DAEMON_LOG" 2>&1 &
DAEMON_PID=$!

EPOCH=""
for i in $(seq 1 15); do
  sleep 1
  if grep -q "transit.ws.connected" "$DAEMON_LOG"; then
    EPOCH=$(grep "transit.ws.connected" "$DAEMON_LOG" | tail -1 | grep -oE 'connection_epoch":[0-9]+' | head -1 | cut -d: -f2)
    green "  daemon pid=$DAEMON_PID daemonbus connected (epoch=$EPOCH, after ${i}s)"
    break
  fi
  if ! kill -0 "$DAEMON_PID" 2>/dev/null; then
    red "  daemon crashed early; last log:"
    tail -10 "$DAEMON_LOG" >&2
    fail "daemon did not start"
  fi
  [ "$i" = "15" ] && {
    tail -10 "$DAEMON_LOG" >&2
    fail "daemon didn't connect in 15s"
  }
done

# -----------------------------------------------------------------------------
# Step 7: final status
# -----------------------------------------------------------------------------
blue "[deploy] step 7: status"
COMMIT=$(git -C "$REPO_ROOT" rev-parse --short HEAD)
WORKER_MTIME=$(stat -c %y bin/coagent-worker | cut -d. -f1)

cat <<EOF

[deploy] ✅ stack up
  commit:        $COMMIT
  server pid:    $SERVER_PID   (binary: $(stat -c %y bin/coagent-server | cut -d. -f1))
  daemon pid:    $DAEMON_PID   (epoch:  $EPOCH)
  worker bin:    $WORKER_MTIME (next spawn auto-picks)
  data dir:      $DATA_DIR
  logs:          $SERVER_LOG / $DAEMON_LOG
  origins:       $COAGENT_DEVICEBUS_ALLOWED_ORIGINS

EOF
