#!/usr/bin/env bash
# e2e-claude-agent.sh — REAL end-to-end run of the second looper (claude).
#
# Flow (mirrors dev-deploy.sh's start-server-then-drive style, but self-contained
# and against an ISOLATED data dir — never touches /tmp/coagent-dev owner data):
#   1. build server, start it on :8899 against /tmp/coagent-e2e (fresh)
#   2. register user → workspace → channel
#   3. POST /api/agents {looper:"claude"}            → a claude-looper agent
#   4. POST /api/channels/:ch/agents {make_default}  → introduce + LIVE spawn
#      (the real claudeagent cell Connects a real `claude` CLI session)
#   5. POST /api/channels/:ch/messages {kind:request} → trigger a turn
#   6. poll GET /api/channels/:ch/messages for the agent's agent.text reply
#   7. tear the server down
#
# Requires: an authed `claude` CLI (claude -p works). No KIMI creds needed —
# the claude looper carries its own auth.
set -uo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

DATA_DIR="/tmp/coagent-e2e"        # e2e scratch — NOT /tmp/coagent-dev
PORT=8899
BASE="http://127.0.0.1:$PORT"
JAR="$DATA_DIR/cookies.txt"
SLOG="$DATA_DIR/server.log"

say()  { printf '\033[34m[e2e] %s\033[0m\n' "$*"; }
ok()   { printf '\033[32m[e2e] %s\033[0m\n' "$*"; }
die()  { printf '\033[31m[e2e] FAIL: %s\033[0m\n' "$*" >&2; [ -f "$SLOG" ] && { echo "--- server.log tail ---"; tail -25 "$SLOG"; }; cleanup; exit 1; }

j() { python3 -c "import sys,json; d=json.load(sys.stdin); print(d$1)" 2>/dev/null; }

SERVER_PID=""
cleanup() { [ -n "$SERVER_PID" ] && kill "$SERVER_PID" 2>/dev/null; }
trap cleanup EXIT

# ---- 0. fresh scratch + build + start server -------------------------------
rm -rf "$DATA_DIR"; mkdir -p "$DATA_DIR/channels"
say "build server"
go build -o bin/coagent-server ./cmd/server || die "build"

# free the port if a previous e2e left something
pkill -f "coagent-server.*:$PORT" 2>/dev/null; sleep 1

say "start server :$PORT (data=$DATA_DIR)"
./bin/coagent-server -db "$DATA_DIR/app.db" -channel-db-dir "$DATA_DIR/channels" -addr ":$PORT" \
  > "$SLOG" 2>&1 &
SERVER_PID=$!
for i in $(seq 1 15); do
  curl -fsS "$BASE/api/identity/me" -o /dev/null 2>/dev/null && break || true
  # /me needs auth → 401 is "listening". Treat any HTTP response as up.
  curl -fsS -o /dev/null -w '' "$BASE/" 2>/dev/null && break
  if ! kill -0 "$SERVER_PID" 2>/dev/null; then die "server crashed on boot"; fi
  sleep 1
  [ "$i" = 15 ] && die "server didn't listen in 15s"
done
ok "server pid=$SERVER_PID up"

# ---- 1. register → workspace → channel -------------------------------------
say "register user"
R=$(curl -sS -c "$JAR" -b "$JAR" -H 'Content-Type: application/json' \
  -d '{"email":"e2e@example.com","password":"secret123","display_name":"E2E"}' \
  "$BASE/api/identity/register")
USER=$(echo "$R" | j "['id']"); [ -n "$USER" ] || die "register: $R"
ok "user=$USER"

WS=$(curl -sS -c "$JAR" -b "$JAR" -H 'Content-Type: application/json' \
  -d '{"name":"E2E WS"}' "$BASE/api/workspaces" | j "['id']")
[ -n "$WS" ] || die "create workspace"
CH=$(curl -sS -c "$JAR" -b "$JAR" -H 'Content-Type: application/json' \
  -d '{"name":"E2E CH"}' "$BASE/api/workspaces/$WS/channels" | j "['id']")
[ -n "$CH" ] || die "create channel"
ok "workspace=$WS channel=$CH"

# ---- 2. create a claude-looper agent ---------------------------------------
say "create claude-looper agent (§五 API)"
A=$(curl -sS -c "$JAR" -b "$JAR" -H 'Content-Type: application/json' \
  -d '{"name":"E2E Claude","looper":"claude"}' "$BASE/api/agents")
AGENT=$(echo "$A" | j "['id']"); LOOPER=$(echo "$A" | j "['looper']")
[ -n "$AGENT" ] || die "create agent: $A"
ok "agent=$AGENT looper=$LOOPER"

# ---- 3. introduce to channel → live spawn (real claude Connect) ------------
say "introduce agent → channel (spawns real claudeagent cell)"
I=$(curl -sS -c "$JAR" -b "$JAR" -H 'Content-Type: application/json' \
  -d "{\"agent_id\":\"$AGENT\",\"make_default\":true}" "$BASE/api/channels/$CH/agents")
LIVE=$(echo "$I" | j "['live']"); INST=$(echo "$I" | j "['instance_id']")
say "introduce result: $I"
[ "$LIVE" = "True" ] && ok "agent cell LIVE (claude CLI connected): $INST" || say "live=$LIVE (claude Connect may have failed — see server.log)"

# ---- 4. send a message → trigger a turn ------------------------------------
say "send a request to the channel (→ default_agent = the claude agent)"
curl -sS -c "$JAR" -b "$JAR" -H 'Content-Type: application/json' \
  -d '{"kind":"request","type":"human.text","payload":{"text":"What is 2+2? Reply with just the number, nothing else."}}' \
  "$BASE/api/channels/$CH/messages" > "$DATA_DIR/send.json"
ok "sent"

# ---- 5. poll for the agent's reply -----------------------------------------
say "poll channel messages for the agent.text reply (≤90s — real claude turn)"
REPLY=""
for i in $(seq 1 45); do
  sleep 2
  MSGS=$(curl -sS -c "$JAR" -b "$JAR" "$BASE/api/channels/$CH/messages")
  # find an agent.text message authored by our agent
  REPLY=$(echo "$MSGS" | python3 -c "
import sys,json
try: d=json.load(sys.stdin)
except: sys.exit(0)
msgs = d if isinstance(d,list) else d.get('messages',[])
for m in msgs:
    e = m.get('envelope', m)  # GET /messages wraps each row as {seq,is_terminal,envelope}
    if e.get('type')=='agent.text' and str((e.get('sender') or {}).get('id','')).startswith('agent:'):
        p=e.get('payload')
        if isinstance(p,str):
            try: p=json.loads(p)
            except: p={}
        t=(p or {}).get('text','')
        if t.strip(): print(t.strip().replace(chr(10),' ')); break
" 2>/dev/null)
  [ -n "$REPLY" ] && break
  printf '.'
done
echo

if [ -n "$REPLY" ]; then
  ok "AGENT REPLIED: $REPLY"
  echo
  ok "✅ e2e PASS — second looper (claude) ran a real turn end-to-end through the §五 API"
else
  say "no agent.text reply within timeout; channel messages + server log for diagnosis:"
  echo "$MSGS" | python3 -m json.tool 2>/dev/null | head -60
  echo "--- server.log tail ---"; tail -30 "$SLOG"
  die "agent did not reply"
fi
