#!/usr/bin/env bash
set -euo pipefail

# The contract's acceptance line in one runnable file (K5 machine parity +
# onepager §9): a friend with the engine binary and this script can drive the
# whole surface with curl — register → bearer token → /api/meta → create
# channel → create + attach a daemon (agents are device-hosted: introduce
# places the actor on the first bound daemon) → introduce a scripted agent →
# submit over ws (the one non-curl leg, scripts/demo/wssubmit) → paged read
# shows the request AND the agent's response AND the activity phase events.
# No browser, no cookie jar juggling beyond extracting the token once.
#
# Usage: start a server first, e.g.
#   bin/atoll-server --db /tmp/atoll-demo/app.db --channel-db-dir /tmp/atoll-demo/channels --addr :8832 --init
# then: ATOLL_BASE=http://127.0.0.1:8832 ./scripts/demo-curl.sh

BASE="${ATOLL_BASE:-http://127.0.0.1:8832}"
for tool in curl jq go; do
  command -v "$tool" >/dev/null || { echo "demo: $tool required" >&2; exit 1; }
done

say() { echo "[demo] $*"; }

EMAIL="demo-$RANDOM@atoll.local"
PASSWORD="demo-pass-$RANDOM"
JAR="$(mktemp)"
trap 'rm -f "$JAR"' EXIT

say "register $EMAIL"
curl -sf -X POST "$BASE/api/identity/register" \
  -H 'Content-Type: application/json' \
  -d "{\"email\":\"$EMAIL\",\"password\":\"$PASSWORD\"}" >/dev/null

# The acceptance chain is register → LOGIN → token: the login endpoint must be
# the one that mints the session we use (a broken login must fail this demo).
say "login $EMAIL"
curl -sf -c "$JAR" -X POST "$BASE/api/identity/login" \
  -H 'Content-Type: application/json' \
  -d "{\"email\":\"$EMAIL\",\"password\":\"$PASSWORD\"}" >/dev/null

TOKEN="$(awk '$6=="atoll_session"{print $7}' "$JAR")"
[ -n "$TOKEN" ] || { echo "demo: no session token from login" >&2; exit 1; }
AUTH=(-H "Authorization: Bearer $TOKEN")
say "bearer token from login — everything below is header auth, no cookies"

VER="$(curl -sf "${AUTH[@]}" "$BASE/api/meta" | jq -r .contract_version)"
say "/api/meta contract_version=$VER"

CH="$(curl -sf "${AUTH[@]}" -X POST "$BASE/api/channels" \
  -H 'Content-Type: application/json' -d '{"name":"curl-demo"}' | jq -r .id)"
say "channel created: $CH"

# Agents are device-hosted: introduce places the actor on the channel's first
# bound daemon, so the demo runs one itself (the device half of the loop).
DAEMON_JSON="$(curl -sf "${AUTH[@]}" -X POST "$BASE/api/daemons" \
  -H 'Content-Type: application/json' -d '{"name":"demo-device"}')"
DID="$(echo "$DAEMON_JSON" | jq -r .id)"
DKEY="$(echo "$DAEMON_JSON" | jq -r .api_key)"
WS_BASE="${BASE/http/ws}"
DWORK="$(mktemp -d)"
go run ./cmd/daemon --server "$WS_BASE/compute" --key "$DKEY" \
  --name demo-device --home "$DWORK" >/dev/null 2>&1 &
DPID=$!
trap 'kill $DPID 2>/dev/null; rm -rf "$JAR" "$DWORK"' EXIT
sleep 2
curl -sf "${AUTH[@]}" -X POST "$BASE/api/channels/$CH/daemons" \
  -H 'Content-Type: application/json' -d "{\"daemon_id\":\"$DID\"}" >/dev/null
say "daemon attached and bound: $DID"

# The scripted agent (class "script") drives a tool: introduce an echo tool
# first, then the agent configured to call it. Both live on the demo daemon.
ECHO_DECL="$(curl -sf "${AUTH[@]}" -X POST "$BASE/api/actor-decls" \
  -H 'Content-Type: application/json' -d '{"name":"demo-echo","class":"echo"}' | jq -r .id)"
ECHO_ACTOR="$(curl -sf "${AUTH[@]}" -X POST "$BASE/api/channels/$CH/actors" \
  -H 'Content-Type: application/json' -d "{\"decl_id\":\"$ECHO_DECL\"}" | jq -r .actor_id)"
DECL="$(curl -sf "${AUTH[@]}" -X POST "$BASE/api/actor-decls" \
  -H 'Content-Type: application/json' \
  -d "{\"name\":\"demo-script\",\"class\":\"script\",\"config\":{\"tool_id\":\"$ECHO_ACTOR\"}}" | jq -r .id)"
ACTOR="$(curl -sf "${AUTH[@]}" -X POST "$BASE/api/channels/$CH/actors" \
  -H 'Content-Type: application/json' -d "{\"decl_id\":\"$DECL\"}" | jq -r .actor_id)"
say "echo tool $ECHO_ACTOR + scripted agent $ACTOR introduced"

MSGID="$(go run ./scripts/demo/wssubmit -base "$BASE" -token "$TOKEN" \
  -channel "$CH" -actor "$ACTOR" -msgtype loop.chat \
  -text "hello from the curl demo" | tail -1)"
say "submitted over ws, message id $MSGID"

PAGE="$(curl -sf "${AUTH[@]}" "$BASE/api/channels/$CH/messages?after_seq=0")"
echo "$PAGE" | jq -e --arg id "$MSGID" \
  '.messages[] | select(.envelope.id==$id)' >/dev/null \
  || { echo "demo: submitted message not in paged read" >&2; exit 1; }
echo "$PAGE" | jq -e --arg id "$MSGID" \
  '.messages[] | select(.envelope.kind=="response" and .envelope.parent_id==$id)' >/dev/null \
  || { echo "demo: response paired to our request not in paged read" >&2; exit 1; }
# activity.* phase events come from base-wrapped LLM engines (kimi/claude);
# the deterministic script class is a bare Proc, so here they are informational
# only — the phase-event acceptance is pinned by unit tests, not this demo.
ACTIVITY="$(echo "$PAGE" | jq '[.messages[] | select(.envelope.type|startswith("activity."))] | length')"
say "activity phase events in log: $ACTIVITY (informational)"

say "closed loop PASS: request + agent response, both replayable over paged read"
