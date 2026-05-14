# Node daemon → Go daemon deployment cutover

Authoritative ops playbook for moving cvmax (and any future coagent host)
off the legacy `lightcone/daemon/` Node process and onto `lightcone/daemon-go/`.

Scope:
- The **server** + **web UI** stay unchanged; both stacks already speak the
  same HTTP / WS protocols against the channel-local sqlite. The cutover
  swaps only the daemon process the machine runs.
- The L4 xhs-creator workflow is the canonical risk path: it issues the
  longest end-to-end chain (user → channel agent → xhs adapter → Chrome
  extension → channel log). Every gate in this doc is anchored to keeping
  that chain working.

Prereqs (confirm BEFORE starting):

- [ ] `daemon-go` built from a tagged commit (CI green for `go test ./...`
      and `golangci-lint run`).
- [ ] T16 e2e suite green on the cutover host
      (`go test -v ./test/e2e/...`).
- [ ] cmd/daemon smoke green
      (`go test -v ./cmd/daemon/...`) — boot + ask round-trip on the
      production composition root.
- [ ] Backup snapshot of every channel's `messages.sqlite`
      under `${COAGENT_HOME}/channels/*/messages.sqlite`.
- [ ] `coagent` (legacy `lightcone/daemon/`) PM2 process recorded so the
      rollback step has an exact handle to restart.
- [ ] At least one operator with sqlite + filesystem access on the machine.

## Phase 0 — Dry-run on a parallel channel (recommended, not required)

Skip on cvmax (`prod`); valuable on staging or a fresh sandbox machine.

1. Pick a non-traffic channel id (e.g. `dryrun-ch-001`).
2. Run `daemon-go` against `${COAGENT_HOME}/dryrun-ch-001/messages.sqlite`
   only — the channel-local sqlite is the unit of isolation, so other
   channels stay served by the Node daemon.
3. Send a synthetic `agent.text` event through `/api/messages` and
   confirm the row lands.
4. Stop `daemon-go`. No data risk because `daemon-go` only wrote to the
   one dry-run channel.

## Phase 1 — Migrate channel data into v4 layout

Each existing Node-managed channel sqlite needs to be transformed into
the v4 schema `daemon-go` uses. `cmd/migrate` ships that step:

```bash
cd lightcone/daemon-go

# Per channel: produce a v4-shaped sqlite alongside the Node one.
for src in ${COAGENT_HOME}/channels/*/messages.sqlite; do
  ch=$(basename "$(dirname "$src")")
  dst="${COAGENT_HOME}/channels/${ch}/messages.v4.sqlite"
  go run ./cmd/migrate from-node --src "$src" --dst "$dst"
done

# Daemon-level sqlite (bootstrap_registry only — fresh table is fine):
go run ./cmd/migrate init-daemon "${COAGENT_HOME}/daemon.sqlite"
```

Verification per channel (read-only — these queries do NOT touch the
serving Node daemon):

```bash
sqlite3 "${COAGENT_HOME}/channels/<ch>/messages.v4.sqlite" <<'SQL'
SELECT COUNT(*) AS row_count FROM messages;
SELECT COUNT(*) AS actor_count FROM actor_registry;
SELECT COUNT(*) AS type_count  FROM type_registry;
SELECT COUNT(*) AS bootstrap_complete FROM bootstrap_registry
 WHERE status = 'completed';
SQL
```

Expectations:
- `row_count` matches the source sqlite row count.
- `actor_count` ≥ 1 + number-of-humans + number-of-agents +
  number-of-tools — the migrate step seeds the same actor_registry rows
  the Node daemon implied at runtime.
- `type_count` ≥ 6 (the xhs adapter types alone).
- `bootstrap_complete` = number of channels surveyed.

## Phase 2 — Double-write observation window (24h, optional but recommended)

The view-sync push interface from T15 means the server can be fed by
EITHER daemon. Running both stacks against the same channel introduces
write contention on `messages.sqlite`, so we do NOT recommend
genuine double-write; instead use double-OBSERVATION:

1. Keep the Node daemon running on `messages.sqlite` (production truth).
2. Run `daemon-go` in **shadow mode** against the v4-migrated copies
   you produced in Phase 1 (`messages.v4.sqlite`).
3. Diff the two sqlite files every hour:
   ```bash
   sqlite3 messages.sqlite     'SELECT id, kind, type FROM messages ORDER BY seq' > /tmp/node.txt
   sqlite3 messages.v4.sqlite  'SELECT id, kind, type FROM messages ORDER BY seq' > /tmp/v4.txt
   diff /tmp/node.txt /tmp/v4.txt   # MUST be empty
   ```
4. If the diff is non-empty for > 4 hours running, abort the cutover and
   open a ticket — there is a migration bug.

Skip Phase 2 only if the team has zero appetite for risk-free observation
and an operator is willing to stand by during Phase 3.

## Phase 3 — Cutover

Atomic per-machine. Recommended sequence:

1. **Quiesce the Node daemon** — stop accepting new traffic but let
   in-flight turns complete:
   ```bash
   pm2 stop lightcone-daemon
   ```
   Wait until `pm2 logs lightcone-daemon` shows no more agent turns
   for at least 60 s (one heartbeat window).

2. **Swap the channel sqlite to the v4 form**:
   ```bash
   for ch in ${COAGENT_HOME}/channels/*/; do
     mv "${ch}messages.sqlite"    "${ch}messages.node.sqlite.bak"
     mv "${ch}messages.v4.sqlite" "${ch}messages.sqlite"
   done
   ```
   The `.node.sqlite.bak` files are the rollback handle for Phase 4.

3. **Start the Go daemon** with the same `COAGENT_HOME`:
   ```bash
   # Build the daemon + worker binaries side-by-side. The supervisor
   # finds the worker via the --worker-binary flag (default
   # `${dirname(daemon)}/worker`), so a shared bin dir keeps the
   # path implicit.
   ( cd lightcone/daemon-go && \
     go build -o bin/daemon  ./cmd/daemon && \
     go build -o bin/worker  ./cmd/worker )

   pm2 start lightcone/daemon-go/bin/daemon \
     --name lightcone-daemon-go \
     --update-env -- \
     --home          "${COAGENT_HOME}" \
     --http-listen   "${DAEMON_HTTP_LISTEN:-:3101}" \
     --auth-token    "${MACHINE_API_KEY}" \
     --worker-binary "${COAGENT_HOME}/bin/worker" \
     --server-url    "${SERVER_URL}"
   ```

   Flag table (matches `cmd/daemon/main.go::parseFlags`):

   | Flag | Default | Purpose |
   |---|---|---|
   | `--home` | `$COAGENT_HOME` / `~/.coagent` | Anchors `--daemon-db` + `--channel-root` defaults |
   | `--daemon-db` | `<home>/daemon.sqlite` | bootstrap_registry sqlite |
   | `--channel-root` | `<home>/channels` | Informational — per-channel workdir lives here |
   | `--http-listen` | `:3101` | TCP bind address |
   | `--auth-token` | env `COAGENT_AUTH_TOKEN` | Bearer token for `daemon_rpc message.send`, `view.resync_channel`, `xhs callback` |
   | `--worker-binary` | `<dirname(daemon-bin)>/worker` | Supervisor target binary; empty = supervisor loops disabled |
   | `--server-url` | empty | View-sync server origin (M1.x — push hook follow-up) |
   | `--xhs-device-id` | empty | Default device_id for the xhs adapter |
   | `--scheduler-period` | `1s` | Long-pending + future scheduler tick cadence |
   | `--supervisor-period` | `10s` | Supervisor scan cadence |
   | `--lease-ttl` | `60` | Worker lease seconds |

4. **Smoke**:
   - `curl http://localhost:${DAEMON_HTTP_PORT:-3101}/api/healthz` —
     daemon HTTP listener up.
   - `curl http://localhost:${DAEMON_HTTP_PORT:-3101}/api/channel/list`
     — every migrated channel id shows up in the JSON array.
   - `curl ${SERVER_URL}/api/healthz` — server still up.
   - Post a benign `agent.text` event through the UI. Confirm the daemon
     log shows `worker.runtime.ready` and the message appears in the UI.
   - Trigger one `xhs.publish` from the channel agent. Confirm the
     Chrome extension receives the WS frame and the response makes it
     back to the channel log within the type's `max_pending_ms` budget.

5. **24h observation**:
   - Monitor `daemon-go` PM2 restart count — must stay at 0.
   - Monitor `system.event payload.kind=view_sync_failed` row count per
     hour — must stay near 0 (T15 contract).
   - Monitor `system.event payload.kind=correlation_gc` per hour — non-
     zero values during steady state indicate adapter callbacks getting
     dropped (operator should investigate device connectivity).

## Phase 4 — Rollback (if needed during the 24h window)

```bash
pm2 stop lightcone-daemon-go
for ch in ${COAGENT_HOME}/channels/*/; do
  mv "${ch}messages.sqlite"          "${ch}messages.v4-broken.sqlite"
  mv "${ch}messages.node.sqlite.bak" "${ch}messages.sqlite"
done
pm2 start lightcone-daemon
```

The Node daemon resumes from its last-known sqlite. Any messages written
by `daemon-go` during the failed window survive inside
`messages.v4-broken.sqlite` for forensic review.

## Phase 5 — Decommission Node code

ONLY after a clean 24h observation window:

1. Open the `node-daemon-retirement.md` PR (see that doc for the file
   list and review template). The PR deletes `lightcone/daemon/` along
   with the matching `package.json` / PM2 ecosystem config entries.
2. Tag the daemon-go release `v1.0.0-go-daemon` so future rollbacks
   stop targeting the Node SHA range.
3. Update the team runbook to reference `lightcone/daemon-go/` as the
   canonical daemon and remove any "Node daemon" pages.

## Cheat sheet — knobs that changed

| Concept | Node daemon | Go daemon (`daemon-go`) |
|---|---|---|
| Channel sqlite | one row per (channel, kind, type) | adds `bootstrap_registry`, `worker_locks`, `action_ledger`, `adapter_correlation` |
| Worker spawn | inline `child_process.spawn` | `internal/worker.NewExecSpawner` exec.Cmd-based, fencing-token aware |
| Heartbeat | none | `supervisor.Heartbeat` CAS, lease 60 s default |
| Long-pending fallback | cron-scheduler ad-hoc | `internal/scheduler` 3-step normative SQL |
| Adapter framework | inline channel-manager glue | `pkg/adapter` framework (F1-F6) |
| HTTP write path | `rpc-server.js` | `internal/harness` daemon_rpc binding (9-step Write) |

## References

- T1-T15 ticket bodies in `.dalek/pm/m1.3-tickets.md`.
- `daemon-go/README.md` — package layout + per-subsystem README pointers.
- `daemon-go/test/e2e/` — five smoke scenarios that gate every cutover.
- `daemon-go/docs/node-daemon-retirement.md` — companion PR checklist.
