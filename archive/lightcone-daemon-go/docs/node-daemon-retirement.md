# Node daemon retirement — companion PR checklist

This doc is the prep work for the separate "delete Node daemon" PR that
M1.3-T16 mandates open AFTER every other ticket merges and the cutover
in `deployment-cutover.md` is observed clean for 24h on cvmax.

**Status as of T16 merge:** Node daemon is still the production code path
on cvmax. THIS PR does NOT delete any Node code; it documents the plan
so the deletion PR is a 5-minute rubber-stamp once the time comes.

## Why a separate PR

T16 lands the e2e smoke + deployment doc + this checklist *before*
the actual deletion. Keeping the deletion in its own PR lets reviewers:

1. Read T16 as "added go-daemon coverage" without diffing 5K lines of
   removed Node code.
2. Land the deletion under a feature flag PR title (e.g.
   `chore(daemon): remove legacy Node daemon (M1.3-T16 follow-up)`) so a
   future bisect for daemon behaviour has a clean before/after split.
3. Block the deletion PR on the 24h observation gate without blocking
   every other M1.3 ticket on a deletion that can wait.

## Pre-merge gate (must all be true)

- [ ] T16 e2e suite green: `go test -v ./test/e2e/...`
- [ ] All M1.3 tickets T1-T15 merged to main.
- [ ] `deployment-cutover.md` Phase 3 executed on cvmax (or whichever
      production host applies).
- [ ] Phase 3 step 5 observation: 24h with zero `daemon-go` PM2 restarts
      AND zero unexpected `view_sync_failed` system events.
- [ ] Tag `v1.0.0-go-daemon` cut on the release branch.

If any of the above is false, hold the deletion PR.

## Files / directories to delete

Top level under `lightcone/daemon/`:

| Path | Role | Replacement in `daemon-go/` |
|---|---|---|
| `lightcone/daemon/package.json` | Node deps manifest | (gone — Go module under `daemon-go/go.mod`) |
| `lightcone/daemon/src/index.js` | process entrypoint | `cmd/daemon/main.go` |
| `lightcone/daemon/src/agent-manager.js` | spawn / manage agents | `internal/worker/` + `internal/supervisor/` |
| `lightcone/daemon/src/channel-manager.js` | dispatch + WS bridge | `pkg/adapter/` + `internal/adapters/xhs/` |
| `lightcone/daemon/src/connection.js` | server WS client | `internal/viewsync/` (push) + daemon HTTP routes |
| `lightcone/daemon/src/cron-scheduler.js` | timeout fallback | `internal/scheduler/` (three-step long-pending) |
| `lightcone/daemon/src/devices/` (6 files) | device WS server | folded into adapter framework + `internal/adapters/xhs/` |
| `lightcone/daemon/src/drivers/{claude,coagent,codex,kimi}.js` | per-LLM driver | `internal/worker/` (go-kimi runtime, single binary path) |
| `lightcone/daemon/src/events.js` | event bus glue | n/a (harness + dispatcher) |
| `lightcone/daemon/src/machine-api-key.js` | key management | unchanged on server side; daemon-go reads via env |
| `lightcone/daemon/src/message-store.js` | sqlite layer | `internal/store/` |
| `lightcone/daemon/src/paths.js` | filesystem layout | `internal/store.OpenChannel` + workdir convention |
| `lightcone/daemon/src/preflight.js` | startup health checks | `cmd/daemon/main.go` boot sequence (T16 follow-up) |
| `lightcone/daemon/src/profile-lock.js` | profile pid-file | `internal/supervisor` worker_locks |
| `lightcone/daemon/src/rpc-server.js` | daemon HTTP API | `internal/harness` daemon_rpc binding |
| `lightcone/daemon/src/time.js` | clock helpers | `pkg/harness.Clock` |
| `lightcone/daemon/src/trigger-gateway.js` | trigger fan-out | `internal/trigger/` |
| `lightcone/daemon/src/workdir-watcher.js` | filesystem watcher | n/a (daemon-go uses explicit ChannelCreate RPC) |
| `lightcone/daemon/src/browser-login.js` | Chrome cookie sync | now handled inside adapter (`internal/adapters/xhs/`) |
| `lightcone/daemon/test/*.test.mjs` | Node-only tests | replaced by `daemon-go/{internal,pkg,test}/**` |
| `lightcone/daemon/scripts/` | Node ops helpers | replaced by `cmd/migrate` + Makefile targets |

Top-level coordination changes (also in the deletion PR):

- `lightcone/package.json` — drop `daemon` workspace entry.
- `lightcone/ecosystem.config.cjs` / `.js` — replace `lightcone-daemon`
  with `lightcone-daemon-go`.
- `pnpm-workspace.yaml` (worktree root) — drop the `daemon` glob.
- `Makefile` — remove `daemon-*` targets that shell out to Node; keep
  the `daemon-go-*` targets.
- Any `.github/workflows/*.yml` that runs `lightcone/daemon` jobs.
- Top-level `lightcone/AGENTS.md` / `lightcone/CLAUDE.md` — drop the
  Node daemon sections (they remain accurate for the Node code right
  until deletion).

## Sanity checks the deletion PR MUST pass

1. `git grep -nE 'lightcone/daemon/' -- . ':(exclude)docs/'` returns
   ONLY references the deletion PR also removes. Stale references in
   non-doc code = bug.
2. `go test ./...` in `lightcone/daemon-go/` stays green (the deletion
   PR touches nothing under `daemon-go/`).
3. `pnpm i` at the worktree root still resolves cleanly — drop the
   daemon workspace entry without breaking the rest.
4. CI `daemon-go-build`, `daemon-go-test`, `daemon-go-lint` jobs all
   green.

## Rollback contract for the deletion PR

The deletion PR is `git revert`-friendly: the daemon-go cutover does
NOT depend on the Node code being absent. If the team needs to bring
the Node daemon back temporarily, the revert restores every file
listed above unchanged. The companion `deployment-cutover.md` Phase 4
explains the operator-level rollback (swap sqlite + restart Node PM2
entry) — that path keeps working as long as one of the
`.node.sqlite.bak` snapshots survives.

## Owners + review template

Suggested reviewers (mirrors the M1.3 trio per `.dalek/pm/m1.3-tickets.md`
§S7): infra owner + adapter owner + on-call operator.

Suggested PR description template:

```
chore(daemon): remove legacy Node daemon (M1.3-T16 follow-up)

Removes lightcone/daemon/ now that lightcone/daemon-go/ has been the
production daemon on cvmax for 24h without regression. Replaces
ecosystem PM2 entry and drops the Node workspace.

Pre-merge gate (from daemon-go/docs/node-daemon-retirement.md):
- [x] T16 e2e green
- [x] T1-T15 merged
- [x] cvmax cutover phase 3 done (commit: <sha>)
- [x] 24h observation report attached (PM2 restarts=0, view_sync_failed=0)
- [x] tag v1.0.0-go-daemon at <sha>

Rollback: git revert + cherry-pick ecosystem.config.cjs change. The
daemon-go cutover keeps working independently.
```
