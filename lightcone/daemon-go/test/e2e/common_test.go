// Package e2e hosts the M1.3-T16 cross-subsystem integration smoke tests.
//
// The 5 test files in this directory each drive one of the v4 audit
// view-A scenarios end-to-end against a real channel-local sqlite,
// composing the actual daemon-go subsystems (harness 9-step write +
// supervisor + ledger + adapter framework + xhs adapter + long-pending
// scheduler) inside a single `go test` process. There is intentionally
// NO production daemon binary spin-up: the ticket's owner constraint
// "用 Go testing 框架 + go test -v" + "mock Chrome extension WS (不调
// 真实 device)" rules out exec.Cmd-based integration. The composition
// approach mirrors `internal/viewsync/end2end_test.go` and keeps every
// assertion observable via standard testing.T tooling.
//
// Scenarios (mapped to v4 audit view A):
//
//  1. scenario1_publish_happy_path_test.go   — user emits "publish xhs"
//                                                event → channel agent
//                                                emits xhs.publish
//                                                request → adapter pushes
//                                                WS frame → mock callback
//                                                → terminal response.
//  2. scenario2_kill_replay_test.go          — worker crash → supervisor
//                                                respawns → action_ledger
//                                                Reserve replays same
//                                                envelope_id → harness
//                                                Step 0.5 dedupes → no
//                                                duplicate xhs.publish.
//  3. scenario3_unanswered_timeout_test.go   — alice ask bob (kind=request)
//                                                → bob silent → mock clock
//                                                jumps past expires_at →
//                                                scheduler emits
//                                                unanswered_timeout terminal.
//  4. scenario4_callback_dedupe_test.go      — adapter receives the same
//                                                callback twice (network
//                                                retry) → ctx.Respond
//                                                dedupes → channel log
//                                                holds exactly one
//                                                terminal response.
//  5. scenario5_receiver_unavailable_test.go — admin deregisters
//                                                tool:xhs-adapter → alice
//                                                ask xhs.publish (raw
//                                                row, bypass harness Step
//                                                5 so we reach the
//                                                receiver-missing window)
//                                                → scheduler Step 3 emits
//                                                receiver_unavailable.
//
// Shared fixtures live in this common_test.go so each scenario file
// reads as a self-contained spec.
package e2e

import (
	"context"
	"database/sql"
	"encoding/json"
	"io"
	"log/slog"
	"path/filepath"
	"sync/atomic"
	"testing"

	internalharness "github.com/coagent-ai/daemon-go/internal/harness"
	"github.com/coagent-ai/daemon-go/internal/registry"
	"github.com/coagent-ai/daemon-go/internal/store"
	pkgharness "github.com/coagent-ai/daemon-go/pkg/harness"
	"github.com/coagent-ai/daemon-go/pkg/v4types"

	"github.com/coagent-ai/daemon-go/internal/adapters/xhs"
	"github.com/coagent-ai/daemon-go/pkg/adapter"
)

// ---------------------------------------------------------------------------
// Shared identifiers
// ---------------------------------------------------------------------------
//
// The actor / type / channel identifiers are stable across every scenario
// so assertions can match against constants and not magic strings buried
// inside individual test bodies.

const (
	// ChannelID is the in-process channel id used by every e2e fixture.
	ChannelID = "ch-e2e"

	// SystemActorID is the canonical sender for all framework-side
	// emits (scheduler fallback responses, view_sync_failed events).
	SystemActorID = "system"

	// Alice is the primary "agent" actor used as the request sender in
	// scenarios 1 / 3 / 5.
	Alice = "alice"

	// Bob is the secondary agent receiver used by scenario 3 (agent →
	// agent ask).
	Bob = "bob"

	// DeviceID is the mock Chrome extension device id every WS push
	// targets. Constant so callback fixtures can echo it back.
	DeviceID = "dev-e2e-001"

	// BizFoo is the business request/response type used by scenarios 3
	// + 5 (alice ask bob / alice ask deregistered xhs-adapter under a
	// generic biz type so we don't conflate xhs.publish semantics with
	// the scheduler fallback path).
	BizFoo = "biz.foo"

	// T0 anchors the deterministic wall-clock baseline. Everything in
	// the e2e fixture references this plus deltas; tests advance via
	// the returned *int64 pointer (atomic-safe under -race).
	T0 = int64(1_700_000_000_000)
)

// ---------------------------------------------------------------------------
// Channel + harness deps fixture
// ---------------------------------------------------------------------------

// E2EFixture bundles every cross-subsystem handle a scenario test needs.
// Returned by openE2EChannel so individual scenario files stay
// focused on their own assertion shape.
type E2EFixture struct {
	DB    *sql.DB
	Deps  pkgharness.Deps
	Clock func() int64

	// NowPtr is the atomic-backed wall-clock pointer. Tests mutate it
	// via atomic.StoreInt64 / atomic.AddInt64 to advance time
	// deterministically (mirrors supervisor.fixedClockPtr).
	NowPtr *int64
}

// openE2EChannel builds a fresh channel-local sqlite under t.TempDir,
// seeds the 4 actors every scenario needs (system / alice / bob /
// tool:xhs-adapter), installs the 6 xhs types from the adapter +
// `biz.foo` so the scheduler scenarios can write business requests
// without colliding with the xhs path, and constructs the harness
// dependency bundle wired to a mutable wall-clock pointer.
//
// The wall-clock is shared between the harness Deps and any scheduler
// / adapter manager the test builds, so a single NowPtr mutation
// advances every subsystem.
func openE2EChannel(t *testing.T) *E2EFixture {
	t.Helper()
	ctx := context.Background()

	dir := t.TempDir()
	db, err := store.OpenChannel(ctx, filepath.Join(dir, "messages.sqlite"), store.OpenOptions{})
	if err != nil {
		t.Fatalf("OpenChannel: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	// Seed actors deterministically. The order mirrors L2 §3.5 install
	// ordering — system first so any subsequent INSERT can audience-target
	// the deregistered-aware sender_deregistered check (system is the only
	// id the harness is allowed to keep emitting under).
	seedActor(t, ctx, db, SystemActorID, "system", "")
	seedActor(t, ctx, db, Alice, "agent", "in_worker_bus")
	seedActor(t, ctx, db, Bob, "agent", "in_worker_bus")
	seedActor(t, ctx, db, xhs.AdapterActorID, "tool", "daemon_rpc")

	// Install the 6 xhs types so adapter.Manager.Install + harness Step 4
	// resolve correctly.
	installXhsTypes(t, ctx, db)
	// Install the generic biz.foo business type used by scheduler
	// scenarios. The schemas are permissive so scheduler fallback
	// payloads (`{status:'failed', reason, missing_actor_id?}`) pass
	// Step 6 untouched.
	installBizFoo(t, ctx, db)

	// Shared atomic clock pointer. Tests advance via
	// atomic.StoreInt64 / atomic.AddInt64. Baseline = T0 (milliseconds).
	var now int64
	atomic.StoreInt64(&now, T0)
	clock := func() int64 { return atomic.LoadInt64(&now) }

	types, err := internalharness.LoadTypeLookup(ctx, db)
	if err != nil {
		t.Fatalf("LoadTypeLookup: %v", err)
	}
	deps := pkgharness.New(
		internalharness.NewSQLiteStore(db),
		internalharness.NewSQLiteActors(db),
		types,
		internalharness.NewSQLiteWorkerLocks(db),
		ChannelID,
	)
	deps.Clock = clock

	return &E2EFixture{
		DB:     db,
		Deps:   deps,
		Clock:  clock,
		NowPtr: &now,
	}
}

// seedActor INSERTs one actor_registry row. The helper is intentionally
// raw SQL (not registry.Register) so the e2e fixture stays decoupled
// from the bootstrap saga — the saga is unit-tested under
// internal/bootstrap; we only need its end-state.
func seedActor(t *testing.T, ctx context.Context, db *sql.DB, id, kind, binding string) {
	t.Helper()
	var bindArg any
	if binding != "" {
		bindArg = binding
	}
	if _, err := db.ExecContext(ctx,
		`INSERT INTO actor_registry (actor_id, actor_kind, actor_binding, created_at, deregistered_at)
		 VALUES (?, ?, ?, ?, NULL)`,
		id, kind, bindArg, T0,
	); err != nil {
		t.Fatalf("seed actor %s: %v", id, err)
	}
}

// installXhsTypes registers the 6 xhs types (5 request/response + 1
// event) bound to xhs.AdapterActorID. The schemas mirror
// internal/adapters/xhs/xhs_test.go's permissive shape so payloads in
// either direction pass Step 6 without per-test schema tuning.
func installXhsTypes(t *testing.T, ctx context.Context, db *sql.DB) {
	t.Helper()
	maxPending := int64(60_000)
	objectSchema := json.RawMessage(`{
	  "request":  {"type": "object"},
	  "response": {
	    "type": "object",
	    "required": ["status"],
	    "properties": {
	      "status": {"type": "string", "enum": ["completed", "failed"]},
	      "reason": {"type": "string"}
	    },
	    "additionalProperties": true
	  }
	}`)
	eventSchema := json.RawMessage(`{
	  "event": {"type": "object", "additionalProperties": true}
	}`)
	rows := []registry.TypeRow{
		mkTypeRow(xhs.TypePublish, []string{"request", "response"}, objectSchema, &maxPending),
		mkTypeRow(xhs.TypeSearch, []string{"request", "response"}, objectSchema, &maxPending),
		mkTypeRow(xhs.TypeNoteFetch, []string{"request", "response"}, objectSchema, &maxPending),
		mkTypeRow(xhs.TypeRecentFetch, []string{"request", "response"}, objectSchema, &maxPending),
		mkTypeRow(xhs.TypeCookieSync, []string{"request", "response"}, objectSchema, &maxPending),
		mkTypeRow(xhs.TypeNoteArchived, []string{"event"}, eventSchema, nil),
	}
	if err := store.WithImmediate(ctx, db, func(c context.Context, conn *sql.Conn) error {
		return registry.Install(c, conn, rows, T0)
	}); err != nil {
		t.Fatalf("install xhs types: %v", err)
	}
}

// installBizFoo registers a generic agent-to-agent request/response
// type used by scheduler scenarios. The response schema accepts both
// happy-path bodies and the scheduler fallback shape
// (`{status, reason, missing_actor_id?}`).
func installBizFoo(t *testing.T, ctx context.Context, db *sql.DB) {
	t.Helper()
	schemas := json.RawMessage(`{
	  "request":  {"type": "object"},
	  "response": {
	    "type": "object",
	    "properties": {
	      "ok": {"type": "boolean"},
	      "status": {"type": "string"},
	      "reason": {"type": "string"},
	      "missing_actor_id": {"type": "string"}
	    },
	    "additionalProperties": true
	  }
	}`)
	rows := []registry.TypeRow{{
		Type:               BizFoo,
		AllowedKinds:       []string{"request", "response"},
		SchemasByKind:      schemas,
		HandlerBinding:     "in_worker_bus",
		TerminalConvention: "single-response",
		HandlerActorID:     "",
	}}
	if err := store.WithImmediate(ctx, db, func(c context.Context, conn *sql.Conn) error {
		return registry.Install(c, conn, rows, T0)
	}); err != nil {
		t.Fatalf("install %s: %v", BizFoo, err)
	}
}

// mkTypeRow assembles a registry.TypeRow with the xhs adapter wire
// conventions. The HandlerActorID binds every xhs type to the adapter
// actor so adapter.Manager.Install sees a consistent type_registry.
func mkTypeRow(typ string, kinds []string, schemas json.RawMessage, maxPending *int64) registry.TypeRow {
	return registry.TypeRow{
		Type:               typ,
		AllowedKinds:       kinds,
		SchemasByKind:      schemas,
		HandlerBinding:     "daemon_rpc",
		MaxPendingMs:       maxPending,
		HandlerActorID:     xhs.AdapterActorID,
		TerminalConvention: "single-response",
		Domain:             "xhs",
	}
}

// ---------------------------------------------------------------------------
// adapter.Manager + xhs.Module assembly
// ---------------------------------------------------------------------------

// E2EAdapterManager bundles the live adapter.Manager + the mock device
// client so a scenario can drive Manager.Dispatch + simulate device
// callbacks without re-deriving plumbing.
type E2EAdapterManager struct {
	Manager *adapter.Manager
	Device  *xhs.MockDeviceClient
}

// buildE2EAdapterManager constructs an adapter.Manager pre-installed
// with the xhs module wired to a MockDeviceClient. The clock the
// manager uses is the same atomic pointer the fixture surface owns
// (so timer / GC tests can advance the same wall-clock).
//
// `defaultDeviceID` lets a scenario omit `device_id` from payloads —
// scenario 1 uses it to mimic the production happy path where the
// channel-agent does not explicitly route the request to a device.
func buildE2EAdapterManager(t *testing.T, fix *E2EFixture, defaultDeviceID string) *E2EAdapterManager {
	t.Helper()
	mock := xhs.NewMockDeviceClient()
	mod := xhs.New(xhs.Config{
		DeviceClient:    mock,
		DefaultDeviceID: defaultDeviceID,
	})
	mgr, err := adapter.NewManager(adapter.ManagerConfig{
		DB:      fix.DB,
		Deps:    fix.Deps,
		Modules: map[string]adapter.Module{xhs.AdapterName: mod},
		Clock:   fix.Clock,
		Logger:  silentLogger(),
	})
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	if err := mgr.Install(context.Background()); err != nil {
		t.Fatalf("Manager.Install: %v", err)
	}
	return &E2EAdapterManager{Manager: mgr, Device: mock}
}

// silentLogger keeps go test output legible. Tests that intentionally
// want to inspect events should construct their own slog.Logger.
func silentLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError}))
}

// ---------------------------------------------------------------------------
// harness write helpers
// ---------------------------------------------------------------------------

// writeHarness wraps pkgharness.Write so scenarios can emit envelopes
// the same way the in_worker_bus binding would, without re-deriving the
// boilerplate per file. Returns the harness.Result on success or fatals.
func writeHarness(t *testing.T, ctx context.Context, fix *E2EFixture, env *v4types.Envelope, callerCtx pkgharness.CallerCtx) *pkgharness.Result {
	t.Helper()
	res, err := pkgharness.Write(ctx, fix.Deps, env, callerCtx)
	if err != nil {
		t.Fatalf("harness.Write(%s): %v", env.ID, err)
	}
	return res
}

// requestEnvelope builds a kind=request envelope addressed to a single
// receiver, with the scenario-shared ts / channel_id baked in. Tests
// fill payload + envelope.id + type. Visibility defaults to public so
// the harness Step 5 audience-narrow check accepts it.
func requestEnvelope(id, senderID, typeName, receiverID, payload string) *v4types.Envelope {
	return &v4types.Envelope{
		ID:         id,
		TS:         T0,
		ChannelID:  ChannelID,
		Sender:     v4types.Sender{Kind: v4types.SenderAgent, ID: senderID},
		Kind:       v4types.KindRequest,
		Type:       typeName,
		Payload:    json.RawMessage(payload),
		Visibility: v4types.VisibilityPublic,
		Audience:   []string{receiverID},
	}
}

// eventEnvelope builds a kind=event envelope with audience=['*']. The
// audience default matches the L1 §5 broadcast semantic — used by
// scenario 1 to seed the "user publish xhs" event.
func eventEnvelope(id, senderID, typeName, payload string) *v4types.Envelope {
	return &v4types.Envelope{
		ID:         id,
		TS:         T0,
		ChannelID:  ChannelID,
		Sender:     v4types.Sender{Kind: v4types.SenderAgent, ID: senderID},
		Kind:       v4types.KindEvent,
		Type:       typeName,
		Payload:    json.RawMessage(payload),
		Visibility: v4types.VisibilityPublic,
		Audience:   []string{"*"},
	}
}

// agentCallerCtx is the caller_ctx the harness expects for an agent
// emit. FencingToken is left zero — the e2e tests don't exercise
// fencing (covered by supervisor/loop_test.go).
func agentCallerCtx(actorID string) pkgharness.CallerCtx {
	return pkgharness.CallerCtx{
		Authenticated:      true,
		ActorID:            actorID,
		DeclaredSenderKind: v4types.SenderAgent,
	}
}

// ---------------------------------------------------------------------------
// Raw-row helpers
// ---------------------------------------------------------------------------
//
// Scheduler scenarios sometimes need to stage a pending request that
// would NOT pass current-state harness validation (e.g. an audience
// pointing at a now-deregistered actor). The helper insertRawRequest
// writes the row directly so the test can drive scheduler.Tick against
// it. Matches the pattern in internal/scheduler/long_pending_test.go's
// insertPendingRequest helper.

// insertRawRequest writes a single kind=request row bypassing the
// harness. expiresAt is optional — passing nil leaves the column NULL
// (scheduler Step 2 / Step 3 both tolerate NULL; Step 1 ignores).
func insertRawRequest(t *testing.T, ctx context.Context, db *sql.DB, id, senderID, receiverID, typeName string, expiresAt *int64) {
	t.Helper()
	aud, _ := json.Marshal([]string{receiverID})
	var expArg any
	if expiresAt != nil {
		expArg = *expiresAt
	}
	if _, err := db.ExecContext(ctx,
		`INSERT INTO messages
		   (id, ts, ts_received, channel_id, sender_kind, sender_id,
		    kind, type, payload, parent_id, correlation_id,
		    visibility, audience, not_before, expires_at, is_terminal)
		 VALUES (?, ?, ?, ?, 'agent', ?,
		         'request', ?, '{}', NULL, ?,
		         'public', ?, NULL, ?, 0)`,
		id, T0, T0, ChannelID, senderID,
		typeName, id, string(aud), expArg,
	); err != nil {
		t.Fatalf("insertRawRequest %s: %v", id, err)
	}
}

// ---------------------------------------------------------------------------
// Channel-log assertions
// ---------------------------------------------------------------------------

// countTerminalResponses returns the number of terminal response rows
// whose parent_id equals requestID. The One Law invariant guarantees
// this never exceeds 1 in production — scenarios 4 + 5 assert the
// invariant holds in their respective adversarial paths.
func countTerminalResponses(t *testing.T, ctx context.Context, db *sql.DB, requestID string) int {
	t.Helper()
	var n int
	if err := db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM messages
		  WHERE parent_id = ? AND kind = 'response' AND is_terminal = 1`,
		requestID,
	).Scan(&n); err != nil {
		t.Fatalf("countTerminalResponses(%s): %v", requestID, err)
	}
	return n
}

// terminalResponse returns the (payload, sender_id) of the latest
// terminal response for requestID. Fatals when none exists — tests
// asserting "no response yet" should use countTerminalResponses
// instead.
func terminalResponse(t *testing.T, ctx context.Context, db *sql.DB, requestID string) (payload, senderID string) {
	t.Helper()
	row := db.QueryRowContext(ctx,
		`SELECT payload, sender_id FROM messages
		  WHERE parent_id = ? AND kind = 'response' AND is_terminal = 1
		  ORDER BY seq DESC LIMIT 1`, requestID)
	if err := row.Scan(&payload, &senderID); err != nil {
		t.Fatalf("terminalResponse(%s): %v", requestID, err)
	}
	return payload, senderID
}

// countMessagesByType returns the row count of messages matching a
// (kind, type) pair. Used by scenario 2 to assert exactly one
// xhs.publish request survived replay.
func countMessagesByType(t *testing.T, ctx context.Context, db *sql.DB, kind, typeName string) int {
	t.Helper()
	var n int
	if err := db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM messages WHERE kind = ? AND type = ?`,
		kind, typeName,
	).Scan(&n); err != nil {
		t.Fatalf("countMessagesByType(%s, %s): %v", kind, typeName, err)
	}
	return n
}
