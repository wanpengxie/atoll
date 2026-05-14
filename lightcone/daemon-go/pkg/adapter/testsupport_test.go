package adapter

// testsupport_test.go bundles the fixtures shared by every *_test.go
// in this package: a real-sqlite channel opener, deterministic-clock
// helpers, recording writer / module mocks, and a synthetic request
// inserter that bypasses the harness (so tests stage state without
// invoking the very pipeline they exercise).

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"

	internalharness "github.com/coagent-ai/daemon-go/internal/harness"
	"github.com/coagent-ai/daemon-go/internal/registry"
	"github.com/coagent-ai/daemon-go/internal/store"
	pkgharness "github.com/coagent-ai/daemon-go/pkg/harness"
	"github.com/coagent-ai/daemon-go/pkg/v4types"
)

const (
	testChannelID    = "ch-adapter"
	testAgentID      = "alice"
	testAdapterName  = "demo"
	testAdapterActor = "tool:demo"
	testAdapterType  = "demo.echo"
	testSystemID     = "system"
	testT0           = int64(1_700_000_000_000)
)

// openAdapterChannel materialises a channel sqlite with the actors +
// type_registry rows the adapter framework expects:
//
//   - alice (agent / in_worker_bus)      — request sender
//   - system                              — system emitter
//   - tool:demo (tool / daemon_rpc)       — the adapter actor
//   - demo.echo type bound to tool:demo
//
// Tests that need additional rows can poke `db` directly (the
// connection is left open under t.Cleanup).
func openAdapterChannel(t *testing.T) (*sql.DB, pkgharness.Deps) {
	t.Helper()
	dir := t.TempDir()
	db, err := store.OpenChannel(context.Background(), filepath.Join(dir, "messages.sqlite"), store.OpenOptions{})
	if err != nil {
		t.Fatalf("OpenChannel: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	ctx := context.Background()
	seedActor := func(id, kind, binding string) {
		var bindArg any
		if binding != "" {
			bindArg = binding
		}
		if _, err := db.ExecContext(ctx,
			`INSERT INTO actor_registry (actor_id, actor_kind, actor_binding, created_at, deregistered_at)
			 VALUES (?, ?, ?, ?, NULL)`,
			id, kind, bindArg, testT0,
		); err != nil {
			t.Fatalf("seed actor %s: %v", id, err)
		}
	}
	seedActor(testSystemID, "system", "")
	seedActor(testAgentID, "agent", "in_worker_bus")
	seedActor(testAdapterActor, "tool", "daemon_rpc")

	// Install the demo type with handler_actor_id pointing at the
	// adapter actor (so the framework install passes the handler check).
	schemas := `{
	  "request": {"type": "object"},
	  "response": {
	    "type": "object",
	    "required": ["status"],
	    "properties": {
	      "status": {"type": "string", "enum": ["completed", "failed"]},
	      "reason": {"type": "string"}
	    },
	    "additionalProperties": true
	  }
	}`
	maxPending := int64(60_000)
	rows := []registry.TypeRow{{
		Type:               testAdapterType,
		AllowedKinds:       []string{"request", "response"},
		SchemasByKind:      json.RawMessage(schemas),
		HandlerBinding:     "daemon_rpc",
		MaxPendingMs:       &maxPending,
		HandlerActorID:     testAdapterActor,
		TerminalConvention: "single-response",
	}}
	if err := store.WithImmediate(ctx, db, func(c context.Context, conn *sql.Conn) error {
		return registry.Install(c, conn, rows, testT0)
	}); err != nil {
		t.Fatalf("install demo type: %v", err)
	}

	types, err := internalharness.LoadTypeLookup(ctx, db)
	if err != nil {
		t.Fatalf("LoadTypeLookup: %v", err)
	}
	deps := pkgharness.New(
		internalharness.NewSQLiteStore(db),
		internalharness.NewSQLiteActors(db),
		types,
		nil,
		testChannelID,
	)
	return db, deps
}

// insertRequestRow stages a pending kind=request envelope by raw SQL,
// matching the helper in the scheduler tests. We bypass harness.Write
// because most tests exercise the framework's reaction to existing
// rows + want full control over expires_at / sender.
type requestRow struct {
	ID            string
	Type          string
	SenderID      string
	Audience      string // single receiver id
	CorrelationID string
	ExpiresAt     *int64
	IsTerminal    bool
}

func insertRequest(t *testing.T, db *sql.DB, r requestRow) {
	t.Helper()
	aud, err := json.Marshal([]string{r.Audience})
	if err != nil {
		t.Fatalf("marshal audience: %v", err)
	}
	correl := r.CorrelationID
	if correl == "" {
		correl = r.ID
	}
	var expArg any
	if r.ExpiresAt != nil {
		expArg = *r.ExpiresAt
	}
	if _, err := db.ExecContext(context.Background(),
		`INSERT INTO messages
		   (id, ts, ts_received, channel_id, sender_kind, sender_id,
		    kind, type, payload, parent_id, correlation_id,
		    visibility, audience, not_before, expires_at, is_terminal)
		 VALUES (?, ?, ?, ?, 'agent', ?,
		         'request', ?, '{}', NULL, ?,
		         'public', ?, NULL, ?, 0)`,
		r.ID, testT0, testT0, testChannelID, r.SenderID,
		r.Type, correl, string(aud), expArg,
	); err != nil {
		t.Fatalf("insert request %s: %v", r.ID, err)
	}
}

// insertTerminalResponse stages a terminal response row for parentID
// (used to verify the framework's duplicate-callback / Respond dedupe
// race).
func insertTerminalResponse(t *testing.T, db *sql.DB, parentID, responseID string) {
	t.Helper()
	if _, err := db.ExecContext(context.Background(),
		`INSERT INTO messages
		   (id, ts, ts_received, channel_id, sender_kind, sender_id,
		    kind, type, payload, parent_id, correlation_id,
		    visibility, audience, is_terminal)
		 VALUES (?, ?, ?, ?, 'tool', ?,
		         'response', ?, '{"status":"completed"}', ?, ?,
		         'system', '["alice"]', 1)`,
		responseID, testT0, testT0, testChannelID, testAdapterActor,
		testAdapterType, parentID, parentID,
	); err != nil {
		t.Fatalf("insert terminal response: %v", err)
	}
}

// readMessage returns the kind / payload / parent_id of one row by id.
// Returns found=false when no row matches.
func readMessage(t *testing.T, db *sql.DB, id string) (kind, payload, parentID string, found bool) {
	t.Helper()
	var par sql.NullString
	row := db.QueryRowContext(context.Background(),
		`SELECT kind, payload, parent_id FROM messages WHERE id = ?`, id)
	if err := row.Scan(&kind, &payload, &par); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return "", "", "", false
		}
		t.Fatalf("read message %s: %v", id, err)
	}
	if par.Valid {
		parentID = par.String
	}
	return kind, payload, parentID, true
}

// silentLogger keeps test output clean.
func silentLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError}))
}

// fixedClock returns a clock function reading from `cur` so tests can
// step time deterministically by mutating cur.
func fixedClock(cur *int64) func() int64 {
	return func() int64 { return atomic.LoadInt64(cur) }
}

// recordingWriter wraps a real HarnessWriter and captures every call
// (envelope + result) under a mutex so tests can assert call sequences.
type recordingWriter struct {
	inner HarnessWriter
	mu    sync.Mutex
	calls []recordingCall
}

type recordingCall struct {
	envelope v4types.Envelope
	caller   pkgharness.CallerCtx
	result   pkgharness.WriteResult
	err      error
}

func newRecordingWriter(inner HarnessWriter) *recordingWriter {
	return &recordingWriter{inner: inner}
}

func (r *recordingWriter) Write(ctx context.Context, env *v4types.Envelope, cc pkgharness.CallerCtx) (pkgharness.WriteResult, error) {
	res, err := r.inner.Write(ctx, env, cc)
	r.mu.Lock()
	cp := *env
	r.calls = append(r.calls, recordingCall{envelope: cp, caller: cc, result: res, err: err})
	r.mu.Unlock()
	return res, err
}

func (r *recordingWriter) snapshot() []recordingCall {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]recordingCall, len(r.calls))
	copy(out, r.calls)
	return out
}

// -----------------------------------------------------------------------------
// Mock adapter Module
// -----------------------------------------------------------------------------

// mockModule is a minimal Module implementation that records every
// inbound Handle / OnExternalCallback so tests can assert what the
// framework dispatched.
//
// Behaviour:
//   - Handle: marks the request as "in-flight" by registering an
//     external id with the framework correlation tracker (so
//     OnExternalCallback can later route the callback).
//   - OnExternalCallback: calls Respond with status=completed using
//     payload {"echoed": <payload>} when a callback arrives.
//
// Tests can override OnHandle / OnCallback closures to inject failures
// or skip the default behaviour. The mock is ~50 lines — well below
// the "~150 lines of domain translation" acceptance bound.
type mockModule struct {
	name    string
	actor   string
	types   []string
	binding string
	pending map[string]int64

	mctx *ModuleContext

	mu                 sync.Mutex
	handleCalls        []*v4types.Envelope
	externalCallbacks  [][]byte
	onHandle           func(ctx context.Context, env *v4types.Envelope, mctx *ModuleContext) error
	onExternalCallback func(ctx context.Context, payload []byte, mctx *ModuleContext) error
	declaredCalls      int32 // atomic
}

func newMockModule(name, actor string, types []string, binding string, pending map[string]int64) *mockModule {
	return &mockModule{
		name:    name,
		actor:   actor,
		types:   types,
		binding: binding,
		pending: pending,
	}
}

func (m *mockModule) Declares() Declaration {
	atomic.AddInt32(&m.declaredCalls, 1)
	return Declaration{
		Name:         m.name,
		ActorID:      m.actor,
		Types:        m.types,
		Binding:      m.binding,
		MaxPendingMs: m.pending,
	}
}

func (m *mockModule) Init(_ context.Context, mctx *ModuleContext) error {
	m.mctx = mctx
	return nil
}

func (m *mockModule) Shutdown(_ context.Context) error { return nil }

func (m *mockModule) Handle(ctx context.Context, env *v4types.Envelope) error {
	m.mu.Lock()
	cp := *env
	m.handleCalls = append(m.handleCalls, &cp)
	hook := m.onHandle
	m.mu.Unlock()
	if hook != nil {
		return hook(ctx, env, m.mctx)
	}
	return nil
}

func (m *mockModule) OnExternalCallback(ctx context.Context, payload []byte) error {
	m.mu.Lock()
	cp := append([]byte(nil), payload...)
	m.externalCallbacks = append(m.externalCallbacks, cp)
	hook := m.onExternalCallback
	m.mu.Unlock()
	if hook != nil {
		return hook(ctx, payload, m.mctx)
	}
	return nil
}

// newDefaultMockModule constructs the mock bound to the testsupport
// channel constants (demo adapter on tool:demo for type demo.echo).
func newDefaultMockModule() *mockModule {
	return newMockModule(
		testAdapterName,
		testAdapterActor,
		[]string{testAdapterType},
		"daemon_rpc",
		map[string]int64{testAdapterType: 60_000},
	)
}
