package v4tool

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	internalharness "github.com/coagent-ai/daemon-go/internal/harness"
	"github.com/coagent-ai/daemon-go/internal/registry"
	"github.com/coagent-ai/daemon-go/internal/store"
	pkgharness "github.com/coagent-ai/daemon-go/pkg/harness"
	"github.com/coagent-ai/daemon-go/pkg/v4types"

	"github.com/wanpengxie/go-kimi/pkg/kimi/types"
)

// -----------------------------------------------------------------------------
// Test fixtures
// -----------------------------------------------------------------------------

// fakeTool is a mock implementation of go-kimi tools.Tool. It records
// every call + replays a caller-supplied script of results.
type fakeTool struct {
	name        string
	description string
	schema      json.RawMessage

	calls      atomic.Int32
	nextResult types.ToolResult
	nextErr    error
}

func (f *fakeTool) Name() string                     { return f.name }
func (f *fakeTool) Description() string              { return f.description }
func (f *fakeTool) ParameterSchema() json.RawMessage { return f.schema }
func (f *fakeTool) Execute(_ context.Context, _ json.RawMessage) (types.ToolResult, error) {
	f.calls.Add(1)
	return f.nextResult, f.nextErr
}

// fixture bundles a fresh channel sqlite + harness deps + seeded actors
// + an installed `fs.read` type so wrapper tests can write without
// reassembling the boilerplate per case.
type fixture struct {
	db    *sql.DB
	deps  pkgharness.Deps
	clock int64
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	ctx := context.Background()
	db, err := store.OpenChannel(ctx, filepath.Join(t.TempDir(), "messages.sqlite"), store.OpenOptions{})
	if err != nil {
		t.Fatalf("open channel: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	now := int64(1700000000)
	seedActor := func(id, kind, binding string) {
		var bindArg any
		if binding != "" {
			bindArg = binding
		}
		if _, err := db.ExecContext(ctx,
			`INSERT INTO actor_registry (actor_id, actor_kind, actor_binding, created_at, deregistered_at)
			 VALUES (?, ?, ?, ?, NULL)`, id, kind, bindArg, now,
		); err != nil {
			t.Fatalf("seed actor %s: %v", id, err)
		}
		if _, err := db.ExecContext(ctx,
			`INSERT INTO actor_cursors (actor_id, last_consumed_seq, last_consumed_id, updated_at)
			 VALUES (?, 0, NULL, ?)`, id, now,
		); err != nil {
			t.Fatalf("seed cursor %s: %v", id, err)
		}
	}
	seedActor("alice", "agent", "in_worker_bus")
	seedActor("tool:fs.read", "tool", "in_worker_bus")
	seedActor("system", "system", "")

	// Install the fs.read type — both request + response schemas accept
	// any object, but include the response status/reason shape so the
	// fallback-branch install check passes.
	requestSchema := map[string]any{"type": "object"}
	responseSchema := map[string]any{
		"type":     "object",
		"required": []string{"status"},
		"properties": map[string]any{
			"status": map[string]any{"type": "string", "enum": []string{"completed", "failed"}},
			"reason": map[string]any{"type": "string"},
		},
	}
	schemas := mustJSON(map[string]any{"request": requestSchema, "response": responseSchema})

	maxPending := int64(5000)
	row := registry.TypeRow{
		Type:           "fs.read",
		AllowedKinds:   []string{"request", "response"},
		SchemasByKind:  schemas,
		HandlerBinding: registry.HandlerBindingInWorkerBus,
		MaxPendingMs:   &maxPending,
		HandlerActorID: "tool:fs.read",
	}
	if err := store.WithImmediate(ctx, db, func(ctx context.Context, conn *sql.Conn) error {
		return registry.Install(ctx, conn, []registry.TypeRow{row}, now)
	}); err != nil {
		t.Fatalf("install fs.read: %v", err)
	}

	typeLookup, err := internalharness.LoadTypeLookup(ctx, db)
	if err != nil {
		t.Fatalf("load types: %v", err)
	}
	clk := int64(1700000001_000)
	deps := pkgharness.New(
		internalharness.NewSQLiteStore(db),
		internalharness.NewSQLiteActors(db),
		typeLookup,
		internalharness.NewSQLiteWorkerLocks(db),
		"ch-1",
	)
	deps.Clock = func() int64 { return clk }
	return &fixture{db: db, deps: deps, clock: clk}
}

func mustJSON(v any) json.RawMessage {
	raw, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return raw
}

func (f *fixture) baseConfig() Config {
	clk := f.clock
	return Config{
		TypeName:      "fs.read",
		ToolActorID:   "tool:fs.read",
		CallerActorID: "alice",
		ChannelID:     "ch-1",
		FencingToken:  0,
		TurnID:        "turn:alice:trig-1",
		LedgerExec:    f.db,
		Deps:          f.deps,
		Clock:         func() int64 { clk += 1; return clk },
		NowSec:        func() int64 { return 1700000002 },
	}
}

// countMessages returns the count of channel messages matching a kind +
// type pair. Tests assert the request/response rows landed.
func (f *fixture) countMessages(t *testing.T, kind, typeName string) int {
	t.Helper()
	row := f.db.QueryRowContext(context.Background(),
		`SELECT COUNT(*) FROM messages WHERE kind = ? AND type = ?`, kind, typeName)
	var n int
	if err := row.Scan(&n); err != nil {
		t.Fatalf("count %s/%s: %v", kind, typeName, err)
	}
	return n
}

// fetchRow loads the single matching message envelope shape from
// sqlite. Tests use it to assert sender / audience / parent_id.
func (f *fixture) fetchRow(t *testing.T, id string) map[string]any {
	t.Helper()
	row := f.db.QueryRowContext(context.Background(),
		`SELECT id, kind, type, sender_kind, sender_id, audience, parent_id,
		        visibility, payload
		   FROM messages WHERE id = ?`, id)
	var (
		gotID, kind, typeName, senderKind, senderID, audience, parentID, visibility, payload sql.NullString
	)
	if err := row.Scan(&gotID, &kind, &typeName, &senderKind, &senderID, &audience, &parentID, &visibility, &payload); err != nil {
		t.Fatalf("fetch %s: %v", id, err)
	}
	out := map[string]any{
		"id":          gotID.String,
		"kind":        kind.String,
		"type":        typeName.String,
		"sender_kind": senderKind.String,
		"sender_id":   senderID.String,
		"audience":    audience.String,
		"parent_id":   parentID.String,
		"visibility":  visibility.String,
		"payload":     payload.String,
	}
	return out
}

// ledgerStatus reads the action_ledger row's status for an envelope id.
func (f *fixture) ledgerStatus(t *testing.T, envelopeID string) string {
	t.Helper()
	row := f.db.QueryRowContext(context.Background(),
		`SELECT status FROM action_ledger WHERE envelope_id = ?`, envelopeID)
	var status string
	if err := row.Scan(&status); err != nil {
		t.Fatalf("ledger lookup %s: %v", envelopeID, err)
	}
	return status
}

// -----------------------------------------------------------------------------
// V4ize validation
// -----------------------------------------------------------------------------

func TestV4ize_RejectsMissingFields(t *testing.T) {
	t.Parallel()
	fix := newFixture(t)
	good := fix.baseConfig()
	tool := &fakeTool{name: "read_file", description: "read", schema: json.RawMessage(`{}`)}

	cases := []struct {
		name string
		mut  func(c *Config)
	}{
		{"missing type", func(c *Config) { c.TypeName = "" }},
		{"missing tool actor", func(c *Config) { c.ToolActorID = "" }},
		{"missing caller", func(c *Config) { c.CallerActorID = "" }},
		{"missing channel", func(c *Config) { c.ChannelID = "" }},
		{"missing turn", func(c *Config) { c.TurnID = "" }},
		{"missing ledger exec", func(c *Config) { c.LedgerExec = nil }},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			cfg := good
			tc.mut(&cfg)
			if _, err := V4ize(tool, cfg); err == nil {
				t.Fatalf("V4ize must reject missing field %q", tc.name)
			}
		})
	}

	if _, err := V4ize(nil, good); err == nil {
		t.Fatalf("V4ize must reject nil inner tool")
	}
}

// -----------------------------------------------------------------------------
// Happy-path round-trip
// -----------------------------------------------------------------------------

func TestExecute_HappyPath_EmitsRequestAndResponse(t *testing.T) {
	t.Parallel()
	fix := newFixture(t)
	tool := &fakeTool{
		name: "read_file", description: "read", schema: json.RawMessage(`{}`),
		nextResult: types.ToolResult{
			Name:  "read_file",
			Value: types.ToolReturnValue{Value: map[string]any{"content": "hi"}},
		},
	}
	w, err := V4ize(tool, fix.baseConfig())
	if err != nil {
		t.Fatalf("V4ize: %v", err)
	}

	res, err := w.Execute(context.Background(), json.RawMessage(`{"path":"a.txt"}`))
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if res.IsError {
		t.Fatalf("expected non-error result, got IsError=true value=%v", res.Value.Value)
	}
	if tool.calls.Load() != 1 {
		t.Fatalf("expected inner.Execute once, got %d", tool.calls.Load())
	}

	// Request + response rows present.
	if got := fix.countMessages(t, "request", "fs.read"); got != 1 {
		t.Fatalf("expected 1 request row, got %d", got)
	}
	if got := fix.countMessages(t, "response", "fs.read"); got != 1 {
		t.Fatalf("expected 1 response row, got %d", got)
	}

	// Locate the request row by querying via type+kind=request.
	row := fix.db.QueryRowContext(context.Background(),
		`SELECT id FROM messages WHERE kind = 'request' AND type = 'fs.read'`)
	var requestID string
	if err := row.Scan(&requestID); err != nil {
		t.Fatalf("request id: %v", err)
	}

	reqMeta := fix.fetchRow(t, requestID)
	if reqMeta["sender_id"] != "alice" || reqMeta["sender_kind"] != "agent" {
		t.Fatalf("request sender mismatch: %+v", reqMeta)
	}
	if !strings.Contains(reqMeta["audience"].(string), "tool:fs.read") {
		t.Fatalf("request audience missing tool actor: %v", reqMeta["audience"])
	}
	if reqMeta["visibility"] != "system" {
		t.Fatalf("request visibility = %q, want system", reqMeta["visibility"])
	}

	// Response row points back to the request.
	row = fix.db.QueryRowContext(context.Background(),
		`SELECT id FROM messages WHERE kind = 'response' AND type = 'fs.read'`)
	var responseID string
	if err := row.Scan(&responseID); err != nil {
		t.Fatalf("response id: %v", err)
	}
	respMeta := fix.fetchRow(t, responseID)
	if respMeta["sender_id"] != "tool:fs.read" || respMeta["sender_kind"] != "tool" {
		t.Fatalf("response sender mismatch: %+v", respMeta)
	}
	if respMeta["parent_id"] != requestID {
		t.Fatalf("response parent_id %q != request %q", respMeta["parent_id"], requestID)
	}
	if respMeta["visibility"] != "system" {
		t.Fatalf("response visibility = %q, want system", respMeta["visibility"])
	}
	if !strings.HasPrefix(respMeta["id"].(string), "response:"+requestID+":") {
		t.Fatalf("response id %q does not encode request id deterministically", respMeta["id"])
	}

	// Ledger row committed.
	if status := fix.ledgerStatus(t, requestID); status != "committed" {
		t.Fatalf("ledger status = %q, want committed", status)
	}
}

// -----------------------------------------------------------------------------
// Replay: same params + same turn → same envelope id, ledger replays
// -----------------------------------------------------------------------------

func TestExecute_Replay_ReusesEnvelopeIDAndDedupes(t *testing.T) {
	t.Parallel()
	fix := newFixture(t)
	tool := &fakeTool{
		name: "read_file", description: "read",
		schema: json.RawMessage(`{}`),
		nextResult: types.ToolResult{
			Value: types.ToolReturnValue{Value: map[string]any{"content": "hi"}},
		},
	}
	cfg := fix.baseConfig()
	w, err := V4ize(tool, cfg)
	if err != nil {
		t.Fatalf("V4ize: %v", err)
	}

	params := json.RawMessage(`{"path":"a.txt"}`)
	r1, err := w.Execute(context.Background(), params)
	if err != nil {
		t.Fatalf("first execute: %v", err)
	}
	t.Logf("first result: IsError=%v value=%v", r1.IsError, r1.Value.Value)
	// Second pass with identical params + turn — wrapper should reuse
	// the ledger row and harness should step-0.5 dedupe the request
	// row instead of inserting a new one.
	r2, err := w.Execute(context.Background(), params)
	if err != nil {
		t.Fatalf("replay execute: %v", err)
	}
	t.Logf("replay result: IsError=%v value=%v", r2.IsError, r2.Value.Value)

	if got := fix.countMessages(t, "request", "fs.read"); got != 1 {
		t.Fatalf("replay should not duplicate request row, got %d", got)
	}
	if got := fix.countMessages(t, "response", "fs.read"); got != 1 {
		t.Fatalf("replay should not duplicate response row, got %d", got)
	}
	// Replay path reconstructed the response from the persisted row —
	// inner.Execute MUST not run a second time (L2 §3.9.4 exactly-once
	// side effect guarantee within a turn).
	if tool.calls.Load() != 1 {
		t.Fatalf("inner.Execute call count = %d, want 1 (replay must reuse prior response)", tool.calls.Load())
	}

	// Both calls returned the same value reconstructed from the stored
	// response payload.
	if r1.IsError || r2.IsError {
		t.Fatalf("unexpected error on either run: r1=%v r2=%v", r1, r2)
	}
}

// -----------------------------------------------------------------------------
// Harness reject (unknown audience) → ToolResult{IsError=true},
// inner not called.
// -----------------------------------------------------------------------------

func TestExecute_RejectBeforeInnerCall(t *testing.T) {
	t.Parallel()
	fix := newFixture(t)
	tool := &fakeTool{
		name: "read_file", description: "read", schema: json.RawMessage(`{}`),
		nextResult: types.ToolResult{
			Value: types.ToolReturnValue{Value: map[string]any{"content": "hi"}},
		},
	}
	cfg := fix.baseConfig()
	// Point the wrapper at an actor that was never registered. Step 5
	// audience check rejects with audience_actor_not_registered.
	cfg.ToolActorID = "tool:does-not-exist"
	cfg.TypeName = "fs.read" // type row already requires handler=tool:fs.read

	w, err := V4ize(tool, cfg)
	if err != nil {
		t.Fatalf("V4ize: %v", err)
	}
	res, err := w.Execute(context.Background(), json.RawMessage(`{"path":"a.txt"}`))
	if err != nil {
		t.Fatalf("Execute should not return infra error, got %v", err)
	}
	if !res.IsError {
		t.Fatalf("expected IsError=true on harness reject")
	}
	if tool.calls.Load() != 0 {
		t.Fatalf("inner.Execute MUST NOT run after harness reject (calls=%d)", tool.calls.Load())
	}
	// Reason should be one of the L1 §10.3.1 closed-set values — we
	// don't pin the exact reason (audience_actor_not_registered vs
	// audience_handler_mismatch is harness-internal), but it should
	// be non-empty.
	val := res.Value.Value.(map[string]any)
	if val["status"] != "failed" {
		t.Fatalf("expected status=failed, got %v", val)
	}
	if val["reason"] == "" || val["reason"] == nil {
		t.Fatalf("expected non-empty reject reason, got %v", val)
	}

	// No rows landed.
	if got := fix.countMessages(t, "request", "fs.read"); got != 0 {
		t.Fatalf("rejected request must not persist, got %d rows", got)
	}
}

// -----------------------------------------------------------------------------
// Inner tool failure → response with status=failed
// -----------------------------------------------------------------------------

func TestExecute_InnerError_LogsResponseFailure(t *testing.T) {
	t.Parallel()
	fix := newFixture(t)
	tool := &fakeTool{
		name: "read_file", description: "read", schema: json.RawMessage(`{}`),
		nextErr: errors.New("file not found"),
	}
	w, err := V4ize(tool, fix.baseConfig())
	if err != nil {
		t.Fatalf("V4ize: %v", err)
	}

	res, err := w.Execute(context.Background(), json.RawMessage(`{"path":"a.txt"}`))
	if err == nil || !strings.Contains(err.Error(), "file not found") {
		t.Fatalf("expected inner error to propagate, got res=%+v err=%v", res, err)
	}

	// Response row records the failure.
	if got := fix.countMessages(t, "response", "fs.read"); got != 1 {
		t.Fatalf("expected 1 response row, got %d", got)
	}
	row := fix.db.QueryRowContext(context.Background(),
		`SELECT payload FROM messages WHERE kind='response' AND type='fs.read'`)
	var payload string
	if err := row.Scan(&payload); err != nil {
		t.Fatalf("scan: %v", err)
	}
	var parsed map[string]any
	if err := json.Unmarshal([]byte(payload), &parsed); err != nil {
		t.Fatalf("parse payload: %v", err)
	}
	if parsed["status"] != "failed" {
		t.Fatalf("status = %v, want failed", parsed["status"])
	}
	if parsed["reason"] != "tool_error" {
		t.Fatalf("reason = %v, want tool_error", parsed["reason"])
	}
}

// -----------------------------------------------------------------------------
// Response-write infra failure: ledger MUST stay reserved + inner runs once
// -----------------------------------------------------------------------------

// failingResponseStore wraps a pkgharness.Store and surfaces a fixed
// infrastructure error from InsertMessage when the envelope kind is
// `response`. Request inserts and all read paths pass through to the
// underlying store unchanged so the harness's earlier steps (Step 0.5
// dedupe / Step 8 parent lookup) behave normally.
//
// The responses counter is shared via *atomic.Int32 so the tx-scoped
// inner wrapper handed to WithTerminalTx increments the same counter
// the outer fixture inspects.
type failingResponseStore struct {
	inner     pkgharness.Store
	responses *atomic.Int32
	injectErr error
}

func newFailingResponseStore(inner pkgharness.Store, injectErr error) *failingResponseStore {
	return &failingResponseStore{
		inner:     inner,
		responses: &atomic.Int32{},
		injectErr: injectErr,
	}
}

func (s *failingResponseStore) FindByID(ctx context.Context, id string) (*v4types.Envelope, error) {
	return s.inner.FindByID(ctx, id)
}
func (s *failingResponseStore) FindParent(ctx context.Context, id string) (*v4types.Envelope, error) {
	return s.inner.FindParent(ctx, id)
}
func (s *failingResponseStore) FindTerminalResponse(ctx context.Context, parentID string) (*v4types.Envelope, error) {
	return s.inner.FindTerminalResponse(ctx, parentID)
}
func (s *failingResponseStore) InsertMessage(ctx context.Context, env *v4types.Envelope, tsReceived int64) error {
	if env.Kind == v4types.KindResponse {
		s.responses.Add(1)
		return s.injectErr
	}
	return s.inner.InsertMessage(ctx, env, tsReceived)
}
func (s *failingResponseStore) WithTerminalTx(ctx context.Context, body func(tx pkgharness.Store) error) error {
	// Wrap the tx-bound store with the same response-fail policy so
	// terminal responses (kind=response IsTerminal=true) also surface
	// the infra error. The counter is shared so test assertions see
	// the tx-scoped attempt.
	return s.inner.WithTerminalTx(ctx, func(tx pkgharness.Store) error {
		return body(&failingResponseStore{inner: tx, injectErr: s.injectErr, responses: s.responses})
	})
}

// TestExecute_ResponseInfraError_LedgerNotCommitted is the FIX-3 R1
// regression (T103 / codex t96 critical) asserting:
//
//   - inner.Execute runs exactly once even though the response write
//     surfaces an infrastructure error;
//   - the wrapper returns the infra error to the caller (not the
//     possibly-successful inner result) so the upper layer reacts;
//   - the action_ledger row stays in `reserved` state — Commit MUST
//     NOT run, so audit + a future "did this finish?" observer can
//     tell the turn never completed.
//
// Prior implementation logged-and-continued, then committed the ledger
// unconditionally. The next replay turn would see status=committed,
// look for the response row (missing), fall through, and re-run inner
// — duplicating the external side effect.
func TestExecute_ResponseInfraError_LedgerNotCommitted(t *testing.T) {
	t.Parallel()
	fix := newFixture(t)

	injected := errors.New("synthetic response store infra failure")
	wrappedStore := newFailingResponseStore(fix.deps.Store, injected)
	deps := fix.deps
	deps.Store = wrappedStore

	tool := &fakeTool{
		name: "read_file", description: "read", schema: json.RawMessage(`{}`),
		nextResult: types.ToolResult{
			Name:  "read_file",
			Value: types.ToolReturnValue{Value: map[string]any{"content": "hi"}},
		},
	}
	cfg := fix.baseConfig()
	cfg.Deps = deps
	w, err := V4ize(tool, cfg)
	if err != nil {
		t.Fatalf("V4ize: %v", err)
	}

	res, err := w.Execute(context.Background(), json.RawMessage(`{"path":"a.txt"}`))
	if err == nil {
		t.Fatalf("expected infra error to surface, got nil; result=%+v", res)
	}
	if !errors.Is(err, injected) && !strings.Contains(err.Error(), "synthetic response store infra failure") {
		t.Fatalf("expected error to wrap injected store error, got %v", err)
	}
	if !res.IsError {
		t.Fatalf("expected IsError=true on response infra failure")
	}
	val, ok := res.Value.Value.(map[string]any)
	if !ok {
		t.Fatalf("expected map error value, got %T", res.Value.Value)
	}
	if val["reason"] != "harness_infra_error" {
		t.Fatalf("reason = %v, want harness_infra_error", val["reason"])
	}

	if got := tool.calls.Load(); got != 1 {
		t.Fatalf("inner.Execute call count = %d, want exactly 1", got)
	}
	if got := wrappedStore.responses.Load(); got != 1 {
		t.Fatalf("response insert attempt count = %d, want exactly 1", got)
	}

	// Request row landed (so replay can find the existing envelope id
	// via Reserve); response row did not (injected error).
	if got := fix.countMessages(t, "request", "fs.read"); got != 1 {
		t.Fatalf("expected 1 request row, got %d", got)
	}
	if got := fix.countMessages(t, "response", "fs.read"); got != 0 {
		t.Fatalf("response row must not persist after infra failure, got %d", got)
	}

	// Ledger row stays reserved — FIX B's core invariant.
	requestRow := fix.db.QueryRowContext(context.Background(),
		`SELECT id FROM messages WHERE kind = 'request' AND type = 'fs.read'`)
	var requestID string
	if err := requestRow.Scan(&requestID); err != nil {
		t.Fatalf("locate request: %v", err)
	}
	if status := fix.ledgerStatus(t, requestID); status != "reserved" {
		t.Fatalf("ledger status = %q, want reserved (Commit must NOT run on response infra failure)", status)
	}
}

// -----------------------------------------------------------------------------
// Metadata pass-through
// -----------------------------------------------------------------------------

func TestWrapper_NameAndDescription_PassThrough(t *testing.T) {
	t.Parallel()
	fix := newFixture(t)
	tool := &fakeTool{name: "read_file", description: "read file from disk",
		schema: json.RawMessage(`{"type":"object"}`)}
	cfg := fix.baseConfig()
	w, err := V4ize(tool, cfg)
	if err != nil {
		t.Fatalf("V4ize: %v", err)
	}
	if got := w.Name(); got != "fs.read" {
		t.Fatalf("Name() = %q, want fs.read (v4 type name)", got)
	}
	if got := w.Description(); got != "read file from disk" {
		t.Fatalf("Description() did not forward, got %q", got)
	}
	if got := string(w.ParameterSchema()); got != `{"type":"object"}` {
		t.Fatalf("ParameterSchema not forwarded: %q", got)
	}
}

// -----------------------------------------------------------------------------
// Payload clamp
// -----------------------------------------------------------------------------

func TestClampPayload_LargeValueTrimmed(t *testing.T) {
	t.Parallel()
	huge := strings.Repeat("x", MaxResponsePayloadChars+100)
	raw, _ := json.Marshal(map[string]any{
		"status": "completed",
		"value":  huge,
	})
	clamped := clampPayload(raw)
	if len(clamped) >= len(raw) {
		t.Fatalf("clamp did not reduce size: %d → %d", len(raw), len(clamped))
	}
	var parsed map[string]any
	if err := json.Unmarshal(clamped, &parsed); err != nil {
		t.Fatalf("clamped output not JSON: %v", err)
	}
	if parsed["status"] != "completed" {
		t.Fatalf("status flipped during clamp: %v", parsed["status"])
	}
}

// -----------------------------------------------------------------------------
// Ledger key derivation
// -----------------------------------------------------------------------------

func TestComputeLedgerKey_Deterministic(t *testing.T) {
	t.Parallel()
	a, err := computeLedgerKey("turn-1", "fs.read", json.RawMessage(`{"path":"a"}`))
	if err != nil {
		t.Fatalf("hash a: %v", err)
	}
	b, err := computeLedgerKey("turn-1", "fs.read", json.RawMessage(`{"path":"a"}`))
	if err != nil {
		t.Fatalf("hash b: %v", err)
	}
	if a != b {
		t.Fatalf("ledger key must be deterministic: %q vs %q", a, b)
	}
	c, err := computeLedgerKey("turn-1", "fs.read", json.RawMessage(`{"path":"b"}`))
	if err != nil {
		t.Fatalf("hash c: %v", err)
	}
	if a == c {
		t.Fatalf("different params must produce different ledger keys")
	}
}

// -----------------------------------------------------------------------------
// Sanity: registry actor binding (ensure seeded actor's kind is detected)
// -----------------------------------------------------------------------------

func TestFixture_ToolActorIsTool(t *testing.T) {
	t.Parallel()
	fix := newFixture(t)
	meta, err := registry.Get(context.Background(), fix.db, "tool:fs.read")
	if err != nil {
		t.Fatalf("registry get: %v", err)
	}
	if meta.Kind != registry.KindTool {
		t.Fatalf("kind = %q, want tool", meta.Kind)
	}
	if meta.Binding != registry.BindingInWorkerBus {
		t.Fatalf("binding = %q, want in_worker_bus", meta.Binding)
	}
	// Validate the harness side sees the same sender kind enum.
	if v4types.SenderKind(meta.Kind) != v4types.SenderTool {
		t.Fatalf("SenderKind conversion mismatch")
	}
}
