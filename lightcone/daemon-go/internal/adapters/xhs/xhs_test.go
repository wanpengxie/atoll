package xhs

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/coagent-ai/daemon-go/pkg/adapter"
	"github.com/coagent-ai/daemon-go/pkg/v4types"

	internalharness "github.com/coagent-ai/daemon-go/internal/harness"
	"github.com/coagent-ai/daemon-go/internal/registry"
	"github.com/coagent-ai/daemon-go/internal/store"
	pkgharness "github.com/coagent-ai/daemon-go/pkg/harness"
)

// Test channel constants. Mirrors the pattern in
// pkg/adapter/testsupport_test.go so production engineers reading both
// suites recognize the fixture shape.
const (
	testChannelID = "ch-xhs-test"
	testAgentID   = "alice"
	testSystemID  = "system"
	testT0        = int64(1_700_000_000_000)
	testDeviceID  = "dev-pri-001"
)

// openXhsChannel builds a real-sqlite channel pre-seeded with the
// actors the framework expects + the 6 xhs types bound to
// AdapterActorID.
func openXhsChannel(t *testing.T) (*sql.DB, pkgharness.Deps) {
	t.Helper()
	dir := t.TempDir()
	db, err := store.OpenChannel(context.Background(),
		filepath.Join(dir, "messages.sqlite"), store.OpenOptions{})
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
	seedActor(AdapterActorID, "tool", adapterBinding)

	maxPending := int64(60_000)
	// L4 §2.2 schemas — keep them permissive (`type:object`) so the
	// harness Step 6 doesn't reject our test payloads; the real
	// install-time schemas come from the bootstrap saga (T3) which we
	// don't exercise here.
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
	  "event": {
	    "type": "object",
	    "required": ["note_id", "archive_path"],
	    "properties": {
	      "note_id": {"type": "string"},
	      "archive_path": {"type": "string"}
	    },
	    "additionalProperties": true
	  }
	}`)
	rows := []registry.TypeRow{
		mkTypeRow(TypePublish, []string{"request", "response"}, objectSchema, &maxPending),
		mkTypeRow(TypeSearch, []string{"request", "response"}, objectSchema, &maxPending),
		mkTypeRow(TypeNoteFetch, []string{"request", "response"}, objectSchema, &maxPending),
		mkTypeRow(TypeRecentFetch, []string{"request", "response"}, objectSchema, &maxPending),
		mkTypeRow(TypeCookieSync, []string{"request", "response"}, objectSchema, &maxPending),
		mkTypeRow(TypeNoteArchived, []string{"event"}, eventSchema, nil),
	}
	if err := store.WithImmediate(ctx, db, func(c context.Context, conn *sql.Conn) error {
		return registry.Install(c, conn, rows, testT0)
	}); err != nil {
		t.Fatalf("install xhs types: %v", err)
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

func mkTypeRow(typ string, kinds []string, schemas json.RawMessage, maxPending *int64) registry.TypeRow {
	return registry.TypeRow{
		Type:               typ,
		AllowedKinds:       kinds,
		SchemasByKind:      schemas,
		HandlerBinding:     adapterBinding,
		MaxPendingMs:       maxPending,
		HandlerActorID:     AdapterActorID,
		TerminalConvention: "single-response",
		Domain:             "xhs",
	}
}

// insertXhsRequest stages one pending request row by raw SQL. We
// bypass harness.Write so each test can drive the framework's
// reaction to a known-good envelope without re-validating every
// step.
func insertXhsRequest(t *testing.T, db *sql.DB, id, typeName string, payload string) {
	t.Helper()
	aud, _ := json.Marshal([]string{AdapterActorID})
	if _, err := db.ExecContext(context.Background(),
		`INSERT INTO messages
		   (id, ts, ts_received, channel_id, sender_kind, sender_id,
		    kind, type, payload, parent_id, correlation_id,
		    visibility, audience, not_before, expires_at, is_terminal)
		 VALUES (?, ?, ?, ?, 'agent', ?,
		         'request', ?, ?, NULL, ?,
		         'public', ?, NULL, NULL, 0)`,
		id, testT0, testT0, testChannelID, testAgentID,
		typeName, payload, id, string(aud),
	); err != nil {
		t.Fatalf("insert request %s: %v", id, err)
	}
}

// readResponse returns (payload, status, found) for the terminal
// response row whose parent_id matches requestID.
func readResponse(t *testing.T, db *sql.DB, requestID string) (string, string, bool) {
	t.Helper()
	row := db.QueryRowContext(context.Background(),
		`SELECT payload, sender_id FROM messages
		  WHERE parent_id = ? AND kind = 'response' AND is_terminal = 1
		  ORDER BY seq DESC LIMIT 1`, requestID)
	var payload, senderID string
	if err := row.Scan(&payload, &senderID); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return "", "", false
		}
		t.Fatalf("scan response: %v", err)
	}
	return payload, senderID, true
}

func silentLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError}))
}

func fixedClock(cur *int64) func() int64 {
	return func() int64 { return atomic.LoadInt64(cur) }
}

func newManagerWithMock(t *testing.T, db *sql.DB, deps pkgharness.Deps, mock *MockDeviceClient) *adapter.Manager {
	t.Helper()
	clock := int64(testT0)
	mod := New(Config{DeviceClient: mock, DefaultDeviceID: testDeviceID})
	mgr, err := adapter.NewManager(adapter.ManagerConfig{
		DB:      db,
		Deps:    deps,
		Modules: map[string]adapter.Module{AdapterName: mod},
		Clock:   fixedClock(&clock),
		Logger:  silentLogger(),
	})
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	if err := mgr.Install(context.Background()); err != nil {
		t.Fatalf("Install: %v", err)
	}
	return mgr
}

// TestDeclares_AllTypes asserts the 6 closed-set types appear in
// Declares() with positive MaxPendingMs entries.
func TestDeclares_AllTypes(t *testing.T) {
	mod := New(Config{DeviceClient: NewMockDeviceClient()})
	decl := mod.Declares()
	if decl.Name != AdapterName {
		t.Fatalf("Name = %q; want %q", decl.Name, AdapterName)
	}
	if decl.ActorID != AdapterActorID {
		t.Fatalf("ActorID = %q; want %q", decl.ActorID, AdapterActorID)
	}
	if decl.Binding != adapterBinding {
		t.Fatalf("Binding = %q; want %q", decl.Binding, adapterBinding)
	}
	want := map[string]bool{
		TypePublish:      true,
		TypeSearch:       true,
		TypeNoteFetch:    true,
		TypeRecentFetch:  true,
		TypeCookieSync:   true,
		TypeNoteArchived: true,
	}
	if len(decl.Types) != len(want) {
		t.Fatalf("Types count = %d; want %d (%v)", len(decl.Types), len(want), decl.Types)
	}
	for _, tname := range decl.Types {
		if !want[tname] {
			t.Fatalf("unexpected type %q in Declares()", tname)
		}
		if v := decl.MaxPendingMs[tname]; v <= 0 {
			t.Fatalf("MaxPendingMs[%q] = %d; want > 0", tname, v)
		}
	}
	if err := decl.Validate(); err != nil {
		t.Fatalf("Declaration.Validate: %v", err)
	}
}

// TestDeclares_MaxPendingOverride confirms Config.MaxPendingMs wins
// when supplied.
func TestDeclares_MaxPendingOverride(t *testing.T) {
	mod := New(Config{
		DeviceClient: NewMockDeviceClient(),
		MaxPendingMs: map[string]int64{TypePublish: 12_345},
	})
	got := mod.Declares().MaxPendingMs[TypePublish]
	if got != 12_345 {
		t.Fatalf("MaxPendingMs[publish] = %d; want 12345", got)
	}
	// Unspecified types should still default.
	if mod.Declares().MaxPendingMs[TypeSearch] != defaultMaxPendingMs {
		t.Fatalf("default MaxPendingMs not applied for unspecified type")
	}
}

// TestNew_NilDeviceClientPanics asserts the constructor refuses to
// build an adapter without a DeviceClient.
func TestNew_NilDeviceClientPanics(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic when DeviceClient is nil")
		}
	}()
	_ = New(Config{})
}

// dispatchEnvelope sends the in-memory envelope through the manager
// and returns any dispatch error.
func dispatchEnvelope(t *testing.T, mgr *adapter.Manager, id, typeName, payload string) {
	t.Helper()
	env := &v4types.Envelope{
		ID:         id,
		TS:         testT0,
		ChannelID:  testChannelID,
		Sender:     v4types.Sender{Kind: v4types.SenderAgent, ID: testAgentID},
		Kind:       v4types.KindRequest,
		Type:       typeName,
		Payload:    json.RawMessage(payload),
		Visibility: v4types.VisibilityPublic,
		Audience:   []string{AdapterActorID},
	}
	if err := mgr.Dispatch(context.Background(), env); err != nil {
		t.Fatalf("Dispatch(%s): %v", typeName, err)
	}
}

// TestHandle_PushesPerTypeFrame iterates every request/response type
// and asserts Handle pushes a WS frame with the matching `cmd` plus
// strips the device_id field from the payload-derived params.
func TestHandle_PushesPerTypeFrame(t *testing.T) {
	cases := []struct {
		typ     string
		payload string
		cmd     string
	}{
		{TypePublish, `{"title":"t","content":"c","device_id":"dev-pri-001"}`, "publish"},
		{TypeSearch, `{"query":"q"}`, "search"},
		{TypeNoteFetch, `{"note_id":"n1"}`, "note.fetch"},
		{TypeRecentFetch, `{"limit":5}`, "recent.fetch"},
		{TypeCookieSync, `{}`, "cookie.sync"},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.typ, func(t *testing.T) {
			db, deps := openXhsChannel(t)
			insertXhsRequest(t, db, "req-"+tc.cmd, tc.typ, tc.payload)
			mock := NewMockDeviceClient()
			mgr := newManagerWithMock(t, db, deps, mock)
			dispatchEnvelope(t, mgr, "req-"+tc.cmd, tc.typ, tc.payload)

			sends := mock.Sends()
			if len(sends) != 1 {
				t.Fatalf("expected 1 push, got %d", len(sends))
			}
			if sends[0].DeviceID != testDeviceID {
				t.Fatalf("DeviceID = %q; want %q", sends[0].DeviceID, testDeviceID)
			}
			if sends[0].Command.Cmd != tc.cmd {
				t.Fatalf("cmd = %q; want %q", sends[0].Command.Cmd, tc.cmd)
			}
			if sends[0].Command.CorrelationID != "req-"+tc.cmd {
				t.Fatalf("correlation_id = %q; want %q", sends[0].Command.CorrelationID, "req-"+tc.cmd)
			}
			if _, leak := sends[0].Command.Params["device_id"]; leak {
				t.Fatalf("device_id should NOT appear in Command.Params")
			}
			if sends[0].Command.Type != "command" {
				t.Fatalf("frame type = %q; want \"command\"", sends[0].Command.Type)
			}
		})
	}
}

// TestRoundTrip_CompletedCallback covers the full happy path: insert
// request → Handle pushes → external callback (status=ok) → Respond
// emits a terminal completed envelope with sender tool:xhs-adapter +
// device_id in payload (not in sender.id).
func TestRoundTrip_CompletedCallback(t *testing.T) {
	db, deps := openXhsChannel(t)
	requestID := "req-publish-1"
	insertXhsRequest(t, db, requestID, TypePublish,
		`{"title":"hello","content":"world","device_id":"dev-pri-001"}`)
	mock := NewMockDeviceClient()
	mgr := newManagerWithMock(t, db, deps, mock)
	dispatchEnvelope(t, mgr, requestID, TypePublish,
		`{"title":"hello","content":"world","device_id":"dev-pri-001"}`)

	// Simulate extension callback (status=ok with note_id + url).
	cb := []byte(`{"correlation_id":"req-publish-1","device_id":"dev-pri-001","status":"ok","result":{"note_id":"n123","url":"https://xhs/n123"}}`)
	if err := mgr.OnExternalCallback(context.Background(), AdapterName, cb); err != nil {
		t.Fatalf("OnExternalCallback: %v", err)
	}

	payload, senderID, ok := readResponse(t, db, requestID)
	if !ok {
		t.Fatalf("no terminal response written for %s", requestID)
	}
	if senderID != AdapterActorID {
		t.Fatalf("response sender_id = %q; want %q", senderID, AdapterActorID)
	}
	var got map[string]any
	if err := json.Unmarshal([]byte(payload), &got); err != nil {
		t.Fatalf("decode response payload: %v", err)
	}
	if got["status"] != "completed" {
		t.Fatalf("status = %v; want \"completed\"", got["status"])
	}
	if got["device_id"] != "dev-pri-001" {
		t.Fatalf("device_id missing in payload; got %v", got["device_id"])
	}
	if got["note_id"] != "n123" {
		t.Fatalf("note_id = %v; want n123", got["note_id"])
	}
	if got["url"] != "https://xhs/n123" {
		t.Fatalf("url = %v; want https://xhs/n123", got["url"])
	}
}

// TestRoundTrip_FailedCallback verifies status=error callbacks land
// as failed terminals with reason propagated from the extension.
func TestRoundTrip_FailedCallback(t *testing.T) {
	db, deps := openXhsChannel(t)
	requestID := "req-publish-fail"
	insertXhsRequest(t, db, requestID, TypePublish, `{}`)
	mock := NewMockDeviceClient()
	mgr := newManagerWithMock(t, db, deps, mock)
	dispatchEnvelope(t, mgr, requestID, TypePublish, `{}`)

	cb := []byte(`{"correlation_id":"req-publish-fail","device_id":"dev-pri-001","status":"error","error":{"reason":"login_expired","retry_after":60}}`)
	if err := mgr.OnExternalCallback(context.Background(), AdapterName, cb); err != nil {
		t.Fatalf("OnExternalCallback: %v", err)
	}
	payload, _, ok := readResponse(t, db, requestID)
	if !ok {
		t.Fatalf("no terminal response written")
	}
	var got map[string]any
	if err := json.Unmarshal([]byte(payload), &got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got["status"] != "failed" {
		t.Fatalf("status = %v; want failed", got["status"])
	}
	if got["reason"] != "login_expired" {
		t.Fatalf("reason = %v; want login_expired", got["reason"])
	}
	if got["retry_after"] != float64(60) {
		t.Fatalf("retry_after = %v; want 60", got["retry_after"])
	}
}

// TestHandle_DeviceOfflineFailsFast: push returns ErrDeviceOffline →
// adapter emits a failed terminal immediately with reason
// device_offline.
func TestHandle_DeviceOfflineFailsFast(t *testing.T) {
	db, deps := openXhsChannel(t)
	requestID := "req-publish-offline"
	insertXhsRequest(t, db, requestID, TypePublish, `{}`)
	mock := NewMockDeviceClient()
	mock.SetPushErr(ErrDeviceOffline)
	mgr := newManagerWithMock(t, db, deps, mock)

	// Manager.Dispatch returns whatever the module returns. Our adapter
	// emits FailTerminal then returns the underlying respond err (nil
	// on happy path). We accept either nil or a non-nil error here —
	// the contract is "terminal response row appears".
	env := &v4types.Envelope{
		ID: requestID, TS: testT0, ChannelID: testChannelID,
		Sender:     v4types.Sender{Kind: v4types.SenderAgent, ID: testAgentID},
		Kind:       v4types.KindRequest,
		Type:       TypePublish,
		Payload:    json.RawMessage(`{}`),
		Visibility: v4types.VisibilityPublic,
		Audience:   []string{AdapterActorID},
	}
	_ = mgr.Dispatch(context.Background(), env)

	payload, _, ok := readResponse(t, db, requestID)
	if !ok {
		t.Fatalf("expected a failed terminal response for offline push")
	}
	if !strings.Contains(payload, `"status":"failed"`) {
		t.Fatalf("response should be failed; got %s", payload)
	}
	if !strings.Contains(payload, `"reason":"device_offline"`) {
		t.Fatalf("reason should be device_offline; got %s", payload)
	}
}

// TestHandle_MissingDeviceIDFailsFast: payload omits device_id and
// Config has no DefaultDeviceID → failed terminal with reason
// device_id_missing.
func TestHandle_MissingDeviceIDFailsFast(t *testing.T) {
	db, deps := openXhsChannel(t)
	requestID := "req-no-device"
	insertXhsRequest(t, db, requestID, TypeSearch, `{"query":"q"}`)
	mock := NewMockDeviceClient()
	clock := int64(testT0)
	// Build a manager with NO DefaultDeviceID so the missing-id path
	// triggers.
	mod := New(Config{DeviceClient: mock})
	mgr, err := adapter.NewManager(adapter.ManagerConfig{
		DB: db, Deps: deps,
		Modules: map[string]adapter.Module{AdapterName: mod},
		Clock:   fixedClock(&clock),
		Logger:  silentLogger(),
	})
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	if err := mgr.Install(context.Background()); err != nil {
		t.Fatalf("Install: %v", err)
	}

	env := &v4types.Envelope{
		ID: requestID, TS: testT0, ChannelID: testChannelID,
		Sender:     v4types.Sender{Kind: v4types.SenderAgent, ID: testAgentID},
		Kind:       v4types.KindRequest,
		Type:       TypeSearch,
		Payload:    json.RawMessage(`{"query":"q"}`),
		Visibility: v4types.VisibilityPublic,
		Audience:   []string{AdapterActorID},
	}
	_ = mgr.Dispatch(context.Background(), env)
	if len(mock.Sends()) != 0 {
		t.Fatalf("expected no push when device_id is missing")
	}
	payload, _, ok := readResponse(t, db, requestID)
	if !ok {
		t.Fatalf("expected a failed terminal response")
	}
	if !strings.Contains(payload, `"reason":"device_id_missing"`) {
		t.Fatalf("reason should be device_id_missing; got %s", payload)
	}
}

// TestOnExternalCallback_OrphanIsDropped verifies that callbacks for
// unknown correlation_ids return nil without writing a message (the
// framework's GC observability already covers stale entries).
func TestOnExternalCallback_OrphanIsDropped(t *testing.T) {
	db, deps := openXhsChannel(t)
	mgr := newManagerWithMock(t, db, deps, NewMockDeviceClient())
	cb := []byte(`{"correlation_id":"never-tracked","status":"ok"}`)
	if err := mgr.OnExternalCallback(context.Background(), AdapterName, cb); err != nil {
		t.Fatalf("OnExternalCallback orphan: %v", err)
	}
	var n int
	if err := db.QueryRowContext(context.Background(),
		`SELECT COUNT(*) FROM messages WHERE kind='response'`).Scan(&n); err != nil {
		t.Fatalf("count responses: %v", err)
	}
	if n != 0 {
		t.Fatalf("expected zero responses on orphan callback; got %d", n)
	}
}

// TestOnExternalCallback_RejectsMissingCorrelation guards the
// JSON-decoded body validator.
func TestOnExternalCallback_RejectsMissingCorrelation(t *testing.T) {
	db, deps := openXhsChannel(t)
	mgr := newManagerWithMock(t, db, deps, NewMockDeviceClient())
	cb := []byte(`{"status":"ok"}`)
	err := mgr.OnExternalCallback(context.Background(), AdapterName, cb)
	if err == nil || !strings.Contains(err.Error(), "correlation_id") {
		t.Fatalf("expected missing correlation_id error; got %v", err)
	}
}

// TestRegister_ProvidesFactory documents the init-time Register hook
// so future daemon main wiring can rely on it.
func TestRegister_ProvidesFactory(t *testing.T) {
	factories := adapter.RegisteredModules()
	if _, ok := factories[AdapterName]; !ok {
		t.Fatalf("expected adapter.Register to publish %q", AdapterName)
	}
}
