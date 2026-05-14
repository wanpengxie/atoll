package adapter

import (
	"context"
	"encoding/json"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coagent-ai/daemon-go/pkg/v4types"
)

// TestManager_InstallHappyPath wires a mock adapter into a Manager
// backed by real channel sqlite. Verifies the install pipeline:
//
//   - apply correlation DDL,
//   - validate adapter actor + types against the registries,
//   - construct + inject the ModuleContext into Module.Init.
func TestManager_InstallHappyPath(t *testing.T) {
	db, deps := openAdapterChannel(t)
	clock := int64(testT0)

	mod := newDefaultMockModule()
	mgr, err := NewManager(ManagerConfig{
		DB:      db,
		Deps:    deps,
		Modules: map[string]Module{mod.name: mod},
		Clock:   fixedClock(&clock),
		Logger:  silentLogger(),
	})
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	if err := mgr.Install(context.Background()); err != nil {
		t.Fatalf("Install: %v", err)
	}

	if mod.mctx == nil {
		t.Fatalf("expected Module.Init to be called with a non-nil ModuleContext")
	}
	if mod.mctx.Name != mod.name || mod.mctx.ActorID != mod.actor {
		t.Fatalf("mctx identity = (%q, %q); want (%q, %q)",
			mod.mctx.Name, mod.mctx.ActorID, mod.name, mod.actor)
	}
	if mod.mctx.ChannelID != testChannelID {
		t.Fatalf("mctx.ChannelID = %q; want %q", mod.mctx.ChannelID, testChannelID)
	}
	if mod.mctx.Correlation == nil || mod.mctx.ErrorPolicy == nil || mod.mctx.Respond == nil {
		t.Fatalf("mctx helpers should be wired; got Correlation=%v ErrorPolicy=%v Respond=%v",
			mod.mctx.Correlation != nil, mod.mctx.ErrorPolicy != nil, mod.mctx.Respond != nil)
	}

	// Verify DDL ran: adapter_correlation should exist.
	row := db.QueryRowContext(context.Background(),
		`SELECT name FROM sqlite_master WHERE type='table' AND name='adapter_correlation'`)
	var name string
	if err := row.Scan(&name); err != nil {
		t.Fatalf("adapter_correlation table missing: %v", err)
	}
}

// TestManager_InstallRejectsBadDeclaration covers the install-time
// validators that surface before Module.Init runs.
func TestManager_InstallRejectsBadDeclaration(t *testing.T) {
	cases := []struct {
		name       string
		mutate     func(m *mockModule)
		wantSubstr string
	}{
		{
			name: "unknown actor",
			mutate: func(m *mockModule) {
				m.actor = "tool:nonexistent"
			},
			wantSubstr: "not registered",
		},
		{
			name: "binding mismatch",
			mutate: func(m *mockModule) {
				m.binding = "in_worker_bus"
			},
			wantSubstr: "binding=",
		},
		{
			name: "unknown type",
			mutate: func(m *mockModule) {
				m.types = []string{"demo.unknown"}
				m.pending = map[string]int64{"demo.unknown": 1000}
			},
			wantSubstr: "not in type_registry",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			db, deps := openAdapterChannel(t)
			clock := int64(testT0)
			mod := newDefaultMockModule()
			tc.mutate(mod)
			mgr, err := NewManager(ManagerConfig{
				DB:      db,
				Deps:    deps,
				Modules: map[string]Module{mod.name: mod},
				Clock:   fixedClock(&clock),
				Logger:  silentLogger(),
			})
			if err != nil {
				t.Fatalf("NewManager: %v", err)
			}
			err = mgr.Install(context.Background())
			if err == nil || !strings.Contains(err.Error(), tc.wantSubstr) {
				t.Fatalf("Install err = %v; want substring %q", err, tc.wantSubstr)
			}
		})
	}
}

// TestManager_FullRoundTrip exercises the L2 §8 acceptance criterion
// "adapter writes ~150 lines and gets request → response":
//
//  1. Insert a pending kind=request envelope addressed to the adapter.
//  2. Manager.Dispatch routes it to Module.Handle.
//  3. Handle calls Correlation.Track + simulates an outbound external API call.
//  4. Manager.OnExternalCallback delivers a callback referencing the
//     same external_id.
//  5. Module.OnExternalCallback Recovers the request_id + calls Respond.
//  6. Channel sqlite ends up with a terminal response row + the F3 timer
//     gets cancelled, and the correlation row is forgotten.
func TestManager_FullRoundTrip(t *testing.T) {
	db, deps := openAdapterChannel(t)
	clock := int64(testT0)

	insertRequest(t, db, requestRow{
		ID:       "req-rt",
		Type:     testAdapterType,
		SenderID: testAgentID,
		Audience: testAdapterActor,
	})

	mod := newDefaultMockModule()
	const externalID = "EXT-RT-9001"
	mod.onHandle = func(ctx context.Context, env *v4types.Envelope, mctx *ModuleContext) error {
		// Track the request ↔ external mapping with a far-future deadline.
		return mctx.Correlation.Track(ctx, env.ID, externalID, testT0+120_000)
	}
	mod.onExternalCallback = func(ctx context.Context, payload []byte, mctx *ModuleContext) error {
		var cb struct {
			ExternalID string `json:"external_id"`
			NoteID     string `json:"note_id"`
		}
		if err := json.Unmarshal(payload, &cb); err != nil {
			return err
		}
		req, ok, err := mctx.Correlation.Recover(ctx, cb.ExternalID)
		if err != nil {
			return err
		}
		if !ok {
			t.Fatalf("Recover should find a tracked request")
		}
		merged := map[string]any{"note_id": cb.NoteID}
		body, _ := json.Marshal(merged)
		_, err = mctx.Respond(ctx, req, body, RespondOptions{Status: StatusCompleted})
		return err
	}

	mgr, err := NewManager(ManagerConfig{
		DB:      db,
		Deps:    deps,
		Modules: map[string]Module{mod.name: mod},
		Clock:   fixedClock(&clock),
		Logger:  silentLogger(),
	})
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	if err := mgr.Install(context.Background()); err != nil {
		t.Fatalf("Install: %v", err)
	}

	// Drive Handle by reading the envelope row + handing it to Dispatch.
	env := &v4types.Envelope{
		ID:         "req-rt",
		TS:         testT0,
		ChannelID:  testChannelID,
		Sender:     v4types.Sender{Kind: v4types.SenderAgent, ID: testAgentID},
		Kind:       v4types.KindRequest,
		Type:       testAdapterType,
		Payload:    json.RawMessage(`{}`),
		Visibility: v4types.VisibilityPublic,
		Audience:   []string{testAdapterActor},
	}
	if err := mgr.Dispatch(context.Background(), env); err != nil {
		t.Fatalf("Dispatch: %v", err)
	}

	// External callback lands.
	cb := []byte(`{"external_id":"EXT-RT-9001","note_id":"n42"}`)
	if err := mgr.OnExternalCallback(context.Background(), mod.name, cb); err != nil {
		t.Fatalf("OnExternalCallback: %v", err)
	}

	// Verify terminal response row exists with `status=completed` +
	// `note_id=n42` payload.
	var respKind, respPayload string
	if err := db.QueryRowContext(context.Background(),
		`SELECT kind, payload FROM messages WHERE parent_id = ? AND kind='response' AND is_terminal=1`,
		"req-rt").Scan(&respKind, &respPayload); err != nil {
		t.Fatalf("terminal response missing: %v", err)
	}
	if !strings.Contains(respPayload, `"status":"completed"`) {
		t.Fatalf("payload missing status=completed: %s", respPayload)
	}
	if !strings.Contains(respPayload, `"note_id":"n42"`) {
		t.Fatalf("payload missing note_id: %s", respPayload)
	}

	// Correlation row should have been Forgotten by Respond's cleanup.
	var n int
	if err := db.QueryRowContext(context.Background(),
		`SELECT COUNT(*) FROM adapter_correlation WHERE adapter_name=? AND request_id=?`,
		mod.name, "req-rt").Scan(&n); err != nil {
		t.Fatalf("count correlation: %v", err)
	}
	if n != 0 {
		t.Fatalf("expected Forget to clear correlation row, count=%d", n)
	}
}

// TestManager_BootRecoverTimers_ExpiredFails covers F6 acceptance:
// daemon crash + restart where the request already expired — the
// framework should immediately emit adapter_default_timeout terminal.
func TestManager_BootRecoverTimers_ExpiredFails(t *testing.T) {
	db, deps := openAdapterChannel(t)
	clock := int64(testT0 + 200_000)

	// Pending request whose expires_at is already in the past.
	expiresAt := int64(testT0 + 10_000) // long before clock
	insertRequest(t, db, requestRow{
		ID:        "req-expired",
		Type:      testAdapterType,
		SenderID:  testAgentID,
		Audience:  testAdapterActor,
		ExpiresAt: &expiresAt,
	})

	mod := newDefaultMockModule()
	mgr, err := NewManager(ManagerConfig{
		DB:      db,
		Deps:    deps,
		Modules: map[string]Module{mod.name: mod},
		Clock:   fixedClock(&clock),
		Logger:  silentLogger(),
	})
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	if err := mgr.Install(context.Background()); err != nil {
		t.Fatalf("Install: %v", err)
	}
	if err := mgr.BootRecoverTimers(context.Background()); err != nil {
		t.Fatalf("BootRecoverTimers: %v", err)
	}

	// Verify adapter_default_timeout terminal landed.
	var status string
	if err := db.QueryRowContext(context.Background(),
		`SELECT json_extract(payload,'$.reason') FROM messages
		  WHERE parent_id = ? AND kind='response' AND is_terminal=1`,
		"req-expired").Scan(&status); err != nil {
		t.Fatalf("terminal missing: %v", err)
	}
	if status != string(v4types.TerminalAdapterDefaultTimeout) {
		t.Fatalf("reason = %q; want %q", status, v4types.TerminalAdapterDefaultTimeout)
	}
}

// TestManager_BootRecoverTimers_RegistersFutureTimer covers F6: a
// pending request with expires_at in the future should have a fresh
// timer registered (fired only when time elapses, not immediately).
func TestManager_BootRecoverTimers_RegistersFutureTimer(t *testing.T) {
	db, deps := openAdapterChannel(t)
	clock := int64(testT0)

	expiresAt := int64(testT0 + 60_000) // 60 s after t0
	insertRequest(t, db, requestRow{
		ID:        "req-future",
		Type:      testAdapterType,
		SenderID:  testAgentID,
		Audience:  testAdapterActor,
		ExpiresAt: &expiresAt,
	})

	mod := newDefaultMockModule()
	mgr, err := NewManager(ManagerConfig{
		DB:      db,
		Deps:    deps,
		Modules: map[string]Module{mod.name: mod},
		Clock:   fixedClock(&clock),
		Logger:  silentLogger(),
	})
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	if err := mgr.Install(context.Background()); err != nil {
		t.Fatalf("Install: %v", err)
	}
	if err := mgr.BootRecoverTimers(context.Background()); err != nil {
		t.Fatalf("BootRecoverTimers: %v", err)
	}

	entry := mgr.modules[mod.name]
	if got := entry.policy.pendingTimerCount(); got != 1 {
		t.Fatalf("pendingTimerCount = %d; want 1", got)
	}

	// No terminal response yet (timer hasn't fired).
	var n int
	if err := db.QueryRowContext(context.Background(),
		`SELECT COUNT(*) FROM messages WHERE parent_id = ? AND kind='response'`,
		"req-future").Scan(&n); err != nil {
		t.Fatalf("count: %v", err)
	}
	if n != 0 {
		t.Fatalf("expected no response yet, count=%d", n)
	}
}

// TestManager_BootRecoverTimers_SkipsResolvedRequest verifies the
// NOT EXISTS terminal filter — requests that already have a terminal
// response are not re-recovered.
func TestManager_BootRecoverTimers_SkipsResolvedRequest(t *testing.T) {
	db, deps := openAdapterChannel(t)
	clock := int64(testT0 + 200_000)

	expiresAt := int64(testT0 + 10_000)
	insertRequest(t, db, requestRow{
		ID:        "req-already-done",
		Type:      testAdapterType,
		SenderID:  testAgentID,
		Audience:  testAdapterActor,
		ExpiresAt: &expiresAt,
	})
	insertTerminalResponse(t, db, "req-already-done", "response:req-already-done:precooked")

	mod := newDefaultMockModule()
	mgr, err := NewManager(ManagerConfig{
		DB:      db,
		Deps:    deps,
		Modules: map[string]Module{mod.name: mod},
		Clock:   fixedClock(&clock),
		Logger:  silentLogger(),
	})
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	if err := mgr.Install(context.Background()); err != nil {
		t.Fatalf("Install: %v", err)
	}
	if err := mgr.BootRecoverTimers(context.Background()); err != nil {
		t.Fatalf("BootRecoverTimers: %v", err)
	}

	// Only the pre-seeded terminal should exist; no extra adapter_default_timeout row.
	var n int
	if err := db.QueryRowContext(context.Background(),
		`SELECT COUNT(*) FROM messages WHERE parent_id = ? AND kind='response'`,
		"req-already-done").Scan(&n); err != nil {
		t.Fatalf("count: %v", err)
	}
	if n != 1 {
		t.Fatalf("expected 1 terminal row, got %d", n)
	}
}

// TestManager_Dispatch_AutoRegistersTimeoutTimer verifies that
// Manager.Dispatch arms the L2 §8.6 timeout timer using
// declares.max_pending_ms when the adapter does not call Timeout
// itself — this is the Ad-2 enforce path.
func TestManager_Dispatch_AutoRegistersTimeoutTimer(t *testing.T) {
	db, deps := openAdapterChannel(t)
	clock := int64(testT0)

	mod := newDefaultMockModule()
	// Adapter ignores the inbound request; we just want to see the
	// framework register the auto-timer.
	mod.onHandle = func(ctx context.Context, env *v4types.Envelope, mctx *ModuleContext) error { return nil }

	mgr, err := NewManager(ManagerConfig{
		DB:      db,
		Deps:    deps,
		Modules: map[string]Module{mod.name: mod},
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
		ID:         "req-auto",
		TS:         testT0,
		ChannelID:  testChannelID,
		Sender:     v4types.Sender{Kind: v4types.SenderAgent, ID: testAgentID},
		Kind:       v4types.KindRequest,
		Type:       testAdapterType,
		Payload:    json.RawMessage(`{}`),
		Visibility: v4types.VisibilityPublic,
		Audience:   []string{testAdapterActor},
	}
	if err := mgr.Dispatch(context.Background(), env); err != nil {
		t.Fatalf("Dispatch: %v", err)
	}
	entry := mgr.modules[mod.name]
	if got := entry.policy.pendingTimerCount(); got != 1 {
		t.Fatalf("pendingTimerCount = %d; want 1", got)
	}
}

// TestManager_RunGCStopsOnCtxCancel verifies the RunGC goroutine
// exits cleanly when the context is cancelled. Sets a very short
// GCPeriod + watches the goroutine wind down within a deadline.
func TestManager_RunGCStopsOnCtxCancel(t *testing.T) {
	db, deps := openAdapterChannel(t)
	clock := int64(testT0)
	mod := newDefaultMockModule()
	mgr, err := NewManager(ManagerConfig{
		DB:       db,
		Deps:     deps,
		Modules:  map[string]Module{mod.name: mod},
		Clock:    fixedClock(&clock),
		Logger:   silentLogger(),
		GCPeriod: 5 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	if err := mgr.Install(context.Background()); err != nil {
		t.Fatalf("Install: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		mgr.RunGC(ctx)
		close(done)
	}()
	time.Sleep(20 * time.Millisecond)
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatalf("RunGC did not exit within 1 s of ctx cancel")
	}
}

// TestManager_Shutdown_StopsTimers verifies Shutdown propagates
// Module.Shutdown + flips ErrorPolicy.shutdown so registered timers
// no longer fire.
func TestManager_Shutdown_StopsTimers(t *testing.T) {
	db, deps := openAdapterChannel(t)
	clock := int64(testT0)
	mod := newDefaultMockModule()

	var shutdownCalls int32
	mod.onHandle = func(ctx context.Context, env *v4types.Envelope, mctx *ModuleContext) error {
		return mctx.ErrorPolicy.Timeout(env.ID, 50, "adapter_default_timeout")
	}
	mgr, err := NewManager(ManagerConfig{
		DB:      db,
		Deps:    deps,
		Modules: map[string]Module{mod.name: mod},
		Clock:   fixedClock(&clock),
		Logger:  silentLogger(),
	})
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	if err := mgr.Install(context.Background()); err != nil {
		t.Fatalf("Install: %v", err)
	}

	// Track Module.Shutdown invocation via a wrapper.
	wrapped := &shutdownCountingModule{Module: mod, count: &shutdownCalls}
	mgr.modules[mod.name].module = wrapped

	env := &v4types.Envelope{
		ID:         "req-shut",
		TS:         testT0,
		ChannelID:  testChannelID,
		Sender:     v4types.Sender{Kind: v4types.SenderAgent, ID: testAgentID},
		Kind:       v4types.KindRequest,
		Type:       testAdapterType,
		Payload:    json.RawMessage(`{}`),
		Visibility: v4types.VisibilityPublic,
		Audience:   []string{testAdapterActor},
	}
	if err := mgr.Dispatch(context.Background(), env); err != nil {
		t.Fatalf("Dispatch: %v", err)
	}
	if err := mgr.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	time.Sleep(100 * time.Millisecond)

	// Module.Shutdown called.
	if got := atomic.LoadInt32(&shutdownCalls); got != 1 {
		t.Fatalf("Module.Shutdown call count = %d; want 1", got)
	}
	// Timers cleared; no terminal response (would have been
	// adapter_default_timeout) appeared.
	var n int
	if err := db.QueryRowContext(context.Background(),
		`SELECT COUNT(*) FROM messages WHERE parent_id = ? AND kind='response'`,
		"req-shut").Scan(&n); err != nil {
		t.Fatalf("count: %v", err)
	}
	if n != 0 {
		t.Fatalf("expected 0 response rows after shutdown, got %d", n)
	}
}

// shutdownCountingModule wraps a Module so tests can count
// Shutdown invocations.
type shutdownCountingModule struct {
	Module
	count *int32
}

func (s *shutdownCountingModule) Shutdown(ctx context.Context) error {
	atomic.AddInt32(s.count, 1)
	return s.Module.Shutdown(ctx)
}

// TestManager_OnExternalCallbackUnknownAdapter rejects callbacks
// addressed to an unknown adapter — protects against routing typos.
func TestManager_OnExternalCallbackUnknownAdapter(t *testing.T) {
	db, deps := openAdapterChannel(t)
	clock := int64(testT0)
	mod := newDefaultMockModule()
	mgr, err := NewManager(ManagerConfig{
		DB:      db,
		Deps:    deps,
		Modules: map[string]Module{mod.name: mod},
		Clock:   fixedClock(&clock),
		Logger:  silentLogger(),
	})
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	if err := mgr.Install(context.Background()); err != nil {
		t.Fatalf("Install: %v", err)
	}
	err = mgr.OnExternalCallback(context.Background(), "nope", []byte("{}"))
	if err == nil || !strings.Contains(err.Error(), "unknown adapter") {
		t.Fatalf("err = %v; want substring 'unknown adapter'", err)
	}
}

// TestManager_BootRecoverTimers_SkipsNonAdapterToolActor ensures
// requests addressed to tool actors NOT bound to this Manager (e.g.
// in-worker v4tool wrappers) are skipped, not errored — protects the
// daemon from crashing on channels with both built-in tool actors
// and adapter actors coexisting.
func TestManager_BootRecoverTimers_SkipsNonAdapterToolActor(t *testing.T) {
	db, deps := openAdapterChannel(t)
	clock := int64(testT0 + 200_000)

	// Seed an unrelated tool actor + a pending request addressed to it.
	if _, err := db.ExecContext(context.Background(),
		`INSERT INTO actor_registry (actor_id, actor_kind, actor_binding, created_at, deregistered_at)
		 VALUES (?, 'tool', 'in_worker_bus', ?, NULL)`,
		"tool:other", testT0); err != nil {
		t.Fatalf("seed unrelated actor: %v", err)
	}
	expiresAt := int64(testT0 + 10_000)
	insertRequest(t, db, requestRow{
		ID:        "req-other",
		Type:      testAdapterType, // type doesn't really matter for this test
		SenderID:  testAgentID,
		Audience:  "tool:other",
		ExpiresAt: &expiresAt,
	})

	mod := newDefaultMockModule()
	mgr, err := NewManager(ManagerConfig{
		DB:      db,
		Deps:    deps,
		Modules: map[string]Module{mod.name: mod},
		Clock:   fixedClock(&clock),
		Logger:  silentLogger(),
	})
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	if err := mgr.Install(context.Background()); err != nil {
		t.Fatalf("Install: %v", err)
	}
	if err := mgr.BootRecoverTimers(context.Background()); err != nil {
		t.Fatalf("BootRecoverTimers: %v", err)
	}
	// No fail terminal should land for req-other.
	var n int
	if err := db.QueryRowContext(context.Background(),
		`SELECT COUNT(*) FROM messages WHERE parent_id = ? AND kind='response'`,
		"req-other").Scan(&n); err != nil {
		t.Fatalf("count: %v", err)
	}
	if n != 0 {
		t.Fatalf("expected no response for unrelated tool, got %d", n)
	}
}

// TestManager_InstallTwiceErrors guards against accidental double-install.
func TestManager_InstallTwiceErrors(t *testing.T) {
	db, deps := openAdapterChannel(t)
	clock := int64(testT0)
	mod := newDefaultMockModule()
	mgr, err := NewManager(ManagerConfig{
		DB:      db,
		Deps:    deps,
		Modules: map[string]Module{mod.name: mod},
		Clock:   fixedClock(&clock),
		Logger:  silentLogger(),
	})
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	if err := mgr.Install(context.Background()); err != nil {
		t.Fatalf("Install 1: %v", err)
	}
	if err := mgr.Install(context.Background()); err == nil || !strings.Contains(err.Error(), "called twice") {
		t.Fatalf("Install 2 err = %v; want 'called twice'", err)
	}
}

// TestManager_ModuleNames returns a deterministic sorted list.
func TestManager_ModuleNames(t *testing.T) {
	db, deps := openAdapterChannel(t)
	clock := int64(testT0)
	mod := newDefaultMockModule()
	mgr, err := NewManager(ManagerConfig{
		DB:      db,
		Deps:    deps,
		Modules: map[string]Module{mod.name: mod},
		Clock:   fixedClock(&clock),
		Logger:  silentLogger(),
	})
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	if err := mgr.Install(context.Background()); err != nil {
		t.Fatalf("Install: %v", err)
	}
	names := mgr.ModuleNames()
	if len(names) != 1 || names[0] != mod.name {
		t.Fatalf("ModuleNames = %v; want [%q]", names, mod.name)
	}
}
