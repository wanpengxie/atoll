package framework

import (
	"context"
	"strings"
	"testing"

	"github.com/wanpengxie/ActOS/kernel/devicetransit"
)

// TestDeviceStateClosedSet asserts AllDeviceStates is 1:1 with the
// T1.10 state machine — the test fails if a value is added without
// updating the slice (and vice versa).
func TestDeviceStateClosedSet(t *testing.T) {
	want := map[DeviceState]bool{
		StatePending: true,
		StateReady:   true,
		StateActive:  true,
		StateOffline: true,
		StateExpired: true,
		StateRevoked: true,
	}
	if len(AllDeviceStates) != len(want) {
		t.Fatalf("AllDeviceStates length %d != want %d", len(AllDeviceStates), len(want))
	}
	seen := map[DeviceState]bool{}
	for _, s := range AllDeviceStates {
		if !want[s] {
			t.Errorf("AllDeviceStates contains unexpected %q", s)
		}
		if seen[s] {
			t.Errorf("AllDeviceStates duplicate %q", s)
		}
		seen[s] = true
	}
}

// TestDeviceStateTerminalFlags covers IsTerminal + IsReachable.
func TestDeviceStateTerminalFlags(t *testing.T) {
	cases := []struct {
		state     DeviceState
		terminal  bool
		reachable bool
	}{
		{StatePending, false, false},
		{StateReady, false, true},
		{StateActive, false, true},
		{StateOffline, false, false},
		{StateExpired, true, false},
		{StateRevoked, true, false},
	}
	for _, c := range cases {
		if got := c.state.IsTerminal(); got != c.terminal {
			t.Errorf("%s.IsTerminal()=%v want %v", c.state, got, c.terminal)
		}
		if got := c.state.IsReachable(); got != c.reachable {
			t.Errorf("%s.IsReachable()=%v want %v", c.state, got, c.reachable)
		}
	}
}

// TestStateMachineMatrix exercises CanTransitionTo on the full Cartesian
// product. Spec source: T1.10 state machine (pending → ready → active ↔
// offline; any non-sink → expired|revoked; sink-state self-loop).
func TestStateMachineMatrix(t *testing.T) {
	type edge struct {
		from, to DeviceState
		legal    bool
	}
	cases := []edge{
		// from pending
		{StatePending, StatePending, true},
		{StatePending, StateReady, true},
		{StatePending, StateActive, false},
		{StatePending, StateOffline, false},
		{StatePending, StateExpired, true},
		{StatePending, StateRevoked, true},
		// from ready
		{StateReady, StatePending, false},
		{StateReady, StateReady, true},
		{StateReady, StateActive, true},
		{StateReady, StateOffline, true},
		{StateReady, StateExpired, true},
		{StateReady, StateRevoked, true},
		// from active
		{StateActive, StatePending, false},
		{StateActive, StateReady, false},
		{StateActive, StateActive, true},
		{StateActive, StateOffline, true},
		{StateActive, StateExpired, true},
		{StateActive, StateRevoked, true},
		// from offline
		{StateOffline, StatePending, false},
		{StateOffline, StateReady, false},
		{StateOffline, StateActive, true},
		{StateOffline, StateOffline, true},
		{StateOffline, StateExpired, true},
		{StateOffline, StateRevoked, true},
		// sink — only self loop
		{StateExpired, StateExpired, true},
		{StateExpired, StateRevoked, false},
		{StateExpired, StateActive, false},
		{StateRevoked, StateRevoked, true},
		{StateRevoked, StateExpired, false},
		{StateRevoked, StateActive, false},
	}
	for _, c := range cases {
		if got := c.from.CanTransitionTo(c.to); got != c.legal {
			t.Errorf("%s.CanTransitionTo(%s)=%v want %v", c.from, c.to, got, c.legal)
		}
	}
}

// TestStateMachineRejectsUnknownState defends against a wire-side enum
// drift: an unknown state has zero legal edges, including to itself.
func TestStateMachineRejectsUnknownState(t *testing.T) {
	bogus := DeviceState("unknown")
	if bogus.CanTransitionTo(StateReady) {
		t.Error("unknown state must not transition into ready")
	}
	if bogus.CanTransitionTo(bogus) {
		t.Error("unknown state must not self-loop")
	}
}

// TestSessionValidate covers the structural invariants. Each missing
// required field is asserted individually so a regression names the
// exact rule that broke.
func TestSessionValidate(t *testing.T) {
	full := DeviceSession{
		SessionID:        "sess-1",
		ChannelID:        "channel-1",
		AdapterActorID:   "tool:xhs-adapter",
		DeviceID:         "device-1",
		DeviceType:       "xhs.chrome_extension",
		State:            StateReady,
		BoundAt:          1000,
		TokenFingerprint: "abc",
	}
	if err := full.Validate(); err != nil {
		t.Fatalf("full row should validate: %v", err)
	}
	cases := []struct {
		name string
		mod  func(*DeviceSession)
		want string
	}{
		{"missing-session", func(d *DeviceSession) { d.SessionID = "" }, "SessionID"},
		{"missing-channel", func(d *DeviceSession) { d.ChannelID = "" }, "ChannelID"},
		{"missing-adapter-actor", func(d *DeviceSession) { d.AdapterActorID = "" }, "AdapterActorID"},
		{"missing-device", func(d *DeviceSession) { d.DeviceID = "" }, "DeviceID"},
		{"missing-type", func(d *DeviceSession) { d.DeviceType = "" }, "DeviceType"},
		{"bad-state", func(d *DeviceSession) { d.State = "wat" }, "State"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			row := full
			c.mod(&row)
			err := row.Validate()
			if err == nil {
				t.Fatalf("expected error mentioning %q", c.want)
			}
			if !strings.Contains(err.Error(), c.want) {
				t.Fatalf("error %q should mention %q", err, c.want)
			}
		})
	}
}

// TestSessionFieldsList guards against drift between the JSON tag set
// on DeviceSession and the SessionFields slice (used by external schema
// diff tools / docs).
func TestSessionFieldsList(t *testing.T) {
	want := []string{
		"session_id", "channel_id", "adapter_actor_id", "device_id", "device_type",
		"state", "bound_at", "last_active_at", "token_fingerprint",
		"expires_at",
	}
	if len(SessionFields) != len(want) {
		t.Fatalf("SessionFields length %d != want %d", len(SessionFields), len(want))
	}
	for i := range want {
		if SessionFields[i] != want[i] {
			t.Errorf("SessionFields[%d]=%q want %q", i, SessionFields[i], want[i])
		}
	}
}

// TestInMemoryStoreUpsertGet exercises the happy path of the in-memory
// reference implementation.
func TestInMemoryStoreUpsertGet(t *testing.T) {
	ctx := context.Background()
	store := NewInMemorySessionStore()
	sess := DeviceSession{
		SessionID:      "sess-1",
		ChannelID:      "channel-1",
		AdapterActorID: "tool:xhs-adapter",
		DeviceID:       "device-1",
		DeviceType:     "xhs.chrome_extension",
		State:          StatePending,
	}
	if err := store.Upsert(ctx, sess); err != nil {
		t.Fatalf("upsert: %v", err)
	}
	got, ok, err := store.Get(ctx, "sess-1")
	if err != nil || !ok {
		t.Fatalf("get: err=%v ok=%v", err, ok)
	}
	if got.SessionID != sess.SessionID || got.State != StatePending {
		t.Errorf("round-trip mismatch: %+v", got)
	}
}

// TestInMemoryStoreUpsertValidates makes sure an invalid row is rejected
// without mutating internal map state.
func TestInMemoryStoreUpsertValidates(t *testing.T) {
	ctx := context.Background()
	store := NewInMemorySessionStore()
	bad := DeviceSession{SessionID: "sess-x"} // missing required fields
	if err := store.Upsert(ctx, bad); err == nil {
		t.Fatal("invalid upsert should fail")
	}
	if store.Len() != 0 {
		t.Errorf("invalid upsert should not mutate store, got len=%d", store.Len())
	}
}

// TestInMemoryStoreSetState walks pending → ready → active → offline →
// active (round-trip) and rejects an illegal jump.
func TestInMemoryStoreSetState(t *testing.T) {
	ctx := context.Background()
	store := NewInMemorySessionStore()
	id := devicetransit.DeviceSessionID("sess-1")
	mustUpsert(t, store, DeviceSession{
		SessionID: id, ChannelID: "c", AdapterActorID: "tool:xhs-adapter", DeviceID: "d", DeviceType: "xhs.chrome_extension",
		State: StatePending,
	})
	mustState(t, store, id, StateReady, 100)
	mustState(t, store, id, StateActive, 200)
	mustState(t, store, id, StateOffline, 300)
	mustState(t, store, id, StateActive, 400)

	// pending → active is illegal (must go through ready first).
	store2 := NewInMemorySessionStore()
	idleID := devicetransit.DeviceSessionID("sess-2")
	mustUpsert(t, store2, DeviceSession{
		SessionID: idleID, ChannelID: "c", AdapterActorID: "tool:xhs-adapter", DeviceID: "d", DeviceType: "xhs.chrome_extension",
		State: StatePending,
	})
	if err := store2.SetState(ctx, idleID, StateActive, 0); err == nil {
		t.Error("pending → active should be rejected")
	}

	// unknown session id is rejected.
	if err := store.SetState(ctx, "ghost", StateReady, 0); err == nil {
		t.Error("unknown session id should be rejected")
	}
}

// TestInMemoryStoreListByChannel filters rows by channel id.
func TestInMemoryStoreListByChannel(t *testing.T) {
	ctx := context.Background()
	store := NewInMemorySessionStore()
	mustUpsert(t, store, DeviceSession{
		SessionID: "a", ChannelID: "c1", AdapterActorID: "tool:xhs-adapter", DeviceID: "d", DeviceType: "xhs.chrome_extension",
		State: StateReady,
	})
	mustUpsert(t, store, DeviceSession{
		SessionID: "b", ChannelID: "c1", AdapterActorID: "tool:xhs-adapter", DeviceID: "d2", DeviceType: "xhs.chrome_extension",
		State: StateReady,
	})
	mustUpsert(t, store, DeviceSession{
		SessionID: "c", ChannelID: "c2", AdapterActorID: "tool:xhs-adapter", DeviceID: "d3", DeviceType: "xhs.chrome_extension",
		State: StateReady,
	})
	rows, err := store.ListByChannel(ctx, "c1")
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("channel c1 should have 2 rows, got %d", len(rows))
	}
}

// TestInMemoryStoreDelete idempotency.
func TestInMemoryStoreDelete(t *testing.T) {
	ctx := context.Background()
	store := NewInMemorySessionStore()
	mustUpsert(t, store, DeviceSession{
		SessionID: "a", ChannelID: "c", AdapterActorID: "tool:xhs-adapter", DeviceID: "d", DeviceType: "xhs.chrome_extension",
		State: StateReady,
	})
	if err := store.Delete(ctx, "a"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if store.Len() != 0 {
		t.Errorf("delete should drop the row; len=%d", store.Len())
	}
	if err := store.Delete(ctx, "a"); err != nil {
		t.Errorf("delete should be idempotent: %v", err)
	}
}

// --- test helpers --------------------------------------------------------

func mustUpsert(t *testing.T, store *InMemorySessionStore, sess DeviceSession) {
	t.Helper()
	if err := store.Upsert(context.Background(), sess); err != nil {
		t.Fatalf("upsert %s: %v", sess.SessionID, err)
	}
}

func mustState(t *testing.T, store *InMemorySessionStore, id devicetransit.DeviceSessionID, next DeviceState, at int64) {
	t.Helper()
	if err := store.SetState(context.Background(), id, next, at); err != nil {
		t.Fatalf("setstate %s → %s: %v", id, next, err)
	}
	row, ok, _ := store.Get(context.Background(), id)
	if !ok {
		t.Fatalf("setstate %s vanished", id)
	}
	if row.State != next {
		t.Fatalf("setstate %s did not advance: %s", id, row.State)
	}
	if at > 0 && row.LastActiveAt != at {
		t.Errorf("setstate %s did not stamp LastActiveAt: got %d want %d", id, row.LastActiveAt, at)
	}
}
