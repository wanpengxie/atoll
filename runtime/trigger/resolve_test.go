package trigger_test

import (
	"context"
	"encoding/json"
	"errors"
	"sort"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/wanpengxie/ActOS/kernel/actor"
	"github.com/wanpengxie/ActOS/kernel/actorreg"
	"github.com/wanpengxie/ActOS/kernel/message"
	"github.com/wanpengxie/ActOS/runtime/trigger"
)

// memRegistry is an in-memory actorreg.Registry for trigger tests. Mirrors
// the harness package's testsupport_test.go layout so the two stay easy
// to compare side-by-side.
type memRegistry struct {
	mu   sync.Mutex
	rows map[actor.ActorID]actorreg.Record
}

func newMemRegistry(recs ...actorreg.Record) *memRegistry {
	r := &memRegistry{rows: map[actor.ActorID]actorreg.Record{}}
	for _, rec := range recs {
		r.rows[rec.ID] = rec
	}
	return r
}

func (r *memRegistry) Lookup(_ context.Context, id actor.ActorID) (actorreg.Record, bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	rec, ok := r.rows[id]
	return rec, ok, nil
}

func (r *memRegistry) Exists(_ context.Context, id actor.ActorID) (bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	_, ok := r.rows[id]
	return ok, nil
}

func (r *memRegistry) ListActive(_ context.Context) ([]actorreg.Record, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]actorreg.Record, 0, len(r.rows))
	for _, rec := range r.rows {
		if rec.IsActive() {
			out = append(out, rec)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out, nil
}

func (r *memRegistry) Insert(_ context.Context, rec actorreg.Record) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.rows[rec.ID] = rec
	return nil
}

func (r *memRegistry) Deregister(_ context.Context, id actor.ActorID, at int64) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	rec, ok := r.rows[id]
	if !ok {
		return nil
	}
	rec.DeregisteredAt = at
	r.rows[id] = rec
	return nil
}

// fakeDeliverer counts calls for Gateway tests.
type fakeDeliverer struct {
	mu       sync.Mutex
	calls    int
	envelope []*message.Envelope
	audience [][]actor.ActorID
	err      error
}

func (f *fakeDeliverer) Deliver(_ context.Context, aud []actor.ActorID, env *message.Envelope) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	f.envelope = append(f.envelope, env)
	f.audience = append(f.audience, append([]actor.ActorID(nil), aud...))
	return f.err
}

func (f *fakeDeliverer) Calls() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

func (f *fakeDeliverer) LastAudience() []actor.ActorID {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.audience) == 0 {
		return nil
	}
	return f.audience[len(f.audience)-1]
}

// ----- Resolve tests --------------------------------------------------

func makeReg() *memRegistry {
	return newMemRegistry(
		actorreg.Record{ID: "agent:alpha", Kind: actor.KindAgent, CreatedAt: 1},
		actorreg.Record{ID: "agent:beta", Kind: actor.KindAgent, CreatedAt: 1},
		actorreg.Record{ID: "user:demo", Kind: actor.KindHuman, CreatedAt: 1},
		actorreg.Record{ID: actor.SystemActorID, Kind: actor.KindSystem, CreatedAt: 1},
	)
}

func TestResolve_ExplicitAudience_OnlyListed(t *testing.T) {
	reg := makeReg()
	env := &message.Envelope{
		ID:         "m-2",
		Sender:     message.Sender{Kind: actor.KindHuman, ID: "user:demo"},
		Kind:       message.KindEvent,
		Type:       "human.text",
		Payload:    json.RawMessage(`{}`),
		Visibility: message.VisibilityPublic,
		Audience:   message.Audience{"agent:alpha", "agent:missing"},
	}
	got, err := trigger.Resolve(context.Background(), env, reg, trigger.Options{})
	if err != nil {
		t.Fatal(err)
	}
	want := []actor.ActorID{"agent:alpha"}
	if !equalIDs(got, want) {
		t.Errorf("audience = %v, want %v", got, want)
	}
}

func TestResolve_DeregisteredActor_DroppedFromExplicitList(t *testing.T) {
	reg := makeReg()
	_ = reg.Deregister(context.Background(), "agent:beta", 1234)
	env := &message.Envelope{
		ID:         "m-3",
		Sender:     message.Sender{Kind: actor.KindHuman, ID: "user:demo"},
		Kind:       message.KindEvent,
		Type:       "human.text",
		Payload:    json.RawMessage(`{}`),
		Visibility: message.VisibilityPublic,
		Audience:   message.Audience{"agent:alpha", "agent:beta"},
	}
	got, err := trigger.Resolve(context.Background(), env, reg, trigger.Options{})
	if err != nil {
		t.Fatal(err)
	}
	want := []actor.ActorID{"agent:alpha"}
	if !equalIDs(got, want) {
		t.Errorf("audience = %v, want %v", got, want)
	}
}

// TestResolve_VisibilityIgnoredByTrigger documents the round-3 cluster F
// split between trigger fanout (audience-driven, proto-layer1 §4.1.2)
// and view fanout (visibility-driven, §4.1.3). Trigger fanout no longer
// filters by visibility — visibility only narrows view-cache delivery
// via ViewFanout. The two paths are independent.
func TestResolve_VisibilityIgnoredByTrigger(t *testing.T) {
	reg := makeReg()
	cases := []struct {
		name string
		vis  message.Visibility
		aud  message.Audience
	}{
		{"public+concrete", message.VisibilityPublic, message.Audience{"agent:beta"}},
		{"private+concrete", message.VisibilityPrivate, message.Audience{"agent:beta"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			env := &message.Envelope{
				ID:         message.ID("m-" + tc.name),
				Sender:     message.Sender{Kind: actor.KindAgent, ID: "agent:alpha"},
				Kind:       message.KindEvent,
				Type:       "agent.text",
				Payload:    json.RawMessage(`{}`),
				Visibility: tc.vis,
				Audience:   tc.aud,
			}
			got, err := trigger.Resolve(context.Background(), env, reg, trigger.Options{})
			if err != nil {
				t.Fatal(err)
			}
			if len(got) == 0 {
				t.Fatalf("trigger fanout must not be suppressed by visibility; got empty audience for %s", tc.name)
			}
		})
	}
}

func TestResolve_SystemHeartbeat_Suppressed(t *testing.T) {
	reg := makeReg()
	env := &message.Envelope{
		ID:         "m-hb",
		Sender:     message.Sender{Kind: actor.KindSystem, ID: actor.SystemActorID},
		Kind:       message.KindEvent,
		Type:       "system.heartbeat",
		Payload:    json.RawMessage(`{}`),
		Visibility: message.VisibilityPrivate,
		Audience:   message.Audience{"agent:beta"},
	}
	got, err := trigger.Resolve(context.Background(), env, reg, trigger.Options{})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 0 {
		t.Errorf("system.heartbeat should suppress fan-out, got %v", got)
	}
}

func TestResolve_SelfAddressed_TriggersSender(t *testing.T) {
	// Post wildcard removal: sender_id in audience literal triggers sender —
	// this is the agent self-schedule entry point (replaces the prior
	// BypassSelfTriggerBan mechanism).
	reg := makeReg()
	env := &message.Envelope{
		ID:         "m-self-schedule",
		Sender:     message.Sender{Kind: actor.KindAgent, ID: "agent:alpha"},
		Kind:       message.KindEvent,
		Type:       "agent.text",
		Payload:    json.RawMessage(`{}`),
		Visibility: message.VisibilityPublic,
		Audience:   message.Audience{"agent:alpha"},
	}
	got, err := trigger.Resolve(context.Background(), env, reg, trigger.Options{})
	if err != nil {
		t.Fatal(err)
	}
	want := []actor.ActorID{"agent:alpha"}
	if !equalIDs(got, want) {
		t.Errorf("audience = %v, want %v", got, want)
	}
}

func TestResolve_EmptyAudience_NoFanout(t *testing.T) {
	// Post wildcard removal: empty audience → empty fanout (no implicit
	// broadcast). Harness step 1 will catch and reject empty audience at
	// the chain boundary; trigger Resolve treats it as a no-op.
	reg := makeReg()
	env := &message.Envelope{
		ID:         "m-empty-aud",
		Sender:     message.Sender{Kind: actor.KindHuman, ID: "user:demo"},
		Kind:       message.KindEvent,
		Type:       "human.text",
		Payload:    json.RawMessage(`{}`),
		Visibility: message.VisibilityPublic,
		Audience:   nil,
	}
	got, err := trigger.Resolve(context.Background(), env, reg, trigger.Options{})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 0 {
		t.Errorf("empty audience should produce empty fan-out, got %v", got)
	}
}

func TestResolve_NilArgs_Error(t *testing.T) {
	reg := makeReg()
	if _, err := trigger.Resolve(context.Background(), nil, reg, trigger.Options{}); err == nil {
		t.Error("nil envelope should error")
	}
	env := &message.Envelope{ID: "x"}
	if _, err := trigger.Resolve(context.Background(), env, nil, trigger.Options{}); err == nil {
		t.Error("nil registry should error")
	}
}

// ----- Gateway tests --------------------------------------------------

func TestGateway_Dispatch_ImmediateInvokesDeliverer(t *testing.T) {
	reg := makeReg()
	d := &fakeDeliverer{}
	gw, err := trigger.New(trigger.Config{
		Registry:  reg,
		Deliverer: d,
		NowFn:     func() int64 { return 1000 },
	})
	if err != nil {
		t.Fatal(err)
	}
	env := &message.Envelope{
		ID:         "m-imm",
		Sender:     message.Sender{Kind: actor.KindHuman, ID: "user:demo"},
		Kind:       message.KindEvent,
		Type:       "human.text",
		Payload:    json.RawMessage(`{}`),
		Visibility: message.VisibilityPublic,
		Audience:   message.Audience{"agent:beta"},
	}
	res, err := gw.Dispatch(context.Background(), env, trigger.Options{})
	if err != nil {
		t.Fatal(err)
	}
	if res.Deferred {
		t.Error("immediate envelope should not be Deferred")
	}
	if d.Calls() != 1 {
		t.Errorf("deliverer calls = %d, want 1", d.Calls())
	}
}

func TestGateway_Dispatch_ReturnsDeliverError(t *testing.T) {
	reg := makeReg()
	wantErr := errors.New("deliver failed")
	d := &fakeDeliverer{err: wantErr}
	gw, err := trigger.New(trigger.Config{
		Registry:  reg,
		Deliverer: d,
		NowFn:     func() int64 { return 1000 },
	})
	if err != nil {
		t.Fatal(err)
	}
	env := &message.Envelope{
		ID:         "m-deliver-error",
		Sender:     message.Sender{Kind: actor.KindHuman, ID: "user:demo"},
		Kind:       message.KindEvent,
		Type:       "human.text",
		Payload:    json.RawMessage(`{}`),
		Visibility: message.VisibilityPublic,
		Audience:   message.Audience{"agent:beta"},
	}
	res, err := gw.Dispatch(context.Background(), env, trigger.Options{})
	if !errors.Is(err, wantErr) {
		t.Fatalf("Dispatch err=%v want %v", err, wantErr)
	}
	if len(res.Audience) == 0 {
		t.Fatal("Dispatch should return resolved audience with deliver error")
	}
	if d.Calls() != 1 {
		t.Errorf("deliverer calls = %d, want 1", d.Calls())
	}
}

func TestGateway_Dispatch_FutureMessageDeferred(t *testing.T) {
	reg := makeReg()
	d := &fakeDeliverer{}
	var now atomic.Int64
	now.Store(1000)
	gw, err := trigger.New(trigger.Config{
		Registry:  reg,
		Deliverer: d,
		NowFn:     now.Load,
	})
	if err != nil {
		t.Fatal(err)
	}
	notBefore := int64(2000)
	env := &message.Envelope{
		ID:         "m-future",
		Sender:     message.Sender{Kind: actor.KindHuman, ID: "user:demo"},
		Kind:       message.KindEvent,
		Type:       "human.text",
		Payload:    json.RawMessage(`{}`),
		Visibility: message.VisibilityPublic,
		Audience:   message.Audience{"agent:beta"},
		NotBefore:  &notBefore,
	}
	res, err := gw.Dispatch(context.Background(), env, trigger.Options{})
	if err != nil {
		t.Fatal(err)
	}
	if !res.Deferred {
		t.Error("future envelope should be Deferred")
	}
	if d.Calls() != 0 {
		t.Errorf("future envelope must not invoke deliverer, got %d calls", d.Calls())
	}
	// Advance the clock past not_before — Dispatch must now proceed.
	now.Store(2500)
	res, err = gw.Dispatch(context.Background(), env, trigger.Options{})
	if err != nil {
		t.Fatal(err)
	}
	if res.Deferred {
		t.Error("clock past not_before — should not be Deferred")
	}
	if d.Calls() != 1 {
		t.Errorf("post-not_before deliverer calls = %d, want 1", d.Calls())
	}
}

func TestGateway_Dispatch_SystemHeartbeatNoDeliverer(t *testing.T) {
	reg := makeReg()
	d := &fakeDeliverer{}
	gw, err := trigger.New(trigger.Config{
		Registry:  reg,
		Deliverer: d,
		NowFn:     func() int64 { return 1000 },
	})
	if err != nil {
		t.Fatal(err)
	}
	env := &message.Envelope{
		ID:         "m-hb",
		Sender:     message.Sender{Kind: actor.KindSystem, ID: actor.SystemActorID},
		Kind:       message.KindEvent,
		Type:       "system.heartbeat",
		Payload:    json.RawMessage(`{}`),
		Visibility: message.VisibilityPrivate,
		Audience:   message.Audience{"agent:beta"},
	}
	res, err := gw.Dispatch(context.Background(), env, trigger.Options{})
	if err != nil {
		t.Fatal(err)
	}
	if res.Deferred {
		t.Error("heartbeat should not be Deferred")
	}
	if d.Calls() != 0 {
		t.Errorf("heartbeat must not invoke deliverer, got %d calls", d.Calls())
	}
}

func TestGateway_New_ValidatesArgs(t *testing.T) {
	if _, err := trigger.New(trigger.Config{}); err == nil {
		t.Error("missing Registry should error")
	}
	if _, err := trigger.New(trigger.Config{Registry: makeReg()}); err == nil {
		t.Error("missing Deliverer should error")
	}
}

// equalIDs is a slice-equality helper that compares actor IDs in order
// (the trigger package guarantees sorted output).
func equalIDs(got, want []actor.ActorID) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range got {
		if got[i] != want[i] {
			return false
		}
	}
	return true
}
