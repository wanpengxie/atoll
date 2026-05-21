package framework

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/wanpengxie/ActOS/kernel/actor"
	"github.com/wanpengxie/ActOS/kernel/adapter"
	"github.com/wanpengxie/ActOS/kernel/channel"
	"github.com/wanpengxie/ActOS/kernel/devicetransit"
	"github.com/wanpengxie/ActOS/kernel/message"
)

// ---- test fakes ---------------------------------------------------------

type fakeTransit struct {
	mu      sync.Mutex
	sent    []devicetransit.SendFrame
	acks    []devicetransit.AckFrame
	nextID  string
	sendErr error
}

func (f *fakeTransit) Send(_ context.Context, frame devicetransit.SendFrame) (devicetransit.FrameID, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.sendErr != nil {
		return "", f.sendErr
	}
	f.sent = append(f.sent, frame)
	if f.nextID != "" {
		id := f.nextID
		f.nextID = ""
		return devicetransit.FrameID(id), nil
	}
	return "frame-default", nil
}

func (f *fakeTransit) Ack(_ context.Context, frame devicetransit.AckFrame) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.acks = append(f.acks, frame)
	return nil
}

type fakeCorrelation struct {
	mu         sync.Mutex
	pending    map[adapter.CorrelationKey]adapter.CorrelationEntry
	done       map[adapter.CorrelationKey]bool
	expired    map[adapter.CorrelationKey]bool
	rejected   map[adapter.CorrelationKey]string
	reserveErr error
}

func newFakeCorrelation() *fakeCorrelation {
	return &fakeCorrelation{
		pending:  map[adapter.CorrelationKey]adapter.CorrelationEntry{},
		done:     map[adapter.CorrelationKey]bool{},
		expired:  map[adapter.CorrelationKey]bool{},
		rejected: map[adapter.CorrelationKey]string{},
	}
}

func (f *fakeCorrelation) Reserve(_ context.Context, e adapter.CorrelationEntry) (adapter.CorrelationEntry, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.reserveErr != nil {
		return adapter.CorrelationEntry{}, f.reserveErr
	}
	if existing, ok := f.pending[e.RequestID]; ok {
		return existing, nil
	}
	f.pending[e.RequestID] = e
	return e, nil
}

func (f *fakeCorrelation) Get(_ context.Context, id adapter.CorrelationKey) (adapter.CorrelationEntry, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	e, ok := f.pending[id]
	return e, ok, nil
}

func (f *fakeCorrelation) MarkDone(_ context.Context, id adapter.CorrelationKey) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.done[id] = true
	return nil
}

func (f *fakeCorrelation) MarkExpired(_ context.Context, id adapter.CorrelationKey) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.expired[id] = true
	return nil
}

func (f *fakeCorrelation) MarkRejected(_ context.Context, id adapter.CorrelationKey, reason string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.rejected[id] = reason
	return nil
}

func (f *fakeCorrelation) ListPending(_ context.Context) ([]adapter.CorrelationEntry, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]adapter.CorrelationEntry, 0, len(f.pending))
	for _, e := range f.pending {
		out = append(out, e)
	}
	return out, nil
}

type fakePolicy struct {
	mu             sync.Mutex
	timers         map[adapter.CorrelationKey]time.Time
	cancelled      map[adapter.CorrelationKey]bool
	externalErrors []externalErrorRecord
	armErr         error
}

type externalErrorRecord struct {
	requestID adapter.CorrelationKey
	reason    message.TerminalFailureReason
	detail    string
}

func newFakePolicy() *fakePolicy {
	return &fakePolicy{timers: map[adapter.CorrelationKey]time.Time{}, cancelled: map[adapter.CorrelationKey]bool{}}
}

func (f *fakePolicy) RegisterTimer(_ context.Context, id adapter.CorrelationKey, t time.Time) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.armErr != nil {
		return f.armErr
	}
	f.timers[id] = t
	return nil
}

func (f *fakePolicy) CancelTimer(_ context.Context, id adapter.CorrelationKey) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.cancelled[id] = true
	return nil
}

func (f *fakePolicy) OnExternalError(_ context.Context, id adapter.CorrelationKey, reason message.TerminalFailureReason, detail string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.externalErrors = append(f.externalErrors, externalErrorRecord{
		requestID: id, reason: reason, detail: detail,
	})
	return nil
}

// ---- helpers ------------------------------------------------------------

func buildProxy(t *testing.T) (*DeviceProxy, *fakeTransit, *fakeCorrelation, *fakePolicy) {
	t.Helper()
	tr := &fakeTransit{}
	cor := newFakeCorrelation()
	pol := newFakePolicy()
	p, err := NewDeviceProxy("xhs", "tool:xhs-adapter", channel.ID("channel-1"), DeviceProxyDeps{
		Transit:     tr,
		Correlation: cor,
		Policy:      pol,
	})
	if err != nil {
		t.Fatalf("NewDeviceProxy: %v", err)
	}
	p.SetClock(func() time.Time { return time.UnixMilli(1_000_000) })
	p.SetFrameIDFactory(func() string { return "frame-fixed" })
	return p, tr, cor, pol
}

func sampleRequestEnv() *message.Envelope {
	return &message.Envelope{
		ID:            "env-1",
		ChannelID:     "channel-1",
		Sender:        message.Sender{Kind: actor.KindAgent, ID: "agent:writer"},
		Kind:          message.KindRequest,
		Type:          "xhs.publish",
		Payload:       []byte(`{"title":"x"}`),
		CorrelationID: "corr-1",
		TS:            1_000_000,
	}
}

// ---- tests --------------------------------------------------------------

func TestNewDeviceProxyValidation(t *testing.T) {
	deps := DeviceProxyDeps{
		Transit: &fakeTransit{}, Correlation: newFakeCorrelation(), Policy: newFakePolicy(),
	}
	if _, err := NewDeviceProxy("", "tool:x", "c", deps); err == nil {
		t.Error("missing adapterName should error")
	}
	if _, err := NewDeviceProxy("x", "", "c", deps); err == nil {
		t.Error("missing actor id should error")
	}
	if _, err := NewDeviceProxy("x", "tool:x", "", deps); err == nil {
		t.Error("missing channel id should error")
	}
	bad := deps
	bad.Transit = nil
	if _, err := NewDeviceProxy("x", "tool:x", "c", bad); err == nil {
		t.Error("missing transit should error")
	}
	bad = deps
	bad.Correlation = nil
	if _, err := NewDeviceProxy("x", "tool:x", "c", bad); err == nil {
		t.Error("missing correlation should error")
	}
	bad = deps
	bad.Policy = nil
	if _, err := NewDeviceProxy("x", "tool:x", "c", bad); err == nil {
		t.Error("missing policy should error")
	}
}

func TestSendRequestHappyPath(t *testing.T) {
	ctx := context.Background()
	p, tr, cor, pol := buildProxy(t)
	tr.nextID = "frame-from-transit"

	env := sampleRequestEnv()
	deadline := int64(2_000_000)
	env.ExpiresAt = &deadline

	frameID, err := p.SendRequest(ctx, env, "sess-1", []byte(`{"cmd":"publish"}`))
	if err != nil {
		t.Fatalf("SendRequest: %v", err)
	}
	if frameID != "frame-from-transit" {
		t.Errorf("frameID=%q want frame-from-transit", frameID)
	}
	if len(tr.sent) != 1 {
		t.Fatalf("expected 1 sent frame, got %d", len(tr.sent))
	}
	got := tr.sent[0]
	if got.ChannelID != channel.ID("channel-1") {
		t.Errorf("channel mismatch: %q", got.ChannelID)
	}
	if got.DeviceSessionID != "sess-1" {
		t.Errorf("session mismatch: %q", got.DeviceSessionID)
	}
	if got.AdapterActorID != actor.ActorID("tool:xhs-adapter") {
		t.Errorf("adapter_actor_id mismatch: %q", got.AdapterActorID)
	}
	var body DeviceTransitBody
	if err := json.Unmarshal(got.Body, &body); err != nil {
		t.Fatalf("decode transit body: %v", err)
	}
	if body.Direction != DirectionToDevice {
		t.Errorf("direction mismatch: %q", body.Direction)
	}
	if body.RequestID != env.ID {
		t.Errorf("request id mismatch: %q", body.RequestID)
	}
	if body.ExpiresAt != deadline {
		t.Errorf("expires_at mismatch: %d", body.ExpiresAt)
	}
	if string(body.Payload) != `{"cmd":"publish"}` {
		t.Errorf("payload mismatch: %q", body.Payload)
	}

	// Correlation reserved + F3 timer armed.
	if _, ok, _ := cor.Get(ctx, adapter.CorrelationKey(env.ID)); !ok {
		t.Error("correlation reserve missing")
	}
	if _, ok := pol.timers[adapter.CorrelationKey(env.ID)]; !ok {
		t.Error("F3 timer not armed")
	}
}

func TestSendRequestRejectsWrongKind(t *testing.T) {
	ctx := context.Background()
	p, _, _, _ := buildProxy(t)
	env := sampleRequestEnv()
	env.Kind = message.KindEvent
	if _, err := p.SendRequest(ctx, env, "sess", []byte(`{}`)); err == nil {
		t.Error("non-request kind should be rejected")
	}
}

func TestSendRequestRejectsBlankIDs(t *testing.T) {
	ctx := context.Background()
	p, _, _, _ := buildProxy(t)
	env := sampleRequestEnv()
	env.ID = ""
	if _, err := p.SendRequest(ctx, env, "sess", []byte(`{}`)); err == nil {
		t.Error("blank envelope id should be rejected")
	}
	env = sampleRequestEnv()
	if _, err := p.SendRequest(ctx, env, "", []byte(`{}`)); err == nil {
		t.Error("blank session id should be rejected")
	}
}

func TestSendRequestTransitFailureRollsBack(t *testing.T) {
	ctx := context.Background()
	p, tr, cor, pol := buildProxy(t)
	sentinel := errors.New("transit gone")
	tr.sendErr = sentinel

	env := sampleRequestEnv()
	_, err := p.SendRequest(ctx, env, "sess-1", []byte(`{}`))
	if err == nil {
		t.Fatal("expected error")
	}
	if !errors.Is(err, sentinel) {
		t.Errorf("error chain should wrap sentinel: %v", err)
	}
	// Timer cancelled + correlation expired so caller can emit terminal.
	if !pol.cancelled[adapter.CorrelationKey(env.ID)] {
		t.Error("F3 timer should be cancelled on transit failure")
	}
	if !cor.expired[adapter.CorrelationKey(env.ID)] {
		t.Error("correlation should be marked expired on transit failure")
	}
}

func TestSendRequestPolicyArmFailureRollsBack(t *testing.T) {
	ctx := context.Background()
	p, _, cor, pol := buildProxy(t)
	pol.armErr = errors.New("arm fail")

	env := sampleRequestEnv()
	if _, err := p.SendRequest(ctx, env, "sess-1", []byte(`{}`)); err == nil {
		t.Fatal("expected timer arm error")
	}
	if !cor.expired[adapter.CorrelationKey(env.ID)] {
		t.Error("correlation must be expired when timer arm fails")
	}
}

func TestCancelInFlight(t *testing.T) {
	ctx := context.Background()
	p, _, cor, pol := buildProxy(t)
	p.CancelInFlight(ctx, "req-1")
	if !pol.cancelled["req-1"] {
		t.Error("timer not cancelled")
	}
	if !cor.expired["req-1"] {
		t.Error("correlation not expired")
	}
}

func TestCompleteInFlight(t *testing.T) {
	ctx := context.Background()
	p, _, cor, pol := buildProxy(t)
	if err := p.CompleteInFlight(ctx, "req-1"); err != nil {
		t.Fatalf("complete: %v", err)
	}
	if !pol.cancelled["req-1"] {
		t.Error("timer not cancelled on complete")
	}
	if !cor.done["req-1"] {
		t.Error("correlation not marked done")
	}
}

func TestLookupInFlight(t *testing.T) {
	ctx := context.Background()
	p, _, cor, _ := buildProxy(t)
	entry := adapter.CorrelationEntry{RequestID: "req-1", State: adapter.CorrelationPending}
	if _, err := cor.Reserve(ctx, entry); err != nil {
		t.Fatalf("reserve: %v", err)
	}
	got, ok, err := p.LookupInFlight(ctx, "req-1")
	if err != nil {
		t.Fatalf("lookup: %v", err)
	}
	if !ok {
		t.Fatal("lookup miss")
	}
	if got.RequestID != "req-1" {
		t.Errorf("lookup id mismatch: %s", got.RequestID)
	}
	if _, ok, _ := p.LookupInFlight(ctx, "ghost"); ok {
		t.Error("ghost should miss")
	}
}

func TestEnvelopeDeadlineFallback(t *testing.T) {
	env := &message.Envelope{TS: 1_000_000}
	got := envelopeDeadline(env, 999_999)
	want := int64(1_000_000) + defaultPendingMs
	if got != want {
		t.Errorf("fallback deadline = %d want %d", got, want)
	}

	exp := int64(2_500_000)
	env.ExpiresAt = &exp
	if got := envelopeDeadline(env, 999_999); got != exp {
		t.Errorf("explicit ExpiresAt should win: got %d want %d", got, exp)
	}

	// No TS + no ExpiresAt → anchor on enqueuedAt.
	bare := &message.Envelope{}
	if got := envelopeDeadline(bare, 5_000); got != 5_000+defaultPendingMs {
		t.Errorf("bare deadline = %d want %d", got, 5_000+defaultPendingMs)
	}
}
