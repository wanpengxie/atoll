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

type forwardCall struct {
	env     *message.Envelope
	payload adapter.ExternalRequestPayload
}

type forwardRecorder struct {
	mu      sync.Mutex
	calls   []forwardCall
	frameID string
	err     error
}

func (f *forwardRecorder) fn(_ context.Context, env *message.Envelope, payload adapter.ExternalRequestPayload) (adapter.ExternalRequestResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return adapter.ExternalRequestResult{}, f.err
	}
	cp := *env
	f.calls = append(f.calls, forwardCall{env: &cp, payload: append(adapter.ExternalRequestPayload(nil), payload...)})
	frameID := f.frameID
	if frameID == "" {
		frameID = "frame-default"
	}
	return adapter.ExternalRequestResult{FrameID: frameID}, nil
}

// ---- helpers ------------------------------------------------------------

func buildProxy(t *testing.T) (*DeviceProxy, *forwardRecorder, *fakeCorrelation, *fakePolicy) {
	t.Helper()
	forward := &forwardRecorder{}
	cor := newFakeCorrelation()
	pol := newFakePolicy()
	p, err := NewDeviceProxy("xhs", "tool:xhs", channel.ID("channel-1"), DeviceProxyDeps{
		Forward:       forward.fn,
		LookupPending: cor.Get,
	})
	if err != nil {
		t.Fatalf("NewDeviceProxy: %v", err)
	}
	p.SetClock(func() time.Time { return time.UnixMilli(1_000_000) })
	p.SetFrameIDFactory(func() string { return "frame-fixed" })
	return p, forward, cor, pol
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
		Audience:      message.Audience{"tool:xhs"},
		TS:            1_000_000,
	}
}

// ---- tests --------------------------------------------------------------

func TestNewDeviceProxyValidation(t *testing.T) {
	deps := DeviceProxyDeps{
		Forward: (&forwardRecorder{}).fn,
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
	bad.Forward = nil
	if _, err := NewDeviceProxy("x", "tool:x", "c", bad); err == nil {
		t.Error("missing forward should error")
	}
}

func TestSendRequestHappyPath(t *testing.T) {
	ctx := context.Background()
	p, forward, cor, pol := buildProxy(t)
	forward.frameID = "frame-from-transit"

	env := sampleRequestEnv()
	deadline := int64(2_000_000)
	env.ExpiresAt = &deadline

	frameID, err := p.SendRequest(ctx, env, []byte(`{"cmd":"publish"}`))
	if err != nil {
		t.Fatalf("SendRequest: %v", err)
	}
	if frameID != "frame-from-transit" {
		t.Errorf("frameID=%q want frame-from-transit", frameID)
	}
	if len(forward.calls) != 1 {
		t.Fatalf("expected 1 forwarded frame, got %d", len(forward.calls))
	}
	got := forward.calls[0]
	if got.env.ChannelID != channel.ID("channel-1") {
		t.Errorf("channel mismatch: %q", got.env.ChannelID)
	}
	if got.env.Audience[0] != actor.ActorID("tool:xhs") {
		t.Errorf("audience mismatch: %q", got.env.Audience)
	}
	if string(got.payload) != `{"cmd":"publish"}` {
		t.Errorf("payload mismatch: %q", got.payload)
	}

	// DeviceProxy must not reserve correlation or arm a second timer;
	// Manager.Dispatch owns that lifecycle.
	if _, ok, _ := cor.Get(ctx, adapter.CorrelationKey(env.ID)); ok {
		t.Error("DeviceProxy must not reserve correlation")
	}
	if _, ok := pol.timers[adapter.CorrelationKey(env.ID)]; ok {
		t.Error("DeviceProxy must not arm F3 timer")
	}
}

func TestSendRequestRejectsWrongKind(t *testing.T) {
	ctx := context.Background()
	p, _, _, _ := buildProxy(t)
	env := sampleRequestEnv()
	env.Kind = message.KindEvent
	if _, err := p.SendRequest(ctx, env, []byte(`{}`)); err == nil {
		t.Error("non-request kind should be rejected")
	}
}

func TestSendRequestRejectsBlankIDs(t *testing.T) {
	ctx := context.Background()
	p, _, _, _ := buildProxy(t)
	env := sampleRequestEnv()
	env.ID = ""
	if _, err := p.SendRequest(ctx, env, []byte(`{}`)); err == nil {
		t.Error("blank envelope id should be rejected")
	}
}

func TestSendRequestTransitFailureRollsBack(t *testing.T) {
	ctx := context.Background()
	p, forward, cor, pol := buildProxy(t)
	sentinel := errors.New("transit gone")
	forward.err = sentinel

	env := sampleRequestEnv()
	_, err := p.SendRequest(ctx, env, []byte(`{}`))
	if err == nil {
		t.Fatal("expected error")
	}
	if !errors.Is(err, sentinel) {
		t.Errorf("error chain should wrap sentinel: %v", err)
	}
	if pol.cancelled[adapter.CorrelationKey(env.ID)] {
		t.Error("DeviceProxy must not cancel timer on forward failure")
	}
	if cor.expired[adapter.CorrelationKey(env.ID)] {
		t.Error("DeviceProxy must not mark correlation expired on forward failure")
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

func TestLifecycleTrackerDoesNotStoreStateWhenEventEmitFails(t *testing.T) {
	sentinel := errors.New("write failed")
	lt, err := NewLifecycleTracker(LifecycleTrackerConfig{
		EventTypes:     map[DeviceState]string{DeviceStateOnline: "xhs.device.online"},
		AdapterActorID: "tool:xhs",
		ChannelID:      "channel-1",
		EmitEvent: func(context.Context, string, json.RawMessage, adapter.EmitEventOptions) (message.ID, error) {
			return "", sentinel
		},
	})
	if err != nil {
		t.Fatalf("NewLifecycleTracker: %v", err)
	}

	err = lt.Apply(context.Background(), devicetransit.LifecycleFrame{Event: devicetransit.LifecycleConnected})
	if !errors.Is(err, sentinel) {
		t.Fatalf("Apply err=%v want sentinel", err)
	}
	if got := lt.State(); got != DeviceStateUnknown {
		t.Fatalf("state=%s want %s", got, DeviceStateUnknown)
	}
}

func TestLifecycleTrackerStoresLocalOnlyTransition(t *testing.T) {
	lt, err := NewLifecycleTracker(LifecycleTrackerConfig{})
	if err != nil {
		t.Fatalf("NewLifecycleTracker: %v", err)
	}
	if err := lt.Apply(context.Background(), devicetransit.LifecycleFrame{Event: devicetransit.LifecycleConnected}); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if got := lt.State(); got != DeviceStateOnline {
		t.Fatalf("state=%s want %s", got, DeviceStateOnline)
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
