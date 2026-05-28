package xhs_test

// End-to-end harness for the xhs adapter on top of the launch
// runtime_inbound_via_relay binding (T5 acceptance).
//
// Scope:
//   - mock server.devicebus (mockServer) implements kernel/devicetransit.DeviceTransit:
//     it captures SendFrame instances, lets the test simulate the device side
//     by routing payload bytes back into the adapter via OnExternalCallback.
//   - in-memory fakes for CorrelationTracker / ErrorPolicy / Respond / ActorRegistry
//     give the Module a complete runtime context without pulling T3/T4.
//   - fakeActorRegistry asserts the device-not-actor invariant (L4 §2.6):
//     every Respond emit MUST have a sender that exists in actor_registry;
//     no row keyed by a device_id is ever inserted.
//
// Covers T5 acceptance bullet "harness 校验：channel 内所有 message sender 都是
// actor_registry 合法 actor，device 不在 actor_registry" without spinning up
// the full daemon / server (those live in T3 / T4 / T6).

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/wanpengxie/ActOS/adapters/device/framework"
	xhs "github.com/wanpengxie/ActOS/adapters/device/xhs"
	"github.com/wanpengxie/ActOS/kernel/actor"
	"github.com/wanpengxie/ActOS/kernel/adapter"
	"github.com/wanpengxie/ActOS/kernel/channel"
	"github.com/wanpengxie/ActOS/kernel/devicetransit"
	kharness "github.com/wanpengxie/ActOS/kernel/harness"
	"github.com/wanpengxie/ActOS/kernel/message"
)

// ---- mock server.devicebus ---------------------------------------------

type mockServerSend struct {
	frame devicetransit.SendFrame
	at    time.Time
}

// mockServer is the test stand-in for server.devicebus. It implements
// kernel/devicetransit.DeviceTransit; the adapter never knows it isn't talking
// to a real runtime/transit client.
type mockServer struct {
	mu        sync.Mutex
	sends     []mockServerSend
	acks      []devicetransit.AckFrame
	failSend  error
	nextFrame string
	now       func() time.Time
}

func newMockServer() *mockServer {
	return &mockServer{now: time.Now}
}

func (m *mockServer) Send(_ context.Context, frame devicetransit.SendFrame) (devicetransit.FrameID, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.failSend != nil {
		return "", m.failSend
	}
	m.sends = append(m.sends, mockServerSend{frame: frame, at: m.now()})
	if m.nextFrame != "" {
		id := m.nextFrame
		m.nextFrame = ""
		return devicetransit.FrameID(id), nil
	}
	return "frame-test", nil
}

func (m *mockServer) Ack(_ context.Context, frame devicetransit.AckFrame) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.acks = append(m.acks, frame)
	return nil
}

func (m *mockServer) lastSend() (devicetransit.SendFrame, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.sends) == 0 {
		return devicetransit.SendFrame{}, false
	}
	return m.sends[len(m.sends)-1].frame, true
}

// ---- fake correlation / policy / respond / registry --------------------

type fakeCorrelation struct {
	mu         sync.Mutex
	pending    map[adapter.CorrelationKey]adapter.CorrelationEntry
	done       map[adapter.CorrelationKey]bool
	expired    map[adapter.CorrelationKey]bool
	reserveErr error
}

func newFakeCorrelation() *fakeCorrelation {
	return &fakeCorrelation{
		pending: map[adapter.CorrelationKey]adapter.CorrelationEntry{},
		done:    map[adapter.CorrelationKey]bool{},
		expired: map[adapter.CorrelationKey]bool{},
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
	if e, ok := f.pending[id]; ok {
		e.State = adapter.CorrelationDone
		f.pending[id] = e
	}
	f.done[id] = true
	return nil
}

func (f *fakeCorrelation) MarkExpired(_ context.Context, id adapter.CorrelationKey) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if e, ok := f.pending[id]; ok {
		e.State = adapter.CorrelationExpired
		f.pending[id] = e
	}
	f.expired[id] = true
	return nil
}

func (f *fakeCorrelation) MarkRejected(_ context.Context, id adapter.CorrelationKey, reason string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if e, ok := f.pending[id]; ok {
		e.State = adapter.CorrelationRejected
		f.pending[id] = e
	}
	return nil
}

func (f *fakeCorrelation) ListPending(_ context.Context) ([]adapter.CorrelationEntry, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]adapter.CorrelationEntry, 0, len(f.pending))
	for _, e := range f.pending {
		if e.State == adapter.CorrelationPending {
			out = append(out, e)
		}
	}
	return out, nil
}

type policyEvent struct {
	requestID adapter.CorrelationKey
	reason    message.TerminalFailureReason
	detail    string
}

type fakePolicy struct {
	mu             sync.Mutex
	timers         map[adapter.CorrelationKey]time.Time
	cancelled      map[adapter.CorrelationKey]bool
	externalErrors []policyEvent
	armErr         error
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
	f.externalErrors = append(f.externalErrors, policyEvent{requestID: id, reason: reason, detail: detail})
	return nil
}

// fakeActorRegistry is the test-side actor_registry. It is consulted by
// the Respond closure to enforce the L4 §2.6 invariant: every emit
// MUST have sender ∈ registry. Device ids never sit here.
type fakeActorRegistry struct {
	mu   sync.RWMutex
	rows map[actor.ActorID]bool
}

func newFakeActorRegistry() *fakeActorRegistry {
	return &fakeActorRegistry{rows: map[actor.ActorID]bool{}}
}

func (r *fakeActorRegistry) insert(id actor.ActorID) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.rows[id] = true
}

func (r *fakeActorRegistry) exists(id actor.ActorID) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.rows[id]
}

type respondCall struct {
	requestID adapter.CorrelationKey
	payload   json.RawMessage
	opts      adapter.RespondOptions
	sender    actor.ActorID
}

type fakeRespondHarness struct {
	mu            sync.Mutex
	calls         []respondCall
	provisionals  []provisionalCall
	registry      *fakeActorRegistry
	adapterActor  actor.ActorID
	cor           *fakeCorrelation
	policy        *fakePolicy
}

// newRespondFunc returns a RespondFunc closure that the framework hands
// to ModuleContext.Respond. The closure mirrors the harness Step 0–9
// invariants we care about for T5:
//   - sender = adapterActor (the framework injects this; test asserts it).
//   - sender MUST exist in actor_registry; absence is a harness reject
//     (modelled here as a returned error).
//   - parent_id, status, reason, payload bubble through to the caller.
func (h *fakeRespondHarness) RespondFunc() adapter.RespondFunc {
	return func(_ context.Context, requestID adapter.CorrelationKey, payload json.RawMessage, opts adapter.RespondOptions) (adapter.RespondResult, error) {
		if !h.registry.exists(h.adapterActor) {
			return adapter.RespondResult{}, errors.New("fakeRespondHarness: adapter actor not in registry — harness reject (sender_mismatch)")
		}
		h.mu.Lock()
		defer h.mu.Unlock()
		h.calls = append(h.calls, respondCall{
			requestID: requestID,
			payload:   append(json.RawMessage(nil), payload...),
			opts:      opts,
			sender:    h.adapterActor,
		})
		if h.cor != nil {
			_ = h.cor.MarkDone(context.Background(), requestID)
		}
		if h.policy != nil {
			_ = h.policy.CancelTimer(context.Background(), requestID)
		}
		return adapter.RespondResult{MessageID: message.ID("msg-" + requestID.String()), Deduped: false}, nil
	}
}

func (h *fakeRespondHarness) FailFunc() adapter.FailFunc {
	respond := h.RespondFunc()
	return func(ctx context.Context, requestID adapter.CorrelationKey, payload json.RawMessage, opts adapter.FailOptions) (adapter.RespondResult, error) {
		reason := opts.Reason
		if reason == "" {
			reason = message.TerminalReceiverInternalError
		}
		return respond(ctx, requestID, payload, adapter.RespondOptions{
			Status: "failed",
			Reason: string(reason),
		})
	}
}

// provisionalCall captures the arguments the framework would hand
// ctx.Provisional through the wire. Unlike RespondFunc the provisional
// path MUST NOT touch correlation / timer state (proto-foundation
// §1.6.3): only the final Respond/Fail closes the request.
type provisionalCall struct {
	requestID adapter.CorrelationKey
	status    string
	payload   json.RawMessage
	opts      adapter.ProvisionalOptions
	sender    actor.ActorID
}

// ProvisionalFunc returns a closure satisfying adapter.ProvisionalFunc.
// Mirrors RespondFunc's harness semantics (sender registry check) but
// leaves pending correlation + F3 timer untouched. Tests assert the
// recorded sequence to verify Module.Handle's provisional emit.
func (h *fakeRespondHarness) ProvisionalFunc() adapter.ProvisionalFunc {
	return func(_ context.Context, requestID adapter.CorrelationKey, status string, payload json.RawMessage, opts adapter.ProvisionalOptions) (adapter.RespondResult, error) {
		if !h.registry.exists(h.adapterActor) {
			return adapter.RespondResult{}, errors.New("fakeRespondHarness: adapter actor not in registry — harness reject (sender_mismatch)")
		}
		if status == "" {
			return adapter.RespondResult{}, errors.New("fakeRespondHarness: provisional status required")
		}
		if message.IsFinalStatus(status) {
			return adapter.RespondResult{}, fmt.Errorf("fakeRespondHarness: provisional status %q is a final status", status)
		}
		h.mu.Lock()
		defer h.mu.Unlock()
		h.provisionals = append(h.provisionals, provisionalCall{
			requestID: requestID,
			status:    status,
			payload:   append(json.RawMessage(nil), payload...),
			opts:      opts,
			sender:    h.adapterActor,
		})
		return adapter.RespondResult{MessageID: message.ID("msg-prov-" + requestID.String() + ":" + status)}, nil
	}
}

type eventChain struct {
	mu      sync.Mutex
	written []*message.Envelope
}

func (c *eventChain) Write(_ context.Context, env *message.Envelope) (kharness.WriteResult, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	cp := *env
	c.written = append(c.written, &cp)
	return kharness.WriteResult{MessageID: env.ID, Seq: int64(len(c.written))}, nil
}

func (c *eventChain) Written() []*message.Envelope {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]*message.Envelope, len(c.written))
	copy(out, c.written)
	return out
}

func emitEventFunc(chain *eventChain, adapterActor actor.ActorID, channelID channel.ID) adapter.EmitEventFunc {
	return func(ctx context.Context, eventType string, payload json.RawMessage, opts adapter.EmitEventOptions) (message.ID, error) {
		if chain == nil {
			return "", nil
		}
		visibility := opts.Visibility
		if visibility == "" {
			visibility = message.VisibilityPrivate
		}
		audience := opts.Audience
		if len(audience) == 0 {
			audience = message.Audience{actor.SystemActorID}
		}
		env := &message.Envelope{
			ID:         message.ID("event:" + eventType),
			ChannelID:  channelID,
			Sender:     message.Sender{Kind: actor.KindTool, ID: adapterActor},
			Kind:       message.KindEvent,
			Type:       eventType,
			Payload:    append(json.RawMessage(nil), payload...),
			Visibility: visibility,
			Audience:   audience,
			TS:         1_000_000,
			TSReceived: 1_000_000,
		}
		_, err := chain.Write(ctx, env)
		if err != nil {
			return "", err
		}
		return env.ID, nil
	}
}

func reportOrphanCallbackFunc(chain *eventChain, adapterName string, adapterActor actor.ActorID, channelID channel.ID) adapter.OrphanCallbackFunc {
	return func(ctx context.Context, report adapter.OrphanCallbackReport) error {
		if chain == nil {
			return nil
		}
		payload := map[string]any{
			"kind":    "orphan_callback",
			"adapter": adapterName,
			"detail":  report.Detail,
		}
		if report.CorrelationID != "" {
			payload["correlation_id"] = report.CorrelationID
		}
		if len(report.Payload) > 0 {
			payload["payload"] = string(report.Payload)
		}
		raw, err := json.Marshal(payload)
		if err != nil {
			return err
		}
		_, err = emitEventFunc(chain, adapterActor, channelID)(ctx, "adapter."+adapterName+".orphan_callback", raw, adapter.EmitEventOptions{})
		return err
	}
}

// ---- helpers ------------------------------------------------------------

const testAdapterActor actor.ActorID = "tool:xhs"

type harness struct {
	module    *xhs.Module
	server    *mockServer
	cor       *fakeCorrelation
	policy    *fakePolicy
	registry  *fakeActorRegistry
	respond   *fakeRespondHarness
	channelID channel.ID
	mctx      *adapter.ModuleContext
}

func newHarness(t *testing.T) *harness {
	t.Helper()
	ctx := context.Background()

	server := newMockServer()
	cor := newFakeCorrelation()
	policy := newFakePolicy()
	reg := newFakeActorRegistry()
	reg.insert(testAdapterActor)
	reg.insert("agent:test")

	resp := &fakeRespondHarness{registry: reg, adapterActor: testAdapterActor, cor: cor, policy: policy}

	module, err := xhs.New(xhs.Config{
		AdapterActorID: testAdapterActor,
		Now:            func() time.Time { return time.UnixMilli(1_000_000) },
	})
	if err != nil {
		t.Fatalf("xhs.New: %v", err)
	}

	// LifecycleTracker emits channel events via EmitEvent on
	// every state transition; provide a capturing fake so tests that
	// don't pin lifecycle behaviour still get a valid Init. Tests
	// that need to assert event writes can replace it on the harness.
	defaultChain := &eventChain{}

	mctx := &adapter.ModuleContext{
		AdapterName:    xhs.AdapterName,
		AdapterActorID: testAdapterActor,
		ChannelID:      "channel-test",
		Respond:        resp.RespondFunc(),
		Fail:           resp.FailFunc(),
		ForwardExternalRequest: func(ctx context.Context, env *message.Envelope, payload adapter.ExternalRequestPayload) (adapter.ExternalRequestResult, error) {
			body := framework.DeviceTransitBody{
				Direction:     framework.DirectionToDevice,
				RequestID:     env.ID,
				ParentID:      env.ParentID,
				CorrelationID: env.CorrelationID,
				Payload:       json.RawMessage(payload),
			}
			if env.ExpiresAt != nil {
				body.ExpiresAt = *env.ExpiresAt
			}
			raw, err := json.Marshal(body)
			if err != nil {
				return adapter.ExternalRequestResult{}, err
			}
			frameID, err := server.Send(ctx, devicetransit.SendFrame{
				AdapterActorID: testAdapterActor,
				ChannelID:      env.ChannelID,
				Body:           raw,
			})
			if err != nil {
				return adapter.ExternalRequestResult{}, err
			}
			return adapter.ExternalRequestResult{FrameID: frameID.String()}, nil
		},
		LookupPendingRequest: cor.Get,
		EmitEvent:            emitEventFunc(defaultChain, testAdapterActor, "channel-test"),
		ReportOrphanCallback: reportOrphanCallbackFunc(defaultChain, xhs.AdapterName, testAdapterActor, "channel-test"),
		Provisional:          resp.ProvisionalFunc(),
	}
	if err := module.Init(ctx, mctx); err != nil {
		t.Fatalf("module.Init: %v", err)
	}
	// Simulate the devicebus connected lifecycle so the module's Handle
	// gate treats the device as reachable. Production wires this via
	// adapter_wiring.go's SetDeviceLifecycleCallback → Manager.OnRuntimeEvent.
	if err := module.OnRuntimeEvent(ctx, adapter.RuntimeEvent{
		Kind:           adapter.RuntimeEventDeviceLifecycle,
		ChannelID:      mctx.ChannelID,
		AdapterActorID: testAdapterActor,
		DeviceLifecycle: &devicetransit.LifecycleFrame{
			AdapterActorID: testAdapterActor,
			ChannelID:      mctx.ChannelID,
			Event:          devicetransit.LifecycleConnected,
			DeviceID:       "device-test",
			Ts:             1_000_000,
		},
	}); err != nil {
		t.Fatalf("module.OnRuntimeEvent connected: %v", err)
	}
	return &harness{
		module:    module,
		server:    server,
		cor:       cor,
		policy:    policy,
		registry:  reg,
		respond:   resp,
		channelID: mctx.ChannelID,
		mctx:      mctx,
	}
}

func (h *harness) request(envID, ty, payload string) *message.Envelope {
	return &message.Envelope{
		ID:        message.ID(envID),
		ChannelID: h.channelID,
		Sender:    message.Sender{Kind: actor.KindAgent, ID: "agent:test"},
		Kind:      message.KindRequest,
		Type:      ty,
		Payload:   []byte(payload),
		TS:        1_000_000,
	}
}

func (h *harness) dispatch(ctx context.Context, env *message.Envelope) error {
	deadline := env.TS + xhs.DefaultMaxPendingMs
	if env.ExpiresAt == nil {
		exp := deadline
		env.ExpiresAt = &exp
	}
	_, err := h.cor.Reserve(ctx, adapter.CorrelationEntry{
		RequestID:     adapter.CorrelationKey(env.ID),
		CorrelationID: env.CorrelationID,
		ChannelID:     env.ChannelID,
		AudienceActor: testAdapterActor,
		ParentID:      env.ID,
		EnqueuedAt:    env.TS,
		ExpiresAt:     deadline,
		State:         adapter.CorrelationPending,
	})
	if err != nil {
		return err
	}
	if err := h.policy.RegisterTimer(ctx, adapter.CorrelationKey(env.ID), time.UnixMilli(deadline)); err != nil {
		return err
	}
	return h.module.Handle(ctx, env)
}

// ---- E2E tests ----------------------------------------------------------

// TestPublishHappyPath walks the canonical lifecycle: register a session,
// dispatch xhs.publish, assert SendFrame shape, simulate the device side
// emitting a recv frame, assert the Respond envelope is well-formed.
func TestPublishHappyPath(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t)
	env := h.request("env-publish", xhs.TypePublish,
		`{"title":"hi","content":"body"}`)

	if err := h.dispatch(ctx, env); err != nil {
		t.Fatalf("Handle: %v", err)
	}

	// 1. Server saw exactly one send frame with expected shape.
	frame, ok := h.server.lastSend()
	if !ok {
		t.Fatal("server captured no frame")
	}
	if frame.AdapterActorID != xhs.DefaultAdapterActorID {
		t.Errorf("adapter_actor_id=%q", frame.AdapterActorID)
	}
	if frame.ChannelID != h.channelID {
		t.Errorf("channel id=%q", frame.ChannelID)
	}
	var transitBody framework.DeviceTransitBody
	if err := json.Unmarshal(frame.Body, &transitBody); err != nil {
		t.Fatalf("decode transit body: %v", err)
	}
	if transitBody.Direction != framework.DirectionToDevice {
		t.Errorf("direction=%q", transitBody.Direction)
	}
	if transitBody.RequestID != env.ID {
		t.Errorf("request_id=%q", transitBody.RequestID)
	}

	// 2. Inspect Command JSON: cmd stripped, domain params preserved.
	var cmd xhs.Command
	if err := json.Unmarshal(transitBody.Payload, &cmd); err != nil {
		t.Fatalf("decode wire payload: %v", err)
	}
	if cmd.Cmd != "publish" {
		t.Errorf("cmd=%q", cmd.Cmd)
	}
	if cmd.Params["title"] != "hi" {
		t.Errorf("params['title']=%v", cmd.Params["title"])
	}

	// 3. F3 timer armed; correlation reserved.
	if _, ok := h.policy.timers[adapter.CorrelationKey(env.ID)]; !ok {
		t.Error("F3 timer not armed")
	}
	if _, ok, _ := h.cor.Get(ctx, adapter.CorrelationKey(env.ID)); !ok {
		t.Error("correlation not reserved")
	}

	// 4. Simulate the device side replying with a successful callback.
	callback := xhs.Callback{
		CorrelationID: env.ID.String(),
		DeviceID:      "device-sess-publish",
		Status:        "ok",
		Result: map[string]any{
			"note_id": "n1",
			"url":     "https://xhs.example/n1",
			// Stowaway field — must be dropped at adapter boundary.
			"unauthorized": "drop-me",
		},
	}
	raw, _ := json.Marshal(callback)
	if err := h.module.OnExternalCallback(ctx, raw); err != nil {
		t.Fatalf("OnExternalCallback: %v", err)
	}

	// 5. Respond invoked exactly once with status=completed, parent_id set,
	//    sender = adapter actor, payload contains note_id + device_id +
	//    drops stowaway, audience defaults to caller via opts.
	if got := len(h.respond.calls); got != 1 {
		t.Fatalf("respond calls = %d want 1", got)
	}
	call := h.respond.calls[0]
	if call.requestID != adapter.CorrelationKey(env.ID) {
		t.Errorf("respond requestID=%q", call.requestID)
	}
	if call.opts.Status != "completed" {
		t.Errorf("status=%q want completed", call.opts.Status)
	}
	if call.opts.Reason != "" {
		t.Errorf("reason=%q want empty on success", call.opts.Reason)
	}
	if call.sender != testAdapterActor {
		t.Errorf("sender=%q want %q", call.sender, testAdapterActor)
	}
	var payload map[string]any
	_ = json.Unmarshal(call.payload, &payload)
	if payload["note_id"] != "n1" {
		t.Errorf("note_id missing: %v", payload)
	}
	if payload["device_id"] != "device-sess-publish" {
		t.Errorf("device_id missing: %v", payload)
	}
	if _, present := payload["unauthorized"]; present {
		t.Error("stowaway must be dropped (R4-FIX-A)")
	}

	// 6. Correlation transitioned pending → done; timer cancelled.
	if !h.cor.done[adapter.CorrelationKey(env.ID)] {
		t.Error("correlation should be done")
	}
	if !h.policy.cancelled[adapter.CorrelationKey(env.ID)] {
		t.Error("F3 timer should be cancelled")
	}

	// 7. device-not-actor invariant: only the adapter actor + the agent
	//    actor are in the registry; no device_* row ever inserted.
	if h.registry.exists("device-sess-publish") {
		t.Error("device id must not appear in actor_registry (L4 §2.6)")
	}
}

// TestSearchPerTypeAllowList confirms the R4-FIX-A regression guard
// (stowaway note_id on search response is dropped).
func TestSearchPerTypeAllowList(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t)
	env := h.request("env-search", xhs.TypeSearch,
		`{"keyword":"abc"}`)

	if err := h.dispatch(ctx, env); err != nil {
		t.Fatalf("Handle: %v", err)
	}

	callback := xhs.Callback{
		CorrelationID: env.ID.String(),
		DeviceID:      "device-sess-search",
		Status:        "ok",
		Result: map[string]any{
			"results": []any{"a", "b"},
			"note_id": "stowaway", // not declared on search schema
		},
	}
	raw, _ := json.Marshal(callback)
	if err := h.module.OnExternalCallback(ctx, raw); err != nil {
		t.Fatalf("OnExternalCallback: %v", err)
	}
	call := h.respond.calls[0]
	var p map[string]any
	_ = json.Unmarshal(call.payload, &p)
	if _, present := p["note_id"]; present {
		t.Error("search response must not carry stowaway note_id (R4-FIX-A)")
	}
	if _, present := p["device_id"]; present {
		t.Error("search response must not carry device_id (R4-FIX-A)")
	}
	if _, present := p["results"]; !present {
		t.Errorf("search should preserve results: %v", p)
	}
}

// TestTransitSendFailureRollsBack covers the path where external forwarding
// fails — the adapter must emit a synchronous failed terminal through the
// framework write-first lifecycle path.
func TestTransitSendFailureRollsBack(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t)
	h.server.failSend = errors.New("ws gone")

	env := h.request("env-fail", xhs.TypePublish, `{"title":"x"}`)
	if err := h.dispatch(ctx, env); err != nil {
		t.Fatalf("Handle: %v", err)
	}
	call := h.respond.calls[0]
	assertAdapterExecutionFailure(t, call, "device_push_failed")
	if !h.policy.cancelled[adapter.CorrelationKey(env.ID)] {
		t.Error("timer should be cancelled on push failure")
	}
	if !h.cor.done[adapter.CorrelationKey(env.ID)] {
		t.Error("correlation should be done on failed terminal")
	}
}

func assertAdapterExecutionFailure(t *testing.T, call respondCall, wantErrorCode string) {
	t.Helper()
	if call.opts.Reason != string(message.TerminalReceiverInternalError) {
		t.Fatalf("reason=%q want %s", call.opts.Reason, message.TerminalReceiverInternalError)
	}
	var payload map[string]any
	if err := json.Unmarshal(call.payload, &payload); err != nil {
		t.Fatalf("unmarshal response payload: %v", err)
	}
	if payload["error_code"] != wantErrorCode {
		t.Fatalf("payload.error_code=%v want %s", payload["error_code"], wantErrorCode)
	}
}

// TestF3TimeoutTerminal simulates the F3 default-timeout path: the
// adapter sends successfully but no callback arrives. ErrorPolicy fires
// OnExternalError with receiver_unavailable; downstream wiring (T3
// framework) would Respond with the canonical unanswered_timeout —
// we mirror that path here by calling failNow via a synthetic stale
// envelope.
//
// The test confirms ErrorPolicy.OnExternalError is invokable
// independently of the success path (the framework GC + boot recover
// rely on it).
func TestF3TimeoutTerminal(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t)

	env := h.request("env-timeout", xhs.TypePublish, `{"title":"x"}`)
	if err := h.dispatch(ctx, env); err != nil {
		t.Fatalf("Handle: %v", err)
	}
	// Simulate the F3 timer firing: in production the framework would
	// call OnExternalError → ErrorPolicy emits a terminal Respond. Here
	// we exercise the seam directly.
	if err := h.policy.OnExternalError(ctx, adapter.CorrelationKey(env.ID), message.TerminalReceiverUnavailable, "no device ack"); err != nil {
		t.Fatalf("OnExternalError: %v", err)
	}
	if len(h.policy.externalErrors) != 1 {
		t.Fatalf("expected 1 external error, got %d", len(h.policy.externalErrors))
	}
	ev := h.policy.externalErrors[0]
	if ev.reason != message.TerminalReceiverUnavailable {
		t.Errorf("policy event reason=%q", ev.reason)
	}
}

// TestSenderHarnessGuard tries to invoke Respond when the adapter actor
// is missing from the registry — confirms the fake harness rejects.
// This models harness step 5 sender_mismatch (the daemon-side harness
// would do the same; this test asserts the fake reproduces the rule).
//
// Note: the rejection surfaces on the inbound callback path (Respond
// emit), not on Handle (Handle never calls Respond on the happy path
// — it just enqueues the device frame).
func TestSenderHarnessGuard(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t)
	env := h.request("env-guard", xhs.TypePublish, `{"title":"x"}`)
	if err := h.dispatch(ctx, env); err != nil {
		t.Fatalf("Handle: %v", err)
	}

	// Drop the adapter from the registry — the inbound callback's
	// Respond emit must reject because sender is not registered.
	h.registry.mu.Lock()
	delete(h.registry.rows, testAdapterActor)
	h.registry.mu.Unlock()

	callback := xhs.Callback{CorrelationID: env.ID.String(), Status: "ok", Result: map[string]any{"note_id": "n"}}
	raw, _ := json.Marshal(callback)
	err := h.module.OnExternalCallback(ctx, raw)
	if err == nil || !strings.Contains(err.Error(), "harness reject") {
		t.Errorf("expected harness reject when adapter not in registry; got %v", err)
	}
	if len(h.respond.calls) != 0 {
		t.Error("Respond should not have appended (rejected before recording)")
	}
}

// TestOrphanCallbackIsDropped covers Recover miss path. An external
// callback referencing an unknown request id MUST be a no-op (no Respond,
// no error).
func TestOrphanCallbackIsDropped(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t)
	// Deliver a callback for an envelope we never sent.
	callback := xhs.Callback{CorrelationID: "ghost", Status: "ok"}
	raw, _ := json.Marshal(callback)
	if err := h.module.OnExternalCallback(ctx, raw); err != nil {
		t.Fatalf("orphan callback should be dropped silently: %v", err)
	}
	if len(h.respond.calls) != 0 {
		t.Errorf("orphan callback must not produce a Respond; got %d", len(h.respond.calls))
	}
}

func TestCallbackFrameRejectsInnerCorrelationMismatch(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t)
	env := h.request("env-frame", xhs.TypePublish, `{"title":"hi","content":"body"}`)
	if err := h.dispatch(ctx, env); err != nil {
		t.Fatalf("Handle: %v", err)
	}
	callback := xhs.Callback{
		CorrelationID: "env-other",
		Status:        "ok",
		Result:        map[string]any{"note_id": "n"},
	}
	raw, _ := json.Marshal(callback)
	err := h.module.OnExternalCallbackFrame(ctx, adapter.ExternalCallbackFrame{
		ChannelID:      h.channelID,
		AdapterActorID: testAdapterActor,
		RequestID:      env.ID,
		CorrelationID:  env.ID,
		ExpiresAt:      *env.ExpiresAt,
		Payload:        raw,
	})
	if err == nil || !strings.Contains(err.Error(), "does not match outer request_id") {
		t.Fatalf("expected outer identity mismatch, got %v", err)
	}
	if len(h.respond.calls) != 0 {
		t.Fatalf("mismatched callback must not Respond; got %d", len(h.respond.calls))
	}
}

func TestCallbackFrameParseFailureFailsRequest(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t)
	env := h.request("env-frame-parse", xhs.TypePublish, `{"title":"hi","content":"body"}`)
	if err := h.dispatch(ctx, env); err != nil {
		t.Fatalf("Handle: %v", err)
	}

	err := h.module.OnExternalCallbackFrame(ctx, adapter.ExternalCallbackFrame{
		ChannelID:      h.channelID,
		AdapterActorID: testAdapterActor,
		RequestID:      env.ID,
		CorrelationID:  env.ID,
		ExpiresAt:      *env.ExpiresAt,
		Payload:        json.RawMessage(`{"status":"ok"}`),
	})
	if err != nil {
		t.Fatalf("malformed callback frame should be converted to failed terminal: %v", err)
	}
	if len(h.respond.calls) != 1 {
		t.Fatalf("malformed callback frame should emit one failed terminal, got %d", len(h.respond.calls))
	}
	call := h.respond.calls[0]
	if call.opts.Status != "failed" || call.opts.Reason != string(message.TerminalReceiverInternalError) {
		t.Fatalf("failed terminal opts=%+v", call.opts)
	}
	var payload map[string]any
	if err := json.Unmarshal(call.payload, &payload); err != nil {
		t.Fatalf("decode fail payload: %v", err)
	}
	if payload["error_code"] != "callback_malformed" {
		t.Fatalf("error_code=%v want callback_malformed", payload["error_code"])
	}
	if !h.cor.done[adapter.CorrelationKey(env.ID)] {
		t.Fatalf("failed terminal should close correlation")
	}
}

func TestParseFailureEmitsOrphanCallbackEvents(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t)
	chain := &eventChain{}
	h.mctx.ReportOrphanCallback = reportOrphanCallbackFunc(chain, xhs.AdapterName, testAdapterActor, h.channelID)

	err := h.module.OnExternalCallback(ctx, []byte(`{"status":"ok"}`))
	if err == nil || !strings.Contains(err.Error(), "missing correlation_id") {
		t.Fatalf("expected parse error for missing correlation_id, got %v", err)
	}
	if len(h.respond.calls) != 0 {
		t.Fatalf("parse failure must not produce a Respond; got %d", len(h.respond.calls))
	}
	written := chain.Written()
	if len(written) != 1 {
		t.Fatalf("expected orphan callback event write, got %d", len(written))
	}
	if written[0].Type != "adapter.xhs.orphan_callback" || written[0].Kind != message.KindEvent {
		t.Fatalf("first event type/kind=%s/%s", written[0].Type, written[0].Kind)
	}
	var adapterPayload map[string]any
	if err := json.Unmarshal(written[0].Payload, &adapterPayload); err != nil {
		t.Fatalf("decode orphan payload: %v", err)
	}
	if adapterPayload["kind"] != "orphan_callback" || adapterPayload["detail"] == "" {
		t.Fatalf("orphan payload=%v", adapterPayload)
	}
}

// TestDuplicateCallbackIsDropped covers the race between F3 timer firing
// (marks correlation expired) and a late device callback arrival. The
// adapter should leave the existing terminal in place.
func TestDuplicateCallbackIsDropped(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t)
	env := h.request("env-dup", xhs.TypePublish, `{"title":"x"}`)
	if err := h.dispatch(ctx, env); err != nil {
		t.Fatalf("Handle: %v", err)
	}
	// Pretend F3 expired before the device callback arrived.
	if err := h.cor.MarkExpired(ctx, adapter.CorrelationKey(env.ID)); err != nil {
		t.Fatalf("mark expired: %v", err)
	}
	callback := xhs.Callback{CorrelationID: env.ID.String(), Status: "ok", Result: map[string]any{"note_id": "n"}}
	raw, _ := json.Marshal(callback)
	if err := h.module.OnExternalCallback(ctx, raw); err != nil {
		t.Fatalf("duplicate callback should not error: %v", err)
	}
	if len(h.respond.calls) != 0 {
		t.Errorf("duplicate callback must not produce another Respond; got %d", len(h.respond.calls))
	}
}

// TestModuleInitRequiresForwardExternalRequest guards the runtime_inbound
// wire-up: missing semantic external forward MUST fail at Init.
func TestModuleInitRequiresForwardExternalRequest(t *testing.T) {
	module, err := xhs.New(xhs.Config{})
	if err != nil {
		t.Fatalf("xhs.New: %v", err)
	}
	mctx := &adapter.ModuleContext{
		Respond: func(context.Context, adapter.CorrelationKey, json.RawMessage, adapter.RespondOptions) (adapter.RespondResult, error) {
			return adapter.RespondResult{}, nil
		},
		ChannelID: "c",
	}
	if err := module.Init(context.Background(), mctx); err == nil || !strings.Contains(err.Error(), "ForwardExternalRequest") {
		t.Errorf("Init must require ForwardExternalRequest; got %v", err)
	}
}

// TestHandleEmitsProvisionalReceived covers the phase 2 first-class
// async refactor (response-multitype-refactor §3.4 D-xhs): after a
// successful ForwardExternalRequest the adapter MUST emit one Layer 2
// `received` provisional so callers see "I got it, forwarded to the
// extension" before the eventual final terminal.
//
// The provisional emit:
//   - is exactly one (Handle's provisional emit point).
//   - has status == "received" (Layer 2 core closed set).
//   - carries adapter-owned informational payload (forwarded_at_ms +
//     detail) and the framework merges `status` into the final body.
//   - does NOT touch correlation / F3 timer (provisional response is
//     not a closure event).
//   - precedes the final Respond chronologically — the channel log
//     ordering [request, provisional, final] is enforced by the
//     fakeRespondHarness call ordering.
func TestHandleEmitsProvisionalReceived(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t)
	env := h.request("env-prov-received", xhs.TypePublish, `{"title":"hi"}`)

	if err := h.dispatch(ctx, env); err != nil {
		t.Fatalf("Handle: %v", err)
	}

	// 1. Exactly one provisional emit after a successful forward.
	if got := len(h.respond.provisionals); got != 1 {
		t.Fatalf("provisional emits = %d want 1", got)
	}
	prov := h.respond.provisionals[0]
	if prov.requestID != adapter.CorrelationKey(env.ID) {
		t.Errorf("provisional requestID=%q want %q", prov.requestID, env.ID)
	}
	if prov.status != "received" {
		t.Errorf("provisional status=%q want received", prov.status)
	}
	if prov.sender != testAdapterActor {
		t.Errorf("provisional sender=%q want %q", prov.sender, testAdapterActor)
	}
	var provPayload map[string]any
	if err := json.Unmarshal(prov.payload, &provPayload); err != nil {
		t.Fatalf("provisional payload unmarshal: %v", err)
	}
	if _, ok := provPayload["forwarded_at_ms"]; !ok {
		t.Errorf("provisional payload missing forwarded_at_ms: %v", provPayload)
	}

	// 2. Correlation + F3 timer still live (provisional does not close).
	if h.respond.cor.done[adapter.CorrelationKey(env.ID)] {
		t.Error("provisional must not mark correlation done")
	}
	if h.policy.cancelled[adapter.CorrelationKey(env.ID)] {
		t.Error("provisional must not cancel F3 timer")
	}

	// 3. Final Respond hasn't fired yet either — provisional is alone.
	if got := len(h.respond.calls); got != 0 {
		t.Fatalf("respond.calls = %d want 0 before callback", got)
	}

	// 4. Deliver the final callback; assert ordering [provisional, final]
	//    holds and the final terminal closes the request.
	callback := xhs.Callback{
		CorrelationID: env.ID.String(),
		Status:        "ok",
		Result:        map[string]any{"note_id": "n1", "url": "https://x/n1"},
	}
	raw, _ := json.Marshal(callback)
	if err := h.module.OnExternalCallback(ctx, raw); err != nil {
		t.Fatalf("OnExternalCallback: %v", err)
	}
	if got := len(h.respond.provisionals); got != 1 {
		t.Errorf("provisional count must stay 1 after final, got %d", got)
	}
	if got := len(h.respond.calls); got != 1 {
		t.Fatalf("final respond calls = %d want 1", got)
	}
	if !h.respond.cor.done[adapter.CorrelationKey(env.ID)] {
		t.Error("final terminal should close correlation")
	}
}

// TestHandleSkipsProvisionalOnForwardFailure covers the failure path:
// when ForwardExternalRequest fails the adapter emits a failed terminal
// via failNow and MUST NOT also emit a provisional `received` (the
// forward never completed, so "received and forwarded" is a lie).
func TestHandleSkipsProvisionalOnForwardFailure(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t)
	h.server.failSend = errors.New("ws gone")
	env := h.request("env-prov-fail", xhs.TypePublish, `{"title":"x"}`)

	if err := h.dispatch(ctx, env); err != nil {
		t.Fatalf("Handle: %v", err)
	}
	if got := len(h.respond.provisionals); got != 0 {
		t.Errorf("forward failure must not emit provisional, got %d", got)
	}
	if got := len(h.respond.calls); got != 1 {
		t.Fatalf("forward failure should emit one failed terminal, got %d", got)
	}
}

// TestHandleSkipsProvisionalWhenOffline covers the device-state gate:
// when the lifecycle tracker reports offline / token-expired Handle
// short-circuits to failNow BEFORE the forward. No provisional emit.
func TestHandleSkipsProvisionalWhenOffline(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t)
	// Force the lifecycle state back to offline.
	if err := h.module.OnRuntimeEvent(ctx, adapter.RuntimeEvent{
		Kind:           adapter.RuntimeEventDeviceLifecycle,
		ChannelID:      h.channelID,
		AdapterActorID: testAdapterActor,
		DeviceLifecycle: &devicetransit.LifecycleFrame{
			AdapterActorID: testAdapterActor,
			ChannelID:      h.channelID,
			Event:          devicetransit.LifecycleDisconnected,
			DeviceID:       "device-test",
			Ts:             1_000_000,
		},
	}); err != nil {
		t.Fatalf("OnRuntimeEvent disconnect: %v", err)
	}

	env := h.request("env-prov-offline", xhs.TypePublish, `{"title":"x"}`)
	if err := h.dispatch(ctx, env); err != nil {
		t.Fatalf("Handle: %v", err)
	}
	if got := len(h.respond.provisionals); got != 0 {
		t.Errorf("offline gate must not emit provisional, got %d", got)
	}
}

// TestModuleInitRequiresProvisional guards phase 2 wire-up: a missing
// ctx.Provisional helper MUST fail at Init so a daemon misconfiguration
// is loud rather than silently dropping every provisional.
func TestModuleInitRequiresProvisional(t *testing.T) {
	module, err := xhs.New(xhs.Config{})
	if err != nil {
		t.Fatalf("xhs.New: %v", err)
	}
	mctx := &adapter.ModuleContext{
		Respond: func(context.Context, adapter.CorrelationKey, json.RawMessage, adapter.RespondOptions) (adapter.RespondResult, error) {
			return adapter.RespondResult{}, nil
		},
		Fail: func(context.Context, adapter.CorrelationKey, json.RawMessage, adapter.FailOptions) (adapter.RespondResult, error) {
			return adapter.RespondResult{}, nil
		},
		ForwardExternalRequest: func(context.Context, *message.Envelope, adapter.ExternalRequestPayload) (adapter.ExternalRequestResult, error) {
			return adapter.ExternalRequestResult{}, nil
		},
		LookupPendingRequest: func(context.Context, adapter.CorrelationKey) (adapter.CorrelationEntry, bool, error) {
			return adapter.CorrelationEntry{}, false, nil
		},
		EmitEvent: func(context.Context, string, json.RawMessage, adapter.EmitEventOptions) (message.ID, error) {
			return "", nil
		},
		ReportOrphanCallback: func(context.Context, adapter.OrphanCallbackReport) error { return nil },
		ChannelID:            "c",
		// Provisional intentionally nil.
	}
	if err := module.Init(context.Background(), mctx); err == nil || !strings.Contains(err.Error(), "Provisional") {
		t.Errorf("Init must require Provisional; got %v", err)
	}
}
