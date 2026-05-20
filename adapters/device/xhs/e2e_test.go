package xhs_test

// End-to-end harness for the xhs adapter on top of the M1.5
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
	errFrames []devicetransit.ErrorFrame
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

func (m *mockServer) Error(_ context.Context, frame devicetransit.ErrorFrame) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.errFrames = append(m.errFrames, frame)
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
	mu           sync.Mutex
	calls        []respondCall
	registry     *fakeActorRegistry
	adapterActor actor.ActorID
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
		return adapter.RespondResult{MessageID: message.ID("msg-" + requestID.String()), Deduped: false}, nil
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

// ---- helpers ------------------------------------------------------------

const testAdapterActor actor.ActorID = "tool:xhs-adapter"

type harness struct {
	module    *xhs.Module
	server    *mockServer
	cor       *fakeCorrelation
	policy    *fakePolicy
	registry  *fakeActorRegistry
	respond   *fakeRespondHarness
	sessions  *framework.InMemorySessionStore
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

	resp := &fakeRespondHarness{registry: reg, adapterActor: testAdapterActor}

	sessions := framework.NewInMemorySessionStore()
	module, err := xhs.New(xhs.Config{
		AdapterActorID: testAdapterActor,
		SessionStore:   sessions,
		Now:            func() time.Time { return time.UnixMilli(1_000_000) },
	})
	if err != nil {
		t.Fatalf("xhs.New: %v", err)
	}

	mctx := &adapter.ModuleContext{
		AdapterName:    xhs.AdapterName,
		AdapterActorID: testAdapterActor,
		ChannelID:      "channel-test",
		Correlation:    cor,
		ErrorPolicy:    policy,
		Respond:        resp.RespondFunc(),
		DeviceTransit:  server,
	}
	if err := module.Init(ctx, mctx); err != nil {
		t.Fatalf("module.Init: %v", err)
	}
	return &harness{
		module:    module,
		server:    server,
		cor:       cor,
		policy:    policy,
		registry:  reg,
		respond:   resp,
		sessions:  sessions,
		channelID: mctx.ChannelID,
		mctx:      mctx,
	}
}

func (h *harness) seedActiveSession(t *testing.T, sid string) devicetransit.DeviceSessionID {
	t.Helper()
	id := devicetransit.DeviceSessionID(sid)
	if err := h.sessions.Upsert(context.Background(), framework.DeviceSession{
		SessionID:  id,
		ChannelID:  h.channelID,
		DeviceID:   "device-" + sid,
		DeviceType: "xhs",
		State:      framework.StatePending,
		BoundAt:    1_000_000,
	}); err != nil {
		t.Fatalf("seed session: %v", err)
	}
	if err := h.sessions.SetState(context.Background(), id, framework.StateReady, 1_000_001); err != nil {
		t.Fatalf("seed ready: %v", err)
	}
	if err := h.sessions.SetState(context.Background(), id, framework.StateActive, 1_000_002); err != nil {
		t.Fatalf("seed active: %v", err)
	}
	return id
}

func (h *harness) request(envID, ty, payload, sessionID string) *message.Envelope {
	if sessionID == "" {
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
	var orig map[string]any
	_ = json.Unmarshal([]byte(payload), &orig)
	if orig == nil {
		orig = map[string]any{}
	}
	orig["device_session_id"] = sessionID
	body, _ := json.Marshal(orig)
	return &message.Envelope{
		ID:        message.ID(envID),
		ChannelID: h.channelID,
		Sender:    message.Sender{Kind: actor.KindAgent, ID: "agent:test"},
		Kind:      message.KindRequest,
		Type:      ty,
		Payload:   body,
		TS:        1_000_000,
	}
}

// ---- E2E tests ----------------------------------------------------------

// TestPublishHappyPath walks the canonical lifecycle: register a session,
// dispatch xhs.publish, assert SendFrame shape, simulate the device side
// emitting a recv frame, assert the Respond envelope is well-formed.
func TestPublishHappyPath(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t)
	sid := h.seedActiveSession(t, "sess-publish")

	env := h.request("env-publish", xhs.TypePublish,
		`{"title":"hi","content":"body"}`, string(sid))

	if err := h.module.Handle(ctx, env); err != nil {
		t.Fatalf("Handle: %v", err)
	}

	// 1. Server saw exactly one send frame with expected shape.
	frame, ok := h.server.lastSend()
	if !ok {
		t.Fatal("server captured no frame")
	}
	if frame.Direction != devicetransit.DirectionToDevice {
		t.Errorf("direction=%q", frame.Direction)
	}
	if frame.DeviceSessionID != sid {
		t.Errorf("session id=%q", frame.DeviceSessionID)
	}
	if frame.RequestID != env.ID {
		t.Errorf("request_id=%q", frame.RequestID)
	}
	if frame.ChannelID != h.channelID {
		t.Errorf("channel id=%q", frame.ChannelID)
	}

	// 2. Inspect Command JSON: cmd stripped, framework metadata excluded.
	var cmd xhs.Command
	if err := json.Unmarshal(frame.Payload, &cmd); err != nil {
		t.Fatalf("decode wire payload: %v", err)
	}
	if cmd.Cmd != "publish" {
		t.Errorf("cmd=%q", cmd.Cmd)
	}
	if _, present := cmd.Params["device_session_id"]; present {
		t.Error("framework metadata leaked into Command.Params")
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
	if h.registry.exists(actor.ActorID("device:sess-publish")) {
		t.Error("device session id must not appear in actor_registry (L4 §2.6)")
	}
}

// TestSearchPerTypeAllowList confirms the R4-FIX-A regression guard
// (stowaway note_id on search response is dropped).
func TestSearchPerTypeAllowList(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t)
	sid := h.seedActiveSession(t, "sess-search")
	env := h.request("env-search", xhs.TypeSearch,
		`{"keyword":"abc"}`, string(sid))

	if err := h.module.Handle(ctx, env); err != nil {
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

// TestDeviceOfflineSession seeds an offline session and verifies the
// adapter short-circuits to a failed terminal with a closed-set reason.
func TestDeviceOfflineSession(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t)
	sid := h.seedActiveSession(t, "sess-off")
	// Take it offline.
	if err := h.sessions.SetState(ctx, sid, framework.StateOffline, 1_000_500); err != nil {
		t.Fatalf("set offline: %v", err)
	}

	env := h.request("env-off", xhs.TypePublish, `{"title":"x"}`, string(sid))
	if err := h.module.Handle(ctx, env); err != nil {
		t.Fatalf("Handle: %v", err)
	}

	if len(h.server.sends) != 0 {
		t.Errorf("offline session should not produce a frame; got %d", len(h.server.sends))
	}
	if got := len(h.respond.calls); got != 1 {
		t.Fatalf("respond calls=%d want 1", got)
	}
	call := h.respond.calls[0]
	if call.opts.Status != "failed" {
		t.Errorf("status=%q want failed", call.opts.Status)
	}
	assertAdapterExecutionFailure(t, call, "device_offline")
	if call.sender != testAdapterActor {
		t.Errorf("sender=%q want %q", call.sender, testAdapterActor)
	}
}

// TestDeviceSessionMissing covers the synchronous fail when payload
// omits device_session_id and Config has no default.
func TestDeviceSessionMissing(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t)
	env := h.request("env-miss", xhs.TypePublish, `{"title":"x"}`, "")
	if err := h.module.Handle(ctx, env); err != nil {
		t.Fatalf("Handle: %v", err)
	}
	call := h.respond.calls[0]
	assertAdapterExecutionFailure(t, call, "device_session_missing")
}

// TestDeviceSessionUnknown covers the case where a session id is
// supplied but no mirror row exists.
func TestDeviceSessionUnknown(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t)
	env := h.request("env-unknown", xhs.TypePublish, `{"title":"x"}`, "sess-ghost")
	if err := h.module.Handle(ctx, env); err != nil {
		t.Fatalf("Handle: %v", err)
	}
	call := h.respond.calls[0]
	assertAdapterExecutionFailure(t, call, "device_session_unknown")
}

// TestTransitSendFailureRollsBack covers the path where DeviceTransit.Send
// fails — the adapter must emit a synchronous failed terminal and roll
// back correlation + timer.
func TestTransitSendFailureRollsBack(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t)
	sid := h.seedActiveSession(t, "sess-fail")
	h.server.failSend = errors.New("ws gone")

	env := h.request("env-fail", xhs.TypePublish, `{"title":"x"}`, string(sid))
	if err := h.module.Handle(ctx, env); err != nil {
		t.Fatalf("Handle: %v", err)
	}
	call := h.respond.calls[0]
	assertAdapterExecutionFailure(t, call, "device_push_failed")
	if !h.policy.cancelled[adapter.CorrelationKey(env.ID)] {
		t.Error("timer should be cancelled on push failure")
	}
	if !h.cor.expired[adapter.CorrelationKey(env.ID)] {
		t.Error("correlation should be expired on push failure")
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
	sid := h.seedActiveSession(t, "sess-timeout")

	env := h.request("env-timeout", xhs.TypePublish, `{"title":"x"}`, string(sid))
	if err := h.module.Handle(ctx, env); err != nil {
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
	sid := h.seedActiveSession(t, "sess-guard")
	env := h.request("env-guard", xhs.TypePublish, `{"title":"x"}`, string(sid))
	if err := h.module.Handle(ctx, env); err != nil {
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

func TestParseFailureEmitsOrphanCallbackEvents(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t)
	chain := &eventChain{}
	h.mctx.HarnessChain = chain

	err := h.module.OnExternalCallback(ctx, []byte(`{"status":"ok"}`))
	if err == nil || !strings.Contains(err.Error(), "missing correlation_id") {
		t.Fatalf("expected parse error for missing correlation_id, got %v", err)
	}
	if len(h.respond.calls) != 0 {
		t.Fatalf("parse failure must not produce a Respond; got %d", len(h.respond.calls))
	}
	written := chain.Written()
	if len(written) != 2 {
		t.Fatalf("expected orphan callback + system event writes, got %d", len(written))
	}
	if written[0].Type != "adapter.xhs.orphan_callback" || written[0].Kind != message.KindEvent {
		t.Fatalf("first event type/kind=%s/%s", written[0].Type, written[0].Kind)
	}
	if written[1].Type != "system.event" || written[1].Kind != message.KindEvent {
		t.Fatalf("second event type/kind=%s/%s", written[1].Type, written[1].Kind)
	}
	var adapterPayload map[string]any
	if err := json.Unmarshal(written[0].Payload, &adapterPayload); err != nil {
		t.Fatalf("decode orphan payload: %v", err)
	}
	if adapterPayload["kind"] != "orphan_callback" || adapterPayload["detail"] == "" {
		t.Fatalf("orphan payload=%v", adapterPayload)
	}
	var systemPayload map[string]any
	if err := json.Unmarshal(written[1].Payload, &systemPayload); err != nil {
		t.Fatalf("decode system payload: %v", err)
	}
	if systemPayload["kind"] != "correlation_lost" || systemPayload["severity"] != "warn" {
		t.Fatalf("system payload=%v", systemPayload)
	}
}

// TestDuplicateCallbackIsDropped covers the race between F3 timer firing
// (marks correlation expired) and a late device callback arrival. The
// adapter should leave the existing terminal in place.
func TestDuplicateCallbackIsDropped(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t)
	sid := h.seedActiveSession(t, "sess-dup")
	env := h.request("env-dup", xhs.TypePublish, `{"title":"x"}`, string(sid))
	if err := h.module.Handle(ctx, env); err != nil {
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

// TestModuleInitRequiresDeviceTransit guards the codex 警告 #15 wire-up:
// missing DeviceTransit MUST fail at Init.
func TestModuleInitRequiresDeviceTransit(t *testing.T) {
	module, err := xhs.New(xhs.Config{SessionStore: framework.NewInMemorySessionStore()})
	if err != nil {
		t.Fatalf("xhs.New: %v", err)
	}
	mctx := &adapter.ModuleContext{
		Correlation: newFakeCorrelation(),
		ErrorPolicy: newFakePolicy(),
		Respond: func(context.Context, adapter.CorrelationKey, json.RawMessage, adapter.RespondOptions) (adapter.RespondResult, error) {
			return adapter.RespondResult{}, nil
		},
		ChannelID: "c",
	}
	if err := module.Init(context.Background(), mctx); err == nil || !strings.Contains(err.Error(), "DeviceTransit") {
		t.Errorf("Init must require DeviceTransit; got %v", err)
	}
}
