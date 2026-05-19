package framework

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/wanpengxie/ActOS/kernel/actor"
	"github.com/wanpengxie/ActOS/kernel/actorreg"
	"github.com/wanpengxie/ActOS/kernel/adapter"
	"github.com/wanpengxie/ActOS/kernel/channel"
	"github.com/wanpengxie/ActOS/kernel/devicetransit"
	"github.com/wanpengxie/ActOS/kernel/harness"
	"github.com/wanpengxie/ActOS/kernel/message"
)

// stubModule is the test adapter implementation. Callers configure
// what Handle / OnExternalCallback do via the closures.
type stubModule struct {
	decl       adapter.Declaration
	mctx       *adapter.ModuleContext
	handle     func(ctx context.Context, env *message.Envelope, mctx *adapter.ModuleContext) error
	onCallback func(ctx context.Context, payload []byte, mctx *adapter.ModuleContext) error
	shutdown   func() error
}

func (m *stubModule) Declares() adapter.Declaration { return m.decl }

func (m *stubModule) Init(_ context.Context, mctx *adapter.ModuleContext) error {
	m.mctx = mctx
	return nil
}

func (m *stubModule) Shutdown(_ context.Context) error {
	if m.shutdown != nil {
		return m.shutdown()
	}
	return nil
}

func (m *stubModule) Handle(ctx context.Context, env *message.Envelope) error {
	if m.handle != nil {
		return m.handle(ctx, env, m.mctx)
	}
	return nil
}

func (m *stubModule) OnExternalCallback(ctx context.Context, payload []byte) error {
	if m.onCallback != nil {
		return m.onCallback(ctx, payload, m.mctx)
	}
	return nil
}

func newTestManager(t *testing.T, mod *stubModule, opts ...func(*ManagerConfig)) (*Manager, *fakeChain, *MemoryRequestLookup, *memoryActorRegistry, *fixedClock) {
	t.Helper()
	clock := newFixedClock(time.Unix(1_700_000_000, 0))
	chain := newFakeChain()
	lookup := NewMemoryRequestLookup(nil)
	registry := newMemoryActorRegistry()

	// Pre-seed actor row matching the module's declaration.
	if err := registry.Insert(context.Background(), actorreg.Record{
		ID:      mod.decl.ActorID,
		Kind:    actor.KindTool,
		Binding: actor.Binding(mod.decl.Binding),
	}); err != nil {
		t.Fatalf("seed actor: %v", err)
	}

	cfg := ManagerConfig{
		ChannelID:     "channel:test",
		ActorRegistry: registry,
		HarnessChain:  chain,
		RequestLookup: lookup,
		Clock:         clock.Now,
		Logger:        &recordingLogger{},
		Metrics:       NewMemoryMetrics(),
		Tracer:        NoopTracer{},
	}
	for _, opt := range opts {
		opt(&cfg)
	}
	mgr, err := NewManager(cfg)
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	if err := mgr.Install(context.Background(), []adapter.Module{mod}); err != nil {
		t.Fatalf("Install: %v", err)
	}
	return mgr, chain, lookup, registry, clock
}

func newTestRequest(channelID channel.ID, sender, typ, requestID string) *message.Envelope {
	return &message.Envelope{
		ID:         requestID,
		TS:         1_700_000_000_000,
		ChannelID:  string(channelID),
		Sender:     message.Sender{Kind: actor.KindAgent, ID: actor.ActorID(sender)},
		Kind:       message.KindRequest,
		Type:       typ,
		Payload:    json.RawMessage(`{"msg":"hi"}`),
		Visibility: message.VisibilityPrivate,
		Audience:   []string{"tool:feishu"},
	}
}

func TestManagerInstallSeedsTypeRegistry(t *testing.T) {
	mod := &stubModule{
		decl: adapter.Declaration{
			Name:         "feishu",
			ActorID:      "tool:feishu",
			Types:        []string{"feishu.chat.send", "feishu.chat.create"},
			Binding:      actor.BindingOutboundHTTP,
			MaxPendingMs: 30_000,
		},
	}
	registry := NewInMemoryTypeRegistry()
	_, _, _, _, _ = newTestManager(t, mod, func(cfg *ManagerConfig) {
		cfg.TypeRegistry = registry
	})

	if _, ok, _ := registry.Lookup(context.Background(), "feishu.chat.send"); !ok {
		t.Fatalf("type_registry missing feishu.chat.send")
	}
	if _, ok, _ := registry.Lookup(context.Background(), "feishu.chat.create"); !ok {
		t.Fatalf("type_registry missing feishu.chat.create")
	}
	orphan, ok, err := registry.Lookup(context.Background(), OrphanCallbackType("feishu"))
	if err != nil {
		t.Fatalf("lookup orphan callback type: %v", err)
	}
	if !ok {
		t.Fatalf("type_registry missing %s", OrphanCallbackType("feishu"))
	}
	if len(orphan.AllowedKinds) != 1 || orphan.AllowedKinds[0] != message.KindEvent {
		t.Fatalf("orphan callback allowed kinds=%v want [event]", orphan.AllowedKinds)
	}
}

func TestManagerInstallRejectsMissingActor(t *testing.T) {
	mod := &stubModule{
		decl: adapter.Declaration{
			Name:         "feishu",
			ActorID:      "tool:does-not-exist",
			Types:        []string{"feishu.chat.send"},
			Binding:      actor.BindingOutboundHTTP,
			MaxPendingMs: 30_000,
		},
	}
	clock := newFixedClock(time.Unix(0, 0))
	mgr, err := NewManager(ManagerConfig{
		ChannelID:     "channel:test",
		ActorRegistry: newMemoryActorRegistry(),
		HarnessChain:  newFakeChain(),
		RequestLookup: NewMemoryRequestLookup(nil),
		Clock:         clock.Now,
	})
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	err = mgr.Install(context.Background(), []adapter.Module{mod})
	if err == nil {
		t.Fatalf("expected install error")
	}
	var ie *InstallError
	if !errors.As(err, &ie) {
		t.Fatalf("expected InstallError, got %T %v", err, err)
	}
	if ie.Reason != message.InstallHandlerActorNotRegistered {
		t.Fatalf("reason got %s want %s", ie.Reason, message.InstallHandlerActorNotRegistered)
	}
}

func TestManagerInstallRejectsBindingMismatch(t *testing.T) {
	registry := newMemoryActorRegistry()
	_ = registry.Insert(context.Background(), actorreg.Record{
		ID:      "tool:feishu",
		Kind:    actor.KindTool,
		Binding: actor.BindingInProcess,
	})
	mod := &stubModule{
		decl: adapter.Declaration{
			Name:         "feishu",
			ActorID:      "tool:feishu",
			Types:        []string{"feishu.chat.send"},
			Binding:      actor.BindingOutboundHTTP,
			MaxPendingMs: 30_000,
		},
	}
	mgr, _ := NewManager(ManagerConfig{
		ChannelID:     "channel:test",
		ActorRegistry: registry,
		HarnessChain:  newFakeChain(),
		RequestLookup: NewMemoryRequestLookup(nil),
		Clock:         time.Now,
	})
	err := mgr.Install(context.Background(), []adapter.Module{mod})
	var ie *InstallError
	if !errors.As(err, &ie) || ie.Reason != message.InstallHandlerActorBindingMismatch {
		t.Fatalf("expected handler_actor_binding_mismatch, got %v", err)
	}
}

func TestManagerInstallRejectsTransitMissing(t *testing.T) {
	registry := newMemoryActorRegistry()
	_ = registry.Insert(context.Background(), actorreg.Record{
		ID:      "tool:xhs",
		Kind:    actor.KindTool,
		Binding: actor.BindingViaServerTransit,
	})
	mod := &stubModule{
		decl: adapter.Declaration{
			Name:         "xhs",
			ActorID:      "tool:xhs",
			Types:        []string{"xhs.publish"},
			Binding:      actor.BindingViaServerTransit,
			MaxPendingMs: 1_000,
		},
	}
	mgr, _ := NewManager(ManagerConfig{
		ChannelID:     "channel:test",
		ActorRegistry: registry,
		HarnessChain:  newFakeChain(),
		RequestLookup: NewMemoryRequestLookup(nil),
		Clock:         time.Now,
	})
	err := mgr.Install(context.Background(), []adapter.Module{mod})
	if err == nil {
		t.Fatalf("expected error for missing DeviceTransit")
	}
}

// recordingTransit captures Send / Ack / Error calls.
type recordingTransit struct {
	mu    sync.Mutex
	sent  []devicetransit.SendFrame
	acks  []devicetransit.AckFrame
	errs  []devicetransit.ErrorFrame
	frame string
}

func (r *recordingTransit) Send(_ context.Context, frame devicetransit.SendFrame) (string, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.sent = append(r.sent, frame)
	r.frame = "frame-1"
	return r.frame, nil
}

func (r *recordingTransit) Ack(_ context.Context, frame devicetransit.AckFrame) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.acks = append(r.acks, frame)
	return nil
}

func (r *recordingTransit) Error(_ context.Context, frame devicetransit.ErrorFrame) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.errs = append(r.errs, frame)
	return nil
}

func TestManagerInstallAcceptsTransitWhenWired(t *testing.T) {
	registry := newMemoryActorRegistry()
	_ = registry.Insert(context.Background(), actorreg.Record{
		ID:      "tool:xhs",
		Kind:    actor.KindTool,
		Binding: actor.BindingViaServerTransit,
	})
	mod := &stubModule{
		decl: adapter.Declaration{
			Name:         "xhs",
			ActorID:      "tool:xhs",
			Types:        []string{"xhs.publish"},
			Binding:      actor.BindingViaServerTransit,
			MaxPendingMs: 1_000,
		},
	}
	transit := &recordingTransit{}
	mgr, _ := NewManager(ManagerConfig{
		ChannelID:     "channel:test",
		ActorRegistry: registry,
		HarnessChain:  newFakeChain(),
		RequestLookup: NewMemoryRequestLookup(nil),
		DeviceTransit: transit,
		Clock:         time.Now,
	})
	if err := mgr.Install(context.Background(), []adapter.Module{mod}); err != nil {
		t.Fatalf("Install: %v", err)
	}
	// ModuleContext.DeviceTransit must be wired.
	if mod.mctx == nil || mod.mctx.DeviceTransit == nil {
		t.Fatalf("DeviceTransit not wired in mctx")
	}
}

func TestManagerDispatchHandlesRequestAndRespond(t *testing.T) {
	mod := &stubModule{
		decl: adapter.Declaration{
			Name:         "feishu",
			ActorID:      "tool:feishu",
			Types:        []string{"feishu.chat.send"},
			Binding:      actor.BindingOutboundHTTP,
			MaxPendingMs: 30_000,
		},
	}
	mod.handle = func(ctx context.Context, env *message.Envelope, mctx *adapter.ModuleContext) error {
		// Immediately respond with completed status.
		_, err := mctx.Respond(ctx, env.ID,
			json.RawMessage(`{"message_id":"feishu-12345"}`),
			adapter.RespondOptions{Status: "completed"},
		)
		return err
	}
	mgr, chain, lookup, _, _ := newTestManager(t, mod)

	req := newTestRequest("channel:test", "agent:author", "feishu.chat.send", "req-1")
	lookup.Put(req)

	if err := mgr.Dispatch(context.Background(), req); err != nil {
		t.Fatalf("Dispatch: %v", err)
	}

	// The chain should have received the response envelope.
	written := chain.Written()
	if len(written) != 1 {
		t.Fatalf("expected 1 write, got %d", len(written))
	}
	resp := written[0]
	if resp.Kind != message.KindResponse {
		t.Fatalf("response kind=%s want response", resp.Kind)
	}
	if resp.Sender.ID != "tool:feishu" {
		t.Fatalf("response sender.id=%s want tool:feishu", resp.Sender.ID)
	}
	if resp.Sender.Kind != actor.KindTool {
		t.Fatalf("response sender.kind=%s want tool", resp.Sender.Kind)
	}
	if resp.ParentID != req.ID {
		t.Fatalf("response parent_id=%s want %s", resp.ParentID, req.ID)
	}
	if resp.Type != req.Type {
		t.Fatalf("response type=%s want %s", resp.Type, req.Type)
	}
	if len(resp.Audience) != 1 || resp.Audience[0] != "agent:author" {
		t.Fatalf("response audience=%v want [agent:author]", resp.Audience)
	}
	// Payload should contain status=completed + message_id=feishu-12345.
	var payload map[string]any
	if err := json.Unmarshal(resp.Payload, &payload); err != nil {
		t.Fatalf("response payload unmarshal: %v", err)
	}
	if payload["status"] != "completed" {
		t.Fatalf("payload.status=%v want completed", payload["status"])
	}
	if payload["message_id"] != "feishu-12345" {
		t.Fatalf("payload.message_id=%v want feishu-12345", payload["message_id"])
	}
}

func TestManagerDispatchRejectsUnknownAudience(t *testing.T) {
	mod := &stubModule{
		decl: adapter.Declaration{
			Name:         "feishu",
			ActorID:      "tool:feishu",
			Types:        []string{"feishu.chat.send"},
			Binding:      actor.BindingOutboundHTTP,
			MaxPendingMs: 30_000,
		},
	}
	mgr, _, _, _, _ := newTestManager(t, mod)
	req := newTestRequest("channel:test", "agent:a", "feishu.chat.send", "r1")
	req.Audience = []string{"tool:nope"}
	err := mgr.Dispatch(context.Background(), req)
	if err == nil {
		t.Fatalf("expected error for unknown audience")
	}
}

func TestManagerDispatchRejectsUnknownType(t *testing.T) {
	mod := &stubModule{
		decl: adapter.Declaration{
			Name:         "feishu",
			ActorID:      "tool:feishu",
			Types:        []string{"feishu.chat.send"},
			Binding:      actor.BindingOutboundHTTP,
			MaxPendingMs: 30_000,
		},
	}
	mgr, _, _, _, _ := newTestManager(t, mod)
	req := newTestRequest("channel:test", "agent:a", "feishu.chat.unknown", "r1")
	err := mgr.Dispatch(context.Background(), req)
	if err == nil {
		t.Fatalf("expected error for undeclared type")
	}
}

func TestManagerDispatchRejectsChannelMismatch(t *testing.T) {
	mod := &stubModule{
		decl: adapter.Declaration{
			Name:         "feishu",
			ActorID:      "tool:feishu",
			Types:        []string{"feishu.chat.send"},
			Binding:      actor.BindingOutboundHTTP,
			MaxPendingMs: 30_000,
		},
	}
	mgr, _, _, _, _ := newTestManager(t, mod)
	req := newTestRequest("channel:other", "agent:a", "feishu.chat.send", "r1")
	err := mgr.Dispatch(context.Background(), req)
	if err == nil {
		t.Fatalf("expected error for channel mismatch")
	}
}

func TestManagerTimerFiresDefaultTimeout(t *testing.T) {
	mod := &stubModule{
		decl: adapter.Declaration{
			Name:         "feishu",
			ActorID:      "tool:feishu",
			Types:        []string{"feishu.chat.send"},
			Binding:      actor.BindingOutboundHTTP,
			MaxPendingMs: 50, // 50ms timeout
		},
		handle: func(ctx context.Context, env *message.Envelope, mctx *adapter.ModuleContext) error {
			// Don't respond — let the timer fire.
			return nil
		},
	}
	mgr, chain, lookup, _, _ := newTestManager(t, mod)
	defer func() { _ = mgr.Shutdown(context.Background()) }()

	req := newTestRequest("channel:test", "agent:author", "feishu.chat.send", "req-timer")
	lookup.Put(req)

	if err := mgr.Dispatch(context.Background(), req); err != nil {
		t.Fatalf("Dispatch: %v", err)
	}

	// Wait for timer to fire (50ms timeout + slack).
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if len(chain.Written()) > 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	written := chain.Written()
	if len(written) != 1 {
		t.Fatalf("expected 1 timer-fired write, got %d", len(written))
	}
	var payload map[string]any
	_ = json.Unmarshal(written[0].Payload, &payload)
	if payload["status"] != "failed" {
		t.Fatalf("payload.status=%v want failed", payload["status"])
	}
	if payload["reason"] != string(message.TerminalAdapterDefaultTimeout) {
		t.Fatalf("payload.reason=%v want adapter_default_timeout", payload["reason"])
	}
}

func TestManagerTimerRetriesTransientRespondWriteErrors(t *testing.T) {
	mod := &stubModule{
		decl: adapter.Declaration{
			Name:         "feishu",
			ActorID:      "tool:feishu",
			Types:        []string{"feishu.chat.send"},
			Binding:      actor.BindingOutboundHTTP,
			MaxPendingMs: 20,
		},
		handle: func(context.Context, *message.Envelope, *adapter.ModuleContext) error {
			return nil
		},
	}
	mgr, chain, lookup, _, _ := newTestManager(t, mod)
	defer func() { _ = mgr.Shutdown(context.Background()) }()
	chain.errs = []error{
		errors.New("transient write 1"),
		errors.New("transient write 2"),
	}

	req := newTestRequest("channel:test", "agent:a", "feishu.chat.send", "req-timer-retry")
	lookup.Put(req)
	if err := mgr.Dispatch(context.Background(), req); err != nil {
		t.Fatalf("Dispatch: %v", err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if len(chain.Written()) >= 3 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	written := chain.Written()
	if len(written) != 3 {
		t.Fatalf("expected 3 response write attempts, got %d", len(written))
	}
	for i, env := range written {
		if env.Kind != message.KindResponse {
			t.Fatalf("write %d kind=%s want response", i+1, env.Kind)
		}
	}
	var payload map[string]any
	if err := json.Unmarshal(written[2].Payload, &payload); err != nil {
		t.Fatalf("decode final payload: %v", err)
	}
	if payload["reason"] != string(message.TerminalAdapterDefaultTimeout) {
		t.Fatalf("payload.reason=%v want adapter_default_timeout", payload["reason"])
	}
}

func TestManagerTimerEmitsSystemEventAfterPermanentRespondWriteFailure(t *testing.T) {
	metrics := NewMemoryMetrics()
	mod := &stubModule{
		decl: adapter.Declaration{
			Name:         "feishu",
			ActorID:      "tool:feishu",
			Types:        []string{"feishu.chat.send"},
			Binding:      actor.BindingOutboundHTTP,
			MaxPendingMs: 20,
		},
		handle: func(context.Context, *message.Envelope, *adapter.ModuleContext) error {
			return nil
		},
	}
	mgr, chain, lookup, _, _ := newTestManager(t, mod, func(cfg *ManagerConfig) {
		cfg.Metrics = metrics
	})
	defer func() { _ = mgr.Shutdown(context.Background()) }()
	chain.errs = []error{
		errors.New("permanent write 1"),
		errors.New("permanent write 2"),
		errors.New("permanent write 3"),
	}

	req := newTestRequest("channel:test", "agent:a", "feishu.chat.send", "req-timer-terminal-failed")
	lookup.Put(req)
	if err := mgr.Dispatch(context.Background(), req); err != nil {
		t.Fatalf("Dispatch: %v", err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if len(chain.Written()) >= 4 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	written := chain.Written()
	if len(written) != 4 {
		t.Fatalf("expected 3 failed responses plus system event, got %d", len(written))
	}
	event := written[3]
	if event.Kind != message.KindEvent || event.Type != "system.event" {
		t.Fatalf("last write kind/type=%s/%s want event/system.event", event.Kind, event.Type)
	}
	var payload map[string]any
	if err := json.Unmarshal(event.Payload, &payload); err != nil {
		t.Fatalf("decode system event: %v", err)
	}
	if payload["kind"] != systemEventTimerTerminalFailed {
		t.Fatalf("system event kind=%v want %s", payload["kind"], systemEventTimerTerminalFailed)
	}
	if payload["request_id"] != "req-timer-terminal-failed" {
		t.Fatalf("system event request_id=%v", payload["request_id"])
	}
	if got := metrics.Counter("adapter.policy.timer_terminal_failed", "adapter", "feishu"); got != 1 {
		t.Fatalf("timer_terminal_failed metric=%d want 1", got)
	}
	entry, ok, err := mod.mctx.Correlation.Get(context.Background(), "req-timer-terminal-failed")
	if err != nil {
		t.Fatalf("correlation get: %v", err)
	}
	if !ok || entry.State != adapter.CorrelationExpired {
		t.Fatalf("correlation state=%v ok=%v want expired", entry.State, ok)
	}
}

func TestManagerRespondCancelsTimer(t *testing.T) {
	var responded atomic.Bool
	mod := &stubModule{
		decl: adapter.Declaration{
			Name:         "feishu",
			ActorID:      "tool:feishu",
			Types:        []string{"feishu.chat.send"},
			Binding:      actor.BindingOutboundHTTP,
			MaxPendingMs: 80,
		},
		handle: func(ctx context.Context, env *message.Envelope, mctx *adapter.ModuleContext) error {
			_, err := mctx.Respond(ctx, env.ID,
				json.RawMessage(`{}`),
				adapter.RespondOptions{Status: "completed"},
			)
			responded.Store(true)
			return err
		},
	}
	mgr, chain, lookup, _, _ := newTestManager(t, mod)
	defer func() { _ = mgr.Shutdown(context.Background()) }()

	req := newTestRequest("channel:test", "agent:a", "feishu.chat.send", "req-cancel")
	lookup.Put(req)

	if err := mgr.Dispatch(context.Background(), req); err != nil {
		t.Fatalf("Dispatch: %v", err)
	}
	// Sleep longer than MaxPendingMs to ensure timer would have fired if not cancelled.
	time.Sleep(200 * time.Millisecond)

	written := chain.Written()
	if len(written) != 1 {
		t.Fatalf("expected exactly 1 write after cancel, got %d", len(written))
	}
}

func TestManagerOnExternalCallbackRoutes(t *testing.T) {
	var seen []string
	mod := &stubModule{
		decl: adapter.Declaration{
			Name:         "feishu",
			ActorID:      "tool:feishu",
			Types:        []string{"feishu.chat.send"},
			Binding:      actor.BindingOutboundHTTP,
			MaxPendingMs: 30_000,
		},
		onCallback: func(_ context.Context, payload []byte, _ *adapter.ModuleContext) error {
			seen = append(seen, string(payload))
			return nil
		},
	}
	mgr, _, _, _, _ := newTestManager(t, mod)
	if err := mgr.OnExternalCallback(context.Background(), "feishu", []byte("hello")); err != nil {
		t.Fatalf("OnExternalCallback: %v", err)
	}
	if len(seen) != 1 || seen[0] != "hello" {
		t.Fatalf("callback not routed: %v", seen)
	}
	err := mgr.OnExternalCallback(context.Background(), "unknown", nil)
	if err == nil {
		t.Fatalf("expected error for unknown adapter")
	}
}

func TestManagerOnExternalCallbackEmitsOrphanEvents(t *testing.T) {
	var routed atomic.Bool
	mod := &stubModule{
		decl: adapter.Declaration{
			Name:         "feishu",
			ActorID:      "tool:feishu",
			Types:        []string{"feishu.chat.send"},
			Binding:      actor.BindingOutboundHTTP,
			MaxPendingMs: 30_000,
		},
		onCallback: func(context.Context, []byte, *adapter.ModuleContext) error {
			routed.Store(true)
			return nil
		},
	}
	mgr, chain, _, _, _ := newTestManager(t, mod)
	if err := mgr.OnExternalCallback(context.Background(), "feishu", []byte(`{"correlation_id":"ghost","status":"ok"}`)); err != nil {
		t.Fatalf("OnExternalCallback: %v", err)
	}
	if routed.Load() {
		t.Fatalf("orphan callback should not route into module")
	}
	written := chain.Written()
	if len(written) != 2 {
		t.Fatalf("expected orphan callback + system event writes, got %d", len(written))
	}
	if written[0].Type != OrphanCallbackType("feishu") || written[0].Kind != message.KindEvent {
		t.Fatalf("first event type/kind=%s/%s", written[0].Type, written[0].Kind)
	}
	if written[1].Type != "system.event" || written[1].Kind != message.KindEvent {
		t.Fatalf("second event type/kind=%s/%s", written[1].Type, written[1].Kind)
	}
	var adapterPayload map[string]any
	if err := json.Unmarshal(written[0].Payload, &adapterPayload); err != nil {
		t.Fatalf("decode orphan payload: %v", err)
	}
	if adapterPayload["kind"] != orphanCallbackPayloadKind || adapterPayload["correlation_id"] != "ghost" {
		t.Fatalf("orphan payload=%v", adapterPayload)
	}
	var systemPayload map[string]any
	if err := json.Unmarshal(written[1].Payload, &systemPayload); err != nil {
		t.Fatalf("decode system payload: %v", err)
	}
	if systemPayload["kind"] != systemEventCorrelationLost || systemPayload["correlation_id"] != "ghost" {
		t.Fatalf("system payload=%v", systemPayload)
	}
}

func TestManagerShutdownCallsModule(t *testing.T) {
	var shutdownCalls int
	mod := &stubModule{
		decl: adapter.Declaration{
			Name:         "feishu",
			ActorID:      "tool:feishu",
			Types:        []string{"feishu.chat.send"},
			Binding:      actor.BindingOutboundHTTP,
			MaxPendingMs: 30_000,
		},
		shutdown: func() error {
			shutdownCalls++
			return nil
		},
	}
	mgr, _, _, _, _ := newTestManager(t, mod)
	if err := mgr.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	if shutdownCalls != 1 {
		t.Fatalf("expected 1 shutdown call, got %d", shutdownCalls)
	}
}

func TestManagerInstalledAdapters(t *testing.T) {
	mod := &stubModule{
		decl: adapter.Declaration{
			Name:         "feishu",
			ActorID:      "tool:feishu",
			Types:        []string{"feishu.chat.send"},
			Binding:      actor.BindingOutboundHTTP,
			MaxPendingMs: 30_000,
		},
	}
	mgr, _, _, _, _ := newTestManager(t, mod)
	got := mgr.InstalledAdapters()
	if len(got) != 1 || got[0] != "feishu" {
		t.Fatalf("InstalledAdapters got %v want [feishu]", got)
	}
}

func TestManagerDeduplicatesResponseFromTerminalDuplicate(t *testing.T) {
	mod := &stubModule{
		decl: adapter.Declaration{
			Name:         "feishu",
			ActorID:      "tool:feishu",
			Types:        []string{"feishu.chat.send"},
			Binding:      actor.BindingOutboundHTTP,
			MaxPendingMs: 30_000,
		},
	}
	var dedupedResult adapter.RespondResult
	mod.handle = func(ctx context.Context, env *message.Envelope, mctx *adapter.ModuleContext) error {
		res, err := mctx.Respond(ctx, env.ID,
			json.RawMessage(`{}`),
			adapter.RespondOptions{Status: "completed"},
		)
		dedupedResult = res
		return err
	}
	mgr, chain, lookup, _, _ := newTestManager(t, mod)
	defer func() { _ = mgr.Shutdown(context.Background()) }()

	// Configure fake chain to return terminal_duplicate.
	chain.results = []harness.WriteResult{
		{
			MessageID:        "",
			RejectReason:     message.HarnessTerminalDuplicate,
			PartialMessageID: "response:existing",
		},
	}

	req := newTestRequest("channel:test", "agent:a", "feishu.chat.send", "req-dup")
	lookup.Put(req)
	if err := mgr.Dispatch(context.Background(), req); err != nil {
		t.Fatalf("Dispatch: %v", err)
	}
	if !dedupedResult.Deduped {
		t.Fatalf("expected Deduped=true, got %+v", dedupedResult)
	}
	if dedupedResult.MessageID != "response:existing" {
		t.Fatalf("expected PartialMessageID surfaced, got %q", dedupedResult.MessageID)
	}
}
