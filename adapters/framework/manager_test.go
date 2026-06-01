package framework

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	proxyfacade "github.com/wanpengxie/ActOS/adapters/framework/proxy_facade"
	"github.com/wanpengxie/ActOS/framework/devicetransit"
	"github.com/wanpengxie/ActOS/kernel/actor"
	"github.com/wanpengxie/ActOS/kernel/actorreg"
	"github.com/wanpengxie/ActOS/kernel/adapter"
	"github.com/wanpengxie/ActOS/kernel/channel"
	"github.com/wanpengxie/ActOS/kernel/harness"
	"github.com/wanpengxie/ActOS/kernel/message"
)

// stubModule is the test adapter implementation. Callers configure
// what Handle / OnExternalCallback do via the closures.
type stubModule struct {
	decl       adapter.Declaration
	mctx       *adapter.ModuleContext
	initErr    error
	handle     func(ctx context.Context, env *message.Envelope, mctx *adapter.ModuleContext) error
	onCallback func(ctx context.Context, payload []byte, mctx *adapter.ModuleContext) error
	onFrame    func(ctx context.Context, frame adapter.ExternalCallbackFrame, mctx *adapter.ModuleContext) error
	shutdown   func() error
}

func (m *stubModule) Declares() adapter.Declaration { return m.decl }

func (m *stubModule) Init(_ context.Context, mctx *adapter.ModuleContext) error {
	if m.initErr != nil {
		return m.initErr
	}
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

func (m *stubModule) OnExternalCallbackFrame(ctx context.Context, frame adapter.ExternalCallbackFrame) error {
	if m.onFrame != nil {
		return m.onFrame(ctx, frame, m.mctx)
	}
	return m.OnExternalCallback(ctx, frame.Payload)
}

type heartbeatModule struct {
	*stubModule
	report adapter.HeartbeatReport
	err    error
	calls  atomic.Int32
}

func (m *heartbeatModule) Heartbeat(context.Context) (adapter.HeartbeatReport, error) {
	m.calls.Add(1)
	return m.report, m.err
}

func newTestManager(t *testing.T, mod adapter.Module, opts ...func(*ManagerConfig)) (*Manager, *fakeChain, *MemoryRequestLookup, *memoryActorRegistry, *fixedClock) {
	t.Helper()
	clock := newFixedClock(time.Unix(1_700_000_000, 0))
	chain := newFakeChain()
	lookup := NewMemoryRequestLookup(nil)
	registry := newMemoryActorRegistry()
	decl := mod.Declares()

	// Pre-seed actor row matching the module's declaration.
	if err := registry.Insert(context.Background(), actorreg.Record{
		ID:      decl.ActorID,
		Kind:    actor.KindTool,
		Binding: actor.Binding(decl.Binding),
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
		ID:         message.ID(requestID),
		TS:         1_700_000_000_000,
		ChannelID:  channelID,
		Sender:     message.Sender{Kind: actor.KindAgent, ID: actor.ActorID(sender)},
		Kind:       message.KindRequest,
		Type:       typ,
		Payload:    json.RawMessage(`{"msg":"hi"}`),
		Visibility: message.VisibilityPrivate,
		Audience:   message.Audience{"tool:feishu-adapter"},
	}
}

func TestManagerInstallSeedsTypeRegistry(t *testing.T) {
	mod := &stubModule{
		decl: adapter.Declaration{
			Name:         "feishu",
			ActorID:      "tool:feishu-adapter",
			Types:        []string{"feishu.chat.send", "feishu.chat.create"},
			Binding:      actor.BindingRuntimeOutbound,
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
	orphan, ok, err := registry.Lookup(context.Background(), orphanCallbackType("feishu"))
	if err != nil {
		t.Fatalf("lookup orphan callback type: %v", err)
	}
	if !ok {
		t.Fatalf("type_registry missing %s", orphanCallbackType("feishu"))
	}
	if len(orphan.AllowedKinds) != 1 || orphan.AllowedKinds[0] != message.KindEvent {
		t.Fatalf("orphan callback allowed kinds=%v want [event]", orphan.AllowedKinds)
	}
}

func TestEmitEventRejectsFrameworkOwnedOrphanCallbackType(t *testing.T) {
	mod := &stubModule{
		decl: adapter.Declaration{
			Name:         "feishu",
			ActorID:      "tool:feishu-adapter",
			Types:        []string{"feishu.chat.send"},
			Binding:      actor.BindingRuntimeOutbound,
			MaxPendingMs: 30_000,
		},
	}
	_, _, _, _, _ = newTestManager(t, mod)

	_, err := mod.mctx.EmitEvent(context.Background(), orphanCallbackType("feishu"), json.RawMessage(`{}`), adapter.EmitEventOptions{})
	if err == nil || !strings.Contains(err.Error(), "ReportOrphanCallback") {
		t.Fatalf("EmitEvent orphan callback should be rejected with ReportOrphanCallback guidance, got %v", err)
	}
}

func TestManagerInstallPersistsDeclarationCatalog(t *testing.T) {
	state := NewMemoryStateStore()
	mod := &stubModule{
		decl: adapter.Declaration{
			Name:         "feishu",
			ActorID:      "tool:feishu-adapter",
			Types:        []string{"feishu.chat.send"},
			Binding:      actor.BindingRuntimeOutbound,
			MaxPendingMs: 30_000,
			Description:  "Feishu messaging",
			SkillDoc:     "Use this actor to send Feishu messages.",
			TypeDeclarations: map[string]adapter.TypeDeclaration{
				"feishu.chat.send": {
					AllowedKinds:   []message.Kind{message.KindRequest, message.KindResponse},
					Description:    "Send a chat message",
					PayloadExample: json.RawMessage(`{"text":"hello"}`),
					PayloadFields: []adapter.FieldDoc{{
						Name:        "text",
						Required:    true,
						Description: "Message text",
					}},
					ErrorCodes: []adapter.ErrorDoc{{
						Code:     "send_failed",
						Recovery: "Retry with a valid chat id",
					}},
					Notes: "Text messages only.",
				},
			},
		},
	}
	_, _, _, _, _ = newTestManager(t, mod, func(cfg *ManagerConfig) {
		cfg.StateStore = state
	})

	raw, ok, err := state.Get(context.Background(), "adapter:feishu:"+adapter.DeclarationConventionStateKey)
	if err != nil {
		t.Fatalf("state get: %v", err)
	}
	if !ok {
		t.Fatalf("declaration catalog not persisted")
	}
	var catalog adapter.DeclarationCatalog
	if err := json.Unmarshal(raw, &catalog); err != nil {
		t.Fatalf("decode catalog: %v", err)
	}
	if catalog.Description != "Feishu messaging" || catalog.SkillDoc == "" {
		t.Fatalf("catalog actor metadata=%+v", catalog)
	}
	doc := catalog.Types["feishu.chat.send"]
	if doc.Description != "Send a chat message" || string(doc.PayloadExample) != `{"text":"hello"}` {
		t.Fatalf("catalog type metadata=%+v", doc)
	}
	if len(doc.PayloadFields) != 1 || doc.PayloadFields[0].Name != "text" {
		t.Fatalf("catalog payload fields=%+v", doc.PayloadFields)
	}
}

func TestManagerInstallHeartbeatUpdatesReadinessAndEmitsTransition(t *testing.T) {
	mod := &heartbeatModule{
		stubModule: &stubModule{
			decl: adapter.Declaration{
				Name:         "feishu",
				ActorID:      "tool:feishu-adapter",
				Types:        []string{"feishu.chat.send"},
				Binding:      actor.BindingRuntimeOutbound,
				MaxPendingMs: 30_000,
			},
		},
		report: adapter.HeartbeatReport{
			Available: false,
			Reason:    "upstream_unreachable",
			Detail:    map[string]any{"probe": "failed"},
			CheckedAt: time.UnixMilli(1_700_000_001_000),
		},
	}
	mgr, chain, _, registry, _ := newTestManager(t, mod, func(cfg *ManagerConfig) {
		cfg.EmitInitialReadinessEvent = true
		cfg.HeartbeatInterval = time.Hour
	})
	t.Cleanup(func() { _ = mgr.Shutdown(context.Background()) })

	if got := mod.calls.Load(); got != 1 {
		t.Fatalf("heartbeat calls=%d want 1 initial probe", got)
	}
	rec, ok, err := registry.Lookup(context.Background(), "tool:feishu-adapter")
	if err != nil || !ok {
		t.Fatalf("lookup actor ok=%v err=%v", ok, err)
	}
	if rec.Readiness.State != actorreg.ReadinessNotReady || rec.Readiness.Reason != "upstream_unreachable" {
		t.Fatalf("readiness=%+v", rec.Readiness)
	}
	if rec.Readiness.LastStateChangeAt != 1_700_000_001_000 {
		t.Fatalf("last_state_change_at=%d", rec.Readiness.LastStateChangeAt)
	}

	written := chain.Written()
	if len(written) != 1 {
		t.Fatalf("written=%d want readiness event", len(written))
	}
	ev := written[0]
	if ev.Type != "actor.readiness.changed" || ev.Sender.ID != actor.SystemActorID || ev.Visibility != message.VisibilityPublic {
		t.Fatalf("readiness event envelope=%+v", ev)
	}
	var payload struct {
		ActorID string `json:"actor_id"`
		Current struct {
			Ready  bool   `json:"ready"`
			State  string `json:"state"`
			Reason string `json:"reason"`
			Detail struct {
				Probe string `json:"probe"`
			} `json:"detail"`
		} `json:"current"`
	}
	if err := json.Unmarshal(ev.Payload, &payload); err != nil {
		t.Fatalf("unmarshal readiness payload: %v", err)
	}
	if payload.ActorID != "tool:feishu-adapter" || payload.Current.Ready || payload.Current.State != "not_ready" ||
		payload.Current.Reason != "upstream_unreachable" || payload.Current.Detail.Probe != "failed" {
		t.Fatalf("readiness payload=%+v", payload)
	}
}

// TestReadinessEmitFollowsCommittedProjection pins the Y8 fix: the durable
// actor.readiness.changed fact is emitted ONLY after the store projection has
// committed, computed by the store's single authoritative transition. A failed
// emit must NOT leave the log claiming a transition the projection denies —
// instead the projection (the source of truth, INVARIANT-2) advances and the
// emit is what fails. The earlier design (emit-before-projection + a second
// manager-side recompute) is the bug being removed here.
func TestReadinessEmitFollowsCommittedProjection(t *testing.T) {
	mod := &stubModule{
		decl: adapter.Declaration{
			Name:         "feishu",
			ActorID:      "tool:feishu-adapter",
			Types:        []string{"feishu.chat.send"},
			Binding:      actor.BindingRuntimeOutbound,
			MaxPendingMs: 30_000,
		},
	}
	_, chain, _, registry, _ := newTestManager(t, mod)
	if _, err := registry.UpdateReadiness(context.Background(), "tool:feishu-adapter", actorreg.ReadinessUpdate{
		State:     actorreg.ReadinessReady,
		Reason:    "ok",
		Detail:    json.RawMessage(`{"probe":"old"}`),
		CheckedAt: 1_000,
	}); err != nil {
		t.Fatalf("seed readiness: %v", err)
	}
	chain.errs = []error{errors.New("write failed")}

	if _, err := mod.mctx.UpdateReadiness(context.Background(), actorreg.ReadinessUpdate{
		State:     actorreg.ReadinessReady,
		Reason:    "ok",
		Detail:    json.RawMessage(`{"probe":"new"}`),
		CheckedAt: 2_000,
	}); err == nil {
		t.Fatal("UpdateReadiness should surface the readiness event write failure")
	}
	rec, ok, err := registry.Lookup(context.Background(), "tool:feishu-adapter")
	if err != nil || !ok {
		t.Fatalf("lookup actor ok=%v err=%v", ok, err)
	}
	// Projection committed first: truth advanced. The log must not be allowed
	// to carry a `changed` fact while the projection lags (the Y8 drift).
	if string(rec.Readiness.Detail) != `{"probe":"new"}` || rec.Readiness.LastReadyAt != 2_000 {
		t.Fatalf("readiness projection should have committed before emit: %+v", rec.Readiness)
	}
	if written := chain.Written(); len(written) != 1 || written[0].Type != "actor.readiness.changed" {
		t.Fatalf("written=%d want failed readiness event attempt", len(written))
	}
}

func TestManagerInstallDoesNotPublishTypeRowsWhenInitFails(t *testing.T) {
	mod := &stubModule{
		decl: adapter.Declaration{
			Name:         "feishu",
			ActorID:      "tool:feishu-adapter",
			Types:        []string{"feishu.chat.send"},
			Binding:      actor.BindingRuntimeOutbound,
			MaxPendingMs: 30_000,
		},
		initErr: errors.New("init failed"),
	}
	registry := newMemoryActorRegistry()
	if err := registry.Insert(context.Background(), actorreg.Record{
		ID:      mod.decl.ActorID,
		Kind:    actor.KindTool,
		Binding: actor.BindingRuntimeOutbound,
	}); err != nil {
		t.Fatalf("seed actor: %v", err)
	}
	types := NewInMemoryTypeRegistry()
	mgr, err := NewManager(ManagerConfig{
		ChannelID:     "channel:test",
		ActorRegistry: registry,
		TypeRegistry:  types,
		HarnessChain:  newFakeChain(),
		RequestLookup: NewMemoryRequestLookup(nil),
		Clock:         time.Now,
	})
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	if err := mgr.Install(context.Background(), []adapter.Module{mod}); err == nil {
		t.Fatalf("Install succeeded; want init failure")
	}
	if _, ok, err := types.Lookup(context.Background(), "feishu.chat.send"); err != nil || ok {
		t.Fatalf("type row after failed init ok=%v err=%v", ok, err)
	}
	if _, ok, err := types.Lookup(context.Background(), orphanCallbackType("feishu")); err != nil || ok {
		t.Fatalf("orphan type row after failed init ok=%v err=%v", ok, err)
	}
}

func TestManagerInstallRejectsMissingActor(t *testing.T) {
	mod := &stubModule{
		decl: adapter.Declaration{
			Name:         "feishu",
			ActorID:      "tool:does-not-exist",
			Types:        []string{"feishu.chat.send"},
			Binding:      actor.BindingRuntimeOutbound,
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
		ID:      "tool:feishu-adapter",
		Kind:    actor.KindTool,
		Binding: actor.BindingEmbedded,
	})
	mod := &stubModule{
		decl: adapter.Declaration{
			Name:         "feishu",
			ActorID:      "tool:feishu-adapter",
			Types:        []string{"feishu.chat.send"},
			Binding:      actor.BindingRuntimeOutbound,
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

func TestManagerInstallRejectsReservedNamespaceType(t *testing.T) {
	mod := &stubModule{
		decl: adapter.Declaration{
			Name:         "bad",
			ActorID:      "tool:bad-adapter",
			Types:        []string{"system.foo"},
			Binding:      actor.BindingEmbedded,
			MaxPendingMs: 30_000,
			TypeDeclarations: map[string]adapter.TypeDeclaration{
				"system.foo": {AllowedKinds: []message.Kind{message.KindEvent}},
			},
		},
	}
	registry := NewInMemoryTypeRegistry()
	mgr, err := NewManager(ManagerConfig{
		ChannelID:     "channel:test",
		ActorRegistry: newMemoryActorRegistry(),
		TypeRegistry:  registry,
		HarnessChain:  newFakeChain(),
		RequestLookup: NewMemoryRequestLookup(nil),
		Clock:         time.Now,
	})
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	if err := mgr.cfg.ActorRegistry.Insert(context.Background(), actorreg.Record{
		ID:      mod.decl.ActorID,
		Kind:    actor.KindTool,
		Binding: actor.BindingEmbedded,
	}); err != nil {
		t.Fatalf("seed actor: %v", err)
	}
	err = mgr.Install(context.Background(), []adapter.Module{mod})
	var ie *InstallError
	if !errors.As(err, &ie) || ie.Reason != message.InstallTypeRegistryReservedNamespace {
		t.Fatalf("expected type_registry_reserved_namespace, got %v", err)
	}
	if _, ok, err := registry.Lookup(context.Background(), "system.foo"); err != nil || ok {
		t.Fatalf("reserved type row written ok=%v err=%v", ok, err)
	}
}

func TestManagerInstallRejectsTransitMissing(t *testing.T) {
	registry := newMemoryActorRegistry()
	_ = registry.Insert(context.Background(), actorreg.Record{
		ID:      "tool:xhs",
		Kind:    actor.KindTool,
		Binding: actor.BindingRuntimeInboundViaRelay,
	})
	mod := &stubModule{
		decl: adapter.Declaration{
			Name:         "xhs",
			ActorID:      "tool:xhs",
			Types:        []string{"xhs.publish"},
			Binding:      actor.BindingRuntimeInboundViaRelay,
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

// recordingTransit captures Send / Ack calls.
type recordingTransit struct {
	mu    sync.Mutex
	sent  []devicetransit.SendFrame
	acks  []devicetransit.AckFrame
	frame string
}

func (r *recordingTransit) Send(_ context.Context, frame devicetransit.SendFrame) (devicetransit.FrameID, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.sent = append(r.sent, frame)
	r.frame = "frame-1"
	return devicetransit.FrameID(r.frame), nil
}

func (r *recordingTransit) Ack(_ context.Context, frame devicetransit.AckFrame) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.acks = append(r.acks, frame)
	return nil
}

func TestManagerInstallAcceptsTransitWhenWired(t *testing.T) {
	registry := newMemoryActorRegistry()
	_ = registry.Insert(context.Background(), actorreg.Record{
		ID:      "tool:xhs",
		Kind:    actor.KindTool,
		Binding: actor.BindingRuntimeInboundViaRelay,
	})
	mod := &stubModule{
		decl: adapter.Declaration{
			Name:         "xhs",
			ActorID:      "tool:xhs",
			Types:        []string{"xhs.publish"},
			Binding:      actor.BindingRuntimeInboundViaRelay,
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
	if mod.mctx == nil || mod.mctx.ForwardExternalRequest == nil {
		t.Fatalf("ForwardExternalRequest not wired in mctx")
	}
}

func TestManagerDispatchHandlesRequestAndRespond(t *testing.T) {
	mod := &stubModule{
		decl: adapter.Declaration{
			Name:         "feishu",
			ActorID:      "tool:feishu-adapter",
			Types:        []string{"feishu.chat.send"},
			Binding:      actor.BindingRuntimeOutbound,
			MaxPendingMs: 30_000,
		},
	}
	mod.handle = func(ctx context.Context, env *message.Envelope, mctx *adapter.ModuleContext) error {
		// Immediately respond with completed status.
		_, err := mctx.Respond(ctx, adapter.CorrelationKey(env.ID),
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
	if resp.Sender.ID != "tool:feishu-adapter" {
		t.Fatalf("response sender.id=%s want tool:feishu-adapter", resp.Sender.ID)
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

func TestManagerRespondRejectsUnownedOrNonResponseCapableParent(t *testing.T) {
	mod := &stubModule{
		decl: adapter.Declaration{
			Name:         "feishu",
			ActorID:      "tool:feishu-adapter",
			Types:        []string{"feishu.chat.send", "feishu.event"},
			Binding:      actor.BindingRuntimeOutbound,
			MaxPendingMs: 30_000,
			TypeDeclarations: map[string]adapter.TypeDeclaration{
				"feishu.chat.send": {AllowedKinds: []message.Kind{message.KindRequest, message.KindResponse}},
				"feishu.event":     {AllowedKinds: []message.Kind{message.KindEvent}},
			},
		},
	}
	mod.handle = func(ctx context.Context, env *message.Envelope, mctx *adapter.ModuleContext) error {
		if _, err := mctx.Respond(ctx, "req-other-actor", json.RawMessage(`{}`), adapter.RespondOptions{Status: "completed"}); err == nil {
			t.Fatal("Respond must reject a parent request owned by another actor")
		}
		if _, err := mctx.Respond(ctx, "req-event-only", json.RawMessage(`{}`), adapter.RespondOptions{Status: "completed"}); err == nil {
			t.Fatal("Respond must reject non response-capable request type")
		}
		_, err := mctx.Respond(ctx, adapter.CorrelationKey(env.ID), json.RawMessage(`{"ok":true}`), adapter.RespondOptions{Status: "completed"})
		return err
	}
	mgr, chain, lookup, _, clock := newTestManager(t, mod)

	other := newTestRequest("channel:test", "agent:author", "feishu.chat.send", "req-other-actor")
	other.Audience = message.Audience{"tool:other"}
	lookup.Put(other)

	eventOnly := newTestRequest("channel:test", "agent:author", "feishu.event", "req-event-only")
	eventOnly.Audience = message.Audience{"tool:feishu-adapter"}
	lookup.Put(eventOnly)
	bm := mgr.byName["feishu"]
	_, err := bm.correlation.Reserve(context.Background(), adapter.CorrelationEntry{
		RequestID:     "req-event-only",
		ChannelID:     "channel:test",
		AudienceActor: "tool:feishu-adapter",
		ParentID:      "req-event-only",
		EnqueuedAt:    clock.Now().UnixMilli(),
		ExpiresAt:     clock.Now().Add(30 * time.Second).UnixMilli(),
		State:         adapter.CorrelationPending,
	})
	if err != nil {
		t.Fatalf("Reserve event-only: %v", err)
	}

	req := newTestRequest("channel:test", "agent:author", "feishu.chat.send", "req-owned-ok")
	lookup.Put(req)
	if err := mgr.Dispatch(context.Background(), req); err != nil {
		t.Fatalf("Dispatch: %v", err)
	}
	written := chain.Written()
	if len(written) != 1 || written[0].ParentID != "req-owned-ok" {
		t.Fatalf("written=%v want only owned response", written)
	}
}

func TestManagerDispatchActorStatusBypassesReadinessPrecheck(t *testing.T) {
	called := false
	mod := &stubModule{
		decl: adapter.Declaration{
			Name:         "feishu",
			ActorID:      "tool:feishu-adapter",
			Types:        []string{"feishu.chat.send"},
			Binding:      actor.BindingRuntimeOutbound,
			MaxPendingMs: 30_000,
		},
		handle: func(context.Context, *message.Envelope, *adapter.ModuleContext) error {
			called = true
			return nil
		},
	}
	mgr, chain, lookup, registry, _ := newTestManager(t, mod)
	_, err := registry.UpdateReadiness(context.Background(), "tool:feishu-adapter", actorreg.ReadinessUpdate{
		State:     actorreg.ReadinessNotReady,
		Reason:    "upstream_unreachable",
		Detail:    json.RawMessage(`{"probe":"failed"}`),
		CheckedAt: 1_700_000_001_000,
	})
	if err != nil {
		t.Fatalf("UpdateReadiness: %v", err)
	}

	req := newTestRequest("channel:test", "agent:author", "actor.status", "req-status")
	lookup.Put(req)
	if err := mgr.Dispatch(context.Background(), req); err != nil {
		t.Fatalf("Dispatch actor.status: %v", err)
	}
	if called {
		t.Fatal("actor.status must be handled by framework, not Module.Handle")
	}
	written := chain.Written()
	if len(written) != 1 {
		t.Fatalf("written=%d want 1", len(written))
	}
	var payload map[string]any
	if err := json.Unmarshal(written[0].Payload, &payload); err != nil {
		t.Fatalf("unmarshal payload: %v", err)
	}
	// channel-lifecycle-reconcile §5 护栏 3 — actor.status.available is realtime
	// liveness, NOT the persisted readiness projection. The registry says
	// not_ready/upstream_unreachable, yet the module (an in-process module with
	// no StatusReporter) is reachable right now, so available=true. The sticky
	// readiness column must NOT drive available anymore.
	if payload["status"] != "completed" || payload["available"] != true {
		t.Fatalf("actor.status payload=%v want completed + available=true (realtime liveness, not sticky readiness)", payload)
	}
}

func TestProxyFacadeReservedDescribeStatusUsesFrameworkState(t *testing.T) {
	decl, err := proxyfacade.DeclarationFromCapability("tool:kimi", json.RawMessage(`{
		"name":"kimi",
		"description":"Local Kimi bridge",
		"types":["kimi.ask"],
		"type_declarations":{"kimi.ask":{
			"AllowedKinds":["request","response"],
			"Description":"Ask local Kimi"
		}},
		"max_pending_ms":12000
	}`))
	if err != nil {
		t.Fatalf("DeclarationFromCapability: %v", err)
	}
	mod, err := proxyfacade.New(decl)
	if err != nil {
		t.Fatalf("proxyfacade.New: %v", err)
	}
	mgr, chain, lookup, registry, _ := newTestManager(t, mod, func(cfg *ManagerConfig) {
		cfg.DeviceTransit = &recordingTransit{}
	})
	defer func() { _ = mgr.Shutdown(context.Background()) }()

	describeReq := newTestRequest("channel:test", "agent:author", "actor.describe", "req-proxy-desc")
	describeReq.Audience = message.Audience{"tool:kimi"}
	lookup.Put(describeReq)
	if err := mgr.Dispatch(context.Background(), describeReq); err != nil {
		t.Fatalf("Dispatch actor.describe: %v", err)
	}
	written := chain.Written()
	if len(written) != 1 {
		t.Fatalf("written=%d want 1 describe response", len(written))
	}
	var describe map[string]any
	if err := json.Unmarshal(written[0].Payload, &describe); err != nil {
		t.Fatalf("describe payload: %v", err)
	}
	if describe["actor_id"] != "tool:kimi" || describe["binding"] != string(actor.BindingRuntimeInboundViaRelay) {
		t.Fatalf("describe=%v", describe)
	}
	types, ok := describe["types"].(map[string]any)
	if !ok {
		t.Fatalf("describe types=%T %v", describe["types"], describe["types"])
	}
	if _, ok := types["kimi.ask"]; !ok {
		t.Fatalf("describe types missing kimi.ask: %v", types)
	}

	readinessEvent, err := json.Marshal(message.Envelope{
		ID:        "evt-proxy-readiness",
		ChannelID: "channel:test",
		Sender:    message.Sender{Kind: actor.KindSystem, ID: actor.SystemActorID},
		Kind:      message.KindEvent,
		Type:      "actor.readiness.changed",
		Payload: json.RawMessage(`{
			"actor_id":"tool:kimi",
			"changed_at":1700000001000,
			"current":{
				"ready":false,
				"reason":"local_unreachable",
				"detail":{"port":10086},
				"last_state_change_at":1700000001000
			}
		}`),
	})
	if err != nil {
		t.Fatalf("marshal readiness event: %v", err)
	}
	if err := mgr.OnExternalCallback(context.Background(), decl.Name, readinessEvent); err != nil {
		t.Fatalf("OnExternalCallback readiness: %v", err)
	}
	rec, ok, err := registry.Lookup(context.Background(), "tool:kimi")
	if err != nil || !ok {
		t.Fatalf("Lookup proxy actor ok=%v err=%v", ok, err)
	}
	// The readiness.changed event still drives the registry projection (②投影);
	// that path is unchanged.
	if rec.Readiness.State != actorreg.ReadinessNotReady || rec.Readiness.Reason != "local_unreachable" {
		t.Fatalf("readiness=%+v", rec.Readiness)
	}

	// channel-lifecycle-reconcile §5 护栏 3 — before any device lifecycle frame
	// the volatile liveness is unknown, so actor.status.available is false
	// (realtime, NOT the persisted readiness). It is also NOT the not_ready
	// readiness reason — it is the transport-liveness reason.
	statusReq := newTestRequest("channel:test", "agent:author", "actor.status", "req-proxy-status")
	statusReq.Audience = message.Audience{"tool:kimi"}
	lookup.Put(statusReq)
	if err := mgr.Dispatch(context.Background(), statusReq); err != nil {
		t.Fatalf("Dispatch actor.status: %v", err)
	}
	written = chain.Written()
	if len(written) != 3 {
		t.Fatalf("written=%d want describe + readiness event + status", len(written))
	}
	var status map[string]any
	if err := json.Unmarshal(written[2].Payload, &status); err != nil {
		t.Fatalf("status payload: %v", err)
	}
	if status["available"] != false || status["reason"] != "device_unreachable" {
		t.Fatalf("status=%v want available=false reason=device_unreachable (volatile liveness, not sticky readiness)", status)
	}
	detail, ok := status["detail"].(map[string]any)
	if !ok || detail["live_state"] != "unknown" {
		t.Fatalf("status detail=%T %v want live_state=unknown", status["detail"], status["detail"])
	}

	// Now a device "connected" lifecycle frame arrives. Liveness flips online ⇒
	// available=true, EVEN THOUGH the persisted readiness is still not_ready —
	// the definitive proof that available follows realtime liveness, not the
	// sticky readiness column.
	lifecyclePayload, err := devicetransit.EncodeLifecycleRuntimeEventPayload(devicetransit.LifecycleFrame{
		AdapterActorID: "tool:kimi",
		ChannelID:      "channel:test",
		Event:          devicetransit.LifecycleConnected,
		Ts:             1_700_000_002_000,
	})
	if err != nil {
		t.Fatalf("lifecycle payload: %v", err)
	}
	if err := mgr.OnRuntimeEvent(context.Background(), adapter.RuntimeEvent{
		Kind:           devicetransit.RuntimeEventKindDeviceLifecycle,
		ChannelID:      "channel:test",
		AdapterActorID: "tool:kimi",
		Payload:        lifecyclePayload,
	}); err != nil {
		t.Fatalf("OnRuntimeEvent connected: %v", err)
	}
	statusReq2 := newTestRequest("channel:test", "agent:author", "actor.status", "req-proxy-status-2")
	statusReq2.Audience = message.Audience{"tool:kimi"}
	lookup.Put(statusReq2)
	if err := mgr.Dispatch(context.Background(), statusReq2); err != nil {
		t.Fatalf("Dispatch actor.status 2: %v", err)
	}
	written = chain.Written()
	var statusLive map[string]any
	if err := json.Unmarshal(written[len(written)-1].Payload, &statusLive); err != nil {
		t.Fatalf("status-live payload: %v", err)
	}
	if statusLive["available"] != true || statusLive["reason"] != "ok" {
		t.Fatalf("status-live=%v want available=true reason=ok after connected lifecycle", statusLive)
	}
	// Registry readiness is still not_ready — liveness and readiness are
	// genuinely decoupled.
	rec, ok, err = registry.Lookup(context.Background(), "tool:kimi")
	if err != nil || !ok {
		t.Fatalf("Lookup proxy actor (2) ok=%v err=%v", ok, err)
	}
	if rec.Readiness.State != actorreg.ReadinessNotReady {
		t.Fatalf("readiness must remain not_ready (decoupled from liveness), got %+v", rec.Readiness)
	}
}

func TestManagerOnExternalCallbackFrameStampsReadinessChannel(t *testing.T) {
	decl := adapter.Declaration{
		Name:         "kimi",
		ActorID:      "tool:kimi",
		Types:        []string{"kimi.ask"},
		Binding:      actor.BindingRuntimeInboundViaRelay,
		MaxPendingMs: 30_000,
		TypeDeclarations: map[string]adapter.TypeDeclaration{
			"kimi.ask": {AllowedKinds: []message.Kind{message.KindRequest, message.KindResponse}},
		},
	}
	mod, err := proxyfacade.New(decl)
	if err != nil {
		t.Fatalf("proxyfacade.New: %v", err)
	}
	mgr, _, _, registry, _ := newTestManager(t, mod, func(cfg *ManagerConfig) {
		cfg.DeviceTransit = &recordingTransit{}
	})
	raw, err := json.Marshal(message.Envelope{
		ID:     "evt-proxy-readiness",
		Sender: message.Sender{Kind: actor.KindSystem, ID: actor.SystemActorID},
		Kind:   message.KindEvent,
		Type:   "actor.readiness.changed",
		Payload: json.RawMessage(`{
			"actor_id":"tool:kimi",
			"changed_at":1700000001000,
			"current":{
				"ready":false,
				"reason":"local_unreachable",
				"detail":{"port":10086},
				"last_state_change_at":1700000001000
			}
		}`),
	})
	if err != nil {
		t.Fatalf("marshal readiness event: %v", err)
	}
	if err := mgr.OnExternalCallbackFrame(context.Background(), decl.Name, adapter.ExternalCallbackFrame{
		ChannelID:      "channel:test",
		AdapterActorID: decl.ActorID,
		Payload:        raw,
	}); err != nil {
		t.Fatalf("OnExternalCallbackFrame readiness: %v", err)
	}
	rec, ok, err := registry.Lookup(context.Background(), "tool:kimi")
	if err != nil || !ok {
		t.Fatalf("Lookup proxy actor ok=%v err=%v", ok, err)
	}
	if rec.Readiness.State != actorreg.ReadinessNotReady || rec.Readiness.Reason != "local_unreachable" {
		t.Fatalf("readiness=%+v", rec.Readiness)
	}
}

func TestManagerDispatchNoReadinessPrecheckGate(t *testing.T) {
	// New world (construction-spec §2 / §3.3): there is NO sticky-readiness
	// dispatch gate. A request is dispatched straight to the adapter even when
	// the registry's readiness projection says NotReady — "callable" is the
	// OUTCOME of the attempt (the adapter answers, or its relay-send fails into
	// receiver_unavailable), never a pre-checked stored copy. The former
	// respondIfNotReady precheck (which short-circuited before Handle) is
	// deleted; this test pins that it stays deleted.
	called := false
	mod := &stubModule{
		decl: adapter.Declaration{
			Name:         "feishu",
			ActorID:      "tool:feishu-adapter",
			Types:        []string{"feishu.chat.send"},
			Binding:      actor.BindingRuntimeOutbound,
			MaxPendingMs: 30_000,
		},
		handle: func(context.Context, *message.Envelope, *adapter.ModuleContext) error {
			called = true
			return nil
		},
	}
	mgr, _, lookup, registry, _ := newTestManager(t, mod)
	_, err := registry.UpdateReadiness(context.Background(), "tool:feishu-adapter", actorreg.ReadinessUpdate{
		State:     actorreg.ReadinessNotReady,
		Reason:    "device_offline",
		Detail:    json.RawMessage(`{"device_state":"offline"}`),
		CheckedAt: 1_700_000_001_000,
	})
	if err != nil {
		t.Fatalf("UpdateReadiness: %v", err)
	}

	req := newTestRequest("channel:test", "agent:author", "feishu.chat.send", "req-offline")
	lookup.Put(req)
	if err := mgr.Dispatch(context.Background(), req); err != nil {
		t.Fatalf("Dispatch: %v", err)
	}
	if !called {
		t.Fatal("no readiness pre-check gate: a NotReady actor's request must still enter Module.Handle (callable is the outcome, not a stored gate)")
	}
}

func TestManagerDispatchRejectsUnknownAudience(t *testing.T) {
	mod := &stubModule{
		decl: adapter.Declaration{
			Name:         "feishu",
			ActorID:      "tool:feishu-adapter",
			Types:        []string{"feishu.chat.send"},
			Binding:      actor.BindingRuntimeOutbound,
			MaxPendingMs: 30_000,
		},
	}
	mgr, _, _, _, _ := newTestManager(t, mod)
	req := newTestRequest("channel:test", "agent:a", "feishu.chat.send", "r1")
	req.Audience = message.Audience{"tool:nope"}
	err := mgr.Dispatch(context.Background(), req)
	if err == nil {
		t.Fatalf("expected error for unknown audience")
	}
}

func TestManagerDispatchRejectsUnknownType(t *testing.T) {
	mod := &stubModule{
		decl: adapter.Declaration{
			Name:         "feishu",
			ActorID:      "tool:feishu-adapter",
			Types:        []string{"feishu.chat.send"},
			Binding:      actor.BindingRuntimeOutbound,
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
			ActorID:      "tool:feishu-adapter",
			Types:        []string{"feishu.chat.send"},
			Binding:      actor.BindingRuntimeOutbound,
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

func TestManagerRespondCancelsTimer(t *testing.T) {
	var responded atomic.Bool
	mod := &stubModule{
		decl: adapter.Declaration{
			Name:         "feishu",
			ActorID:      "tool:feishu-adapter",
			Types:        []string{"feishu.chat.send"},
			Binding:      actor.BindingRuntimeOutbound,
			MaxPendingMs: 80,
		},
		handle: func(ctx context.Context, env *message.Envelope, mctx *adapter.ModuleContext) error {
			_, err := mctx.Respond(ctx, adapter.CorrelationKey(env.ID),
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
			ActorID:      "tool:feishu-adapter",
			Types:        []string{"feishu.chat.send"},
			Binding:      actor.BindingRuntimeOutbound,
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

func TestCallbackCorrelationIDUsesResponseParentID(t *testing.T) {
	raw, err := json.Marshal(message.Envelope{
		ID:            "resp-1",
		Kind:          message.KindResponse,
		ParentID:      "req-1",
		CorrelationID: "trace-1",
	})
	if err != nil {
		t.Fatalf("marshal response envelope: %v", err)
	}
	if got := callbackCorrelationID(raw); got != "req-1" {
		t.Fatalf("callbackCorrelationID(response envelope)=%q want parent_id", got)
	}
	if got := callbackCorrelationID([]byte(`{"correlation_id":"legacy-corr","request_id":"legacy-req"}`)); got != "legacy-corr" {
		t.Fatalf("callbackCorrelationID(legacy)=%q want legacy-corr", got)
	}
}

func TestManagerOnExternalCallbackFrameRejectsPayloadIdentityMismatch(t *testing.T) {
	routed := atomic.Bool{}
	mod := &stubModule{
		decl: adapter.Declaration{
			Name:         "xhs",
			ActorID:      "tool:xhs",
			Types:        []string{"xhs.search"},
			Binding:      actor.BindingRuntimeInboundViaRelay,
			MaxPendingMs: 30_000,
			TypeDeclarations: map[string]adapter.TypeDeclaration{
				"xhs.search": {AllowedKinds: []message.Kind{message.KindRequest, message.KindResponse}},
			},
		},
		onFrame: func(context.Context, adapter.ExternalCallbackFrame, *adapter.ModuleContext) error {
			routed.Store(true)
			return nil
		},
	}
	mgr, _, _, _, _ := newTestManager(t, mod, func(cfg *ManagerConfig) {
		cfg.DeviceTransit = &recordingTransit{}
	})
	err := mgr.OnExternalCallbackFrame(context.Background(), mod.decl.Name, adapter.ExternalCallbackFrame{
		ChannelID:      "channel:test",
		AdapterActorID: mod.decl.ActorID,
		RequestID:      "req-good",
		CorrelationID:  "req-good",
		Payload:        json.RawMessage(`{"correlation_id":"req-evil","status":"ok"}`),
	})
	if err == nil || !strings.Contains(err.Error(), "payload correlation") {
		t.Fatalf("expected payload identity mismatch, got %v", err)
	}
	if routed.Load() {
		t.Fatal("mismatched callback must not route into module")
	}
}

func TestManagerOnExternalCallbackEmitsOrphanEvents(t *testing.T) {
	var routed atomic.Bool
	mod := &stubModule{
		decl: adapter.Declaration{
			Name:         "feishu",
			ActorID:      "tool:feishu-adapter",
			Types:        []string{"feishu.chat.send"},
			Binding:      actor.BindingRuntimeOutbound,
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
	if written[0].Type != orphanCallbackType("feishu") || written[0].Kind != message.KindEvent {
		t.Fatalf("first event type/kind=%s/%s", written[0].Type, written[0].Kind)
	}
	if written[1].Type != "core.system_event" || written[1].Kind != message.KindEvent {
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
			ActorID:      "tool:feishu-adapter",
			Types:        []string{"feishu.chat.send"},
			Binding:      actor.BindingRuntimeOutbound,
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
			ActorID:      "tool:feishu-adapter",
			Types:        []string{"feishu.chat.send"},
			Binding:      actor.BindingRuntimeOutbound,
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
			ActorID:      "tool:feishu-adapter",
			Types:        []string{"feishu.chat.send"},
			Binding:      actor.BindingRuntimeOutbound,
			MaxPendingMs: 30_000,
		},
	}
	var dedupedResult adapter.RespondResult
	mod.handle = func(ctx context.Context, env *message.Envelope, mctx *adapter.ModuleContext) error {
		res, err := mctx.Respond(ctx, adapter.CorrelationKey(env.ID),
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

func TestManagerTerminalDuplicateCompletesLifecycle(t *testing.T) {
	mod := &stubModule{
		decl: adapter.Declaration{
			Name:         "feishu",
			ActorID:      "tool:feishu-adapter",
			Types:        []string{"feishu.chat.send"},
			Binding:      actor.BindingRuntimeOutbound,
			MaxPendingMs: 30_000,
		},
	}
	mod.handle = func(ctx context.Context, env *message.Envelope, mctx *adapter.ModuleContext) error {
		_, err := mctx.Respond(ctx, adapter.CorrelationKey(env.ID), json.RawMessage(`{}`), adapter.RespondOptions{Status: "completed"})
		return err
	}
	mgr, chain, lookup, _, _ := newTestManager(t, mod)
	defer func() { _ = mgr.Shutdown(context.Background()) }()
	chain.results = []harness.WriteResult{{
		RejectReason:     message.HarnessTerminalDuplicate,
		PartialMessageID: "response:existing",
	}}

	req := newTestRequest("channel:test", "agent:a", "feishu.chat.send", "req-dup-lifecycle")
	lookup.Put(req)
	if err := mgr.Dispatch(context.Background(), req); err != nil {
		t.Fatalf("Dispatch: %v", err)
	}
	bm := mgr.byName["feishu"]
	entry, ok, err := bm.correlation.Get(context.Background(), "req-dup-lifecycle")
	if err != nil || !ok {
		t.Fatalf("correlation ok=%v err=%v", ok, err)
	}
	if entry.State != adapter.CorrelationDone {
		t.Fatalf("correlation state=%s want done", entry.State)
	}
	bm.policy.mu.Lock()
	_, timerStillArmed := bm.policy.timers["req-dup-lifecycle"]
	bm.policy.mu.Unlock()
	if timerStillArmed {
		t.Fatal("terminal_duplicate must cancel F3 timer")
	}
}

func TestProxyFacadeCallbackCompletesAndCancelsTimer(t *testing.T) {
	decl, err := proxyfacade.DeclarationFromCapability("tool:kimi", json.RawMessage(`{
		"name":"kimi",
		"types":["kimi.ask"],
		"type_declarations":{"kimi.ask":{"AllowedKinds":["request","response"]}},
		"max_pending_ms":30000
	}`))
	if err != nil {
		t.Fatalf("DeclarationFromCapability: %v", err)
	}
	mod, err := proxyfacade.New(decl)
	if err != nil {
		t.Fatalf("proxyfacade.New: %v", err)
	}
	mgr, chain, lookup, _, _ := newTestManager(t, mod, func(cfg *ManagerConfig) {
		cfg.DeviceTransit = &recordingTransit{}
	})
	defer func() { _ = mgr.Shutdown(context.Background()) }()

	req := newTestRequest("channel:test", "agent:a", "kimi.ask", "req-proxy-ok")
	req.Audience = message.Audience{"tool:kimi"}
	req.CorrelationID = "corr-proxy-ok"
	lookup.Put(req)
	if err := mgr.Dispatch(context.Background(), req); err != nil {
		t.Fatalf("Dispatch: %v", err)
	}
	resp := message.Envelope{
		ID:            "resp-proxy-ok",
		ChannelID:     "channel:test",
		Sender:        message.Sender{Kind: actor.KindTool, ID: "tool:kimi"},
		Kind:          message.KindResponse,
		Type:          "kimi.ask",
		ParentID:      req.ID,
		CorrelationID: req.CorrelationID,
		Payload:       json.RawMessage(`{"status":"completed","answer":"ok"}`),
		Audience:      message.Audience{"agent:a"},
	}
	raw, _ := json.Marshal(resp)
	if err := mgr.OnExternalCallback(context.Background(), decl.Name, raw); err != nil {
		t.Fatalf("OnExternalCallback: %v", err)
	}
	// The framework sync/async refactor routes the final callback through
	// ctx.Resolve, which constructs the canonical final envelope itself
	// (sender = adapter actor, derived id, parent = request id). The
	// receiver-supplied envelope id is intentionally discarded — only the
	// status / payload / reason flow through. Assert exactly one terminal
	// was written for this request.
	if written := chain.Written(); len(written) != 1 || written[0].ParentID != req.ID || written[0].Sender.ID != "tool:kimi" {
		t.Fatalf("written=%v want single proxy response (parent=%s sender=tool:kimi)", written, req.ID)
	}
	bm := mgr.byName[decl.Name]
	entry, ok, err := bm.correlation.Get(context.Background(), "req-proxy-ok")
	if err != nil || !ok {
		t.Fatalf("correlation ok=%v err=%v", ok, err)
	}
	if entry.State != adapter.CorrelationDone {
		t.Fatalf("correlation state=%s want done", entry.State)
	}
	bm.policy.mu.Lock()
	_, timerStillArmed := bm.policy.timers["req-proxy-ok"]
	bm.policy.mu.Unlock()
	if timerStillArmed {
		t.Fatal("proxy callback completion must cancel F3 timer")
	}
}

// TestProxyFacadeForgedCallbackSenderRejected verifies the F6 consistency
// check: a final callback whose claimed sender does not match the adapter
// actor that owns the pending request is REJECTED at the proxy_facade
// boundary (the lightweight channel/sender/correlation checks the old
// CompleteExternalResponse enforced, restored after the Resolve refactor
// dropped them). A forged foreign sender (tool:xhs on tool:kimi's request)
// is refused and the pending request is left unresolved — not silently
// re-signed.
func TestProxyFacadeForgedCallbackSenderRejected(t *testing.T) {
	decl, err := proxyfacade.DeclarationFromCapability("tool:kimi", json.RawMessage(`{
		"name":"kimi",
		"types":["kimi.ask"],
		"type_declarations":{"kimi.ask":{"AllowedKinds":["request","response"]}},
		"max_pending_ms":30000
	}`))
	if err != nil {
		t.Fatalf("DeclarationFromCapability: %v", err)
	}
	mod, err := proxyfacade.New(decl)
	if err != nil {
		t.Fatalf("proxyfacade.New: %v", err)
	}
	mgr, chain, lookup, _, _ := newTestManager(t, mod, func(cfg *ManagerConfig) {
		cfg.DeviceTransit = &recordingTransit{}
	})
	defer func() { _ = mgr.Shutdown(context.Background()) }()

	req := newTestRequest("channel:test", "agent:a", "kimi.ask", "req-proxy-forged")
	req.Audience = message.Audience{"tool:kimi"}
	lookup.Put(req)
	if err := mgr.Dispatch(context.Background(), req); err != nil {
		t.Fatalf("Dispatch: %v", err)
	}
	forged := message.Envelope{
		ID:        "resp-proxy-forged",
		ChannelID: "channel:test",
		Sender:    message.Sender{Kind: actor.KindTool, ID: "tool:xhs"},
		Kind:      message.KindResponse,
		Type:      "kimi.ask",
		ParentID:  req.ID,
		Payload:   json.RawMessage(`{"status":"completed"}`),
		Audience:  message.Audience{"agent:a"},
	}
	raw, _ := json.Marshal(forged)
	// F6: a final callback whose claimed sender does not match the adapter
	// actor is REJECTED at the proxy_facade boundary. It must NOT resolve the
	// pending request — the request stays pending until its genuine final / F3.
	if err := mgr.OnExternalCallback(context.Background(), decl.Name, raw); err == nil {
		t.Fatal("OnExternalCallback: forged-sender final callback must be rejected, got nil error")
	}
	if written := chain.Written(); len(written) != 0 {
		t.Fatalf("forged callback must not write a terminal, got %d writes", len(written))
	}
	bm := mgr.byName[decl.Name]
	entry, ok, err := bm.correlation.Get(context.Background(), "req-proxy-forged")
	if err != nil || !ok {
		t.Fatalf("correlation ok=%v err=%v", ok, err)
	}
	if entry.State != adapter.CorrelationPending {
		t.Fatalf("correlation state=%s want pending (forged callback rejected, request unresolved)", entry.State)
	}
}

// TestResolveRejectsUnownedRequest locks the receiver-side authority
// boundary that survived the CompleteExternalResponse → Resolve migration.
// Resolve constructs the canonical final envelope itself (sender / audience
// / correlation / type are derived from the pending request, not supplied
// by the receiver), so the only forgery vector left is the request id: an
// id that does not name a pending request this adapter owns must be
// rejected before any chain write, leaving the real pending request intact.
func TestResolveRejectsUnownedRequest(t *testing.T) {
	cases := []struct {
		name      string
		resolveID func(reqID message.ID) message.ID
	}{
		{
			name:      "unknown_request",
			resolveID: func(message.ID) message.ID { return "req-missing" },
		},
		{
			name:      "empty_request",
			resolveID: func(message.ID) message.ID { return "" },
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			decl, err := proxyfacade.DeclarationFromCapability("tool:kimi", json.RawMessage(`{
				"name":"kimi",
				"types":["kimi.ask"],
				"type_declarations":{"kimi.ask":{"AllowedKinds":["request","response"]}},
				"max_pending_ms":30000
			}`))
			if err != nil {
				t.Fatalf("DeclarationFromCapability: %v", err)
			}
			mod, err := proxyfacade.New(decl)
			if err != nil {
				t.Fatalf("proxyfacade.New: %v", err)
			}
			mgr, chain, lookup, _, _ := newTestManager(t, mod, func(cfg *ManagerConfig) {
				cfg.DeviceTransit = &recordingTransit{}
			})
			defer func() { _ = mgr.Shutdown(context.Background()) }()

			req := newTestRequest("channel:test", "agent:a", "kimi.ask", "req-proxy-"+tc.name)
			req.Audience = message.Audience{"tool:kimi"}
			req.CorrelationID = message.ID("corr-proxy-" + tc.name)
			lookup.Put(req)
			if err := mgr.Dispatch(context.Background(), req); err != nil {
				t.Fatalf("Dispatch: %v", err)
			}

			bm := mgr.byName[decl.Name]
			err = bm.mctx.Resolve(context.Background(), tc.resolveID(req.ID), adapter.ResolveRequest{
				Status:  "completed",
				Payload: json.RawMessage(`{}`),
			})
			if err == nil {
				t.Fatalf("%s: Resolve on un-owned request id should be rejected", tc.name)
			}
			if written := chain.Written(); len(written) != 0 {
				t.Fatalf("%s rejected Resolve must not write, got %d writes", tc.name, len(written))
			}
			// The real pending request must remain untouched.
			entry, ok, err := bm.correlation.Get(context.Background(), adapter.CorrelationKey(req.ID))
			if err != nil || !ok {
				t.Fatalf("correlation ok=%v err=%v", ok, err)
			}
			if entry.State != adapter.CorrelationPending {
				t.Fatalf("correlation state=%s want pending", entry.State)
			}
		})
	}
}

func TestForwardExternalRequestRejectsNonPendingRequest(t *testing.T) {
	mod := &stubModule{
		decl: adapter.Declaration{
			Name:         "xhs",
			ActorID:      "tool:xhs",
			Types:        []string{"xhs.search"},
			Binding:      actor.BindingRuntimeInboundViaRelay,
			MaxPendingMs: 30_000,
			TypeDeclarations: map[string]adapter.TypeDeclaration{
				"xhs.search": {AllowedKinds: []message.Kind{message.KindRequest, message.KindResponse}},
			},
		},
	}
	transit := &recordingTransit{}
	_, _, lookup, _, _ := newTestManager(t, mod, func(cfg *ManagerConfig) {
		cfg.DeviceTransit = transit
	})
	exp := int64(1_700_000_030_000)
	req := newTestRequest("channel:test", "agent:a", "xhs.search", "req-not-pending")
	req.Audience = message.Audience{"tool:xhs"}
	req.ExpiresAt = &exp
	lookup.Put(req)

	if _, err := mod.mctx.ForwardExternalRequest(context.Background(), req, adapter.ExternalRequestPayload(`{}`)); err == nil {
		t.Fatal("ForwardExternalRequest must reject request without pending correlation")
	}
	transit.mu.Lock()
	sent := len(transit.sent)
	transit.mu.Unlock()
	if sent != 0 {
		t.Fatalf("ForwardExternalRequest sent %d frames for non-pending request", sent)
	}
}

func TestForwardExternalRequestStampsTransitBodyFromRequest(t *testing.T) {
	mod := &stubModule{
		decl: adapter.Declaration{
			Name:         "xhs",
			ActorID:      "tool:xhs",
			Types:        []string{"xhs.search"},
			Binding:      actor.BindingRuntimeInboundViaRelay,
			MaxPendingMs: 30_000,
			TypeDeclarations: map[string]adapter.TypeDeclaration{
				"xhs.search": {AllowedKinds: []message.Kind{message.KindRequest, message.KindResponse}},
			},
		},
		handle: func(ctx context.Context, env *message.Envelope, mctx *adapter.ModuleContext) error {
			_, err := mctx.ForwardExternalRequest(ctx, env, adapter.ExternalRequestPayload(`{"request_id":"evil","correlation_id":"evil","expires_at":1}`))
			return err
		},
	}
	transit := &recordingTransit{}
	mgr, _, lookup, _, _ := newTestManager(t, mod, func(cfg *ManagerConfig) {
		cfg.DeviceTransit = transit
	})
	exp := int64(1_700_000_030_000)
	req := newTestRequest("channel:test", "agent:a", "xhs.search", "req-forward")
	req.Audience = message.Audience{"tool:xhs"}
	req.ParentID = "parent-1"
	req.CorrelationID = "corr-forward"
	req.ExpiresAt = &exp
	lookup.Put(req)

	if err := mgr.Dispatch(context.Background(), req); err != nil {
		t.Fatalf("Dispatch: %v", err)
	}
	transit.mu.Lock()
	defer transit.mu.Unlock()
	if len(transit.sent) != 1 {
		t.Fatalf("sent=%d want 1", len(transit.sent))
	}
	var body externalRequestTransitBody
	if err := json.Unmarshal(transit.sent[0].Body, &body); err != nil {
		t.Fatalf("decode transit body: %v", err)
	}
	if body.Direction != "to_device" || body.RequestID != req.ID || body.ParentID != req.ParentID ||
		body.CorrelationID != req.CorrelationID || body.ExpiresAt != exp {
		t.Fatalf("framework-stamped body=%+v request=%+v", body, req)
	}
	if string(body.Payload) != `{"request_id":"evil","correlation_id":"evil","expires_at":1}` {
		t.Fatalf("payload=%s", body.Payload)
	}
}

func TestManagerHandlePanicEmitsReceiverInternalError(t *testing.T) {
	mod := &stubModule{
		decl: adapter.Declaration{
			Name:         "feishu",
			ActorID:      "tool:feishu-adapter",
			Types:        []string{"feishu.chat.send"},
			Binding:      actor.BindingRuntimeOutbound,
			MaxPendingMs: 30_000,
		},
		handle: func(context.Context, *message.Envelope, *adapter.ModuleContext) error {
			panic("boom")
		},
	}
	mgr, chain, lookup, _, _ := newTestManager(t, mod)
	defer func() { _ = mgr.Shutdown(context.Background()) }()

	req := newTestRequest("channel:test", "agent:a", "feishu.chat.send", "req-panic")
	lookup.Put(req)
	if err := mgr.Dispatch(context.Background(), req); err == nil {
		t.Fatal("Dispatch should surface adapter panic")
	}

	written := chain.Written()
	if len(written) != 1 {
		t.Fatalf("written=%d want 1 failed terminal", len(written))
	}
	if written[0].Sender.ID != "tool:feishu-adapter" || written[0].Sender.Kind != actor.KindTool {
		t.Fatalf("panic fallback sender=(%s,%s) want adapter actor (tool,tool:feishu-adapter)", written[0].Sender.Kind, written[0].Sender.ID)
	}
	var payload map[string]any
	if err := json.Unmarshal(written[0].Payload, &payload); err != nil {
		t.Fatalf("unmarshal payload: %v", err)
	}
	if payload["reason"] != string(message.TerminalReceiverInternalError) {
		t.Fatalf("payload.reason=%v want %s", payload["reason"], message.TerminalReceiverInternalError)
	}
}

func TestManagerExternalCallbackPanicEmitsReceiverInternalError(t *testing.T) {
	mod := &stubModule{
		decl: adapter.Declaration{
			Name:         "feishu",
			ActorID:      "tool:feishu-adapter",
			Types:        []string{"feishu.chat.send"},
			Binding:      actor.BindingRuntimeOutbound,
			MaxPendingMs: 30_000,
		},
		onCallback: func(context.Context, []byte, *adapter.ModuleContext) error {
			panic("callback boom")
		},
	}
	mgr, chain, lookup, _, _ := newTestManager(t, mod)
	defer func() { _ = mgr.Shutdown(context.Background()) }()

	req := newTestRequest("channel:test", "agent:a", "feishu.chat.send", "req-callback-panic")
	lookup.Put(req)
	if err := mgr.Dispatch(context.Background(), req); err != nil {
		t.Fatalf("Dispatch: %v", err)
	}
	err := mgr.OnExternalCallback(context.Background(), "feishu", []byte(`{"correlation_id":"req-callback-panic"}`))
	if err == nil {
		t.Fatal("OnExternalCallback should surface adapter callback panic")
	}

	written := chain.Written()
	if len(written) != 1 {
		t.Fatalf("written=%d want 1 failed terminal", len(written))
	}
	var payload map[string]any
	if err := json.Unmarshal(written[0].Payload, &payload); err != nil {
		t.Fatalf("unmarshal payload: %v", err)
	}
	if payload["reason"] != string(message.TerminalReceiverInternalError) {
		t.Fatalf("payload.reason=%v want %s", payload["reason"], message.TerminalReceiverInternalError)
	}
}

// TestManagerInstallRejectsStrictModeGap — when an adapter declares
// TypeDeclarations (opting into strict mode) but a Types entry is
// missing from the map, install MUST fail-closed with
// InstallTypeRegistryInvalid rather than silently fall back to the
// permissive default ({event, request, response}).
//
// Rationale: a partially-declared TypeDeclarations map is a drift
// signal — the adapter wanted strict per-type allowed_kinds but forgot
// a row. The missing row would default to "all three kinds allowed"
// which can admit spec-disallowed kinds (e.g. kind=event on xhs.publish).
//
// Level A (proto-layer0 §1.4.1 / proto-layer1 §1.3): payload schema is
// NOT declared at the protocol layer; TypeDeclaration carries only
// allowed_kinds.
func TestManagerInstallRejectsStrictModeGap(t *testing.T) {
	registry := newMemoryActorRegistry()
	_ = registry.Insert(context.Background(), actorreg.Record{
		ID:      "tool:feishu-adapter",
		Kind:    actor.KindTool,
		Binding: actor.BindingRuntimeOutbound,
	})
	mod := &stubModule{
		decl: adapter.Declaration{
			Name:         "feishu",
			ActorID:      "tool:feishu-adapter",
			Types:        []string{"feishu.chat.send", "feishu.chat.create"},
			Binding:      actor.BindingRuntimeOutbound,
			MaxPendingMs: 30_000,
			// Strict mode opt-in: TypeDeclarations non-nil, but missing
			// the "feishu.chat.create" row.
			TypeDeclarations: map[string]adapter.TypeDeclaration{
				"feishu.chat.send": {
					AllowedKinds: []message.Kind{message.KindRequest, message.KindResponse},
				},
			},
		},
	}
	mgr, err := NewManager(ManagerConfig{
		ChannelID:     "channel:test",
		ActorRegistry: registry,
		HarnessChain:  newFakeChain(),
		RequestLookup: NewMemoryRequestLookup(nil),
		Clock:         time.Now,
	})
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	err = mgr.Install(context.Background(), []adapter.Module{mod})
	if err == nil {
		t.Fatal("expected install to fail-closed on strict-mode gap")
	}
	var ie *InstallError
	if !errors.As(err, &ie) {
		t.Fatalf("expected *InstallError, got %T: %v", err, err)
	}
	if ie.Reason != message.InstallTypeRegistryInvalid {
		t.Errorf("reason=%s want %s", ie.Reason, message.InstallTypeRegistryInvalid)
	}
}

// TestManagerInstallStrictModeAcceptsCompleteDeclarations — the
// positive case: adapter declares TypeDeclarations with every Types
// entry covered → install succeeds.
func TestManagerInstallStrictModeAcceptsCompleteDeclarations(t *testing.T) {
	registry := newMemoryActorRegistry()
	_ = registry.Insert(context.Background(), actorreg.Record{
		ID:      "tool:feishu-adapter",
		Kind:    actor.KindTool,
		Binding: actor.BindingRuntimeOutbound,
	})
	mod := &stubModule{
		decl: adapter.Declaration{
			Name:         "feishu",
			ActorID:      "tool:feishu-adapter",
			Types:        []string{"feishu.chat.send", "feishu.chat.create"},
			Binding:      actor.BindingRuntimeOutbound,
			MaxPendingMs: 30_000,
			TypeDeclarations: map[string]adapter.TypeDeclaration{
				"feishu.chat.send": {
					AllowedKinds: []message.Kind{message.KindRequest, message.KindResponse},
				},
				"feishu.chat.create": {
					AllowedKinds: []message.Kind{message.KindRequest, message.KindResponse},
				},
			},
		},
	}
	mgr, err := NewManager(ManagerConfig{
		ChannelID:     "channel:test",
		ActorRegistry: registry,
		HarnessChain:  newFakeChain(),
		RequestLookup: NewMemoryRequestLookup(nil),
		Clock:         time.Now,
	})
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	if err := mgr.Install(context.Background(), []adapter.Module{mod}); err != nil {
		t.Fatalf("Install (complete declarations): %v", err)
	}
}

// TestF4ReceiverOwnerRegisteredBeforeTimerArm is the F4 regression: the router
// receiverOwner index must be written BEFORE the F3 timer is armed. With a
// deadline that is already in the past, RegisterTimer fires the fallback
// (near-)immediately; the fallback final's ObserveResponse must be able to
// locate the receiverOwner to close lifecycle (MarkDone + CancelTimer) and drop
// the owner entry. If the owner were recorded only AFTER the timer arm (the
// bug), the fallback could fire first, find no owner, and leak corr + the owner
// index. Here the fallback write SUCCEEDS, so we assert: correlation Done +
// receiverOwner cleared.
// TestF5ReceiverOwnerCleanedWhenFallbackWriteFails is the F5 regression: when
// the F3 fallback exhausts its write retries (permanent write failure), the
// final is NEVER written, so the router's ObserveResponse never runs and would
// never drop the receiverOwner entry. The bindOnExpire hook must clean it up
// directly so the reqID does not leak.
// TestCallbackAfterReArmWithinLiveDeadlineAccepted is the N4:F3 temporal R1
// extension: a callback that arrives AFTER the original request expires_at but
// BEFORE the heartbeat-extended (re-armed) live deadline must be ACCEPTED, not
// force-rejected as expired. frame.ExpiresAt is the original immutable anchor
// the device echoes verbatim (it never learns about re-arm), so it must still
// strictly equal entry.ExpiresAt (identity check) — but the temporal liveness
// gate must judge against max(ExpiresAt, RearmedExpiresAt). Before the fix the
// gate compared now > frame.ExpiresAt and permanently rejected every late
// callback of a still-live, heartbeat-extended receiver.
// TestCallbackAfterLiveDeadlineRejected is the negative companion: once wall
// clock passes the re-armed live deadline too, a late callback is correctly
// rejected as expired — the fix widens the acceptance window to the live
// deadline, it does not disable expiry.
