package proxy_facade

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/wanpengxie/ActOS/kernel/actor"
	"github.com/wanpengxie/ActOS/kernel/actorreg"
	"github.com/wanpengxie/ActOS/kernel/adapter"
	"github.com/wanpengxie/ActOS/kernel/channel"
	"github.com/wanpengxie/ActOS/kernel/devicetransit"
	"github.com/wanpengxie/ActOS/kernel/message"
)

type captureExternal struct {
	channelID  channel.ID
	actorID    actor.ActorID
	payload    adapter.ExternalRequestPayload
	resolved   bool
	resolveID  adapter.RequestID
	resolveReq adapter.ResolveRequest
}

func TestProxyFacadeHandleSendsEnvelopeViaDeviceTransit(t *testing.T) {
	decl, err := DeclarationFromCapability("tool:kimi", json.RawMessage(`{"types":["kimi.ask"],"max_pending_ms":12000}`))
	if err != nil {
		t.Fatalf("DeclarationFromCapability: %v", err)
	}
	if decl.Binding != actor.BindingRuntimeInboundViaRelay || decl.MaxPendingMs != 12000 || len(decl.Types) != 1 {
		t.Fatalf("decl=%+v", decl)
	}
	mod, err := New(decl)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	capture := &captureExternal{}
	if err := mod.Init(context.Background(), &adapter.ModuleContext{
		AdapterName:    decl.Name,
		AdapterActorID: decl.ActorID,
		ChannelID:      "ch-1",
		Resolve: func(context.Context, adapter.RequestID, adapter.ResolveRequest) error {
			return nil
		},
		Provisional: func(context.Context, adapter.CorrelationKey, string, json.RawMessage, adapter.ProvisionalOptions) (adapter.RespondResult, error) {
			return adapter.RespondResult{}, nil
		},
		UpdateReadiness: func(context.Context, actorreg.ReadinessUpdate) (actorreg.ReadinessTransition, error) {
			return actorreg.ReadinessTransition{}, nil
		},
		ForwardExternalRequest: func(_ context.Context, env *message.Envelope, payload adapter.ExternalRequestPayload) (adapter.ExternalRequestResult, error) {
			capture.channelID = env.ChannelID
			capture.actorID = decl.ActorID
			capture.payload = append(adapter.ExternalRequestPayload(nil), payload...)
			return adapter.ExternalRequestResult{FrameID: "frame-1"}, nil
		},
	}); err != nil {
		t.Fatalf("Init: %v", err)
	}

	env := &message.Envelope{
		ID:        "req-1",
		ChannelID: "ch-1",
		Sender:    message.Sender{Kind: actor.KindAgent, ID: "agent:author"},
		Kind:      message.KindRequest,
		Type:      "kimi.ask",
		Payload:   json.RawMessage(`{"prompt":"hi"}`),
		Audience:  message.Audience{"tool:kimi"},
	}
	if err := mod.Handle(context.Background(), env); err != nil {
		t.Fatalf("Handle: %v", err)
	}
	if capture.actorID != "tool:kimi" || capture.channelID != channel.ID("ch-1") {
		t.Fatalf("forward actor/channel=%s/%s", capture.actorID, capture.channelID)
	}
	var got message.Envelope
	if err := json.Unmarshal(capture.payload, &got); err != nil {
		t.Fatalf("payload envelope: %v", err)
	}
	if got.ID != env.ID || got.Type != env.Type || got.Audience[0] != "tool:kimi" {
		t.Fatalf("payload=%+v", got)
	}
}

func TestProxyFacadeCallbackWritesEnvelope(t *testing.T) {
	mod, err := New(adapter.Declaration{
		Name:         "kimi",
		ActorID:      "tool:kimi",
		Types:        []string{"kimi.ask"},
		Binding:      actor.BindingRuntimeInboundViaRelay,
		MaxPendingMs: 30_000,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	capture := &captureExternal{}
	if err := mod.Init(context.Background(), &adapter.ModuleContext{
		AdapterName:    "kimi",
		AdapterActorID: "tool:kimi",
		ChannelID:      "ch-1",
		ForwardExternalRequest: func(context.Context, *message.Envelope, adapter.ExternalRequestPayload) (adapter.ExternalRequestResult, error) {
			return adapter.ExternalRequestResult{}, nil
		},
		Provisional: func(context.Context, adapter.CorrelationKey, string, json.RawMessage, adapter.ProvisionalOptions) (adapter.RespondResult, error) {
			return adapter.RespondResult{}, nil
		},
		UpdateReadiness: func(context.Context, actorreg.ReadinessUpdate) (actorreg.ReadinessTransition, error) {
			return actorreg.ReadinessTransition{}, nil
		},
		Resolve: func(_ context.Context, id adapter.RequestID, r adapter.ResolveRequest) error {
			capture.resolved = true
			capture.resolveID = id
			capture.resolveReq = r
			return nil
		},
	}); err != nil {
		t.Fatalf("Init: %v", err)
	}
	raw, _ := json.Marshal(message.Envelope{
		ID:            "resp-1",
		ChannelID:     "ch-1",
		Sender:        message.Sender{Kind: actor.KindTool, ID: "tool:kimi"},
		Kind:          message.KindResponse,
		Type:          "kimi.ask",
		ParentID:      "req-1",
		CorrelationID: "req-1",
		Payload:       json.RawMessage(`{"status":"completed","echo":"pong"}`),
		Audience:      message.Audience{"agent:author"},
	})
	if err := mod.OnExternalCallback(context.Background(), raw); err != nil {
		t.Fatalf("OnExternalCallback: %v", err)
	}
	// The framework sync/async refactor routes finals through ctx.Resolve.
	// The request envelope id (callback parent_id) is the wait anchor; the
	// typed status carries the terminal status and the stripped payload
	// retains user fields (status removed, re-injected by the framework).
	if !capture.resolved {
		t.Fatalf("final callback must route through ctx.Resolve")
	}
	if capture.resolveID != "req-1" {
		t.Fatalf("resolve id=%s want req-1", capture.resolveID)
	}
	if capture.resolveReq.Status != "completed" {
		t.Fatalf("resolve status=%s want completed", capture.resolveReq.Status)
	}
	var body map[string]any
	if err := json.Unmarshal(capture.resolveReq.Payload, &body); err != nil {
		t.Fatalf("decode resolve payload: %v raw=%s", err, string(capture.resolveReq.Payload))
	}
	if _, hasStatus := body["status"]; hasStatus {
		t.Fatalf("resolve payload must not carry status; got %v", body)
	}
	if body["echo"] != "pong" {
		t.Fatalf("resolve payload missing user fields; got %v", body)
	}
}

func TestProxyFacadeReadinessRejectsForgedAuthority(t *testing.T) {
	mod, err := New(adapter.Declaration{
		Name:         "kimi",
		ActorID:      "tool:kimi",
		Types:        []string{"kimi.ask"},
		Binding:      actor.BindingRuntimeInboundViaRelay,
		MaxPendingMs: 30_000,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	var updates int
	if err := mod.Init(context.Background(), &adapter.ModuleContext{
		AdapterName:    "kimi",
		AdapterActorID: "tool:kimi",
		ChannelID:      "ch-1",
		ForwardExternalRequest: func(context.Context, *message.Envelope, adapter.ExternalRequestPayload) (adapter.ExternalRequestResult, error) {
			return adapter.ExternalRequestResult{}, nil
		},
		Resolve: func(context.Context, adapter.RequestID, adapter.ResolveRequest) error {
			return nil
		},
		Provisional: func(context.Context, adapter.CorrelationKey, string, json.RawMessage, adapter.ProvisionalOptions) (adapter.RespondResult, error) {
			return adapter.RespondResult{}, nil
		},
		UpdateReadiness: func(context.Context, actorreg.ReadinessUpdate) (actorreg.ReadinessTransition, error) {
			updates++
			return actorreg.ReadinessTransition{}, nil
		},
	}); err != nil {
		t.Fatalf("Init: %v", err)
	}

	base := func() *message.Envelope {
		return &message.Envelope{
			ID:        "evt-ready",
			ChannelID: "ch-1",
			Sender:    message.Sender{Kind: actor.KindSystem, ID: actor.SystemActorID},
			Kind:      message.KindEvent,
			Type:      "actor.readiness.changed",
			Payload: json.RawMessage(`{
				"actor_id":"tool:kimi",
				"changed_at":1700000001000,
				"current":{"ready":true,"reason":"ok","detail":{},"last_state_change_at":1700000001000}
			}`),
		}
	}
	cases := []struct {
		name   string
		mutate func(*message.Envelope)
	}{
		{
			name: "wrong_sender",
			mutate: func(env *message.Envelope) {
				env.Sender = message.Sender{Kind: actor.KindTool, ID: "tool:kimi"}
			},
		},
		{
			name: "wrong_type",
			mutate: func(env *message.Envelope) {
				env.Type = "kimi.ask"
			},
		},
		{
			name: "empty_channel",
			mutate: func(env *message.Envelope) {
				env.ChannelID = ""
			},
		},
		{
			name: "wrong_channel",
			mutate: func(env *message.Envelope) {
				env.ChannelID = "ch-other"
			},
		},
		{
			name: "missing_actor",
			mutate: func(env *message.Envelope) {
				env.Payload = json.RawMessage(`{
					"changed_at":1700000001000,
					"current":{"ready":true,"reason":"ok","detail":{},"last_state_change_at":1700000001000}
				}`)
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			before := updates
			env := base()
			tc.mutate(env)
			if err := mod.updateReadinessFromEvent(context.Background(), env); err == nil {
				t.Fatalf("%s readiness event should be rejected", tc.name)
			}
			if updates != before {
				t.Fatalf("%s must not update readiness, updates before=%d after=%d", tc.name, before, updates)
			}
		})
	}
}

// captureCalls counts invocations on the Provisional / Resolve paths so
// tests can assert proxy_facade routes provisional vs final correctly.
type captureCalls struct {
	provisional []provisionalCall
	finals      []resolveCall
	pending     map[adapter.CorrelationKey]adapter.CorrelationEntry
}

type resolveCall struct {
	id  adapter.RequestID
	req adapter.ResolveRequest
}

type provisionalCall struct {
	requestID adapter.CorrelationKey
	status    string
	payload   json.RawMessage
	opts      adapter.ProvisionalOptions
}

func newModuleWithCapture(t *testing.T) (*ProxyFacadeModule, *captureCalls) {
	t.Helper()
	mod, err := New(adapter.Declaration{
		Name:         "proxy-echo",
		ActorID:      "tool:proxy-echo",
		Types:        []string{"proxy.echo"},
		Binding:      actor.BindingRuntimeInboundViaRelay,
		MaxPendingMs: 30_000,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	calls := &captureCalls{
		pending: map[adapter.CorrelationKey]adapter.CorrelationEntry{
			"req-1": {
				ParentID:      "req-1",
				CorrelationID: "req-1",
				ChannelID:     "ch-1",
			},
		},
	}
	if err := mod.Init(context.Background(), &adapter.ModuleContext{
		AdapterName:      "proxy-echo",
		AdapterActorID:   "tool:proxy-echo",
		AdapterActorKind: actor.KindTool,
		ChannelID:        "ch-1",
		ForwardExternalRequest: func(context.Context, *message.Envelope, adapter.ExternalRequestPayload) (adapter.ExternalRequestResult, error) {
			return adapter.ExternalRequestResult{}, nil
		},
		Resolve: func(_ context.Context, id adapter.RequestID, r adapter.ResolveRequest) error {
			calls.finals = append(calls.finals, resolveCall{id: id, req: r})
			return nil
		},
		Provisional: func(_ context.Context, requestID adapter.CorrelationKey, status string, payload json.RawMessage, opts adapter.ProvisionalOptions) (adapter.RespondResult, error) {
			calls.provisional = append(calls.provisional, provisionalCall{
				requestID: requestID,
				status:    status,
				payload:   append(json.RawMessage(nil), payload...),
				opts:      opts,
			})
			return adapter.RespondResult{MessageID: "msg-prov"}, nil
		},
		UpdateReadiness: func(context.Context, actorreg.ReadinessUpdate) (actorreg.ReadinessTransition, error) {
			return actorreg.ReadinessTransition{}, nil
		},
		LookupPendingRequest: func(_ context.Context, requestID adapter.CorrelationKey) (adapter.CorrelationEntry, bool, error) {
			entry, ok := calls.pending[requestID]
			return entry, ok, nil
		},
	}); err != nil {
		t.Fatalf("Init: %v", err)
	}
	return mod, calls
}

func mustEncodeEnvelope(t *testing.T, env message.Envelope) []byte {
	t.Helper()
	raw, err := json.Marshal(env)
	if err != nil {
		t.Fatalf("encode envelope: %v", err)
	}
	return raw
}

func provisionalEnvelope(id, status string) message.Envelope {
	return message.Envelope{
		ID:            message.ID(id),
		ChannelID:     "ch-1",
		Sender:        message.Sender{Kind: actor.KindTool, ID: "tool:proxy-echo"},
		Kind:          message.KindResponse,
		Type:          "proxy.echo",
		ParentID:      "req-1",
		CorrelationID: "req-1",
		Payload:       json.RawMessage(`{"status":"` + status + `","note":"working"}`),
		Audience:      message.Audience{"agent:author"},
	}
}

func finalEnvelope(id, status string) message.Envelope {
	return message.Envelope{
		ID:            message.ID(id),
		ChannelID:     "ch-1",
		Sender:        message.Sender{Kind: actor.KindTool, ID: "tool:proxy-echo"},
		Kind:          message.KindResponse,
		Type:          "proxy.echo",
		ParentID:      "req-1",
		CorrelationID: "req-1",
		Payload:       json.RawMessage(`{"status":"` + status + `","echo":"ping"}`),
		Audience:      message.Audience{"agent:author"},
	}
}

func TestProxyFacadeCallbackRoutesProvisionalThroughProvisional(t *testing.T) {
	mod, calls := newModuleWithCapture(t)

	raw := mustEncodeEnvelope(t, provisionalEnvelope("prov-1", "processing"))
	if err := mod.OnExternalCallback(context.Background(), raw); err != nil {
		t.Fatalf("OnExternalCallback provisional: %v", err)
	}
	if len(calls.finals) != 0 {
		t.Fatalf("provisional must NOT call Resolve; got %d final calls", len(calls.finals))
	}
	if len(calls.provisional) != 1 {
		t.Fatalf("provisional must call Provisional exactly once; got %d", len(calls.provisional))
	}
	got := calls.provisional[0]
	if got.requestID != adapter.CorrelationKey("req-1") {
		t.Fatalf("provisional requestID=%s want req-1", got.requestID)
	}
	if got.status != "processing" {
		t.Fatalf("provisional status=%s want processing", got.status)
	}
	if len(got.opts.Audience) != 1 || got.opts.Audience[0] != "agent:author" {
		t.Fatalf("provisional audience=%v want [agent:author]", got.opts.Audience)
	}
	// payload forwarded to Provisional must NOT pre-contain status —
	// the framework helper re-injects it. We DO expect the user-supplied
	// fields to survive (note=working).
	var payload map[string]any
	if err := json.Unmarshal(got.payload, &payload); err != nil {
		t.Fatalf("decode forwarded payload: %v raw=%s", err, string(got.payload))
	}
	if _, hasStatus := payload["status"]; hasStatus {
		t.Fatalf("forwarded payload must not carry status; got %v", payload)
	}
	if payload["note"] != "working" {
		t.Fatalf("forwarded payload missing user fields; got %v", payload)
	}
}

func TestProxyFacadeCallbackRoutesFinalThroughResolve(t *testing.T) {
	mod, calls := newModuleWithCapture(t)

	for _, status := range []string{"completed", "failed"} {
		raw := mustEncodeEnvelope(t, finalEnvelope("final-"+status, status))
		if err := mod.OnExternalCallback(context.Background(), raw); err != nil {
			t.Fatalf("OnExternalCallback final %s: %v", status, err)
		}
	}
	if len(calls.provisional) != 0 {
		t.Fatalf("final responses must NOT call Provisional; got %d provisional calls", len(calls.provisional))
	}
	if len(calls.finals) != 2 {
		t.Fatalf("expected 2 final calls; got %d", len(calls.finals))
	}
	// Each final routes through ctx.Resolve keyed on the request id
	// (callback parent_id) with the typed terminal status.
	for i, want := range []string{"completed", "failed"} {
		if calls.finals[i].id != "req-1" {
			t.Fatalf("resolve[%d] id=%s want req-1", i, calls.finals[i].id)
		}
		if calls.finals[i].req.Status != want {
			t.Fatalf("resolve[%d] status=%s want %s", i, calls.finals[i].req.Status, want)
		}
	}
}

func TestProxyFacadeCallbackFinalEmptyCorrelationRejected(t *testing.T) {
	mod, calls := newModuleWithCapture(t)
	env := finalEnvelope("final-empty-corr", "completed")
	env.CorrelationID = ""

	if err := mod.OnExternalCallback(context.Background(), mustEncodeEnvelope(t, env)); err == nil {
		t.Fatal("final callback with empty correlation_id must be rejected")
	}
	if len(calls.finals) != 0 || len(calls.provisional) != 0 {
		t.Fatalf("rejected final must not invoke helpers; got final=%d provisional=%d",
			len(calls.finals), len(calls.provisional))
	}
}

func TestProxyFacadeCallbackFinalCorrelationMismatchRejected(t *testing.T) {
	mod, calls := newModuleWithCapture(t)
	env := finalEnvelope("final-bad-corr", "completed")
	env.CorrelationID = "corr-other"

	if err := mod.OnExternalCallback(context.Background(), mustEncodeEnvelope(t, env)); err == nil {
		t.Fatal("final callback with mismatched correlation_id must be rejected")
	}
	if len(calls.finals) != 0 || len(calls.provisional) != 0 {
		t.Fatalf("rejected final must not invoke helpers; got final=%d provisional=%d",
			len(calls.finals), len(calls.provisional))
	}
}

func TestProxyFacadeCallbackFinalSenderKindMismatchRejected(t *testing.T) {
	mod, calls := newModuleWithCapture(t)
	env := finalEnvelope("final-bad-kind", "completed")
	env.Sender = message.Sender{Kind: actor.KindAgent, ID: "tool:proxy-echo"}

	if err := mod.OnExternalCallback(context.Background(), mustEncodeEnvelope(t, env)); err == nil {
		t.Fatal("final callback with mismatched sender kind must be rejected")
	}
	if len(calls.finals) != 0 || len(calls.provisional) != 0 {
		t.Fatalf("rejected final must not invoke helpers; got final=%d provisional=%d",
			len(calls.finals), len(calls.provisional))
	}
}

func TestProxyFacadeCallbackProvisionalThenFinalLeavesCorrelationUntilFinal(t *testing.T) {
	mod, calls := newModuleWithCapture(t)

	// First: two provisionals must not touch Resolve,
	// meaning correlation/F3 remain in place.
	for i, status := range []string{"received", "processing"} {
		raw := mustEncodeEnvelope(t, provisionalEnvelope(fmt.Sprintf("prov-%d", i), status))
		if err := mod.OnExternalCallback(context.Background(), raw); err != nil {
			t.Fatalf("OnExternalCallback provisional %s: %v", status, err)
		}
		if len(calls.finals) != 0 {
			t.Fatalf("after %d provisionals, finals must remain 0; got %d", i+1, len(calls.finals))
		}
	}
	if len(calls.provisional) != 2 {
		t.Fatalf("expected 2 provisional calls; got %d", len(calls.provisional))
	}
	// Then: the final response closes the correlation (ctx.Resolve).
	raw := mustEncodeEnvelope(t, finalEnvelope("final-1", "completed"))
	if err := mod.OnExternalCallback(context.Background(), raw); err != nil {
		t.Fatalf("OnExternalCallback final: %v", err)
	}
	if len(calls.finals) != 1 {
		t.Fatalf("expected 1 final call after final response; got %d", len(calls.finals))
	}
	if len(calls.provisional) != 2 {
		t.Fatalf("provisional count must stay at 2; got %d", len(calls.provisional))
	}
}

func TestProxyFacadeCallbackProvisionalChannelMismatchRejected(t *testing.T) {
	mod, calls := newModuleWithCapture(t)
	env := provisionalEnvelope("prov-mismatch", "processing")
	env.ChannelID = "ch-other"
	raw := mustEncodeEnvelope(t, env)
	if err := mod.OnExternalCallback(context.Background(), raw); err == nil {
		t.Fatalf("provisional callback with wrong channel must be rejected")
	}
	if len(calls.provisional) != 0 || len(calls.finals) != 0 {
		t.Fatalf("rejected provisional must not invoke any path; got prov=%d final=%d",
			len(calls.provisional), len(calls.finals))
	}
}

func TestProxyFacadeCallbackProvisionalSenderMismatchRejected(t *testing.T) {
	mod, calls := newModuleWithCapture(t)
	env := provisionalEnvelope("prov-bad-sender", "processing")
	env.Sender = message.Sender{Kind: actor.KindTool, ID: "tool:not-us"}
	raw := mustEncodeEnvelope(t, env)
	if err := mod.OnExternalCallback(context.Background(), raw); err == nil {
		t.Fatalf("provisional callback with foreign sender must be rejected")
	}
	if len(calls.provisional) != 0 || len(calls.finals) != 0 {
		t.Fatalf("rejected provisional must not invoke any path; got prov=%d final=%d",
			len(calls.provisional), len(calls.finals))
	}
}

func TestProxyFacadeCallbackProvisionalMissingParentRejected(t *testing.T) {
	mod, calls := newModuleWithCapture(t)
	env := provisionalEnvelope("prov-no-parent", "processing")
	env.ParentID = ""
	raw := mustEncodeEnvelope(t, env)
	if err := mod.OnExternalCallback(context.Background(), raw); err == nil {
		t.Fatalf("provisional callback without parent_id must be rejected")
	}
	if len(calls.provisional) != 0 || len(calls.finals) != 0 {
		t.Fatalf("rejected provisional must not invoke any path; got prov=%d final=%d",
			len(calls.provisional), len(calls.finals))
	}
}

// TestProxyFacadeStatusReflectsLifecycle pins the ③实时态 contract: the
// devicebus connect / disconnect / token_expired signals routed through
// OnRuntimeEvent must drive actor.status.available + reason. Before the
// producer side existed the facade was stuck on liveUnknown forever
// (available=false, device_unreachable) regardless of real reachability.
func TestProxyFacadeStatusReflectsLifecycle(t *testing.T) {
	mod, err := New(adapter.Declaration{
		Name:    "kimi",
		ActorID: "tool:kimi",
		Types:   []string{"kimi.ask"},
		Binding: actor.BindingRuntimeInboundViaRelay,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	// Fresh module: no lifecycle frame seen yet → unknown / not available.
	if rep, err := mod.Status(context.Background()); err != nil {
		t.Fatalf("Status(initial): %v", err)
	} else if rep.Available || rep.Reason != "device_unreachable" {
		t.Fatalf("initial status=%+v; want available=false reason=device_unreachable", rep)
	}

	apply := func(event devicetransit.LifecycleEvent) {
		t.Helper()
		if err := mod.OnRuntimeEvent(context.Background(), adapter.RuntimeEvent{
			Kind:           adapter.RuntimeEventDeviceLifecycle,
			ChannelID:      "ch-proxy",
			AdapterActorID: "tool:kimi",
			DeviceLifecycle: &devicetransit.LifecycleFrame{
				AdapterActorID: "tool:kimi",
				ChannelID:      "ch-proxy",
				Event:          event,
				Ts:             123,
			},
		}); err != nil {
			t.Fatalf("OnRuntimeEvent(%s): %v", event, err)
		}
	}

	cases := []struct {
		event         devicetransit.LifecycleEvent
		wantAvailable bool
		wantReason    string
	}{
		{devicetransit.LifecycleConnected, true, "ok"},
		{devicetransit.LifecycleDisconnected, false, "device_offline"},
		{devicetransit.LifecycleTokenExpired, false, "token_expired"},
		{devicetransit.LifecycleConnected, true, "ok"},
	}
	for _, tc := range cases {
		apply(tc.event)
		rep, err := mod.Status(context.Background())
		if err != nil {
			t.Fatalf("Status after %s: %v", tc.event, err)
		}
		if rep.Available != tc.wantAvailable || rep.Reason != tc.wantReason {
			t.Fatalf("after %s: status=%+v; want available=%v reason=%s",
				tc.event, rep, tc.wantAvailable, tc.wantReason)
		}
	}
}
