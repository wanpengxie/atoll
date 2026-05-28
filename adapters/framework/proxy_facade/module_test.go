package proxy_facade

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/wanpengxie/ActOS/kernel/actor"
	"github.com/wanpengxie/ActOS/kernel/actorreg"
	"github.com/wanpengxie/ActOS/kernel/adapter"
	"github.com/wanpengxie/ActOS/kernel/channel"
	"github.com/wanpengxie/ActOS/kernel/message"
)

type captureExternal struct {
	channelID channel.ID
	actorID   actor.ActorID
	payload   adapter.ExternalRequestPayload
	env       *message.Envelope
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
		CompleteExternalResponse: func(context.Context, *message.Envelope) (adapter.RespondResult, error) {
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
		UpdateReadiness: func(context.Context, actorreg.ReadinessUpdate) (actorreg.ReadinessTransition, error) {
			return actorreg.ReadinessTransition{}, nil
		},
		CompleteExternalResponse: func(_ context.Context, env *message.Envelope) (adapter.RespondResult, error) {
			capture.env = env
			return adapter.RespondResult{MessageID: env.ID}, nil
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
		Payload:       json.RawMessage(`{"status":"completed"}`),
		Audience:      message.Audience{"agent:author"},
	})
	if err := mod.OnExternalCallback(context.Background(), raw); err != nil {
		t.Fatalf("OnExternalCallback: %v", err)
	}
	if capture.env == nil || capture.env.ID != "resp-1" || capture.env.Sender.ID != "tool:kimi" {
		t.Fatalf("completed env=%+v", capture.env)
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
		CompleteExternalResponse: func(context.Context, *message.Envelope) (adapter.RespondResult, error) {
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
