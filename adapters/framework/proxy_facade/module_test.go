package proxy_facade

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/wanpengxie/ActOS/kernel/actor"
	"github.com/wanpengxie/ActOS/kernel/adapter"
	"github.com/wanpengxie/ActOS/kernel/channel"
	"github.com/wanpengxie/ActOS/kernel/devicetransit"
	khar "github.com/wanpengxie/ActOS/kernel/harness"
	"github.com/wanpengxie/ActOS/kernel/message"
)

type captureTransit struct {
	frame devicetransit.SendFrame
}

func (t *captureTransit) Send(_ context.Context, frame devicetransit.SendFrame) (devicetransit.FrameID, error) {
	t.frame = frame
	return "frame-1", nil
}

func (t *captureTransit) Ack(context.Context, devicetransit.AckFrame) error { return nil }

type captureChain struct {
	env *message.Envelope
}

func (c *captureChain) Write(_ context.Context, env *message.Envelope) (khar.WriteResult, error) {
	c.env = env
	return khar.WriteResult{MessageID: env.ID, Seq: 1}, nil
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
	transit := &captureTransit{}
	chain := &captureChain{}
	if err := mod.Init(context.Background(), &adapter.ModuleContext{
		AdapterName:    decl.Name,
		AdapterActorID: decl.ActorID,
		ChannelID:      "ch-1",
		DeviceTransit:  transit,
		HarnessChain:   chain,
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
	if transit.frame.AdapterActorID != "tool:kimi" || transit.frame.ChannelID != channel.ID("ch-1") {
		t.Fatalf("send frame=%+v", transit.frame)
	}
	var body transitBody
	if err := json.Unmarshal(transit.frame.Body, &body); err != nil {
		t.Fatalf("body JSON: %v", err)
	}
	if body.Direction != "to_device" || body.RequestID != "req-1" {
		t.Fatalf("body=%+v", body)
	}
	var got message.Envelope
	if err := json.Unmarshal(body.Payload, &got); err != nil {
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
	chain := &captureChain{}
	if err := mod.Init(context.Background(), &adapter.ModuleContext{
		AdapterName:    "kimi",
		AdapterActorID: "tool:kimi",
		ChannelID:      "ch-1",
		DeviceTransit:  &captureTransit{},
		HarnessChain:   chain,
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
	if chain.env == nil || chain.env.ID != "resp-1" || chain.env.Sender.ID != "tool:kimi" {
		t.Fatalf("written env=%+v", chain.env)
	}
}
