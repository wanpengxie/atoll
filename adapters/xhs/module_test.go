package xhs_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/wanpengxie/ActOS/adapters/xhs"
	"github.com/wanpengxie/ActOS/kernel/actor"
	"github.com/wanpengxie/ActOS/kernel/adapter"
	"github.com/wanpengxie/ActOS/kernel/message"
)

// fakeRespond records every Respond call so tests can assert on what
// the scaffold emitted.
type fakeRespond struct {
	calls []respondCall
	err   error
}

type respondCall struct {
	requestID adapter.CorrelationKey
	payload   json.RawMessage
	opts      adapter.RespondOptions
}

func (f *fakeRespond) fn(_ context.Context, id adapter.CorrelationKey, payload json.RawMessage, opts adapter.RespondOptions) (adapter.RespondResult, error) {
	f.calls = append(f.calls, respondCall{requestID: id, payload: payload, opts: opts})
	return adapter.RespondResult{MessageID: message.ID("response:" + id.String())}, f.err
}

func newRequest(t *testing.T, id, typ string) *message.Envelope {
	t.Helper()
	return &message.Envelope{
		ID:         message.ID(id),
		TS:         1000,
		ChannelID:  "ch-1",
		Sender:     message.Sender{Kind: actor.KindAgent, ID: "agent:author"},
		Kind:       message.KindRequest,
		Type:       typ,
		Payload:    json.RawMessage(`{"title":"hello"}`),
		Visibility: message.VisibilityPrivate,
		Audience:   message.Audience{xhs.DefaultAdapterActorID},
	}
}

// TestDeclares ensures the scaffold owns the canonical tool actor id +
// declares the 6 closed-set types with TypeSchemas.
func TestDeclares(t *testing.T) {
	mod := xhs.New(xhs.Config{})
	d := mod.Declares()
	if d.Name != xhs.AdapterName {
		t.Errorf("Name=%s want %s", d.Name, xhs.AdapterName)
	}
	if d.ActorID != xhs.DefaultAdapterActorID {
		t.Errorf("ActorID=%s want %s", d.ActorID, xhs.DefaultAdapterActorID)
	}
	if d.Binding != actor.BindingInProcess {
		t.Errorf("Binding=%s want in_process", d.Binding)
	}
	if d.MaxPendingMs != xhs.DefaultMaxPendingMs {
		t.Errorf("MaxPendingMs=%d want %d", d.MaxPendingMs, xhs.DefaultMaxPendingMs)
	}
	if len(d.Types) != len(xhs.AllTypes) {
		t.Errorf("Types=%v want %v", d.Types, xhs.AllTypes)
	}
	if got := d.TypeSchemas[xhs.TypePublish]; len(got.AllowedKinds) != 2 {
		t.Errorf("TypeSchemas[%s].AllowedKinds=%v", xhs.TypePublish, got.AllowedKinds)
	}
	if got := d.TypeSchemas[xhs.TypeNoteArchived]; len(got.AllowedKinds) != 1 || got.AllowedKinds[0] != message.KindEvent {
		t.Errorf("TypeSchemas[%s].AllowedKinds=%v want [event]", xhs.TypeNoteArchived, got.AllowedKinds)
	}
}

// TestInit_RejectsMissingRespond enforces mctx.Respond must be set.
func TestInit_RejectsMissingRespond(t *testing.T) {
	mod := xhs.New(xhs.Config{})
	if err := mod.Init(context.Background(), &adapter.ModuleContext{ChannelID: "ch-1"}); err == nil {
		t.Fatal("Init must reject nil Respond")
	}
}

// TestHandle_MockSuccess covers the happy mock path: Handle synchronously
// calls Respond with status=completed and a non-empty payload.
func TestHandle_MockSuccess(t *testing.T) {
	resp := &fakeRespond{}
	mod := xhs.New(xhs.Config{})
	if err := mod.Init(context.Background(), &adapter.ModuleContext{
		ChannelID:      "ch-1",
		AdapterActorID: xhs.DefaultAdapterActorID,
		Respond:        resp.fn,
	}); err != nil {
		t.Fatalf("Init: %v", err)
	}

	env := newRequest(t, "req-1", xhs.TypePublish)
	if err := mod.Handle(context.Background(), env); err != nil {
		t.Fatalf("Handle: %v", err)
	}
	if len(resp.calls) != 1 {
		t.Fatalf("expected 1 Respond call, got %d", len(resp.calls))
	}
	call := resp.calls[0]
	if call.requestID != "req-1" {
		t.Errorf("requestID=%s want req-1", call.requestID)
	}
	if call.opts.Status != "completed" {
		t.Errorf("status=%s want completed", call.opts.Status)
	}
	var payload map[string]any
	if err := json.Unmarshal(call.payload, &payload); err != nil {
		t.Fatalf("payload unmarshal: %v", err)
	}
	if _, ok := payload["note_id"]; !ok {
		t.Errorf("payload missing note_id: %v", payload)
	}
}

// TestHandle_CustomPayload uses the Config.ResponsePayload override.
func TestHandle_CustomPayload(t *testing.T) {
	resp := &fakeRespond{}
	mod := xhs.New(xhs.Config{
		ResponsePayload: json.RawMessage(`{"results":["a","b"]}`),
	})
	_ = mod.Init(context.Background(), &adapter.ModuleContext{
		ChannelID:      "ch-1",
		AdapterActorID: xhs.DefaultAdapterActorID,
		Respond:        resp.fn,
	})
	if err := mod.Handle(context.Background(), newRequest(t, "req-2", xhs.TypeSearch)); err != nil {
		t.Fatalf("Handle: %v", err)
	}
	if string(resp.calls[0].payload) != `{"results":["a","b"]}` {
		t.Errorf("custom payload not honored: %s", resp.calls[0].payload)
	}
}

// TestHandle_PanicOnDemand verifies the panic-mode used by acceptance
// #4: Handle panics synchronously so framework Error Policy can recover
// it into a failed terminal.
func TestHandle_PanicOnDemand(t *testing.T) {
	mod := xhs.New(xhs.Config{PanicOnHandle: true})
	_ = mod.Init(context.Background(), &adapter.ModuleContext{
		ChannelID:      "ch-1",
		AdapterActorID: xhs.DefaultAdapterActorID,
		Respond:        (&fakeRespond{}).fn,
	})
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected Handle to panic")
		}
	}()
	_ = mod.Handle(context.Background(), newRequest(t, "req-3", xhs.TypePublish))
}

// TestHandle_SkipRespond verifies the no-op mode used by acceptance
// #5: Handle returns nil without calling Respond so the F3 timer
// eventually emits adapter_default_timeout.
func TestHandle_SkipRespond(t *testing.T) {
	resp := &fakeRespond{}
	mod := xhs.New(xhs.Config{SkipRespond: true})
	_ = mod.Init(context.Background(), &adapter.ModuleContext{
		ChannelID:      "ch-1",
		AdapterActorID: xhs.DefaultAdapterActorID,
		Respond:        resp.fn,
	})
	if err := mod.Handle(context.Background(), newRequest(t, "req-4", xhs.TypePublish)); err != nil {
		t.Errorf("Handle: %v", err)
	}
	if len(resp.calls) != 0 {
		t.Errorf("SkipRespond should not call Respond, got %d calls", len(resp.calls))
	}
}

// TestHandle_RejectsNonRequest enforces the kind=request precondition.
func TestHandle_RejectsNonRequest(t *testing.T) {
	mod := xhs.New(xhs.Config{})
	_ = mod.Init(context.Background(), &adapter.ModuleContext{
		ChannelID:      "ch-1",
		AdapterActorID: xhs.DefaultAdapterActorID,
		Respond:        (&fakeRespond{}).fn,
	})
	env := newRequest(t, "evt-1", xhs.TypeNoteArchived)
	env.Kind = message.KindEvent
	if err := mod.Handle(context.Background(), env); err == nil {
		t.Fatal("Handle should reject non-request envelope")
	}
}
