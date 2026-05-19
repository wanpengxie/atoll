package harness

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/wanpengxie/ActOS/kernel/actor"
	"github.com/wanpengxie/ActOS/kernel/channel"
	"github.com/wanpengxie/ActOS/kernel/message"
)

// chainCallerCtx wraps a caller into ctx for use in tests.
func chainCallerCtx(actorID actor.ActorID) context.Context {
	return CtxWithCaller(context.Background(), CallerContext{
		ActorID:   actorID,
		ChannelID: channel.ID("ch-1"),
	})
}

// TestChain_Step1_MissingCaller covers step 1 reject (auth_failed).
func TestChain_Step1_MissingCaller(t *testing.T) {
	c, _, _, _ := newTestChain(t)
	env := newEvent("agent:alpha", "agent.text", json.RawMessage(`{"text":"hi"}`))
	res, err := c.Write(context.Background(), env)
	if err != nil {
		t.Fatalf("Write: %v", err)
	}
	if res.RejectReason != message.HarnessAuthFailed {
		t.Fatalf("expected auth_failed, got %s", res.RejectReason)
	}
}

// TestChain_Step1_ChannelMismatch — caller ctx for ch-2 against deps ch-1.
func TestChain_Step1_ChannelMismatch(t *testing.T) {
	c, _, _, _ := newTestChain(t)
	ctx := CtxWithCaller(context.Background(), CallerContext{
		ActorID: "agent:alpha", ChannelID: "ch-other",
	})
	env := newEvent("agent:alpha", "agent.text", json.RawMessage(`{"text":"hi"}`))
	res, _ := c.Write(ctx, env)
	if res.RejectReason != message.HarnessAuthFailed {
		t.Fatalf("expected auth_failed, got %s", res.RejectReason)
	}
}

// TestChain_Step2_MissingFields covers nil payload + missing kind etc.
func TestChain_Step2_MissingFields(t *testing.T) {
	c, _, _, _ := newTestChain(t)
	env := &message.Envelope{
		Sender: message.Sender{ID: "agent:alpha"},
		// no ID, no Type — required-field reject expected.
	}
	res, _ := c.Write(chainCallerCtx("agent:alpha"), env)
	if res.RejectReason != message.HarnessMissingRequiredField {
		t.Fatalf("expected missing_required_field, got %s detail=%s",
			res.RejectReason, res.RejectDetail)
	}
}

// TestChain_Step2_KindInvalid covers junk in envelope.kind.
func TestChain_Step2_KindInvalid(t *testing.T) {
	c, _, _, _ := newTestChain(t)
	env := newEvent("agent:alpha", "agent.text", json.RawMessage(`{"text":"hi"}`))
	env.Kind = "bogus"
	res, _ := c.Write(chainCallerCtx("agent:alpha"), env)
	if res.RejectReason != message.HarnessKindInvalid {
		t.Fatalf("expected kind_invalid, got %s", res.RejectReason)
	}
}

// TestChain_Step2_ResponseMissingParent covers the One Law pairing
// extra-strong constraint.
func TestChain_Step2_ResponseMissingParent(t *testing.T) {
	c, _, _, _ := newTestChain(t)
	env := newResponse("r-1", "agent:alpha", "", "agent.text", json.RawMessage(`{"status":"completed"}`))
	res, _ := c.Write(chainCallerCtx("agent:alpha"), env)
	if res.RejectReason != message.HarnessResponseMissingParentID {
		t.Fatalf("expected response_missing_parent_id, got %s", res.RejectReason)
	}
}

// TestChain_Step3_SenderMismatch covers step 3 sender_mismatch.
func TestChain_Step3_SenderMismatch(t *testing.T) {
	c, _, _, _ := newTestChain(t)
	env := newEvent("agent:alpha", "agent.text", json.RawMessage(`{"text":"hi"}`))
	env.Sender.ID = "agent:other"
	res, _ := c.Write(chainCallerCtx("agent:alpha"), env)
	if res.RejectReason != message.HarnessSenderMismatch {
		t.Fatalf("expected sender_mismatch, got %s", res.RejectReason)
	}
}

// TestChain_Step3_SenderKindTamper covers sender_kind_mismatch when
// caller fabricates a different kind.
func TestChain_Step3_SenderKindTamper(t *testing.T) {
	c, _, _, _ := newTestChain(t)
	env := newEvent("agent:alpha", "agent.text", json.RawMessage(`{"text":"hi"}`))
	env.Sender.Kind = actor.KindSystem // alpha is an agent.
	res, _ := c.Write(chainCallerCtx("agent:alpha"), env)
	if res.RejectReason != message.HarnessSenderKindMismatch {
		t.Fatalf("expected sender_kind_mismatch, got %s", res.RejectReason)
	}
}

// TestChain_Step3_SenderKindOverwrite — caller omits kind, harness
// forces overwrite from actor_registry.
func TestChain_Step3_SenderKindOverwrite(t *testing.T) {
	c, _, _, _ := newTestChain(t)
	env := newEvent("agent:alpha", "agent.text", json.RawMessage(`{"text":"hi"}`))
	env.Sender.Kind = ""
	res, err := c.Write(chainCallerCtx("agent:alpha"), env)
	if err != nil {
		t.Fatalf("Write: %v", err)
	}
	if !res.Accepted() {
		t.Fatalf("expected accept, got reject=%s detail=%s", res.RejectReason, res.RejectDetail)
	}
	if env.Sender.Kind != actor.KindAgent {
		t.Fatalf("expected sender.kind=agent (forced), got %s", env.Sender.Kind)
	}
}

// TestChain_Step3_Deregistered covers sender_deregistered after
// soft-delete.
func TestChain_Step3_Deregistered(t *testing.T) {
	c, areg, _, _ := newTestChain(t)
	_ = areg.Deregister(context.Background(), "agent:alpha", 999999)
	env := newEvent("agent:alpha", "agent.text", json.RawMessage(`{"text":"hi"}`))
	res, _ := c.Write(chainCallerCtx("agent:alpha"), env)
	if res.RejectReason != message.HarnessSenderDeregistered {
		t.Fatalf("expected sender_deregistered, got %s", res.RejectReason)
	}
}

// TestChain_Step4_UnknownType covers business type unknown to registry.
func TestChain_Step4_UnknownType(t *testing.T) {
	c, _, _, _ := newTestChain(t)
	env := &message.Envelope{
		ID:        "m-1",
		ChannelID: "ch-1",
		Type:      "biz.nonexistent",
		Kind:      message.KindEvent,
		Sender:    message.Sender{ID: "agent:alpha"},
		Payload:   json.RawMessage(`{}`),
		Audience:  []string{"*"},
	}
	res, _ := c.Write(chainCallerCtx("agent:alpha"), env)
	if res.RejectReason != message.HarnessUnknownType {
		t.Fatalf("expected unknown_type, got %s", res.RejectReason)
	}
}

// TestChain_Step5_RequestAudienceInvalid covers request without single
// concrete receiver.
func TestChain_Step5_RequestAudienceInvalid(t *testing.T) {
	c, _, _, treg := newTestChain(t)
	treg.Add(TypeView{
		Type:           "feishu.chat.send",
		AllowedKinds:   []message.Kind{message.KindRequest, message.KindResponse},
		HandlerActorID: "tool:feishu",
	})
	env := newRequest("req-1", "agent:alpha", "feishu.chat.send", "*", json.RawMessage(`{"title":"x"}`))
	res, _ := c.Write(chainCallerCtx("agent:alpha"), env)
	if res.RejectReason != message.HarnessRequestAudienceInvalid {
		t.Fatalf("expected request_audience_invalid, got %s", res.RejectReason)
	}
}

// TestChain_Step5_AudienceActorNotRegistered.
func TestChain_Step5_AudienceActorNotRegistered(t *testing.T) {
	c, _, _, treg := newTestChain(t)
	treg.Add(TypeView{
		Type:           "feishu.chat.send",
		AllowedKinds:   []message.Kind{message.KindRequest, message.KindResponse},
		HandlerActorID: "tool:feishu",
	})
	env := newRequest("req-1", "agent:alpha", "feishu.chat.send", "tool:does-not-exist", json.RawMessage(`{"title":"x"}`))
	res, _ := c.Write(chainCallerCtx("agent:alpha"), env)
	if res.RejectReason != message.HarnessAudienceActorNotRegistered {
		t.Fatalf("expected audience_actor_not_registered, got %s", res.RejectReason)
	}
}

// TestChain_Step5_AudienceHandlerMismatch.
func TestChain_Step5_AudienceHandlerMismatch(t *testing.T) {
	c, areg, _, treg := newTestChain(t)
	_ = areg.Insert(context.Background(), actor.Record{ID: "tool:other", Kind: actor.KindTool, CreatedAt: 1})
	treg.Add(TypeView{
		Type:           "feishu.chat.send",
		AllowedKinds:   []message.Kind{message.KindRequest, message.KindResponse},
		HandlerActorID: "tool:feishu",
	})
	env := newRequest("req-1", "agent:alpha", "feishu.chat.send", "tool:other", json.RawMessage(`{"title":"x"}`))
	res, _ := c.Write(chainCallerCtx("agent:alpha"), env)
	if res.RejectReason != message.HarnessAudienceHandlerMismatch {
		t.Fatalf("expected audience_handler_mismatch, got %s detail=%s",
			res.RejectReason, res.RejectDetail)
	}
}

// TestChain_Step5_KindNotAllowed.
func TestChain_Step5_KindNotAllowed(t *testing.T) {
	c, _, _, treg := newTestChain(t)
	// Type only allows request — agent emits event → reject.
	treg.Add(TypeView{
		Type:           "feishu.chat.send",
		AllowedKinds:   []message.Kind{message.KindRequest},
		HandlerActorID: "tool:feishu",
	})
	env := newEvent("agent:alpha", "feishu.chat.send", json.RawMessage(`{}`))
	res, _ := c.Write(chainCallerCtx("agent:alpha"), env)
	if res.RejectReason != message.HarnessKindNotAllowed {
		t.Fatalf("expected kind_not_allowed, got %s", res.RejectReason)
	}
}

// TestChain_Step6_PayloadSchema — install a validator that always
// rejects, send a request, observe reject reason.
func TestChain_Step6_PayloadSchema(t *testing.T) {
	c, _, _, treg := newTestChain(t)
	treg.Add(TypeView{
		Type:         "feishu.chat.send",
		AllowedKinds: []message.Kind{message.KindRequest, message.KindResponse},
		SchemasByKind: map[message.Kind]json.RawMessage{
			message.KindRequest: json.RawMessage(`{"type":"object","required":["title"]}`),
		},
		HandlerActorID: "tool:feishu",
	})
	prev := DefaultPayloadValidator
	SetPayloadValidator(func(_, _ []byte) error { return errFakeSchema })
	defer SetPayloadValidator(prev)
	env := newRequest("req-1", "agent:alpha", "feishu.chat.send", "tool:feishu", json.RawMessage(`{}`))
	res, _ := c.Write(chainCallerCtx("agent:alpha"), env)
	if res.RejectReason != message.HarnessPayloadSchemaViolation {
		t.Fatalf("expected payload_schema_violation, got %s", res.RejectReason)
	}
}

var errFakeSchema = &schemaError{msg: "missing field"}

type schemaError struct{ msg string }

func (e *schemaError) Error() string { return e.msg }

// TestChain_Step7_DocRefsInvalid covers absolute / parent-escape paths.
func TestChain_Step7_DocRefsInvalid(t *testing.T) {
	c, _, _, _ := newTestChain(t)
	bad := []string{"/abs/path"}
	env := newEvent("agent:alpha", "agent.text", json.RawMessage(`{"text":"hi"}`))
	env.DocRefs = &bad
	res, _ := c.Write(chainCallerCtx("agent:alpha"), env)
	if res.RejectReason != message.HarnessDocRefsInvalid {
		t.Fatalf("expected doc_refs_invalid, got %s", res.RejectReason)
	}
	traversal := []string{"foo/../etc/passwd"}
	env.DocRefs = &traversal
	res, _ = c.Write(chainCallerCtx("agent:alpha"), env)
	if res.RejectReason != message.HarnessDocRefsInvalid {
		t.Fatalf("expected doc_refs_invalid traversal, got %s", res.RejectReason)
	}
}

// TestChain_Step8_ResponseParentInvalid.
func TestChain_Step8_ResponseParentInvalid(t *testing.T) {
	c, _, _, _ := newTestChain(t)
	env := newResponse("r-1", "agent:alpha", "missing-parent-id", "agent.text",
		json.RawMessage(`{"status":"completed"}`))
	res, _ := c.Write(chainCallerCtx("agent:alpha"), env)
	if res.RejectReason != message.HarnessResponseParentInvalid {
		t.Fatalf("expected response_parent_invalid, got %s", res.RejectReason)
	}
}

// TestChain_Step8_TerminalDuplicate.
func TestChain_Step8_TerminalDuplicate(t *testing.T) {
	c, _, log, treg := newTestChain(t)
	treg.Add(TypeView{
		Type:           "feishu.chat.send",
		AllowedKinds:   []message.Kind{message.KindRequest, message.KindResponse},
		HandlerActorID: "tool:feishu",
	})
	// Seed a parent request and a terminal response.
	req := newRequest("req-1", "agent:alpha", "feishu.chat.send", "tool:feishu",
		json.RawMessage(`{"title":"x"}`))
	if r, err := c.Write(chainCallerCtx("agent:alpha"), req); err != nil || !r.Accepted() {
		t.Fatalf("seed request: r=%+v err=%v", r, err)
	}
	resp := newResponse("resp-1", "tool:feishu", "req-1", "feishu.chat.send",
		json.RawMessage(`{"status":"completed"}`))
	if r, err := c.Write(chainCallerCtx("tool:feishu"), resp); err != nil || !r.Accepted() {
		t.Fatalf("seed response: r=%+v err=%v", r, err)
	}
	// Second different response for the same parent → terminal_duplicate.
	resp2 := newResponse("resp-2", "tool:feishu", "req-1", "feishu.chat.send",
		json.RawMessage(`{"status":"failed"}`))
	res, err := c.Write(chainCallerCtx("tool:feishu"), resp2)
	if err != nil {
		t.Fatalf("Write second response: %v", err)
	}
	if res.RejectReason != message.HarnessTerminalDuplicate {
		t.Fatalf("expected terminal_duplicate, got %s detail=%s", res.RejectReason, res.RejectDetail)
	}
	_ = log
}

// TestChain_Step0_5_Dedupe — same id same content returns Deduped=true.
func TestChain_Step0_5_Dedupe(t *testing.T) {
	c, _, _, _ := newTestChain(t)
	payload := json.RawMessage(`{"text":"hi"}`)
	env1 := newEvent("agent:alpha", "agent.text", payload)
	env1.ID = "evt-shared-id"
	if r, err := c.Write(chainCallerCtx("agent:alpha"), env1); err != nil || !r.Accepted() {
		t.Fatalf("first append: %+v %v", r, err)
	}

	env2 := newEvent("agent:alpha", "agent.text", payload)
	env2.ID = "evt-shared-id"
	r, err := c.Write(chainCallerCtx("agent:alpha"), env2)
	if err != nil {
		t.Fatalf("dedupe append: %v", err)
	}
	if !r.Deduped {
		t.Fatalf("expected Deduped=true, got %+v", r)
	}
}

// TestChain_Step0_5_MessageIDConflict — same id different content.
func TestChain_Step0_5_MessageIDConflict(t *testing.T) {
	c, _, _, _ := newTestChain(t)
	env1 := newEvent("agent:alpha", "agent.text", json.RawMessage(`{"text":"hi"}`))
	env1.ID = "evt-conflict"
	if r, err := c.Write(chainCallerCtx("agent:alpha"), env1); err != nil || !r.Accepted() {
		t.Fatalf("first append: %+v %v", r, err)
	}
	env2 := newEvent("agent:alpha", "agent.text", json.RawMessage(`{"text":"different"}`))
	env2.ID = "evt-conflict"
	r, err := c.Write(chainCallerCtx("agent:alpha"), env2)
	if err != nil {
		t.Fatalf("conflict append: %v", err)
	}
	if r.RejectReason != message.HarnessMessageIDConflict {
		t.Fatalf("expected message_id_conflict, got %+v", r)
	}
}

// TestChain_AcceptAndAppendSetsSeq covers the happy path.
func TestChain_AcceptAndAppendSetsSeq(t *testing.T) {
	c, _, log, _ := newTestChain(t)
	env := newEvent("agent:alpha", "agent.text", json.RawMessage(`{"text":"hi"}`))
	env.ID = "evt-ok"
	r, err := c.Write(chainCallerCtx("agent:alpha"), env)
	if err != nil {
		t.Fatalf("Write: %v", err)
	}
	if !r.Accepted() || r.Seq == 0 {
		t.Fatalf("expected accept with seq>0, got %+v", r)
	}
	if env.TSReceived == 0 {
		t.Fatalf("expected TSReceived populated")
	}
	if env.Visibility != message.VisibilityPublic {
		t.Fatalf("expected default visibility=public, got %s", env.Visibility)
	}
	if _, ok, _ := log.FindByID(context.Background(), "ch-1", "evt-ok"); !ok {
		t.Fatalf("expected row persisted to log")
	}
}

// TestChain_CoreTypeKindLocked — system.event must stay kind=event.
func TestChain_CoreTypeKindLocked(t *testing.T) {
	c, _, _, _ := newTestChain(t)
	env := &message.Envelope{
		ID:        "evt-sys-1",
		ChannelID: "ch-1",
		Type:      "system.event",
		Kind:      message.KindRequest, // not allowed for system.event
		Sender:    message.Sender{ID: actor.SystemActorID},
		Payload:   json.RawMessage(`{}`),
		Audience:  []string{"*"},
	}
	res, _ := c.Write(chainCallerCtx(actor.SystemActorID), env)
	if res.RejectReason != message.HarnessKindNotAllowed {
		t.Fatalf("expected kind_not_allowed, got %s", res.RejectReason)
	}
}
