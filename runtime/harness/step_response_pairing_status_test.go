package harness

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/wanpengxie/ActOS/kernel/actor"
	"github.com/wanpengxie/ActOS/kernel/actorreg"
	"github.com/wanpengxie/ActOS/kernel/message"
)

// TestChain_Step8_Layer1FinalSetsTerminal — payload.status ∈ {completed,
// failed} accepts and is_terminal=true per proto-layer0 §2.5.1.
func TestChain_Step8_Layer1FinalSetsTerminal(t *testing.T) {
	for _, status := range []string{"completed", "failed"} {
		status := status
		t.Run(status, func(t *testing.T) {
			c, _, log, treg := newTestChain(t)
			treg.Add(TypeView{
				Type:           "feishu.chat.send",
				AllowedKinds:   []message.Kind{message.KindRequest, message.KindResponse},
				MaxPendingMs:   10_000,
				HandlerActorID: "tool:feishu-adapter",
			})
			req := newRequest("req-final-"+status, "agent:alpha", "feishu.chat.send",
				"tool:feishu-adapter", json.RawMessage(`{"title":"x"}`))
			if r, err := c.Write(chainCallerCtx("agent:alpha"), req); err != nil || !r.Accepted() {
				t.Fatalf("seed request: r=%+v err=%v", r, err)
			}
			payload := `{"status":"` + status + `"}`
			if status == "failed" {
				payload = `{"status":"failed","reason":"receiver_internal_error"}`
			}
			resp := newResponse("resp-final-"+status, "tool:feishu-adapter",
				"req-final-"+status, "feishu.chat.send", json.RawMessage(payload))
			resp.Audience = message.Audience{"agent:alpha"}
			res, err := c.Write(chainCallerCtx("tool:feishu-adapter"), resp)
			if err != nil {
				t.Fatalf("write response: %v", err)
			}
			if !res.Accepted() {
				t.Fatalf("expected accept, reject=%s detail=%s", res.RejectReason, res.RejectDetail)
			}
			stored, ok, err := log.FindByID(context.Background(), "ch-1", message.ID("resp-final-"+status))
			if err != nil || !ok {
				t.Fatalf("lookup stored: ok=%v err=%v", ok, err)
			}
			if !stored.IsTerminal {
				t.Fatalf("Layer 1 final status %q must set is_terminal=true, got false", status)
			}
		})
	}
}

// TestChain_Step8_Layer2ProvisionalNotTerminal — the Layer 2 core
// provisional closed set accepts and is_terminal=false per
// proto-layer0 §2.5.2.
func TestChain_Step8_Layer2ProvisionalNotTerminal(t *testing.T) {
	provisional := []string{"received", "queued", "processing", "deferred", "unavailable"}
	for _, status := range provisional {
		status := status
		t.Run(status, func(t *testing.T) {
			c, _, log, treg := newTestChain(t)
			treg.Add(TypeView{
				Type:           "feishu.chat.send",
				AllowedKinds:   []message.Kind{message.KindRequest, message.KindResponse},
				MaxPendingMs:   10_000,
				HandlerActorID: "tool:feishu-adapter",
			})
			req := newRequest("req-prov-"+status, "agent:alpha", "feishu.chat.send",
				"tool:feishu-adapter", json.RawMessage(`{"title":"x"}`))
			if r, err := c.Write(chainCallerCtx("agent:alpha"), req); err != nil || !r.Accepted() {
				t.Fatalf("seed request: r=%+v err=%v", r, err)
			}
			resp := newResponse("resp-prov-"+status, "tool:feishu-adapter",
				"req-prov-"+status, "feishu.chat.send",
				json.RawMessage(`{"status":"`+status+`"}`))
			resp.Audience = message.Audience{"agent:alpha"}
			res, err := c.Write(chainCallerCtx("tool:feishu-adapter"), resp)
			if err != nil {
				t.Fatalf("write provisional: %v", err)
			}
			if !res.Accepted() {
				t.Fatalf("expected accept, reject=%s detail=%s", res.RejectReason, res.RejectDetail)
			}
			stored, ok, err := log.FindByID(context.Background(), "ch-1", message.ID("resp-prov-"+status))
			if err != nil || !ok {
				t.Fatalf("lookup stored: ok=%v err=%v", ok, err)
			}
			if stored.IsTerminal {
				t.Fatalf("Layer 2 provisional status %q must NOT set is_terminal, got true", status)
			}
		})
	}
}

// TestChain_Step8_Layer3NamespaceOwnershipAccept — Layer 3 status with
// namespace == sender local-name is accepted per proto-layer0 §2.5.3.
func TestChain_Step8_Layer3NamespaceOwnershipAccept(t *testing.T) {
	c, areg, log, treg := newTestChain(t)
	_ = areg.Insert(context.Background(), actorreg.Record{
		ID: "tool:xhs", Kind: actor.KindTool, CreatedAt: 1,
	})
	treg.Add(TypeView{
		Type:           "xhs.publish",
		AllowedKinds:   []message.Kind{message.KindRequest, message.KindResponse},
		MaxPendingMs:   10_000,
		HandlerActorID: "tool:xhs",
	})
	req := newRequest("req-xhs", "agent:alpha", "xhs.publish", "tool:xhs",
		json.RawMessage(`{"note":"x"}`))
	if r, err := c.Write(chainCallerCtx("agent:alpha"), req); err != nil || !r.Accepted() {
		t.Fatalf("seed request: r=%+v err=%v", r, err)
	}
	resp := newResponse("resp-xhs-l3", "tool:xhs", "req-xhs", "xhs.publish",
		json.RawMessage(`{"status":"xhs.login_queued"}`))
	resp.Audience = message.Audience{"agent:alpha"}
	res, err := c.Write(chainCallerCtx("tool:xhs"), resp)
	if err != nil {
		t.Fatalf("write layer3 response: %v", err)
	}
	if !res.Accepted() {
		t.Fatalf("expected accept for xhs.login_queued from tool:xhs, reject=%s detail=%s",
			res.RejectReason, res.RejectDetail)
	}
	stored, ok, err := log.FindByID(context.Background(), "ch-1", "resp-xhs-l3")
	if err != nil || !ok {
		t.Fatalf("lookup stored: ok=%v err=%v", ok, err)
	}
	if stored.IsTerminal {
		t.Fatalf("Layer 3 status must NOT set is_terminal, got true")
	}
}

// TestChain_Step8_Layer3NamespaceMismatchSpoofing — Layer 3 status whose
// namespace does not match sender.id local-name is rejected with
// harness_response_status_namespace_mismatch per proto-layer0 §2.5.3.
func TestChain_Step8_Layer3NamespaceMismatchSpoofing(t *testing.T) {
	c, areg, _, treg := newTestChain(t)
	_ = areg.Insert(context.Background(), actorreg.Record{
		ID: "tool:xhs", Kind: actor.KindTool, CreatedAt: 1,
	})
	// Register the planner agent. It will try to emit xhs.login_queued —
	// namespace owned by tool:xhs, not agent:planner → spoofing.
	_ = areg.Insert(context.Background(), actorreg.Record{
		ID: "agent:planner", Kind: actor.KindAgent, CreatedAt: 1,
	})
	treg.Add(TypeView{
		Type:           "planner.run",
		AllowedKinds:   []message.Kind{message.KindRequest, message.KindResponse},
		MaxPendingMs:   10_000,
		HandlerActorID: "agent:planner",
	})
	req := newRequest("req-planner", "agent:alpha", "planner.run", "agent:planner",
		json.RawMessage(`{"goal":"x"}`))
	if r, err := c.Write(chainCallerCtx("agent:alpha"), req); err != nil || !r.Accepted() {
		t.Fatalf("seed request: r=%+v err=%v", r, err)
	}
	resp := newResponse("resp-spoof", "agent:planner", "req-planner", "planner.run",
		json.RawMessage(`{"status":"xhs.login_queued"}`))
	resp.Audience = message.Audience{"agent:alpha"}
	res, err := c.Write(chainCallerCtx("agent:planner"), resp)
	if err != nil {
		t.Fatalf("write spoof response: %v", err)
	}
	if res.RejectReason != message.HarnessResponseStatusNamespaceMismatch {
		t.Fatalf("expected harness_response_status_namespace_mismatch, got %s detail=%s",
			res.RejectReason, res.RejectDetail)
	}
}

// TestChain_Step8_Layer3InvalidGrammar — Layer 3 status strings that
// fail the regex grammar reject with harness_response_status_invalid
// per proto-layer0 §2.5.3.
func TestChain_Step8_Layer3InvalidGrammar(t *testing.T) {
	// Each row picks an out-of-grammar status string. We expect the
	// invalid grammar reject regardless of sender namespace — extractor
	// fails before namespace ownership runs.
	cases := []struct {
		name   string
		status string
	}{
		{"empty_namespace", ".x"},
		{"empty_name", "x."},
		{"three_parts", "a.b.c"},
		{"uppercase", "Xhs.X"},
		{"single_segment", "no_dot"},
		{"leading_digit_namespace", "1xhs.foo"},
		{"namespace_collides_layer2_received", "received.foo"},
		{"namespace_collides_layer1_completed", "completed.foo"},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			c, areg, _, treg := newTestChain(t)
			_ = areg.Insert(context.Background(), actorreg.Record{
				ID: "tool:xhs", Kind: actor.KindTool, CreatedAt: 1,
			})
			treg.Add(TypeView{
				Type:           "xhs.publish",
				AllowedKinds:   []message.Kind{message.KindRequest, message.KindResponse},
				MaxPendingMs:   10_000,
				HandlerActorID: "tool:xhs",
			})
			req := newRequest("req-"+tc.name, "agent:alpha", "xhs.publish", "tool:xhs",
				json.RawMessage(`{"note":"x"}`))
			if r, err := c.Write(chainCallerCtx("agent:alpha"), req); err != nil || !r.Accepted() {
				t.Fatalf("seed request: r=%+v err=%v", r, err)
			}
			resp := newResponse("resp-"+tc.name, "tool:xhs", "req-"+tc.name, "xhs.publish",
				json.RawMessage(`{"status":"`+tc.status+`"}`))
			resp.Audience = message.Audience{"agent:alpha"}
			res, err := c.Write(chainCallerCtx("tool:xhs"), resp)
			if err != nil {
				t.Fatalf("write invalid grammar response: %v", err)
			}
			if res.RejectReason != message.HarnessResponseStatusInvalid {
				t.Fatalf("expected harness_response_status_invalid for %q, got %s detail=%s",
					tc.status, res.RejectReason, res.RejectDetail)
			}
		})
	}
}

// TestChain_Step8_ProvisionalAfterFinalRejects — once a final response
// is stored, any subsequent provisional for the same parent rejects with
// harness_provisional_after_final per proto-layer1 §2.8 #8.
func TestChain_Step8_ProvisionalAfterFinalRejects(t *testing.T) {
	c, _, _, treg := newTestChain(t)
	treg.Add(TypeView{
		Type:           "feishu.chat.send",
		AllowedKinds:   []message.Kind{message.KindRequest, message.KindResponse},
		MaxPendingMs:   10_000,
		HandlerActorID: "tool:feishu-adapter",
	})
	req := newRequest("req-zombie", "agent:alpha", "feishu.chat.send",
		"tool:feishu-adapter", json.RawMessage(`{"title":"x"}`))
	if r, err := c.Write(chainCallerCtx("agent:alpha"), req); err != nil || !r.Accepted() {
		t.Fatalf("seed request: r=%+v err=%v", r, err)
	}
	// Seed final first.
	final := newResponse("resp-final", "tool:feishu-adapter", "req-zombie", "feishu.chat.send",
		json.RawMessage(`{"status":"completed"}`))
	final.Audience = message.Audience{"agent:alpha"}
	if r, err := c.Write(chainCallerCtx("tool:feishu-adapter"), final); err != nil || !r.Accepted() {
		t.Fatalf("seed final: r=%+v err=%v", r, err)
	}
	// Now a provisional for the same parent must reject.
	zombie := newResponse("resp-zombie-prov", "tool:feishu-adapter", "req-zombie",
		"feishu.chat.send", json.RawMessage(`{"status":"processing"}`))
	zombie.Audience = message.Audience{"agent:alpha"}
	res, err := c.Write(chainCallerCtx("tool:feishu-adapter"), zombie)
	if err != nil {
		t.Fatalf("write zombie provisional: %v", err)
	}
	if res.RejectReason != message.HarnessProvisionalAfterFinal {
		t.Fatalf("expected harness_provisional_after_final, got %s detail=%s",
			res.RejectReason, res.RejectDetail)
	}
}

// TestChain_Step8_FinalAfterFinalRejects — a second final for the same
// parent rejects with harness_terminal_duplicate per proto-layer1 §2.8.
// Already covered by TestChain_Step8_TerminalDuplicate; this variant
// exercises the new pre-check path (Step 8 lookup precedes the engine
// UNIQUE INDEX) so the reject surface is consistent under single-writer.
func TestChain_Step8_FinalAfterFinalRejects(t *testing.T) {
	c, _, _, treg := newTestChain(t)
	treg.Add(TypeView{
		Type:           "feishu.chat.send",
		AllowedKinds:   []message.Kind{message.KindRequest, message.KindResponse},
		MaxPendingMs:   10_000,
		HandlerActorID: "tool:feishu-adapter",
	})
	req := newRequest("req-dup", "agent:alpha", "feishu.chat.send",
		"tool:feishu-adapter", json.RawMessage(`{"title":"x"}`))
	if r, err := c.Write(chainCallerCtx("agent:alpha"), req); err != nil || !r.Accepted() {
		t.Fatalf("seed request: r=%+v err=%v", r, err)
	}
	first := newResponse("resp-dup-1", "tool:feishu-adapter", "req-dup", "feishu.chat.send",
		json.RawMessage(`{"status":"completed"}`))
	first.Audience = message.Audience{"agent:alpha"}
	if r, err := c.Write(chainCallerCtx("tool:feishu-adapter"), first); err != nil || !r.Accepted() {
		t.Fatalf("seed first final: r=%+v err=%v", r, err)
	}
	second := newResponse("resp-dup-2", "tool:feishu-adapter", "req-dup", "feishu.chat.send",
		json.RawMessage(`{"status":"failed","reason":"receiver_internal_error"}`))
	second.Audience = message.Audience{"agent:alpha"}
	res, err := c.Write(chainCallerCtx("tool:feishu-adapter"), second)
	if err != nil {
		t.Fatalf("write duplicate final: %v", err)
	}
	if res.RejectReason != message.HarnessTerminalDuplicate {
		t.Fatalf("expected harness_terminal_duplicate, got %s detail=%s",
			res.RejectReason, res.RejectDetail)
	}
}

// TestChain_Step8_ProvisionalThenFinalAccepts — multiple provisional
// responses followed by a final all accept; final is the only one
// flagged is_terminal.
func TestChain_Step8_ProvisionalThenFinalAccepts(t *testing.T) {
	c, _, log, treg := newTestChain(t)
	treg.Add(TypeView{
		Type:           "feishu.chat.send",
		AllowedKinds:   []message.Kind{message.KindRequest, message.KindResponse},
		MaxPendingMs:   10_000,
		HandlerActorID: "tool:feishu-adapter",
	})
	req := newRequest("req-stream", "agent:alpha", "feishu.chat.send",
		"tool:feishu-adapter", json.RawMessage(`{"title":"x"}`))
	if r, err := c.Write(chainCallerCtx("agent:alpha"), req); err != nil || !r.Accepted() {
		t.Fatalf("seed request: r=%+v err=%v", r, err)
	}
	for i, status := range []string{"received", "queued", "processing"} {
		resp := newResponse("resp-stream-prov-"+status, "tool:feishu-adapter", "req-stream",
			"feishu.chat.send", json.RawMessage(`{"status":"`+status+`"}`))
		resp.Audience = message.Audience{"agent:alpha"}
		r, err := c.Write(chainCallerCtx("tool:feishu-adapter"), resp)
		if err != nil || !r.Accepted() {
			t.Fatalf("write provisional #%d (%s): r=%+v err=%v", i, status, r, err)
		}
	}
	final := newResponse("resp-stream-final", "tool:feishu-adapter", "req-stream",
		"feishu.chat.send", json.RawMessage(`{"status":"completed"}`))
	final.Audience = message.Audience{"agent:alpha"}
	if r, err := c.Write(chainCallerCtx("tool:feishu-adapter"), final); err != nil || !r.Accepted() {
		t.Fatalf("write final: r=%+v err=%v", r, err)
	}
	// Spot-check: final row carries is_terminal=true, provisional rows
	// carry false.
	for _, id := range []message.ID{"resp-stream-prov-received", "resp-stream-prov-queued", "resp-stream-prov-processing"} {
		row, ok, err := log.FindByID(context.Background(), "ch-1", id)
		if err != nil || !ok {
			t.Fatalf("lookup %s: ok=%v err=%v", id, ok, err)
		}
		if row.IsTerminal {
			t.Fatalf("provisional %s must not be terminal", id)
		}
	}
	finalRow, ok, err := log.FindByID(context.Background(), "ch-1", "resp-stream-final")
	if err != nil || !ok {
		t.Fatalf("lookup final: ok=%v err=%v", ok, err)
	}
	if !finalRow.IsTerminal {
		t.Fatalf("final must be terminal, got false")
	}
}

// TestChain_Step8_ResponseExpiresAtCleared — Step Normalize must clear
// caller-supplied expires_at on kind=response per proto-layer0 §2.7 +
// §4.6 (provisional / final response carries no independent SLA).
func TestChain_Step8_ResponseExpiresAtCleared(t *testing.T) {
	c, _, log, treg := newTestChain(t)
	treg.Add(TypeView{
		Type:           "feishu.chat.send",
		AllowedKinds:   []message.Kind{message.KindRequest, message.KindResponse},
		MaxPendingMs:   10_000,
		HandlerActorID: "tool:feishu-adapter",
	})
	req := newRequest("req-exp", "agent:alpha", "feishu.chat.send",
		"tool:feishu-adapter", json.RawMessage(`{"title":"x"}`))
	if r, err := c.Write(chainCallerCtx("agent:alpha"), req); err != nil || !r.Accepted() {
		t.Fatalf("seed request: r=%+v err=%v", r, err)
	}
	resp := newResponse("resp-exp", "tool:feishu-adapter", "req-exp", "feishu.chat.send",
		json.RawMessage(`{"status":"processing"}`))
	resp.Audience = message.Audience{"agent:alpha"}
	bogus := int64(testTS + 1_000_000)
	resp.ExpiresAt = &bogus
	r, err := c.Write(chainCallerCtx("tool:feishu-adapter"), resp)
	if err != nil || !r.Accepted() {
		t.Fatalf("write response: r=%+v err=%v", r, err)
	}
	stored, ok, err := log.FindByID(context.Background(), "ch-1", "resp-exp")
	if err != nil || !ok {
		t.Fatalf("lookup response: ok=%v err=%v", ok, err)
	}
	if stored.ExpiresAt != nil {
		t.Fatalf("response expires_at must be cleared by Step Normalize, got %d", *stored.ExpiresAt)
	}
}
