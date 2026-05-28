package framework

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/wanpengxie/ActOS/kernel/actor"
	"github.com/wanpengxie/ActOS/kernel/adapter"
	"github.com/wanpengxie/ActOS/kernel/message"
)

// TestManagerProvisionalEmitsResponseWithoutResolvingClosure verifies that
// ctx.Provisional writes a kind=response envelope with the requested
// provisional status, but leaves pending correlation / F3 timer intact so
// the request can still be closed by a subsequent Respond.
func TestManagerProvisionalEmitsResponseWithoutResolvingClosure(t *testing.T) {
	mod := &stubModule{
		decl: adapter.Declaration{
			Name:         "xhs",
			ActorID:      "tool:xhs",
			Types:        []string{"xhs.publish"},
			Binding:      actor.BindingRuntimeOutbound,
			MaxPendingMs: 30_000,
		},
	}
	mod.handle = func(ctx context.Context, env *message.Envelope, mctx *adapter.ModuleContext) error {
		// Emit two provisional responses (Layer 2 core), then a final one.
		if _, err := mctx.Provisional(ctx, adapter.CorrelationKey(env.ID), "received",
			json.RawMessage(`{}`),
			adapter.ProvisionalOptions{},
		); err != nil {
			t.Fatalf("Provisional received: %v", err)
		}
		if _, err := mctx.Provisional(ctx, adapter.CorrelationKey(env.ID), "processing",
			json.RawMessage(`{"progress_percent":0.4}`),
			adapter.ProvisionalOptions{},
		); err != nil {
			t.Fatalf("Provisional processing: %v", err)
		}
		_, err := mctx.Respond(ctx, adapter.CorrelationKey(env.ID),
			json.RawMessage(`{"note_id":"n-1"}`),
			adapter.RespondOptions{Status: "completed"},
		)
		return err
	}

	mgr, chain, lookup, _, _ := newTestManager(t, mod)
	req := newTestRequest("channel:test", "agent:author", "xhs.publish", "req-prov-1")
	req.Audience = message.Audience{"tool:xhs"}
	lookup.Put(req)
	if err := mgr.Dispatch(context.Background(), req); err != nil {
		t.Fatalf("Dispatch: %v", err)
	}

	written := chain.Written()
	if len(written) != 3 {
		t.Fatalf("expected 3 writes (received + processing + completed), got %d", len(written))
	}

	expected := []struct {
		status   string
		hasFinal bool
	}{
		{"received", false},
		{"processing", false},
		{"completed", true},
	}
	for i, want := range expected {
		env := written[i]
		if env.Kind != message.KindResponse {
			t.Fatalf("write[%d] kind=%s want response", i, env.Kind)
		}
		if env.Sender.Kind != actor.KindTool || env.Sender.ID != "tool:xhs" {
			t.Fatalf("write[%d] sender=%+v want tool:xhs", i, env.Sender)
		}
		if env.ParentID != req.ID {
			t.Fatalf("write[%d] parent_id=%s want %s", i, env.ParentID, req.ID)
		}
		if len(env.Audience) != 1 || env.Audience[0] != "agent:author" {
			t.Fatalf("write[%d] audience=%v want [agent:author]", i, env.Audience)
		}
		var payload map[string]any
		if err := json.Unmarshal(env.Payload, &payload); err != nil {
			t.Fatalf("write[%d] payload: %v", i, err)
		}
		if payload["status"] != want.status {
			t.Fatalf("write[%d] payload.status=%v want %s", i, payload["status"], want.status)
		}
		if want.hasFinal {
			if payload["note_id"] != "n-1" {
				t.Fatalf("final write payload.note_id=%v want n-1", payload["note_id"])
			}
		}
	}

	// The final Respond closed the correlation; no entry should remain
	// pending.
	bm := mgr.byName["xhs"]
	entry, ok, err := bm.correlation.Get(context.Background(), adapter.CorrelationKey(req.ID))
	if err != nil {
		t.Fatalf("correlation get: %v", err)
	}
	if !ok {
		t.Fatalf("correlation entry missing post-respond")
	}
	if entry.State != adapter.CorrelationDone {
		t.Fatalf("correlation state=%s want done", entry.State)
	}
}

// TestManagerProvisionalRejectsFinalStatus checks that Provisional refuses
// final statuses (completed / failed) — those belong on Respond / Fail.
func TestManagerProvisionalRejectsFinalStatus(t *testing.T) {
	mod := &stubModule{
		decl: adapter.Declaration{
			Name:         "xhs",
			ActorID:      "tool:xhs",
			Types:        []string{"xhs.publish"},
			Binding:      actor.BindingRuntimeOutbound,
			MaxPendingMs: 30_000,
		},
	}
	mod.handle = func(ctx context.Context, env *message.Envelope, mctx *adapter.ModuleContext) error {
		if _, err := mctx.Provisional(ctx, adapter.CorrelationKey(env.ID), "completed",
			json.RawMessage(`{}`),
			adapter.ProvisionalOptions{},
		); err == nil || !strings.Contains(err.Error(), "final status") {
			t.Fatalf("Provisional must reject status=completed, got err=%v", err)
		}
		if _, err := mctx.Provisional(ctx, adapter.CorrelationKey(env.ID), "failed",
			json.RawMessage(`{}`),
			adapter.ProvisionalOptions{},
		); err == nil || !strings.Contains(err.Error(), "final status") {
			t.Fatalf("Provisional must reject status=failed, got err=%v", err)
		}
		// Close out the pending request so dispatch returns clean.
		_, err := mctx.Respond(ctx, adapter.CorrelationKey(env.ID),
			json.RawMessage(`{}`),
			adapter.RespondOptions{Status: "completed"},
		)
		return err
	}
	mgr, _, lookup, _, _ := newTestManager(t, mod)
	req := newTestRequest("channel:test", "agent:author", "xhs.publish", "req-prov-rej")
	req.Audience = message.Audience{"tool:xhs"}
	lookup.Put(req)
	if err := mgr.Dispatch(context.Background(), req); err != nil {
		t.Fatalf("Dispatch: %v", err)
	}
}

// TestManagerProvisionalAcceptsLayer3Namespace verifies that a
// provisional response with a Layer 3 extension status (matching the
// adapter local-name) is built and written without preflight rejection;
// harness namespace ownership is enforced at Step 8, not in
// buildProvisional (this test uses the fakeChain so the harness rules
// are not in scope — it exercises the framework helper construction
// path only).
func TestManagerProvisionalAcceptsLayer3Namespace(t *testing.T) {
	mod := &stubModule{
		decl: adapter.Declaration{
			Name:         "xhs",
			ActorID:      "tool:xhs",
			Types:        []string{"xhs.publish"},
			Binding:      actor.BindingRuntimeOutbound,
			MaxPendingMs: 30_000,
		},
	}
	mod.handle = func(ctx context.Context, env *message.Envelope, mctx *adapter.ModuleContext) error {
		if _, err := mctx.Provisional(ctx, adapter.CorrelationKey(env.ID), "xhs.login_queued",
			json.RawMessage(`{"queue_position":3}`),
			adapter.ProvisionalOptions{},
		); err != nil {
			t.Fatalf("Provisional layer3: %v", err)
		}
		_, err := mctx.Respond(ctx, adapter.CorrelationKey(env.ID),
			json.RawMessage(`{}`),
			adapter.RespondOptions{Status: "completed"},
		)
		return err
	}
	mgr, chain, lookup, _, _ := newTestManager(t, mod)
	req := newTestRequest("channel:test", "agent:author", "xhs.publish", "req-prov-layer3")
	req.Audience = message.Audience{"tool:xhs"}
	lookup.Put(req)
	if err := mgr.Dispatch(context.Background(), req); err != nil {
		t.Fatalf("Dispatch: %v", err)
	}
	written := chain.Written()
	if len(written) != 2 {
		t.Fatalf("expected 2 writes, got %d", len(written))
	}
	var payload map[string]any
	if err := json.Unmarshal(written[0].Payload, &payload); err != nil {
		t.Fatalf("payload unmarshal: %v", err)
	}
	if payload["status"] != "xhs.login_queued" {
		t.Fatalf("payload.status=%v want xhs.login_queued", payload["status"])
	}
	if v, _ := payload["queue_position"].(float64); v != 3 {
		t.Fatalf("payload.queue_position=%v want 3", payload["queue_position"])
	}
}
