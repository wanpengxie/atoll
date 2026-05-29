package framework

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/wanpengxie/ActOS/kernel/actor"
	"github.com/wanpengxie/ActOS/kernel/adapter"
	"github.com/wanpengxie/ActOS/kernel/message"
)

// TestCallerContextWired asserts the new caller/receiver ModuleContext fields
// are injected.
func TestCallerContextWired(t *testing.T) {
	mod := &stubModule{
		decl: adapter.Declaration{
			Name:         "outbound",
			ActorID:      "tool:outbound",
			Types:        []string{"outbound.x"},
			Binding:      actor.BindingRuntimeOutbound,
			MaxPendingMs: 1_000,
		},
	}
	mgr, _, _, _, _ := newTestManager(t, mod)
	defer func() { _ = mgr.Shutdown(context.Background()) }()

	c := mod.mctx
	for name, f := range map[string]any{
		"Submit":   c.Submit,
		"Await":    c.Await,
		"Watch":    c.Watch,
		"AwaitAll": c.AwaitAll,
		"Call":     c.Call,
		"Abandon":  c.Abandon,
		"Resolve":  c.Resolve,
	} {
		if f == nil {
			t.Fatalf("ModuleContext.%s not wired", name)
		}
	}
}

// TestDeferredThenResolve covers the receiver-side Deferred/Resolve path:
// Handle returns adapter.Deferred() (no finalize, pending+F3 stay alive), then
// a later Resolve writes the final and the router (single lifecycle center)
// closes the correlation.
func TestDeferredThenResolve(t *testing.T) {
	var captured *adapter.ModuleContext
	mod := &stubModule{
		decl: adapter.Declaration{
			Name:         "xhs",
			ActorID:      "tool:xhs",
			Types:        []string{"xhs.publish"},
			Binding:      actor.BindingRuntimeOutbound,
			MaxPendingMs: 5_000,
		},
		handle: func(ctx context.Context, env *message.Envelope, mctx *adapter.ModuleContext) error {
			captured = mctx
			return adapter.Deferred()
		},
	}
	mgr, chain, lookup, _, _ := newTestManager(t, mod)
	defer func() { _ = mgr.Shutdown(context.Background()) }()

	req := newTestRequest("channel:test", "agent:a", "xhs.publish", "req-def-1")
	req.Audience = message.Audience{"tool:xhs"}
	lookup.Put(req)
	if err := mgr.Dispatch(context.Background(), req); err != nil {
		t.Fatalf("Dispatch (deferred) returned error: %v", err)
	}
	// Deferred: nothing written yet, correlation still pending.
	if n := len(chain.Written()); n != 0 {
		t.Fatalf("deferred Handle wrote %d (want 0)", n)
	}
	entry, ok, err := mgr.byActor["tool:xhs"].correlation.Get(context.Background(), adapter.CorrelationKey("req-def-1"))
	if err != nil || !ok {
		t.Fatalf("correlation get: ok=%v err=%v", ok, err)
	}
	if entry.State != adapter.CorrelationPending {
		t.Fatalf("after deferred state=%s want pending", entry.State)
	}

	// Now Resolve completes it.
	if err := captured.Resolve(context.Background(), "req-def-1", adapter.ResolveRequest{
		Status:  "completed",
		Payload: json.RawMessage(`{"note_id":"n1"}`),
	}); err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	written := chain.Written()
	if len(written) != 1 {
		t.Fatalf("after Resolve wrote %d (want 1)", len(written))
	}
	if written[0].ParentID != "req-def-1" {
		t.Fatalf("final parent_id=%s", written[0].ParentID)
	}
	if written[0].Sender.ID != "tool:xhs" {
		t.Fatalf("final sender=%s want tool:xhs", written[0].Sender.ID)
	}
	entry, _, _ = mgr.byActor["tool:xhs"].correlation.Get(context.Background(), adapter.CorrelationKey("req-def-1"))
	if entry.State != adapter.CorrelationDone {
		t.Fatalf("after Resolve state=%s want done (router single lifecycle center)", entry.State)
	}
}

// TestResolveFailed covers the failed-terminal Resolve path.
func TestResolveFailed(t *testing.T) {
	var captured *adapter.ModuleContext
	mod := &stubModule{
		decl: adapter.Declaration{
			Name:         "xhs",
			ActorID:      "tool:xhs",
			Types:        []string{"xhs.publish"},
			Binding:      actor.BindingRuntimeOutbound,
			MaxPendingMs: 5_000,
		},
		handle: func(ctx context.Context, env *message.Envelope, mctx *adapter.ModuleContext) error {
			captured = mctx
			return adapter.Deferred()
		},
	}
	mgr, chain, lookup, _, _ := newTestManager(t, mod)
	defer func() { _ = mgr.Shutdown(context.Background()) }()

	req := newTestRequest("channel:test", "agent:a", "xhs.publish", "req-def-2")
	req.Audience = message.Audience{"tool:xhs"}
	lookup.Put(req)
	if err := mgr.Dispatch(context.Background(), req); err != nil {
		t.Fatalf("Dispatch: %v", err)
	}
	if err := captured.Resolve(context.Background(), "req-def-2", adapter.ResolveRequest{
		Status: "failed",
		Reason: string(message.TerminalReceiverInternalError),
	}); err != nil {
		t.Fatalf("Resolve failed: %v", err)
	}
	written := chain.Written()
	if len(written) != 1 {
		t.Fatalf("after Resolve(failed) wrote %d (want 1)", len(written))
	}
	var payload map[string]any
	_ = json.Unmarshal(written[0].Payload, &payload)
	if payload["status"] != "failed" {
		t.Fatalf("status=%v want failed", payload["status"])
	}
	if payload["reason"] != string(message.TerminalReceiverInternalError) {
		t.Fatalf("reason=%v", payload["reason"])
	}
}

// TestSubmitRegistersBeforeWrite asserts Submit writes the request and returns
// a usable ack, and that an Await on the request id resolves once a final is
// observed by the router.
func TestSubmitThenAwait(t *testing.T) {
	mod := &stubModule{
		decl: adapter.Declaration{
			Name:         "caller",
			ActorID:      "tool:caller",
			Types:        []string{"caller.x"},
			Binding:      actor.BindingRuntimeOutbound,
			MaxPendingMs: 2_000,
		},
	}
	mgr, chain, _, _, _ := newTestManager(t, mod)
	defer func() { _ = mgr.Shutdown(context.Background()) }()

	c := mod.mctx
	sr, err := c.Submit(context.Background(), adapter.CallRequest{
		TargetActor: "tool:downstream",
		Type:        "downstream.do",
		Payload:     json.RawMessage(`{"k":"v"}`),
	})
	if err != nil {
		t.Fatalf("Submit: %v", err)
	}
	if sr.RequestID == "" || !sr.Ack.Accepted || sr.Ack.Status != "accepted" {
		t.Fatalf("bad submit result: %+v", sr)
	}
	if n := len(chain.Written()); n != 1 {
		t.Fatalf("Submit wrote %d (want 1 request)", n)
	}
	reqWritten := chain.Written()[0]
	if reqWritten.Kind != message.KindRequest || reqWritten.Audience[0] != "tool:downstream" {
		t.Fatalf("submit wrote wrong envelope: %+v", reqWritten)
	}

	// Simulate the downstream final arriving and being observed by the router.
	done := make(chan adapter.Terminal, 1)
	errc := make(chan error, 1)
	go func() {
		term, err := c.Await(context.Background(), sr.RequestID, time.Second)
		if err != nil {
			errc <- err
			return
		}
		done <- term
	}()
	time.Sleep(20 * time.Millisecond)
	final := &message.Envelope{
		ID:       message.ID("resp:" + sr.RequestID.String()),
		ParentID: sr.RequestID,
		Kind:     message.KindResponse,
		Payload:  json.RawMessage(`{"status":"completed","out":1}`),
	}
	mgr.router.ObserveResponse(context.Background(), final)

	select {
	case term := <-done:
		if !term.OK || term.Status != "completed" {
			t.Fatalf("await terminal=%+v", term)
		}
	case err := <-errc:
		t.Fatalf("await err: %v", err)
	case <-time.After(2 * time.Second):
		t.Fatal("await did not resolve after ObserveResponse final")
	}
}
