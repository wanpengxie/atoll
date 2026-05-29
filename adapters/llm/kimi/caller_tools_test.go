package kimi

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/wanpengxie/ActOS/kernel/message"
)

// TestAwaitResultResolvesFinal asserts await_result blocks until the named
// request's final arrives and returns the final payload inline.
func TestAwaitResultResolvesFinal(t *testing.T) {
	b := &Bridge{}
	caller := b.caller()
	caller.futures.Register("req-await")

	go func() {
		time.Sleep(10 * time.Millisecond)
		final := toolTriggerWithStatus(message.KindResponse, "resp-await", "req-await", "completed").Envelope
		final.Payload = []byte(`{"status":"completed","note_id":"n-9"}`)
		caller.Deliver(&final)
	}()

	tool := &AwaitResultTool{bridge: b}
	result, err := tool.Execute(context.Background(), json.RawMessage(`{"request_id":"req-await","timeout_ms":2000}`))
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if result.IsError {
		t.Fatalf("await_result should resolve the final: %#v", result.Value.Value)
	}
	value := result.Value.Value.(map[string]any)
	if value["note_id"] != "n-9" {
		t.Fatalf("await_result final=%#v", value)
	}
}

// TestAwaitResultUnknownRequest asserts await_result on an unknown id returns
// an actor-CLI error (already collected / abandoned / worker-restart).
func TestAwaitResultUnknownRequest(t *testing.T) {
	b := &Bridge{}
	tool := &AwaitResultTool{bridge: b}
	result, err := tool.Execute(context.Background(), json.RawMessage(`{"request_id":"nope"}`))
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if !result.IsError {
		t.Fatalf("await_result on unknown id should error: %#v", result.Value.Value)
	}
}

// TestAwaitResultTimeoutReturnsStillPendingAck asserts await_result returns a
// still-pending ack (not an error) when its window elapses without a final,
// leaving the future in flight.
func TestAwaitResultTimeoutReturnsStillPendingAck(t *testing.T) {
	b := &Bridge{}
	caller := b.caller()
	caller.futures.Register("req-slow-await")

	tool := &AwaitResultTool{bridge: b}
	result, err := tool.Execute(context.Background(), json.RawMessage(`{"request_id":"req-slow-await","timeout_ms":20}`))
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if result.IsError {
		t.Fatalf("await_result timeout should be an ack, not an error: %#v", result.Value.Value)
	}
	root := result.Value.Value.(map[string]any)
	if root["status"] != "accepted" {
		t.Fatalf("await_result timeout ack status=%v", root["status"])
	}
	if !caller.futures.Registered("req-slow-await") {
		t.Fatal("await_result timeout erased the in-flight future")
	}
}

// TestAbandonThenFinalBecomesTrigger asserts that after abandon, a final loops
// through routeTriggers as a NEW TURN TRIGGER (NoActiveWaiter + future gone).
func TestAbandonThenFinalBecomesTrigger(t *testing.T) {
	b := &Bridge{}
	caller := b.caller()
	caller.futures.Register("req-aband-tool")

	abandon := &AbandonTool{bridge: b}
	res, err := abandon.Execute(context.Background(), json.RawMessage(`{"request_id":"req-aband-tool"}`))
	if err != nil {
		t.Fatalf("abandon Execute: %v", err)
	}
	if res.IsError {
		t.Fatalf("abandon should not error: %#v", res.Value.Value)
	}
	if caller.futures.Registered("req-aband-tool") {
		t.Fatal("abandon did not release the local waiter")
	}

	// A final after abandon → routeTriggers surfaces it as a new turn trigger.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	in := make(chan TriggerPayload, 1)
	out := b.routeTriggers(ctx, nil, in)
	in <- toolTriggerWithStatus(message.KindResponse, "resp-aband-tool", "req-aband-tool", "completed")
	close(in)

	got, ok := <-out
	if !ok || got.Envelope.ID != "resp-aband-tool" {
		t.Fatalf("post-abandon final should become a trigger: got=%v ok=%v", got.Envelope.ID, ok)
	}
}

// TestListPendingReturnsIdListOnly asserts list_pending returns only the id
// list (no status aggregation, §2.3.4).
func TestListPendingReturnsIdListOnly(t *testing.T) {
	b := &Bridge{}
	caller := b.caller()
	caller.futures.Register("lp-1")
	caller.futures.Register("lp-2")

	tool := &ListPendingTool{bridge: b}
	result, err := tool.Execute(context.Background(), nil)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	root := result.Value.Value.(map[string]any)
	if root["count"] != 2 {
		t.Fatalf("list_pending count=%v want 2", root["count"])
	}
	pending, ok := root["pending"].([]string)
	if !ok || len(pending) != 2 {
		t.Fatalf("list_pending pending=%#v", root["pending"])
	}
	// No status field per id — only the id list.
	for _, id := range pending {
		if id != "lp-1" && id != "lp-2" {
			t.Fatalf("unexpected pending id %q", id)
		}
	}
}

// TestM4WorkerRestartFinalBecomesTrigger simulates a worker restart: a fresh
// Bridge (empty registry) receives a final whose request was submitted by the
// previous process. With no local waiter it is surfaced as a new turn trigger,
// never quarantined (§5.2 M4).
func TestM4WorkerRestartFinalBecomesTrigger(t *testing.T) {
	b := &Bridge{} // fresh process — empty caller registry
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	in := make(chan TriggerPayload, 1)
	out := b.routeTriggers(ctx, nil, in)

	in <- toolTriggerWithStatus(message.KindResponse, "restart-final", "req-from-prev-life", "completed")
	close(in)

	got, ok := <-out
	if !ok {
		t.Fatal("M4: worker-restart final was dropped, not surfaced as a trigger")
	}
	if got.Envelope.ID != "restart-final" {
		t.Fatalf("M4: surfaced id=%s want restart-final", got.Envelope.ID)
	}
}
