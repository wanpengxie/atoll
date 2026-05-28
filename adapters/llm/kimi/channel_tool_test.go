package kimi

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/wanpengxie/ActOS/kernel/actor"
	"github.com/wanpengxie/ActOS/kernel/message"
)

func TestDispatchToolResponseIgnoresNonResponseWithMatchingParent(t *testing.T) {
	b := &Bridge{}
	ch := b.registerPendingTool("tool-req-1")
	defer b.unregisterPendingTool("tool-req-1")

	if b.dispatchToolResponse(toolTrigger(message.KindEvent, "event-1", "tool-req-1")) {
		t.Fatalf("event with matching parent stole pending tool slot")
	}
	if !b.dispatchToolResponse(toolTrigger(message.KindResponse, "response-1", "tool-req-1")) {
		t.Fatalf("response with matching parent was not dispatched")
	}
	select {
	case got := <-ch:
		if got.trigger.Envelope.ID != "response-1" {
			t.Fatalf("response id=%s", got.trigger.Envelope.ID)
		}
	case <-time.After(time.Second):
		t.Fatal("pending tool did not receive response")
	}
}

func TestRouteTriggersQuarantinesLateToolResponse(t *testing.T) {
	b := &Bridge{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	in := make(chan TriggerPayload, 2)
	out := b.routeTriggers(ctx, nil, in)

	in <- toolTrigger(message.KindResponse, "late-response", "tool-req-missing")
	in <- toolTrigger(message.KindRequest, "normal-trigger", "")
	close(in)

	got, ok := <-out
	if !ok {
		t.Fatal("routeTriggers closed before normal trigger")
	}
	if got.Envelope.ID != "normal-trigger" {
		t.Fatalf("routed id=%s want normal-trigger", got.Envelope.ID)
	}
	if _, ok := <-out; ok {
		t.Fatal("late response leaked into trigger stream")
	}
}

func TestDispatchToolResponseConcurrentPendingToolsIndependent(t *testing.T) {
	b := &Bridge{}
	ch1 := b.registerPendingTool("tool-req-1")
	ch2 := b.registerPendingTool("tool-req-2")

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		if !b.dispatchToolResponse(toolTrigger(message.KindResponse, "response-1", "tool-req-1")) {
			t.Errorf("response-1 not dispatched")
		}
	}()
	go func() {
		defer wg.Done()
		if !b.dispatchToolResponse(toolTrigger(message.KindResponse, "response-2", "tool-req-2")) {
			t.Errorf("response-2 not dispatched")
		}
	}()
	wg.Wait()

	got1 := <-ch1
	got2 := <-ch2
	if got1.trigger.Envelope.ID != "response-1" {
		t.Fatalf("ch1 got %s", got1.trigger.Envelope.ID)
	}
	if got2.trigger.Envelope.ID != "response-2" {
		t.Fatalf("ch2 got %s", got2.trigger.Envelope.ID)
	}
}

func TestDispatchToolResponseAfterUnregisterIsQuarantined(t *testing.T) {
	b := &Bridge{}
	_ = b.registerPendingTool("tool-req-canceled")
	b.unregisterPendingTool("tool-req-canceled")
	if !b.dispatchToolResponse(toolTrigger(message.KindResponse, "late-response", "tool-req-canceled")) {
		t.Fatal("late response after unregister should be quarantined")
	}
	b.pendingMu.Lock()
	_, stillPending := b.pendingTools["tool-req-canceled"]
	b.pendingMu.Unlock()
	if stillPending {
		t.Fatal("pending tool entry leaked after unregister")
	}
}

func TestDispatchToolResponseDuplicateRedeliveryIsQuarantined(t *testing.T) {
	b := &Bridge{}
	ch := b.registerPendingTool("tool-req-redelivered")

	if !b.dispatchToolResponse(toolTrigger(message.KindResponse, "response-1", "tool-req-redelivered")) {
		t.Fatal("first response was not dispatched")
	}
	select {
	case <-ch:
	case <-time.After(time.Second):
		t.Fatal("pending tool did not receive first response")
	}
	if !b.dispatchToolResponse(toolTrigger(message.KindResponse, "response-1-redelivery", "tool-req-redelivered")) {
		t.Fatal("duplicate terminal response should be quarantined")
	}
}

func toolTrigger(kind message.Kind, id, parentID string) TriggerPayload {
	return TriggerPayload{
		Envelope: message.Envelope{
			ID:            message.ID(id),
			ChannelID:     "ch-kimi",
			Type:          "xhs.publish",
			Kind:          kind,
			Sender:        message.Sender{Kind: actor.KindTool, ID: "tool:xhs"},
			ParentID:      message.ID(parentID),
			CorrelationID: message.ID(parentID),
			Payload:       []byte(`{"status":"completed"}`),
		},
		CorrelationID: message.ID(parentID),
	}
}

// toolTriggerWithStatus is the provisional-aware variant of toolTrigger.
// status is written verbatim into payload.status so tests can exercise
// every position of the proto-layer0 §2.5.1 closed-set lattice: Layer 1
// final (completed/failed), Layer 2 core provisional (processing,
// queued, …), and Layer 3 business namespace (xhs.login_queued, …).
func toolTriggerWithStatus(kind message.Kind, id, parentID, status string) TriggerPayload {
	tp := toolTrigger(kind, id, parentID)
	tp.Envelope.Payload = []byte(`{"status":"` + status + `"}`)
	return tp
}

// TestDispatchToolResponseProvisionalDoesNotResolveFuture asserts that
// kind=response envelopes carrying a Layer 2 provisional status
// (`processing` in the closed core set per proto-layer0 §2.5.1) are
// quarantined from the LLM trigger stream but DO NOT close the pending
// tool entry. A subsequent final (`completed`) resolves the future as
// usual. This is the v1 provisional behaviour from
// response-multitype-refactor.md §3.4 D.
func TestDispatchToolResponseProvisionalDoesNotResolveFuture(t *testing.T) {
	b := &Bridge{}
	ch := b.registerPendingTool("tool-req-1")
	defer b.unregisterPendingTool("tool-req-1")

	// Provisional `processing` — Layer 2 core. Should be quarantined
	// (returns true) but neither push to ch nor close it.
	if !b.dispatchToolResponse(toolTriggerWithStatus(message.KindResponse, "response-prov", "tool-req-1", "processing")) {
		t.Fatal("provisional response was not quarantined")
	}
	select {
	case got, ok := <-ch:
		if !ok {
			t.Fatal("pending tool channel closed by provisional response")
		}
		t.Fatalf("provisional response leaked into pending tool channel: %v", got.trigger.Envelope.ID)
	case <-time.After(50 * time.Millisecond):
		// expected — pending tool stays parked.
	}
	b.pendingMu.Lock()
	_, stillPending := b.pendingTools["tool-req-1"]
	b.pendingMu.Unlock()
	if !stillPending {
		t.Fatal("pending tool entry erased by provisional response (should persist until final)")
	}

	// Final `completed` — Layer 1. Should resolve the future and close
	// the channel as the legacy single-response path did.
	if !b.dispatchToolResponse(toolTriggerWithStatus(message.KindResponse, "response-final", "tool-req-1", "completed")) {
		t.Fatal("final response was not dispatched")
	}
	select {
	case got, ok := <-ch:
		if !ok {
			t.Fatal("final response closed channel before sending payload")
		}
		if got.trigger.Envelope.ID != "response-final" {
			t.Fatalf("future resolved with id=%s want response-final", got.trigger.Envelope.ID)
		}
	case <-time.After(time.Second):
		t.Fatal("future did not resolve on final response")
	}
}

// TestDispatchToolResponseLayer3ProvisionalNamespace asserts that the
// Layer 3 business namespace provisional pattern (`<adapter>.<name>`,
// e.g. `xhs.login_queued`) is treated identically to Layer 2 core
// provisional — keeps the pending entry alive. proto-layer0 §2.5.1
// places anything matching `<ns>.<name>` outside the Layer 1 final
// closed set, so it must not resolve the LLM's future.
func TestDispatchToolResponseLayer3ProvisionalNamespace(t *testing.T) {
	b := &Bridge{}
	ch := b.registerPendingTool("tool-req-l3")
	defer b.unregisterPendingTool("tool-req-l3")

	if !b.dispatchToolResponse(toolTriggerWithStatus(message.KindResponse, "response-l3", "tool-req-l3", "xhs.login_queued")) {
		t.Fatal("Layer 3 provisional was not quarantined")
	}
	select {
	case got := <-ch:
		t.Fatalf("Layer 3 provisional leaked into pending channel: %s", got.trigger.Envelope.ID)
	case <-time.After(50 * time.Millisecond):
	}
	b.pendingMu.Lock()
	_, stillPending := b.pendingTools["tool-req-l3"]
	b.pendingMu.Unlock()
	if !stillPending {
		t.Fatal("Layer 3 provisional erased pending entry")
	}
}

// TestDispatchToolResponseProvisionalStatusVariants exercises every
// Layer 2 core provisional status from proto-layer0 §2.5.1 closed set:
// received / queued / processing / deferred / unavailable. All five
// must quarantine without closing the pending future.
func TestDispatchToolResponseProvisionalStatusVariants(t *testing.T) {
	for _, status := range []string{"received", "queued", "processing", "deferred", "unavailable"} {
		status := status
		t.Run(status, func(t *testing.T) {
			b := &Bridge{}
			parent := message.ID("tool-req-" + status)
			ch := b.registerPendingTool(parent)
			defer b.unregisterPendingTool(parent)

			if !b.dispatchToolResponse(toolTriggerWithStatus(message.KindResponse, "response-"+status, parent.String(), status)) {
				t.Fatalf("provisional %q not quarantined", status)
			}
			select {
			case got := <-ch:
				t.Fatalf("provisional %q leaked into pending channel: %s", status, got.trigger.Envelope.ID)
			case <-time.After(20 * time.Millisecond):
			}
			b.pendingMu.Lock()
			_, ok := b.pendingTools[parent]
			b.pendingMu.Unlock()
			if !ok {
				t.Fatalf("provisional %q erased pending entry", status)
			}
		})
	}
}

// TestDispatchToolResponseFinalFailedResolvesFuture asserts the Layer 1
// `failed` status (the other half of the closed final set alongside
// `completed`) also resolves the pending future. Without this the LLM
// would wait until F3 timeout on every error path.
func TestDispatchToolResponseFinalFailedResolvesFuture(t *testing.T) {
	b := &Bridge{}
	ch := b.registerPendingTool("tool-req-fail")
	defer b.unregisterPendingTool("tool-req-fail")

	if !b.dispatchToolResponse(toolTriggerWithStatus(message.KindResponse, "response-fail", "tool-req-fail", "failed")) {
		t.Fatal("failed final response not dispatched")
	}
	select {
	case got, ok := <-ch:
		if !ok {
			t.Fatal("channel closed without payload")
		}
		if got.trigger.Envelope.ID != "response-fail" {
			t.Fatalf("future resolved with id=%s", got.trigger.Envelope.ID)
		}
	case <-time.After(time.Second):
		t.Fatal("failed final did not resolve future")
	}
}
