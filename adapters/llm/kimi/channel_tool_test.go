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
	out := b.routeTriggers(ctx, in)

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
			Sender:        message.Sender{Kind: actor.KindTool, ID: "tool:xhs-adapter"},
			ParentID:      message.ID(parentID),
			CorrelationID: message.ID(parentID),
			Payload:       []byte(`{"status":"completed"}`),
		},
		CorrelationID: message.ID(parentID),
	}
}
