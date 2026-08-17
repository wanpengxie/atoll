package base

import (
	"testing"

	"github.com/wanpengxie/atoll/drivers/agents/runtimeproto"
)

func TestToolPressureDoesNotBlockLifecycleEvent(t *testing.T) {
	q := newLoopInbox(1)
	port := &runtimePort{queue: q}
	for i := 0; i < q.capacity+1; i++ {
		port.Tool("turn", runtimeproto.ToolEvent{CallID: "tool"})
	}
	port.TurnEnded("turn", runtimeproto.TurnStatusOK, "done", "", runtimeproto.TurnUsage{})

	for i := 0; i < q.capacity; i++ {
		fact, ok := q.pop()
		if !ok {
			t.Fatalf("tool observation %d missing", i)
		}
		event, ok := fact.(runtimeEvent)
		if !ok || event.kind != evTool {
			t.Fatalf("fact %d=%T/%+v want Tool", i, fact, event)
		}
	}
	fact, ok := q.pop()
	if !ok {
		t.Fatal("lifecycle event missing")
	}
	event, ok := fact.(runtimeEvent)
	if !ok || event.kind != evTurnEnded {
		t.Fatalf("final fact=%T/%+v want TurnEnded", fact, event)
	}
	port.mu.Lock()
	sealed := port.sealed
	port.mu.Unlock()
	if sealed {
		t.Fatal("tool pressure sealed the Runtime events port")
	}
}
