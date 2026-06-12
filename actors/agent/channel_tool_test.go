package agent

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/wanpengxie/ActOS/protocol/actor"
	"github.com/wanpengxie/ActOS/protocol/message"
	"github.com/wanpengxie/ActOS/runtime/harness"
)

// stubWriter is an in-package harness.Writer double.
type stubWriter struct {
	mu      sync.Mutex
	written []message.Envelope
	err     error
	reject  harness.HarnessRejectReason
}

func (w *stubWriter) Write(_ context.Context, env *message.Envelope) (harness.WriteResult, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.err != nil {
		return harness.WriteResult{}, w.err
	}
	if w.reject != "" {
		return harness.WriteResult{RejectReason: w.reject}, nil
	}
	w.written = append(w.written, *env)
	return harness.WriteResult{MessageID: env.ID}, nil
}

func newToolTestBridge(t *testing.T, w *stubWriter) *Bridge {
	t.Helper()
	b, err := NewBridge(Config{APIKey: "k", Model: "m"}, "agent:tt", "ch-tt", w)
	if err != nil {
		t.Fatalf("NewBridge: %v", err)
	}
	return b
}

func TestEnqueueTurn_OverflowEvictsOldestAndNotes(t *testing.T) {
	w := &stubWriter{}
	b := newToolTestBridge(t, w)
	b.turnQ = make(chan turnItem, 2)

	for i := 0; i < 3; i++ {
		b.enqueueTurn(message.Envelope{
			ID: message.ID(fmt.Sprintf("e%d", i)), ChannelID: "ch-tt", Type: "human.text",
			Sender: message.Sender{Kind: actor.KindHuman, ID: "u"},
		})
	}
	// Oldest (e0) evicted; queue holds e1, e2.
	first := <-b.turnQ
	second := <-b.turnQ
	if first.env.ID != "e1" || second.env.ID != "e2" {
		t.Fatalf("queue order after eviction: %s, %s", first.env.ID, second.env.ID)
	}
	w.mu.Lock()
	noted := len(w.written)
	w.mu.Unlock()
	if noted != 1 {
		t.Fatalf("want 1 overflow note, got %d", noted)
	}
}

func TestExtractRuntimeContext_OutsideTurn(t *testing.T) {
	rc := extractRuntimeContext(context.Background())
	if rc.InTurn() {
		t.Fatal("zero context must not count as in-turn")
	}
}
