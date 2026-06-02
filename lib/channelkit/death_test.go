package channelkit_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/wanpengxie/ActOS/kernel/actor"
	khrn "github.com/wanpengxie/ActOS/kernel/harness"
	"github.com/wanpengxie/ActOS/kernel/message"
	"github.com/wanpengxie/ActOS/lib/channelkit"
)

// panicActor panics on Receive — simulates a cell dying abnormally.
type panicActor struct{}

func (panicActor) Receive(context.Context, *message.Envelope) error { panic("boom") }

// fakeChain records written terminals (implements kernel/harness.Chain).
type fakeChain struct {
	mu      sync.Mutex
	written []*message.Envelope
}

func (f *fakeChain) Write(_ context.Context, env *message.Envelope) (khrn.WriteResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.written = append(f.written, env)
	return khrn.WriteResult{MessageID: env.ID}, nil
}
func (f *fakeChain) count() int { f.mu.Lock(); defer f.mu.Unlock(); return len(f.written) }

// fakeOpenReqs returns canned in-flight requests for a dead actor.
type fakeOpenReqs struct{ reqs []message.Envelope }

func (f fakeOpenReqs) OpenRequestsForActor(context.Context, actor.ActorID, int) ([]message.Envelope, error) {
	return f.reqs, nil
}

// TestOnDeath_MaterialisesReceiverUnavailable proves closure author #3 is WIRED:
// a cell dying → supervisor writes a system-authored receiver_unavailable
// terminal for the dead actor's in-flight request (NOT just Despawn).
func TestOnDeath_MaterialisesReceiverUnavailable(t *testing.T) {
	req := message.Envelope{
		ID:        "req-1",
		ChannelID: "ch",
		Kind:      message.KindRequest,
		Type:      "x.do",
		Sender:    message.Sender{Kind: actor.KindAgent, ID: "caller"},
		Audience:  message.Audience{"worker"},
	}
	fc := &fakeChain{}
	ch := channelkit.New(channelkit.Config{
		ChannelID:    "ch",
		Chain:        fc,
		OpenRequests: fakeOpenReqs{reqs: []message.Envelope{req}},
		Clock:        time.Now,
	})
	ch.Cells().Spawn("worker", panicActor{})

	// Deliver a request → Receive panics → cell death → OnDeath.
	_ = ch.Cells().Deliver(context.Background(), []actor.ActorID{"worker"},
		&message.Envelope{ID: "trigger", ChannelID: "ch", Kind: message.KindRequest, Type: "x.do",
			Sender: message.Sender{Kind: actor.KindAgent, ID: "caller"}, Audience: message.Audience{"worker"}})

	// Wait for OnDeath to materialise the terminal.
	deadline := time.Now().Add(2 * time.Second)
	for fc.count() == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if fc.count() == 0 {
		t.Fatal("death signal NOT materialised — OnDeath wrote no terminal (black hole regression)")
	}
	term := fc.written[0]
	if term.Kind != message.KindResponse {
		t.Fatalf("terminal kind=%s, want response", term.Kind)
	}
	if term.ParentID != "req-1" {
		t.Fatalf("terminal parent_id=%s, want req-1", term.ParentID)
	}
	if term.Sender.ID != actor.SystemActorID {
		t.Fatalf("terminal sender=%s, want system (substrate-death author)", term.Sender.ID)
	}
	// payload must carry status=failed + reason=receiver_unavailable
	if !contains(string(term.Payload), "receiver_unavailable") || !contains(string(term.Payload), "failed") {
		t.Fatalf("terminal payload=%s, want status=failed reason=receiver_unavailable", term.Payload)
	}
}

func contains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
