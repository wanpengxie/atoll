package localdevice

import (
	"context"
	"testing"
	"time"

	"github.com/wanpengxie/ActOS/kernel/actor"
	"github.com/wanpengxie/ActOS/kernel/message"
	"github.com/wanpengxie/ActOS/runtime/actorrt"
)

// collectActor records envelopes it receives.
type collectActor struct {
	got chan *message.Envelope
}

func (c *collectActor) Receive(_ context.Context, env *message.Envelope) error {
	c.got <- env
	return nil
}

// Verify interface (compile-time check that collectActor is a valid Actor).
var _ actorrt.Actor = (*collectActor)(nil)

// Silence unused import warning for actor package.
var _ = actor.KindTool

// TestForwardAndTake proves ForwardFunc records requests and Take drains them.
func TestForwardAndTake(t *testing.T) {
	tr := New(nil)
	fwd := tr.ForwardFunc("tool1")

	id1, err := fwd(context.Background(), &message.Envelope{ID: "env-1"}, []byte("p1"))
	if err != nil {
		t.Fatalf("Forward 1: %v", err)
	}
	id2, err := fwd(context.Background(), &message.Envelope{ID: "env-2"}, []byte("p2"))
	if err != nil {
		t.Fatalf("Forward 2: %v", err)
	}
	if id1 == id2 {
		t.Fatalf("frame IDs should be unique, got %q and %q", id1, id2)
	}

	items := tr.Take()
	if len(items) != 2 {
		t.Fatalf("Take returned %d items, want 2", len(items))
	}
	if items[0].Envelope.ID != "env-1" {
		t.Fatalf("first item ID = %q, want env-1", items[0].Envelope.ID)
	}

	// Second take should be empty.
	if len(tr.Take()) != 0 {
		t.Fatal("second Take should be empty")
	}
}

// TestCallback proves Callback routes an envelope to the target actor via the
// Deliverer.
func TestCallback(t *testing.T) {
	rt, del, _ := actorrt.New(actorrt.Config{})
	defer rt.StopAll()

	ca := &collectActor{got: make(chan *message.Envelope, 1)}
	rt.Spawn("tool1", ca)

	tr := New(del)
	env := &message.Envelope{ID: "cb-1", Kind: message.KindResponse, Type: "test.cb"}
	if err := tr.Callback("tool1", env); err != nil {
		t.Fatalf("Callback: %v", err)
	}

	select {
	case got := <-ca.got:
		if got.ID != "cb-1" {
			t.Fatalf("received ID = %q, want cb-1", got.ID)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Callback envelope never delivered to cell")
	}
}

// TestCallback_NoDeliverer proves Callback returns error when no deliverer is
// wired.
func TestCallback_NoDeliverer(t *testing.T) {
	tr := New(nil)
	err := tr.Callback("tool1", &message.Envelope{})
	if err == nil {
		t.Fatal("expected error with nil deliverer")
	}
}

// TestForwardFunc_DifferentActors proves ForwardFunc scopes by actor.
func TestForwardFunc_DifferentActors(t *testing.T) {
	tr := New(nil)
	fwd1 := tr.ForwardFunc("tool1")
	fwd2 := tr.ForwardFunc("tool2")

	_, _ = fwd1(context.Background(), &message.Envelope{ID: "e1"}, nil)
	_, _ = fwd2(context.Background(), &message.Envelope{ID: "e2"}, nil)

	items := tr.Take()
	if len(items) != 2 {
		t.Fatalf("Take returned %d items, want 2", len(items))
	}
	if items[0].Self != "tool1" || items[1].Self != "tool2" {
		t.Fatalf("Self fields: %q, %q", items[0].Self, items[1].Self)
	}
}
