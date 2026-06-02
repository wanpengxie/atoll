package fleet

import (
	"context"
	"testing"

	"github.com/wanpengxie/ActOS/kernel/actor"
	"github.com/wanpengxie/ActOS/kernel/harness"
	"github.com/wanpengxie/ActOS/kernel/message"
	"github.com/wanpengxie/ActOS/wire/computebus"
)

// nopChain satisfies harness.Chain for a fleet that only routes (no truth here).
type nopChain struct{}

func (nopChain) Write(context.Context, *message.Envelope) (harness.WriteResult, error) {
	return harness.WriteResult{}, nil
}

// TestFleet_DeathFrameRoutesToHomeAndDropsActor proves the FrameDeath handler is
// no longer a no-op: a death frame from a compute (a) invokes the home收口 seam
// (onDeath, which materialises receiver_unavailable) and (b) drops the dead
// actor's routing entry so no further request is dispatched into the void.
func TestFleet_DeathFrameRoutesToHomeAndDropsActor(t *testing.T) {
	f := New(nopChain{}, "")
	conn := &computeConn{id: "c1"}
	f.register(conn, []actor.ActorID{"a1"})

	var closed actor.ActorID
	f.SetOnDeath(func(_ context.Context, a actor.ActorID) { closed = a })

	f.handleFrame(context.Background(), conn, computebus.Frame{
		Type:  computebus.FrameDeath,
		Death: &computebus.DeathFrame{Actor: "a1", Cause: "boom"},
	})

	if closed != "a1" {
		t.Fatalf("onDeath home收口 not invoked for dead actor (got %q) — FrameDeath still a no-op", closed)
	}
	if f.Dispatch("a1", &message.Envelope{}) {
		t.Fatal("dead actor still routable — dropActor not wired, requests dispatch into a dead cell")
	}
}
