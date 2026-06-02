package host

import (
	"context"
	"testing"
	"time"

	"github.com/wanpengxie/ActOS/kernel/actor"
	"github.com/wanpengxie/ActOS/kernel/message"
	"github.com/wanpengxie/ActOS/lib/agentactor"
)

// TestSpawnAgent_RoutesRequestToTrigger proves the agent-cell hosting mechanism
// is WIRED (agentactor was zero-constructed before): SpawnAgent builds an
// AgentActor cell, and an inbound request routes to its worker-session trigger on
// the cell goroutine. The trigger is the runtime/workerhost backend seam; here a
// stand-in captures the request to prove the routing half.
func TestSpawnAgent_RoutesRequestToTrigger(t *testing.T) {
	h := New(nil, nil)
	defer h.Stop()

	got := make(chan message.ID, 1)
	trigger := func(_ context.Context, env *message.Envelope) error {
		got <- env.ID
		return nil
	}
	h.SpawnAgent("agent1", "ch", agentactor.TriggerFunc(trigger))

	_ = h.Cells().Deliver(context.Background(), []actor.ActorID{"agent1"},
		&message.Envelope{
			ID: "areq", ChannelID: "ch", Kind: message.KindRequest, Type: "agent.do",
			Sender: message.Sender{Kind: actor.KindAgent, ID: "caller"}, Audience: message.Audience{"agent1"},
		})

	select {
	case id := <-got:
		if id != "areq" {
			t.Fatalf("trigger got request %s, want areq", id)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("agent cell never routed the request to its trigger — SpawnAgent/agentactor not wired")
	}
}
