package host

import (
	"context"
	"testing"
	"time"

	"github.com/wanpengxie/ActOS/kernel/actor"
	"github.com/wanpengxie/ActOS/kernel/message"
)

// panicActor dies on the first envelope it receives — the substrate catches the
// panic and routes it to the supervisor (Host) as a death signal.
type panicActor struct{}

func (panicActor) Receive(_ context.Context, _ *message.Envelope) error { panic("boom") }

// TestHostSupervisor_PropagatesDeathUpTheWire proves the compute-side death seam
// is WIRED: Host is the actorrt.Supervisor for its cells, so when a hosted cell
// panics the substrate calls Host.OnDeath, which reports the death UP via
// DeathFunc (the homelink FrameDeath sender). Without this seam a compute cell
// death is invisible to the home and the waiting caller hangs forever.
func TestHostSupervisor_PropagatesDeathUpTheWire(t *testing.T) {
	deaths := make(chan actor.ActorID, 1)
	h := New(nil, func(a actor.ActorID, _ string) { deaths <- a })
	defer h.Stop()

	h.Cells().Spawn("doomed", panicActor{})
	_ = h.Cells().Deliver(context.Background(), []actor.ActorID{"doomed"},
		&message.Envelope{Kind: message.KindRequest, Type: "x.kill", ChannelID: "ch"})

	select {
	case got := <-deaths:
		if got != "doomed" {
			t.Fatalf("DeathFunc reported actor=%s, want doomed", got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Host.OnDeath never propagated the death UP — compute cell death is a black hole (supervisor not wired)")
	}
}
