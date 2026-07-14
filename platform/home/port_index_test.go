package home

import (
	"context"
	"testing"

	"github.com/wanpengxie/atoll/platform/internal/link"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/runtime/actorrt"
)

type portIndexActor struct{}

func (portIndexActor) Receive(context.Context, *message.Envelope) error { return nil }

func TestHomePortIndexReplacementAndLateExitArePointerConditional(t *testing.T) {
	rt, _ := actorrt.New(actorrt.Config{})
	defer rt.StopAll()
	old, _, err := rt.SpawnIfAbsent("tool:a", actor.KindTool, func(actorrt.Incarnation) actorrt.Actor { return portIndexActor{} })
	if err != nil {
		t.Fatal(err)
	}
	rt.Despawn(old)
	successor, _, err := rt.SpawnIfAbsent("tool:a", actor.KindTool, func(actorrt.Incarnation) actorrt.Actor { return portIndexActor{} })
	if err != nil {
		t.Fatal(err)
	}
	h := &Home{portIndex: map[actor.ActorID]homePortEntry{}}
	index := homePortIndex{h: h}
	index.Register(link.PortOwner(1), old)
	index.Register(link.PortOwner(2), successor)

	// A predecessor's late terminal callback cannot remove its same-id successor.
	index.Remove(link.PortOwner(1), old)
	if _, ok := index.Take(link.PortOwner(1), old.ID()); ok {
		t.Fatal("stale owner took successor")
	}
	got, ok := index.Take(link.PortOwner(2), successor.ID())
	if !ok || got != successor {
		t.Fatalf("successor missing after predecessor exit: got=%v ok=%v", got.ID(), ok)
	}
}

func TestHomePortIndexTakeOwnerDoesNotCrossLinks(t *testing.T) {
	rt, _ := actorrt.New(actorrt.Config{})
	defer rt.StopAll()
	a, _, _ := rt.SpawnIfAbsent("tool:a", actor.KindTool, func(actorrt.Incarnation) actorrt.Actor { return portIndexActor{} })
	b, _, _ := rt.SpawnIfAbsent("tool:b", actor.KindTool, func(actorrt.Incarnation) actorrt.Actor { return portIndexActor{} })
	h := &Home{portIndex: map[actor.ActorID]homePortEntry{}}
	index := homePortIndex{h: h}
	index.Register(link.PortOwner(1), a)
	index.Register(link.PortOwner(2), b)
	got := index.TakeOwner(link.PortOwner(1))
	if len(got) != 1 || got[0] != a {
		t.Fatalf("TakeOwner(1)=%v want only a", got)
	}
	if survivor, ok := index.Take(link.PortOwner(2), b.ID()); !ok || survivor != b {
		t.Fatal("TakeOwner crossed into another link")
	}
}
