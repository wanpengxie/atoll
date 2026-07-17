package home

import (
	"context"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/platform/internal/link"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/runtime/actorrt"
	"github.com/wanpengxie/atoll/runtime/storespec"
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
	h.portIndex[old.ID()] = homePortEntry{owner: link.PortOwner(1), inc: old}
	h.portIndex[successor.ID()] = homePortEntry{owner: link.PortOwner(2), inc: successor}

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

func TestValidateAttachmentShrinkRetiresRemovedPort(t *testing.T) {
	h := openWhiteboxHome(t)
	h.disablePoke.Store(true)
	ctx := context.Background()
	placement, _ := storespec.NewDaemonPlacement("daemon-shrink")
	declared, err := h.Declare(ctx, DeclareRequest{
		SourceDeclID: "decl:shrink", Principal: "shrink", Kind: actor.KindTool,
		Class: "shrink", Placement: placement, CreatedAt: time.Now().UnixMilli(),
	})
	if err != nil {
		t.Fatal(err)
	}
	id := declared.Row.ID
	_, _ = h.liveness.AcceptDelivery(id, &message.Envelope{Kind: message.KindRequest})
	h.reconcileDaemonIntent(ctx)
	intent := h.liveness.AttachmentIntent(id)
	inc, _, err := h.channel.Cells().SpawnIfAbsent(id, actor.KindTool, func(actorrt.Incarnation) actorrt.Actor { return portIndexActor{} })
	if err != nil {
		t.Fatal(err)
	}
	if verdict := h.liveness.Attach(id, intent.Ticket, intent.Version, inc, runtimeDeliveryCarrier{id: id, deliverer: h.channel.Deliverer()}); verdict != transitionApplied {
		t.Fatalf("attach=%v", verdict)
	}
	const owner = link.PortOwner(9)
	h.portIndex[id] = homePortEntry{owner: owner, inc: inc}
	if allowed, err := (homeDeclarationCoordinator{h: h}).ValidateAttachment(ctx, owner, "daemon-shrink", nil); err != nil || len(allowed) != 0 {
		t.Fatalf("shrink=(%v,%v)", allowed, err)
	}
	if _, live := h.channel.Cells().CurrentIncarnation(id); live {
		t.Fatal("removed declaration left old cell live")
	}
	standing, _ := h.liveness.WakeStanding(id)
	if standing.Occ != occDetached || standing.HasCarrier {
		t.Fatalf("removed declaration liveness=%+v", standing)
	}
}

func TestHomePortIndexTakeOwnerDoesNotCrossLinks(t *testing.T) {
	rt, _ := actorrt.New(actorrt.Config{})
	defer rt.StopAll()
	a, _, _ := rt.SpawnIfAbsent("tool:a", actor.KindTool, func(actorrt.Incarnation) actorrt.Actor { return portIndexActor{} })
	b, _, _ := rt.SpawnIfAbsent("tool:b", actor.KindTool, func(actorrt.Incarnation) actorrt.Actor { return portIndexActor{} })
	h := &Home{portIndex: map[actor.ActorID]homePortEntry{}}
	index := homePortIndex{h: h}
	h.portIndex[a.ID()] = homePortEntry{owner: link.PortOwner(1), inc: a}
	h.portIndex[b.ID()] = homePortEntry{owner: link.PortOwner(2), inc: b}
	got := index.TakeOwner(link.PortOwner(1))
	if len(got) != 1 || got[0] != a {
		t.Fatalf("TakeOwner(1)=%v want only a", got)
	}
	if survivor, ok := index.Take(link.PortOwner(2), b.ID()); !ok || survivor != b {
		t.Fatal("TakeOwner crossed into another link")
	}
}
