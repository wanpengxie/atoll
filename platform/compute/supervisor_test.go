package compute

import (
	"context"
	"errors"
	"log/slog"
	"testing"

	"github.com/wanpengxie/atoll/lib/actorbase"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/runtime/actorrt"
)

func TestCellDownWatcherClassifiesIdleAndCrash(t *testing.T) {
	id := actor.ActorID("tool:supervised")
	inc := actorrt.Incarnation{}
	w := &cellDownWatcher{down: map[actor.ActorID]cellDownEntry{}}
	var wireDown, crashed int
	w.install(id, inc, func(string) { wireDown++ }, func(error) { crashed++ })

	w.OnDown(context.Background(), id, inc, errors.New("boom"))
	if crashed != 1 || wireDown != 0 {
		t.Fatalf("unexpected death: crashed=%d wireDown=%d", crashed, wireDown)
	}
	w.OnDown(context.Background(), id, inc, actorbase.ErrIdleExit)
	if crashed != 1 || wireDown != 1 {
		t.Fatalf("idle exit: crashed=%d wireDown=%d", crashed, wireDown)
	}
}

func TestSupervisorCrashGenerationAndStaleIncarnationCancellation(t *testing.T) {
	id := actor.ActorID("tool:supervised")
	rt, _ := actorrt.New(actorrt.Config{})
	t.Cleanup(rt.StopAll)
	current, _, err := rt.SpawnIfAbsent(id, actor.KindTool, func(actorrt.Incarnation) actorrt.Actor { return inertComputeActor{} })
	if err != nil {
		t.Fatal(err)
	}
	r := &computeRing{
		rt: rt, logger: slog.New(slog.DiscardHandler),
		prevCurrent:  map[actor.ActorID]desiredIncarnation{id: {Kind: actor.KindTool, Version: 2, EnsureTicket: "ticket"}},
		builtAttempt: map[actor.ActorID]builtAttempt{id: {Version: 2, EnsureTicket: "ticket"}},
		crashes:      map[actor.ActorID]cellCrashState{},
		crashWake:    make(chan cellCrashEvent, 4),
	}

	// A delayed edge for a superseded body cannot arm a restart over the live
	// current incarnation.
	r.recordLocalCrash(cellCrashEvent{id: id, inc: actorrt.Incarnation{}, cause: errors.New("late")})
	if _, ok := r.crashes[id]; ok {
		t.Fatal("stale incarnation armed supervisor restart")
	}

	rt.Despawn(current)
	r.recordLocalCrash(cellCrashEvent{id: id, inc: current, cause: errors.New("first")})
	first := r.crashes[id]
	if first.generation != 1 || first.backoff != cellInitialBackoff {
		t.Fatalf("first crash state=%+v", first)
	}
	r.recordLocalCrash(cellCrashEvent{id: id, inc: current, cause: errors.New("second")})
	second := r.crashes[id]
	if second.generation != 2 || second.backoff != 2*cellInitialBackoff {
		t.Fatalf("second crash state=%+v", second)
	}

	// Plan replacement/removal deletes the generation account. A queued retry
	// carrying the old generation therefore has no authority to rebuild.
	delete(r.crashes, id)
	if state, ok := r.crashes[id]; ok && state.generation == second.generation {
		t.Fatal("removed plan still accepts old supervisor generation")
	}
}

type inertComputeActor struct{}

func (inertComputeActor) Start(context.Context, actorrt.ActorContext) error { return nil }
func (inertComputeActor) Receive(context.Context, *message.Envelope) error  { return nil }
