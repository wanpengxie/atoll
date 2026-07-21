package home

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/lib/actorbase"
	"github.com/wanpengxie/atoll/platform"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/runtime/actorrt"
)

type anchorResolver struct {
	births  atomic.Int32
	handled atomic.Int32
}

func (r *anchorResolver) BuildClass(channel.ID, actor.ActorID, string, json.RawMessage) (platform.ActorFactory, bool) {
	return platform.ActorFactory{Proc: actorbase.Def{New: func() (actorbase.Proc, error) {
		r.births.Add(1)
		return func(sys actorbase.Sys) error {
			for {
				msg, err := sys.Recv()
				if err != nil {
					return err
				}
				if msg.Kind == message.KindRequest {
					r.handled.Add(1)
				}
			}
		}, nil
	}}}, true
}

func TestCarrierHandoffAnchorRedeliveryIsOncePerIncarnation(t *testing.T) {
	resolver := &anchorResolver{}
	h, err := Open(Config{
		ChannelID: "anchor-redelivery", DBPath: filepath.Join(t.TempDir(), "channel.sqlite"),
		CompositionResolver: resolver,
		ReconcileInterval:   10 * time.Millisecond, Bootstrap: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = h.closeInternal("test") })
	ctx := context.Background()
	parent, err := h.admit(ctx, actor.KindHuman, "anchor-parent")
	if err != nil {
		t.Fatal(err)
	}
	child, err := h.forkAdmission(ctx, parent, 1, actorrt.ForkSpec{Kind: actor.KindAgent, Class: "anchor-worker"}, "anchor-child")
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	expires := now.Add(time.Hour).UnixMilli()
	result, err := h.systemPen.Write(ctx, &message.Envelope{
		ID: "anchor-request", Kind: message.KindRequest, Type: "work",
		Audience: message.Audience{child}, ExpiresAt: &expires,
		TS: now.UnixMilli(), TSReceived: now.UnixMilli(), Visibility: message.VisibilitySystem,
	})
	if err != nil || !result.Accepted() {
		t.Fatalf("write=(%+v,%v)", result, err)
	}
	waitHomeCondition(t, func() bool { return resolver.handled.Load() == 1 })
	first, ok := h.channel.Cells().CurrentIncarnation(child)
	if !ok {
		t.Fatal("child not live after anchor wake")
	}

	// Re-scanning against the same live serve ledger must not run the handler a
	// second time for the same request id.
	h.redeliverOpenRequests(ctx, child)
	time.Sleep(30 * time.Millisecond)
	if got := resolver.handled.Load(); got != 1 {
		t.Fatalf("same-incarnation redelivery reran handler %d times", got)
	}

	// The request is still open. After the carrier is replaced, the next single
	// anchor scan admits it into the successor and runs it again.
	h.channel.Cells().Despawn(first)
	h.pokeReconcile()
	waitHomeCondition(t, func() bool { return resolver.births.Load() >= 2 && resolver.handled.Load() == 2 })
}

func TestCarrierFullLeavesRequestOpenAndNextHandoffRedeliversOnlyRequest(t *testing.T) {
	h, err := Open(Config{
		ChannelID: "anchor-full-handoff", DBPath: filepath.Join(t.TempDir(), "channel.sqlite"),
		CompositionResolver: emptyCompositionResolver{},
		ReconcileInterval:   time.Hour, Bootstrap: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = h.closeInternal("test") })
	h.reconcileStop()
	<-h.reconcileDone

	ctx := context.Background()
	parent, err := h.admit(ctx, actor.KindHuman, "anchor-full-parent")
	if err != nil {
		t.Fatal(err)
	}
	child, err := h.forkAdmission(ctx, parent, 1, actorrt.ForkSpec{
		Kind: actor.KindAgent, Class: "anchor-full-worker",
	}, "anchor-full-child")
	if err != nil {
		t.Fatal(err)
	}
	full := &testCarrier{err: actorrt.ErrMailboxFull}
	ticket, verdict := h.liveness.BeginEnsure(child, 1, 0)
	if verdict != transitionApplied || h.liveness.PublishLocal(child, ticket, noInc, full) != transitionApplied {
		t.Fatalf("publish full carrier: ticket=%q verdict=%v", ticket, verdict)
	}

	now := time.Now().UnixMilli()
	expires := time.Now().Add(time.Hour).UnixMilli()
	request := &message.Envelope{
		ID: "full-open-request", Kind: message.KindRequest, Type: "work",
		Audience: message.Audience{child}, Visibility: message.VisibilitySystem,
		TS: now, TSReceived: now, ExpiresAt: &expires,
	}
	event := &message.Envelope{
		ID: "full-dropped-event", Kind: message.KindEvent, Type: "notice",
		Audience: message.Audience{child}, Visibility: message.VisibilitySystem,
		TS: now, TSReceived: now,
	}
	for _, env := range []*message.Envelope{request, event} {
		res, writeErr := h.systemPen.Write(ctx, env)
		if writeErr != nil || !res.Accepted() {
			t.Fatalf("write %s=(%+v,%v)", env.ID, res, writeErr)
		}
	}
	waitHomeCondition(t, func() bool {
		full.mu.Lock()
		defer full.mu.Unlock()
		return len(full.envs) == 2
	})
	state, _ := h.liveness.stateForTest(child)
	if state.occ != occRunning || state.dirty {
		t.Fatalf("carrier rejection mutated liveness: %+v", state)
	}
	open, err := h.cs.Query.OpenRequestsForActor(ctx, child)
	if err != nil || len(open) != 1 || open[0].Envelope.ID != request.ID {
		t.Fatalf("open requests after full=(%+v,%v)", open, err)
	}

	if _, verdict := h.liveness.Retire(child, false); verdict != transitionApplied {
		t.Fatalf("retire verdict=%v", verdict)
	}
	nextTicket, verdict := h.liveness.BeginEnsure(child, 1, 0)
	next := &testCarrier{}
	if verdict != transitionApplied || h.liveness.PublishLocal(child, nextTicket, noInc, next) != transitionApplied {
		t.Fatalf("publish successor: ticket=%q verdict=%v", nextTicket, verdict)
	}
	h.redeliverOpenRequests(ctx, child)
	next.mu.Lock()
	defer next.mu.Unlock()
	if len(next.envs) != 1 || next.envs[0].ID != request.ID {
		t.Fatalf("successor anchor deliveries=%+v; event must not be retained", next.envs)
	}
	if !errors.Is(full.err, actorrt.ErrMailboxFull) {
		t.Fatalf("test carrier lost full verdict: %v", full.err)
	}
}
