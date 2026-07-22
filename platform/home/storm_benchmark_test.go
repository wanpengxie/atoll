package home

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/runtime/actorrt"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

type stormCarrier struct{ accepted *atomic.Uint64 }

func (c stormCarrier) Enqueue(*message.Envelope) error {
	if c.accepted != nil {
		c.accepted.Add(1)
	}
	return nil
}

func openStormHome(b *testing.B) *Home {
	b.Helper()
	h, err := Open(Config{
		ChannelID: "actor-storm", DBPath: filepath.Join(b.TempDir(), "channel.sqlite"),
		CompositionResolver:  emptyCompositionResolver{},
		IntroductionResolver: fixedIntroductionResolver{kind: actor.KindAgent},
		ReconcileInterval:    time.Hour, Bootstrap: true,
	})
	if err != nil {
		b.Fatal(err)
	}
	h.reconcileStop()
	<-h.reconcileDone
	b.Cleanup(func() { _ = h.closeInternal("test") })
	return h
}

func BenchmarkActorStorm(b *testing.B) {
	b.Run("ForkAdmission", func(b *testing.B) {
		h := openStormHome(b)
		ctx := context.Background()
		parent, err := h.admit(ctx, actor.KindHuman, "storm-fork-parent")
		if err != nil {
			b.Fatal(err)
		}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if _, err := h.forkAdmission(ctx, parent, 1, actorrt.ForkSpec{
				Kind: actor.KindAgent, Class: "storm-worker", NameHint: "worker",
			}, fmt.Sprintf("fork-%d", i)); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("DeepSubtreeRemove", func(b *testing.B) {
		h := openStormHome(b)
		ctx := context.Background()
		parent, err := h.admit(ctx, actor.KindHuman, "storm-remove-parent")
		if err != nil {
			b.Fatal(err)
		}
		const depth = 12
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			sponsor := parent
			var root actor.ActorID
			for d := 0; d < depth; d++ {
				child, forkErr := h.forkAdmission(ctx, sponsor, 1, actorrt.ForkSpec{
					Kind: actor.KindAgent, Class: "storm-node", NameHint: "node",
				}, fmt.Sprintf("tree-%d-%d", i, d))
				if forkErr != nil {
					b.Fatal(forkErr)
				}
				if d == 0 {
					root = child
				}
				sponsor = child
			}
			if err := h.systemEndHandle().End(ctx, root, "storm"); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("AuthorGateRead", func(b *testing.B) {
		h := openStormHome(b)
		ctx := context.Background()
		id, err := h.admit(ctx, actor.KindHuman, "storm-gate-author")
		if err != nil {
			b.Fatal(err)
		}
		stamp := storespec.AuthorStamp{ID: id, BirthVersion: 1}
		b.ReportAllocs()
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				if verdict, err := h.controlIndex.CheckAuthor(ctx, stamp); err != nil || verdict != storespec.AuthorOK {
					b.Fatalf("gate=(%v,%v)", verdict, err)
				}
			}
		})
	})

	b.Run("DeclarationApply", func(b *testing.B) {
		h := openStormHome(b)
		ctx := context.Background()
		declared, err := h.declare(ctx, DeclareRequest{
			SourceDeclID: "storm:apply", Kind: actor.KindAgent,
			Class: "storm-v1", Placement: storespec.NewServerPlacement(),
			TIdle: int64(time.Hour / time.Millisecond), CreatedAt: time.Now().UnixMilli(),
		})
		if err != nil {
			b.Fatal(err)
		}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			config := json.RawMessage(fmt.Sprintf(`{"version":%d}`, i+2))
			result, err := h.opEntry.applyResolvedDeclaration(ctx, declared.Row.ID, declared.Row.SourceDeclID, declared.Row.Class, config)
			if err != nil || result.Status != storespec.DeclarationApplied {
				b.Fatal(err)
			}
		}
	})

	b.Run("AnchorRedelivery", func(b *testing.B) {
		h := openStormHome(b)
		ctx := context.Background()
		parent, err := h.admit(ctx, actor.KindHuman, "storm-anchor-parent")
		if err != nil {
			b.Fatal(err)
		}
		child, err := h.forkAdmission(ctx, parent, 1, actorrt.ForkSpec{
			Kind: actor.KindAgent, Class: "storm-anchor",
		}, "storm-anchor-child")
		if err != nil {
			b.Fatal(err)
		}
		var accepted atomic.Uint64
		ticket, verdict := h.liveness.BeginEnsure(child, 1)
		if verdict != transitionApplied || h.liveness.PublishLocal(child, ticket, noInc, stormCarrier{accepted: &accepted}) != transitionApplied {
			b.Fatalf("publish anchor carrier: ticket=%q verdict=%v", ticket, verdict)
		}
		now := time.Now().UnixMilli()
		expires := time.Now().Add(time.Hour).UnixMilli()
		res, err := h.systemPen.Write(ctx, &message.Envelope{
			ID: "storm-open-request", Kind: message.KindRequest, Type: "storm.work",
			Audience: message.Audience{child}, Visibility: message.VisibilitySystem,
			TS: now, TSReceived: now, ExpiresAt: &expires,
		})
		if err != nil || !res.Accepted() {
			b.Fatalf("seed request=(%+v,%v)", res, err)
		}
		deadline := time.Now().Add(time.Second)
		for accepted.Load() == 0 && time.Now().Before(deadline) {
			time.Sleep(time.Millisecond)
		}
		if accepted.Load() == 0 {
			b.Fatal("seed request was not delivered")
		}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			h.redeliverOpenRequests(ctx, child)
		}
	})
}
