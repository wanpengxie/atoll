package channelhost_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/wanpengxie/ActOS/kernel/actor"
	"github.com/wanpengxie/ActOS/kernel/message"
	"github.com/wanpengxie/ActOS/runtime/harness"
	"github.com/wanpengxie/ActOS/server/channelhost"
)

type panicActor struct{}

func (panicActor) Receive(context.Context, *message.Envelope) error { panic("boom") }

// TestDeath_E2E_ReceiverUnavailable proves closure author #3 is WIRED through the
// REAL home chain: a hosted cell panics on dispatch → supervisor materialises a
// system-authored receiver_unavailable terminal in truth (not just Despawn).
func TestDeath_E2E_ReceiverUnavailable(t *testing.T) {
	var now int64 = 1_000_000
	clock := func() int64 { return atomic.LoadInt64(&now) }
	ctx := context.Background()
	home, err := channelhost.New(ctx, channelhost.Config{ChannelID: "ch", DBPath: ":memory:", NowMs: clock})
	if err != nil {
		t.Fatal(err)
	}
	if err := home.Registry().Insert(ctx, actor.Record{ID: "caller", Kind: actor.KindAgent}); err != nil {
		t.Fatal(err)
	}
	if err := home.Registry().Insert(ctx, actor.Record{ID: "worker", Kind: actor.KindTool, Binding: actor.BindingEmbedded}); err != nil {
		t.Fatal(err)
	}
	if _, err := home.TypeRegistry().Upsert(ctx, message.TypeRow{
		Type: "x.do", HandlerActorID: "worker", HandlerBinding: actor.BindingEmbedded,
		MaxPendingMs: 5000, AllowedKinds: []message.Kind{message.KindEvent, message.KindRequest, message.KindResponse},
	}); err != nil {
		t.Fatal(err)
	}
	// Spawn a business cell that panics on Receive.
	home.Channel().Cells().Spawn("worker", panicActor{})

	exp := now + 5000
	req := &message.Envelope{
		ID: "r1", TS: now, ChannelID: "ch", Kind: message.KindRequest, Type: "x.do",
		Sender:   message.Sender{Kind: actor.KindAgent, ID: "caller"},
		Audience: message.Audience{"worker"}, Payload: []byte("{}"), ExpiresAt: &exp,
	}
	cctx := harness.CtxWithCaller(ctx, harness.CallerContext{ActorID: "caller", ChannelID: "ch", AllowProvidedSenderKind: true})
	dres, derr := home.Dispatch(cctx, req)
	if derr != nil || dres.RejectReason != "" {
		t.Fatalf("dispatch: err=%v reject=%s detail=%s", derr, dres.RejectReason, dres.RejectDetail)
	}

	// Cell panics → OnDeath materialises receiver_unavailable. Wait for it.
	deadline := time.Now().Add(2 * time.Second)
	var has bool
	for time.Now().Before(deadline) {
		has, _ = home.Messages().HasFinalResponse(ctx, "ch", "r1")
		if has {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if !has {
		t.Fatal("cell panicked but NO terminal in truth — death signal black hole (author#3 not wired through real chain)")
	}
	sender, ok, err := home.Messages().FinalResponseSender(ctx, "ch", "r1")
	if err != nil || !ok || sender != actor.SystemActorID {
		t.Fatalf("death terminal sender=%s ok=%v err=%v, want system (substrate-death author#3)", sender, ok, err)
	}
}
