package channelhost_test

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/wanpengxie/ActOS/kernel/actor"
	"github.com/wanpengxie/ActOS/kernel/message"

	"github.com/wanpengxie/ActOS/runtime/harness"
	"github.com/wanpengxie/ActOS/server/channelhost"
)

// TestClosureScan_CallerScopedUnansweredTimeout proves closure author #2 is WIRED
// end-to-end: an expired pending request (no receiver ever answers) is closed by
// a caller-authored unanswered_timeout terminal — through the REAL home write
// chain (harness 9 steps + sqlite truth + Step 8 callerSelfClose authorisation).
func TestClosureScan_CallerScopedUnansweredTimeout(t *testing.T) {
	var now int64 = 1_000_000
	clock := func() int64 { return atomic.LoadInt64(&now) }

	ctx := context.Background()
	home, err := channelhost.New(ctx, channelhost.Config{ChannelID: "ch", DBPath: ":memory:", NowMs: clock})
	if err != nil {
		t.Fatal(err)
	}

	// Register caller(agent) + receiver(tool) + type.
	if err := home.Registry().Insert(ctx, actor.Record{ID: "caller", Kind: actor.KindAgent}); err != nil {
		t.Fatal(err)
	}
	if err := home.Registry().Insert(ctx, actor.Record{ID: "tool1", Kind: actor.KindTool, Binding: actor.BindingEmbedded}); err != nil {
		t.Fatal(err)
	}
	if _, err := home.TypeRegistry().Upsert(ctx, message.TypeRow{
		Type: "x.do", HandlerActorID: "tool1", HandlerBinding: actor.BindingEmbedded,
		MaxPendingMs: 500, AllowedKinds: []message.Kind{message.KindEvent, message.KindRequest, message.KindResponse},
	}); err != nil {
		t.Fatal(err)
	}

	// Write a request through the REAL chain (caller-authenticated ctx).
	exp := now + 500
	req := &message.Envelope{
		ID: "req-1", TS: now, ChannelID: "ch", Kind: message.KindRequest, Type: "x.do",
		Sender:   message.Sender{Kind: actor.KindAgent, ID: "caller"},
		Audience: message.Audience{"tool1"}, Payload: []byte("{}"),
		ExpiresAt: &exp,
	}
	cctx := harness.CtxWithCaller(ctx, harness.CallerContext{ActorID: "caller", ChannelID: "ch", AllowProvidedSenderKind: true})
	res, err := home.Chain().Write(cctx, req)
	if err != nil || res.RejectReason != "" {
		t.Fatalf("write request: err=%v reject=%s detail=%s", err, res.RejectReason, res.RejectDetail)
	}

	// No receiver answers; advance past expires_at; run the closure scan.
	atomic.StoreInt64(&now, exp+1)
	home.ClosurePass(ctx)

	// Truth must now carry a caller-authored unanswered_timeout final.
	has, err := home.Messages().HasFinalResponse(ctx, "ch", "req-1")
	if err != nil {
		t.Fatal(err)
	}
	if !has {
		t.Fatal("no final response after closure scan — author#2 caller-scoped timeout NOT wired (request hangs forever)")
	}
	sender, ok, err := home.Messages().FinalResponseSender(ctx, "ch", "req-1")
	if err != nil || !ok {
		t.Fatalf("final sender lookup: ok=%v err=%v", ok, err)
	}
	if sender != "caller" {
		t.Fatalf("final response sender=%s, want caller (caller-scoped author#2)", sender)
	}
}
