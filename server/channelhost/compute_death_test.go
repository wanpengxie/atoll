package channelhost_test

import (
	"context"
	"testing"

	"github.com/wanpengxie/ActOS/kernel/actor"
	"github.com/wanpengxie/ActOS/kernel/message"

	"github.com/wanpengxie/ActOS/runtime/harness"
	"github.com/wanpengxie/ActOS/server/channelhost"
)

// TestMaterialiseComputeDeath_ReceiverUnavailable proves the death signal CROSSES
// THE WIRE end-to-end on the home side: when an attached compute reports a cell
// death (the fleet calls MaterialiseComputeDeath — exactly what FrameDeath does),
// the home — which owns truth the compute lacks — writes a SYSTEM-authored
// receiver_unavailable terminal for the dead actor's in-flight request. The
// request is NEVER expired, so this is the death path, not the author#2 timeout:
// the distinguishing signal is sender==system (substrate author, harness Step 8).
func TestMaterialiseComputeDeath_ReceiverUnavailable(t *testing.T) {
	clock := func() int64 { return 1_000_000 } // frozen — request never expires

	ctx := context.Background()
	home, err := channelhost.New(ctx, channelhost.Config{ChannelID: "ch", DBPath: ":memory:", NowMs: clock})
	if err != nil {
		t.Fatal(err)
	}

	// Register caller(agent) + a compute-hosted receiver(tool) + the type.
	if err := home.Registry().Insert(ctx, actor.Record{ID: "caller", Kind: actor.KindAgent}); err != nil {
		t.Fatal(err)
	}
	if err := home.Registry().Insert(ctx, actor.Record{ID: "remote-tool", Kind: actor.KindTool, Binding: actor.BindingRuntimeOutbound}); err != nil {
		t.Fatal(err)
	}
	if _, err := home.TypeRegistry().Upsert(ctx, message.TypeRow{
		Type: "x.remote", HandlerActorID: "remote-tool", HandlerBinding: actor.BindingRuntimeOutbound,
		MaxPendingMs: 60_000, AllowedKinds: []message.Kind{message.KindEvent, message.KindRequest, message.KindResponse},
	}); err != nil {
		t.Fatal(err)
	}

	// In-flight request to the compute-hosted actor (far from expiry).
	exp := clock() + 60_000
	req := &message.Envelope{
		ID: "req-remote", TS: clock(), ChannelID: "ch", Kind: message.KindRequest, Type: "x.remote",
		Sender:   message.Sender{Kind: actor.KindAgent, ID: "caller"},
		Audience: message.Audience{"remote-tool"}, Payload: []byte("{}"),
		ExpiresAt: &exp,
	}
	cctx := harness.CtxWithCaller(ctx, harness.CallerContext{ActorID: "caller", ChannelID: "ch", AllowProvidedSenderKind: true})
	if res, err := home.Chain().Write(cctx, req); err != nil || res.RejectReason != "" {
		t.Fatalf("write request: err=%v reject=%s detail=%s", err, res.RejectReason, res.RejectDetail)
	}

	// Compute reports the cell died (this is what fleet.handleFrame(FrameDeath)
	// invokes). The request has NOT expired — only death closes it.
	home.MaterialiseComputeDeath(ctx, "remote-tool")

	has, err := home.Messages().HasFinalResponse(ctx, "ch", "req-remote")
	if err != nil {
		t.Fatal(err)
	}
	if !has {
		t.Fatal("no final after compute death — DeathFrame收口 NOT wired (compute cell death is a black hole at the home)")
	}
	sender, ok, err := home.Messages().FinalResponseSender(ctx, "ch", "req-remote")
	if err != nil || !ok {
		t.Fatalf("final sender lookup: ok=%v err=%v", ok, err)
	}
	if sender != actor.SystemActorID {
		t.Fatalf("final response sender=%s, want %s (substrate death author #3, not caller-scoped timeout)", sender, actor.SystemActorID)
	}
}
