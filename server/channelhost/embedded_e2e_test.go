package channelhost_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/wanpengxie/ActOS/kernel/actor"
	"github.com/wanpengxie/ActOS/kernel/message"
	"github.com/wanpengxie/ActOS/lib/behavior"

	"github.com/wanpengxie/ActOS/runtime/harness"
	"github.com/wanpengxie/ActOS/server/channelhost"
)

// echoModule is a minimal real adapter: it Responds to echo.say by echoing the
// request payload back as a completed terminal. It exercises the full happy
// path (Handle → mctx.Respond → chain.Write) with no external IO.
type echoModule struct{ mctx *behavior.ModuleContext }

func (m *echoModule) Declares() behavior.Declaration {
	return behavior.Declaration{
		Name: "echo", ActorID: "echo", Binding: actor.BindingEmbedded,
		Types: []string{"echo.say"}, MaxPendingMs: 30_000,
	}
}
func (m *echoModule) Init(_ context.Context, mctx *behavior.ModuleContext) error {
	m.mctx = mctx
	return nil
}
func (m *echoModule) Shutdown(context.Context) error                   { return nil }
func (m *echoModule) OnExternalCallback(context.Context, []byte) error { return nil }
func (m *echoModule) Handle(ctx context.Context, env *message.Envelope) error {
	_, err := m.mctx.Respond(ctx, behavior.CorrelationKey(env.ID), env.Payload, behavior.RespondOptions{})
	return err
}

// TestEmbeddedAdapter_EndToEndRequestResponse proves the system HOSTS SOMETHING:
// a real adapter is installed as a home cell, a client request dispatched through
// truth reaches the cell's Handle, and its Respond writes a completed terminal
// back into truth — the full request→response happy path (closure author #1, the
// receiver's voluntary answer). Before this the system hosted nothing (modules
// 恒空) and no request could ever be answered end to end.
func TestEmbeddedAdapter_EndToEndRequestResponse(t *testing.T) {
	clock := func() int64 { return 2_000_000 }
	ctx := context.Background()
	home, err := channelhost.New(ctx, channelhost.Config{ChannelID: "ch", DBPath: ":memory:", NowMs: clock})
	if err != nil {
		t.Fatal(err)
	}

	// Host a real adapter cell on the home (registers actor + publishes type rows).
	if _, err := home.InstallEmbeddedAdapter(ctx, &echoModule{}); err != nil {
		t.Fatalf("install embedded adapter: %v", err)
	}
	// Register the caller.
	if err := home.Registry().Insert(ctx, actor.Record{ID: "caller", Kind: actor.KindAgent}); err != nil {
		t.Fatal(err)
	}

	// Client request → home.Dispatch writes truth + fans out to the echo cell.
	exp := clock() + 30_000
	req := &message.Envelope{
		ID: "req-echo", TS: clock(), ChannelID: "ch", Kind: message.KindRequest, Type: "echo.say",
		Sender:   message.Sender{Kind: actor.KindAgent, ID: "caller"},
		Audience: message.Audience{"echo"}, Payload: json.RawMessage(`{"msg":"hi"}`),
		ExpiresAt: &exp,
	}
	cctx := harness.CtxWithCaller(ctx, harness.CallerContext{ActorID: "caller", ChannelID: "ch", AllowProvidedSenderKind: true})
	if res, err := home.Dispatch(cctx, req); err != nil || res.RejectReason != "" {
		t.Fatalf("dispatch request: err=%v reject=%s detail=%s", err, res.RejectReason, res.RejectDetail)
	}

	// Handle runs async on the cell goroutine; await the response in truth.
	var has bool
	for i := 0; i < 200; i++ {
		if has, err = home.Messages().HasFinalResponse(ctx, "ch", "req-echo"); err != nil {
			t.Fatal(err)
		}
		if has {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if !has {
		t.Fatal("no response after dispatch — the hosted adapter never answered (hosts-something happy path NOT wired)")
	}
	sender, ok, err := home.Messages().FinalResponseSender(ctx, "ch", "req-echo")
	if err != nil || !ok {
		t.Fatalf("final sender lookup: ok=%v err=%v", ok, err)
	}
	if sender != "echo" {
		t.Fatalf("response sender=%s, want echo (the receiver's voluntary answer, author #1)", sender)
	}
}
