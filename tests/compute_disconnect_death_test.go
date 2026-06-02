package tests

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/wanpengxie/ActOS/daemon/homelink"
	"github.com/wanpengxie/ActOS/daemon/host"
	"github.com/wanpengxie/ActOS/kernel/actor"
	"github.com/wanpengxie/ActOS/kernel/message"
	"github.com/wanpengxie/ActOS/lib/adapterhost"
	"github.com/wanpengxie/ActOS/lib/behavior"
	"github.com/wanpengxie/ActOS/runtime/harness"
	"github.com/wanpengxie/ActOS/server/channelhost"
	"github.com/wanpengxie/ActOS/server/fleet"
	"github.com/wanpengxie/ActOS/wire/computebus"
)

// deferAdapter accepts a request and never answers (defers) — the closure must
// come from death (compute disconnect), not the receiver.
type deferAdapter struct{}

func (deferAdapter) Declares() behavior.Declaration {
	return behavior.Declaration{
		Name: "slow", ActorID: "slow", Binding: actor.BindingRuntimeOutbound,
		Types: []string{"slow.do"}, MaxPendingMs: 600_000,
	}
}
func (deferAdapter) Init(context.Context, *behavior.ModuleContext) error { return nil }
func (deferAdapter) Shutdown(context.Context) error                      { return nil }
func (deferAdapter) OnExternalCallback(context.Context, []byte) error    { return nil }
func (deferAdapter) Handle(context.Context, *message.Envelope) error {
	return behavior.ErrHandleDeferred // never responds
}

// TestComputeDisconnectDeath_EndToEnd proves the SECOND death source (compute
// disconnect, not cell panic): a request routed to a compute-hosted actor that
// never answers is closed with receiver_unavailable when the compute drops its
// connection — the home materialises it across the wire. Without this the caller
// would hang until the (10-minute) timeout.
func TestComputeDisconnectDeath_EndToEnd(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	home, err := channelhost.New(ctx, channelhost.Config{ChannelID: "ch", DBPath: ":memory:"})
	if err != nil {
		t.Fatal(err)
	}
	flt := fleet.New(home.Chain(), "")
	home.SetRemoteDispatch(flt.Dispatch)
	flt.SetOnDeath(home.MaterialiseComputeDeath)
	flt.SetOnAttach(home.RegisterComputeActors)
	flt.SetOnPresence(home.MarkPresence)

	srv := httptest.NewServer(http.HandlerFunc(flt.ServeWS))
	defer srv.Close()
	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http")

	var h *host.Host
	hl, err := homelink.Connect(ctx, wsURL, "", "compute-x",
		[]computebus.AttachDeclaration{{
			ActorID: "slow", Kind: actor.KindTool, Binding: actor.BindingRuntimeOutbound,
			Types: []string{"slow.do"}, MaxPendingMs: 600_000,
		}},
		func(df computebus.DispatchFrame) {
			if h != nil {
				_ = h.Dispatch(ctx, df)
			}
		})
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	h = host.New(hl.Emit, hl.SendDeath)
	if _, err := h.InstallAdapter(ctx, deferAdapter{}, adapterhost.InstallDeps{ChannelID: "ch"}); err != nil {
		t.Fatalf("install: %v", err)
	}

	if err := home.Registry().Insert(ctx, actor.Record{ID: "caller", Kind: actor.KindAgent}); err != nil {
		t.Fatal(err)
	}
	exp := time.Now().UnixMilli() + 600_000
	req := &message.Envelope{
		ID: "req-slow", TS: time.Now().UnixMilli(), ChannelID: "ch", Kind: message.KindRequest, Type: "slow.do",
		Sender: message.Sender{Kind: actor.KindAgent, ID: "caller"}, Audience: message.Audience{"slow"},
		Payload: json.RawMessage(`{}`), ExpiresAt: &exp,
	}
	cctx := harness.CtxWithCaller(ctx, harness.CallerContext{ActorID: "caller", ChannelID: "ch", AllowProvidedSenderKind: true})
	if res, err := home.Dispatch(cctx, req); err != nil || res.RejectReason != "" {
		t.Fatalf("dispatch: err=%v reject=%s", err, res.RejectReason)
	}

	// The adapter defers — no answer yet.
	time.Sleep(50 * time.Millisecond)
	if has, _ := home.Messages().HasFinalResponse(ctx, "ch", "req-slow"); has {
		t.Fatal("request answered while adapter deferred — test premise broken")
	}

	// Compute disconnects → fleet unregister → death materialises the terminal.
	h.Stop()
	_ = hl.Close()

	var has bool
	for i := 0; i < 400; i++ {
		if has, err = home.Messages().HasFinalResponse(ctx, "ch", "req-slow"); err != nil {
			t.Fatal(err)
		}
		if has {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if !has {
		t.Fatal("no terminal after compute disconnect — second death source (disconnect) not wired; caller hangs to timeout")
	}
	sender, ok, err := home.Messages().FinalResponseSender(ctx, "ch", "req-slow")
	if err != nil || !ok {
		t.Fatalf("final sender: ok=%v err=%v", ok, err)
	}
	if sender != actor.SystemActorID {
		t.Fatalf("terminal sender=%s, want %s (substrate death author)", sender, actor.SystemActorID)
	}
}
