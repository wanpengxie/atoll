// Package tests holds cross-tier end-to-end tests (server ⇄ wire ⇄ daemon) that
// no single tier component may import.
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

// wireEcho is a real adapter hosted on the attached compute (NOT the home).
type wireEcho struct{ mctx *behavior.ModuleContext }

func (m *wireEcho) Declares() behavior.Declaration {
	return behavior.Declaration{
		Name: "wire-echo", ActorID: "wire-echo", Binding: actor.BindingRuntimeOutbound,
		Types: []string{"wire.echo"}, MaxPendingMs: 30_000,
	}
}
func (m *wireEcho) Init(_ context.Context, mctx *behavior.ModuleContext) error {
	m.mctx = mctx
	return nil
}
func (m *wireEcho) Shutdown(context.Context) error                   { return nil }
func (m *wireEcho) OnExternalCallback(context.Context, []byte) error { return nil }
func (m *wireEcho) Handle(ctx context.Context, env *message.Envelope) error {
	_, err := m.mctx.Respond(ctx, behavior.CorrelationKey(env.ID), env.Payload, behavior.RespondOptions{})
	return err
}

// TestComputeCrossWire_EndToEnd is the truth-flip headline proof: a request
// written into HOME truth is routed DOWN the wire to an actor hosted on an
// ATTACHED COMPUTE (a separate process in production; an httptest WS here), the
// compute adapter handles it and emits the response UP, and the home writes that
// response into truth. No local truth on the compute, no fakes — the full
// home⇄wire⇄compute round trip.
func TestComputeCrossWire_EndToEnd(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// --- Home (server, holds truth) ---
	home, err := channelhost.New(ctx, channelhost.Config{ChannelID: "ch", DBPath: ":memory:"})
	if err != nil {
		t.Fatal(err)
	}
	flt := fleet.New(home.Chain(), "")
	home.SetRemoteDispatch(flt.Dispatch)
	flt.SetOnDeath(home.MaterialiseComputeDeath)
	flt.SetOnAttach(home.RegisterComputeActors)

	srv := httptest.NewServer(http.HandlerFunc(flt.ServeWS))
	defer srv.Close()
	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http")

	// --- Compute (daemon, no truth) ---
	var h *host.Host
	hl, err := homelink.Connect(ctx, wsURL, "", "compute-1",
		[]computebus.AttachDeclaration{{
			ActorID: "wire-echo", Kind: actor.KindTool, Binding: actor.BindingRuntimeOutbound,
			Types: []string{"wire.echo"}, MaxPendingMs: 30_000,
		}},
		func(df computebus.DispatchFrame) {
			if h != nil {
				_ = h.Dispatch(ctx, df)
			}
		})
	if err != nil {
		t.Fatalf("homelink connect: %v", err)
	}
	defer func() { _ = hl.Close() }()
	h = host.New(hl.Emit, hl.SendDeath)
	defer h.Stop()
	if _, err := h.InstallAdapter(ctx, &wireEcho{}, adapterhost.InstallDeps{ChannelID: "ch"}); err != nil {
		t.Fatalf("install adapter: %v", err)
	}

	// --- Client request into home truth → routed down the wire ---
	if err := home.Registry().Insert(ctx, actor.Record{ID: "caller", Kind: actor.KindAgent}); err != nil {
		t.Fatal(err)
	}
	exp := time.Now().UnixMilli() + 30_000
	req := &message.Envelope{
		ID: "req-wire", TS: time.Now().UnixMilli(), ChannelID: "ch", Kind: message.KindRequest, Type: "wire.echo",
		Sender: message.Sender{Kind: actor.KindAgent, ID: "caller"}, Audience: message.Audience{"wire-echo"},
		Payload: json.RawMessage(`{"x":1}`), ExpiresAt: &exp,
	}
	cctx := harness.CtxWithCaller(ctx, harness.CallerContext{ActorID: "caller", ChannelID: "ch", AllowProvidedSenderKind: true})
	if res, err := home.Dispatch(cctx, req); err != nil || res.RejectReason != "" {
		t.Fatalf("dispatch: err=%v reject=%s detail=%s", err, res.RejectReason, res.RejectDetail)
	}

	// The response must come back UP the wire into home truth.
	var has bool
	for i := 0; i < 400; i++ {
		if has, err = home.Messages().HasFinalResponse(ctx, "ch", "req-wire"); err != nil {
			t.Fatal(err)
		}
		if has {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if !has {
		t.Fatal("no response in home truth — compute cross-wire round trip broken (request never reached the compute adapter, or its emit never wrote back)")
	}
	sender, ok, err := home.Messages().FinalResponseSender(ctx, "ch", "req-wire")
	if err != nil || !ok {
		t.Fatalf("final sender: ok=%v err=%v", ok, err)
	}
	if sender != "wire-echo" {
		t.Fatalf("response sender=%s, want wire-echo (the compute-hosted adapter)", sender)
	}
}
