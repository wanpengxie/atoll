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

// readyEcho updates its readiness to ready on first Handle (emitting
// actor.readiness.changed addressed to the system actor) then responds.
type readyEcho struct{ mctx *behavior.ModuleContext }

func (m *readyEcho) Declares() behavior.Declaration {
	return behavior.Declaration{
		Name: "ready-echo", ActorID: "ready-echo", Binding: actor.BindingEmbedded,
		Types: []string{"echo.say"}, MaxPendingMs: 30_000,
	}
}
func (m *readyEcho) Init(_ context.Context, mctx *behavior.ModuleContext) error {
	m.mctx = mctx
	return nil
}
func (m *readyEcho) Shutdown(context.Context) error                   { return nil }
func (m *readyEcho) OnExternalCallback(context.Context, []byte) error { return nil }
func (m *readyEcho) Handle(ctx context.Context, env *message.Envelope) error {
	_, _ = m.mctx.UpdateReadiness(ctx, actor.ReadinessUpdate{State: actor.ReadinessReady, CheckedAt: env.TS})
	_, err := m.mctx.Respond(ctx, behavior.CorrelationKey(env.ID), env.Payload, behavior.RespondOptions{})
	return err
}

// collector is a caller cell that captures the actor.list response delivered to
// it by the fanout chain — proving responses reach a local waiting cell (the
// client-push foundation) AND letting the test read the composed catalog.
type collector struct{ ch chan *message.Envelope }

func (c *collector) Receive(_ context.Context, env *message.Envelope) error {
	if env.Kind == message.KindResponse && env.Type == "actor.list" {
		select {
		case c.ch <- env:
		default:
		}
	}
	return nil
}

// TestSysactorReadinessLink_EndToEnd proves the owner-named sysactor link is
// truly wired: an adapter's readiness change EVENT fans out to the system actor
// cell (previously dead because Dispatch only fanned requests + the event had no
// audience), the system actor folds it into its advisory projection, and a
// subsequent actor.list reflects it — composed inside the cell, delivered to the
// caller cell via the same fanout path. End to end, no fakes.
func TestSysactorReadinessLink_EndToEnd(t *testing.T) {
	clock := func() int64 { return 3_000_000 }
	ctx := context.Background()
	home, err := channelhost.New(ctx, channelhost.Config{ChannelID: "ch", DBPath: ":memory:", NowMs: clock})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := home.InstallEmbeddedAdapter(ctx, &readyEcho{}); err != nil {
		t.Fatalf("install adapter: %v", err)
	}

	// A caller cell that captures the actor.list response.
	col := &collector{ch: make(chan *message.Envelope, 4)}
	if err := home.Registry().Insert(ctx, actor.Record{ID: "collector", Kind: actor.KindAgent}); err != nil {
		t.Fatal(err)
	}
	home.Channel().Cells().Spawn("collector", col)

	cctx := harness.CtxWithCaller(ctx, harness.CallerContext{ActorID: "collector", ChannelID: "ch", AllowProvidedSenderKind: true})

	// 1. Trigger a readiness change: dispatch echo.say → adapter UpdateReadiness
	//    (fans actor.readiness.changed into the sysactor cell) → Respond.
	exp := clock() + 30_000
	req := &message.Envelope{
		ID: "req-trigger", TS: clock(), ChannelID: "ch", Kind: message.KindRequest, Type: "echo.say",
		Sender: message.Sender{Kind: actor.KindAgent, ID: "collector"}, Audience: message.Audience{"ready-echo"},
		Payload: json.RawMessage(`{}`), ExpiresAt: &exp,
	}
	if res, err := home.Dispatch(cctx, req); err != nil || res.RejectReason != "" {
		t.Fatalf("dispatch trigger: err=%v reject=%s", err, res.RejectReason)
	}
	// Await the echo response → guarantees the readiness event was already
	// enqueued into the sysactor mailbox (emitted before Respond, same Handle).
	awaitFinal(t, home, "req-trigger")

	// 2. Ask the system actor for the catalog (enqueued AFTER readiness → the
	//    serial sysactor cell has already applied readiness).
	listReq := &message.Envelope{
		ID: "req-list", TS: clock(), ChannelID: "ch", Kind: message.KindRequest, Type: "actor.list",
		Sender: message.Sender{Kind: actor.KindAgent, ID: "collector"}, Audience: message.Audience{actor.SystemActorID},
		Payload: json.RawMessage(`{}`), ExpiresAt: &exp,
	}
	if res, err := home.Dispatch(cctx, listReq); err != nil || res.RejectReason != "" {
		t.Fatalf("dispatch actor.list: err=%v reject=%s", err, res.RejectReason)
	}

	// 3. The catalog response is delivered to the collector cell by the fanout.
	select {
	case env := <-col.ch:
		var body struct {
			Actors []struct {
				ID        string `json:"id"`
				Readiness string `json:"readiness"`
			} `json:"actors"`
		}
		if err := json.Unmarshal(env.Payload, &body); err != nil {
			t.Fatalf("parse catalog: %v", err)
		}
		var found string
		for _, a := range body.Actors {
			if a.ID == "ready-echo" {
				found = a.Readiness
			}
		}
		if found != string(actor.ReadinessReady) {
			t.Fatalf("ready-echo readiness in catalog=%q, want ready — readiness.changed event never reached the sysactor cell (fanout→sysactor link dead)", found)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("actor.list response never delivered to the caller cell — fanout does not deliver responses to local cells (client-push foundation broken)")
	}
}

func awaitFinal(t *testing.T, home *channelhost.ChannelHome, reqID message.ID) {
	t.Helper()
	for i := 0; i < 200; i++ {
		has, err := home.Messages().HasFinalResponse(context.Background(), "ch", reqID)
		if err != nil {
			t.Fatal(err)
		}
		if has {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("no final response for %s", reqID)
}
