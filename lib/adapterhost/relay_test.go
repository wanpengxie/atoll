package adapterhost

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/wanpengxie/ActOS/kernel/actor"
	"github.com/wanpengxie/ActOS/kernel/message"
	"github.com/wanpengxie/ActOS/lib/behavior"
)

type stubCtx struct{ id actor.ActorID }

func (s stubCtx) Self() actor.ActorID             { return s.id }
func (s stubCtx) Deliver(*message.Envelope) error { return nil }

// relayModule mirrors proxyfacade's contract: Init HARD-requires the Resolve +
// ForwardExternalRequest seams; Handle forwards and defers; the terminal arrives
// later via an external callback routed through ctx.Resolve.
type relayModule struct {
	mctx      *behavior.ModuleContext
	forwarded bool
}

func (m *relayModule) Declares() behavior.Declaration {
	return behavior.Declaration{
		Name: "relay", ActorID: "relay1", Binding: actor.BindingRuntimeInboundViaRelay,
		Types: []string{"r.do"},
	}
}
func (m *relayModule) Init(_ context.Context, mctx *behavior.ModuleContext) error {
	if mctx.ForwardExternalRequest == nil {
		return errors.New("relay: ForwardExternalRequest required")
	}
	if mctx.Resolve == nil {
		return errors.New("relay: Resolve required")
	}
	m.mctx = mctx
	return nil
}
func (m *relayModule) Shutdown(context.Context) error                   { return nil }
func (m *relayModule) OnExternalCallback(context.Context, []byte) error { return nil }
func (m *relayModule) Handle(ctx context.Context, env *message.Envelope) error {
	if _, err := m.mctx.ForwardExternalRequest(ctx, env, nil); err != nil {
		return err
	}
	m.forwarded = true
	return behavior.ErrHandleDeferred // terminal lands via callback → Resolve
}

// TestRelaySeams_ForwardThenResolve proves the relay seams are WIRED (P1 #10):
// the adapter Init-s only because Resolve + ForwardExternalRequest are injected
// (previously恒 nil → proxyfacade-class adapters failed Init "Resolve required");
// Handle forwards + defers (no premature terminal); and a simulated external
// callback routed through ctx.Resolve writes the adapter-signed final.
func TestRelaySeams_ForwardThenResolve(t *testing.T) {
	fc := &recChain{}
	var forwardCalls int
	fwd := func(context.Context, *message.Envelope, behavior.ExternalRequestPayload) (behavior.ExternalRequestResult, error) {
		forwardCalls++
		return behavior.ExternalRequestResult{}, nil
	}
	mod := &relayModule{}
	res, err := Install(context.Background(), mod, InstallDeps{
		ChannelID: "ch", Chain: fc, Forward: fwd, Clock: time.Now,
	})
	if err != nil {
		t.Fatalf("install: %v", err)
	}
	a := res.Actor.(*adapterActor)
	a.tickEvery = time.Hour // avoid ticker noise in the test
	if err := a.Start(context.Background(), stubCtx{"relay1"}); err != nil {
		t.Fatalf("Start/Init failed — relay seams NOT injected: %v", err)
	}
	defer func() { _ = a.Stop(context.Background()) }()

	// Dispatch a request: Handle forwards + defers, no terminal yet.
	req := &message.Envelope{
		ID: "rq1", ChannelID: "ch", Kind: message.KindRequest, Type: "r.do",
		Sender: message.Sender{Kind: actor.KindAgent, ID: "caller"}, Audience: message.Audience{"relay1"},
	}
	if err := a.Receive(context.Background(), req); err != nil {
		t.Fatalf("Receive: %v", err)
	}
	if !mod.forwarded || forwardCalls != 1 {
		t.Fatalf("Handle did not forward via the seam (forwarded=%v calls=%d)", mod.forwarded, forwardCalls)
	}
	if len(fc.written) != 0 {
		t.Fatalf("deferred relay wrote %d terminals, want 0", len(fc.written))
	}

	// External callback arrives → routed through ctx.Resolve → final written.
	if err := a.mctx.Resolve(context.Background(), "rq1", behavior.ResolveRequest{
		Status: "completed", Payload: json.RawMessage(`{"ok":true}`),
	}); err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if len(fc.written) != 1 {
		t.Fatalf("Resolve wrote %d terminals, want 1 (callback→final path broken)", len(fc.written))
	}
	term := fc.written[0]
	if term.Kind != message.KindResponse || term.ParentID != "rq1" || term.Sender.ID != "relay1" {
		t.Fatalf("bad final: kind=%s parent=%s sender=%s", term.Kind, term.ParentID, term.Sender.ID)
	}
}
