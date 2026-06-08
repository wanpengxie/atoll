package tests

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/wanpengxie/ActOS/platform/host"
	"github.com/wanpengxie/ActOS/platform/localdevice"
	"github.com/wanpengxie/ActOS/protocol/actor"
	"github.com/wanpengxie/ActOS/protocol/message"
	"github.com/wanpengxie/ActOS/lib/adapterhost"
	"github.com/wanpengxie/ActOS/lib/behavior"
	"github.com/wanpengxie/ActOS/platform/computebus"
)

// relayMod is a proxyfacade-class relay adapter: Handle forwards to the device
// and defers; the device's callback (OnExternalCallbackFrame) resolves the final.
type relayMod struct{ mctx *behavior.ModuleContext }

func (m *relayMod) Declares() behavior.Declaration {
	return behavior.Declaration{
		Name: "relay", ActorID: "relay1", Binding: actor.BindingRuntimeInboundViaRelay,
		Types: []string{"relay.do"}, MaxPendingMs: 60_000,
	}
}
func (m *relayMod) Init(_ context.Context, mctx *behavior.ModuleContext) error {
	if mctx.ForwardExternalRequest == nil || mctx.Resolve == nil {
		return errMissingSeams
	}
	m.mctx = mctx
	return nil
}
func (m *relayMod) Shutdown(context.Context) error { return nil }
func (m *relayMod) Handle(ctx context.Context, env *message.Envelope) error {
	if _, err := m.mctx.ForwardExternalRequest(ctx, env, nil); err != nil {
		return err
	}
	return behavior.ErrHandleDeferred
}
func (m *relayMod) OnExternalCallback(context.Context, []byte) error { return nil }
func (m *relayMod) OnExternalCallbackFrame(ctx context.Context, frame behavior.ExternalCallbackFrame) error {
	return m.mctx.Resolve(ctx, frame.RequestID, behavior.ResolveRequest{Status: "completed", Payload: frame.Payload})
}

var errMissingSeams = errStr("relay: missing Forward/Resolve seams")

type errStr string

func (e errStr) Error() string { return string(e) }

// TestDeviceTransit_EndToEnd proves the v2 device transit (localdevice was a pure
// doc.go shell): a relay adapter forwards a request to the device via the Forward
// seam, the device (here the test) picks it up and posts a callback, and the
// transit routes that callback back to the adapter cell — on its goroutine — where
// ctx.Resolve writes the final, which emits UP. The full relay round trip:
// cell → Forward → device → Callback → cell.Resolve → emit.
func TestDeviceTransit_EndToEnd(t *testing.T) {
	emitted := make(chan *message.Envelope, 8)
	emit := func(_ context.Context, ef computebus.EmitFrame) (computebus.EmitAck, error) {
		emitted <- ef.Envelope
		return computebus.EmitAck{MessageID: ef.Envelope.ID}, nil
	}
	h := host.New(emit, nil)
	defer h.Stop()
	transit := localdevice.New(h)

	if _, err := h.InstallAdapter(context.Background(), &relayMod{}, adapterhost.InstallDeps{
		ChannelID: "ch", Forward: transit.ForwardFunc("relay1"),
	}); err != nil {
		t.Fatalf("install relay: %v", err)
	}

	// Dispatch a request to the relay cell (as if from the home down the wire).
	req := &message.Envelope{
		ID: "rq1", ChannelID: "ch", Kind: message.KindRequest, Type: "relay.do",
		Sender: message.Sender{Kind: actor.KindAgent, ID: "caller"}, Audience: message.Audience{"relay1"},
		Payload: json.RawMessage(`{}`),
	}
	if err := h.Dispatch(context.Background(), computebus.DispatchFrame{Target: "relay1", Envelope: req}); err != nil {
		t.Fatalf("dispatch: %v", err)
	}

	// The device picks up the forwarded request (poll until the cell forwarded).
	var fwd []localdevice.Forwarded
	for i := 0; i < 200 && len(fwd) == 0; i++ {
		fwd = transit.Take()
		if len(fwd) == 0 {
			time.Sleep(5 * time.Millisecond)
		}
	}
	if len(fwd) != 1 || fwd[0].Envelope.ID != "rq1" {
		t.Fatalf("device never received the forwarded request (Forward seam not wired): %+v", fwd)
	}

	// Device returns its result → transit routes the callback to the adapter cell.
	if err := transit.Callback(behavior.ExternalCallbackFrame{
		ChannelID: "ch", AdapterActorID: "relay1", RequestID: "rq1", ParentID: "rq1",
		Payload: json.RawMessage(`{"device":"ok"}`),
	}); err != nil {
		t.Fatalf("callback route failed: %v", err)
	}

	// Resolve emitted the final response UP.
	select {
	case env := <-emitted:
		if env.Kind != message.KindResponse || env.ParentID != "rq1" || env.Sender.ID != "relay1" {
			t.Fatalf("bad final: kind=%s parent=%s sender=%s", env.Kind, env.ParentID, env.Sender.ID)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("no final emitted after device callback — device→cell→Resolve path broken")
	}
}
