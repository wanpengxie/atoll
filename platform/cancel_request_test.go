package platform_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/platform/internal/link"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/runtime/actorrt"
)

// cancelBlockingCell parks in Receive on a request until its reqCtx is
// cancelled — the only way out proves the cancel actually reached this
// daemon-hosted cell's request scope.
type cancelBlockingCell struct {
	started   chan struct{}
	cancelled chan struct{}
}

func (b *cancelBlockingCell) Receive(ctx context.Context, env *message.Envelope) error {
	if env.Kind != message.KindRequest {
		return nil
	}
	close(b.started)
	<-ctx.Done()
	close(b.cancelled)
	return nil
}

// cancelDaemonHost is the minimal test-local daemon side: an actorrt.Runtime
// (the cells) that dispatches inbound envelopes to the installed cell — it
// mirrors what platform.RunCompute wires inline (link_test.go's daemonHost).
type cancelDaemonHost struct {
	rt  *actorrt.Runtime
	del actorrt.Deliverer
}

func newCancelDaemonHost() *cancelDaemonHost {
	rt, del := actorrt.New(actorrt.Config{})
	return &cancelDaemonHost{rt: rt, del: del}
}

func (h *cancelDaemonHost) install(id actor.ActorID, impl actorrt.Actor) {
	h.rt.Spawn(id, actor.KindAgent, func(actorrt.Incarnation) actorrt.Actor { return impl })
}

func (h *cancelDaemonHost) dispatch(target actor.ActorID, env *message.Envelope) error {
	_, err := h.del.Deliver([]actor.ActorID{target}, env)
	return err
}

// TestHomeCancelRequest_CrossWire (DoD §7.5): Home.CancelRequest — the thin
// public capability (no Acceptor indirection) — reaches a daemon-hosted port's
// in-flight reqCtx across the real wire, over httptest+websocket (the link
// package's own end-to-end form, not net.Pipe).
func TestHomeCancelRequest_CrossWire(t *testing.T) {
	ch := newClosureHome(t)

	const toolID = actor.ActorID("tool:cancel-probe")
	const senderID = actor.ActorID("user:cancel-caller")
	registerActor(t, ch, toolID, actor.KindTool)
	registerActor(t, ch, senderID, actor.KindHuman)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ch.ServeAttach(w, r, "daemon-1")
	}))
	defer srv.Close()
	wsURL := "ws" + srv.URL[4:]

	d, err := link.Dial(context.Background(), wsURL, "daemon-1",
		[]link.Declaration{{ActorID: toolID, Kind: actor.KindTool, Binding: actor.BindingEmbedded}}, nil)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer func() { _ = d.Close() }()

	host := newCancelDaemonHost()
	defer host.rt.StopAll()

	cell := &cancelBlockingCell{started: make(chan struct{}), cancelled: make(chan struct{})}
	arms, err := d.OpenStream(toolID, func(env *message.Envelope) error {
		return host.dispatch(toolID, env)
	}, func(requestID message.ID) { host.rt.CancelRequest(toolID, requestID) })
	if err != nil {
		t.Fatalf("OpenStream: %v", err)
	}
	_ = arms
	host.install(toolID, cell)
	d.Start()

	pen := spawnWithPen(t, ch, senderID, actor.KindHuman)
	reqID := writeRequest(t, pen, toolID, "cancel.probe", nil)

	select {
	case <-cell.started:
	case <-time.After(10 * time.Second):
		t.Fatal("cell never entered Receive on the request")
	}

	// Home's public capability — no Acceptor indirection — reaches the
	// daemon-hosted port's reqCtx across the wire.
	ch.CancelRequest(toolID, reqID)

	select {
	case <-cell.cancelled:
	case <-time.After(10 * time.Second):
		t.Fatal("Home.CancelRequest never cancelled the cross-wire hosted cell's reqCtx")
	}
}
