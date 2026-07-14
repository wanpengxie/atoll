package home_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/lib/actorcaps"
	"github.com/wanpengxie/atoll/platform"
	"github.com/wanpengxie/atoll/platform/home"
	"github.com/wanpengxie/atoll/platform/internal/link"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/runtime/actorrt"
)

// cancelBlockingCell parks in Receive on a request until its own per-request
// cancel fires via the RequestCanceller hook — proving the cancel reached this
// daemon-hosted occupant (期10 S5: the cell's built-in reqCtx machine was
// retired; the occupant owns the request-cancel disposition, one-hop-delivered).
type cancelBlockingCell struct {
	started   chan struct{}
	cancelled chan struct{}
	mu        sync.Mutex
	cancel    context.CancelFunc
}

func (b *cancelBlockingCell) Receive(ctx context.Context, env *message.Envelope) error {
	if env.Kind != message.KindRequest {
		return nil
	}
	reqCtx, cancel := context.WithCancel(ctx)
	b.mu.Lock()
	b.cancel = cancel
	b.mu.Unlock()
	close(b.started)
	<-reqCtx.Done()
	close(b.cancelled)
	return nil
}

func (b *cancelBlockingCell) CancelRequest(_ message.ID) {
	b.mu.Lock()
	c := b.cancel
	b.mu.Unlock()
	if c != nil {
		c()
	}
}

// cancelDaemonHost is the minimal test-local daemon side: an actorrt.Runtime
// (the cells) that dispatches inbound envelopes to the installed cell — it
// mirrors what compute.Run wires inline (link_test.go's daemonHost).
type cancelDaemonHost struct {
	rt  *actorrt.Runtime
	del actorrt.Deliverer
}

func newCancelDaemonHost() *cancelDaemonHost {
	rt, del := actorrt.New(actorrt.Config{})
	return &cancelDaemonHost{rt: rt, del: del}
}

func (h *cancelDaemonHost) install(id actor.ActorID, impl actorrt.Actor) {
	_, _, _ = h.rt.SpawnIfAbsent(id, actor.KindAgent, func(actorrt.Incarnation) actorrt.Actor { return impl })
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

	toolID := actor.ActorID("tool:cancel-probe")
	senderID := actor.ActorID("user:cancel-caller")
	registerActor(t, ch, &toolID, actor.KindTool)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ch.ServeAttach(w, r, "daemon-1")
	}))
	defer srv.Close()
	wsURL := "ws" + srv.URL[4:]

	d, err := link.Dial(context.Background(), wsURL, "daemon-1",
		[]link.Declaration{{ActorID: toolID, Kind: actor.KindTool, Binding: actor.BindingEmbedded}}, link.DialConfig{}, nil)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer func() { _ = d.Close() }()

	host := newCancelDaemonHost()
	defer host.rt.StopAll()

	cell := &cancelBlockingCell{started: make(chan struct{}), cancelled: make(chan struct{})}
	arms, err := d.OpenStream(context.Background(), toolID, 0, func(env *message.Envelope) error {
		return host.dispatch(toolID, env)
	}, func(requestID message.ID) { host.rt.CancelRequest(toolID, requestID) })
	if err != nil {
		t.Fatalf("OpenStream: %v", err)
	}
	_ = arms
	host.install(toolID, cell)
	d.Start()

	pen := spawnWithPen(t, ch, &senderID, actor.KindHuman)
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

// TestCancelUpstream_Branches drives the four failure branches + the happy path of
// Home.handleCancelUpstream (the KindCancelRequest disposition) through the test
// seam, deterministically, over one home-hosted blocking target. The upstream
// wire relay itself is covered by the port-level and cross-wire tests; here the
// focus is the reverse-resolve + sender-validation logic: the caller self-reports
// neither its target nor its identity — the home takes both from truth.
func TestCancelUpstream_Branches(t *testing.T) {
	ch := newClosureHome(t)

	targetID := actor.ActorID("agent:up-target")
	callerID := actor.ActorID("user:up-caller")
	const otherID = actor.ActorID("user:up-other")

	// Home-hosted target parks in Receive until its reqCtx is cancelled.
	cell := &cancelBlockingCell{started: make(chan struct{}), cancelled: make(chan struct{})}
	registerActor(t, ch, &targetID, actor.KindAgent)
	var err error
	targetID, err = home.SpawnForTest(ch, targetID, actor.KindAgent, platform.CapsFactory(func(actorcaps.Caps) actorrt.Actor {
		return cell
	}))
	if err != nil {
		t.Fatalf("spawn target: %v", err)
	}

	// The authentic caller authors an in-flight request to the target.
	callerPen := spawnWithPen(t, ch, &callerID, actor.KindHuman)
	reqID := writeRequest(t, callerPen, targetID, "up.probe", nil)

	select {
	case <-cell.started:
	case <-time.After(10 * time.Second):
		t.Fatal("target never entered Receive on the request")
	}

	stillBlocked := func(branch string) {
		t.Helper()
		select {
		case <-cell.cancelled:
			t.Fatalf("%s: target's reqCtx was cancelled but the branch should have dropped", branch)
		case <-time.After(150 * time.Millisecond):
		}
	}

	// Branch: not found — an unknown request id resolves to nothing.
	home.HandleCancelUpstreamForTest(ch, callerID, message.ID("does-not-exist"))
	stillBlocked("not-found")

	// Branch: sender mismatch — the request exists (authored by callerID) but a
	// DIFFERENT bound id tries to revoke it (a half-trusted daemon may only revoke
	// a request it actually authored).
	home.HandleCancelUpstreamForTest(ch, otherID, reqID)
	stillBlocked("sender-mismatch")

	// Branch: non-request kind — an event row in the log is not a cancellable request.
	eventID := message.ID("up-event-1")
	eventEnv := &message.Envelope{
		ID:         eventID,
		TS:         time.Now().UnixMilli(),
		Kind:       message.KindEvent,
		Type:       "up.note",
		Payload:    json.RawMessage(`{}`),
		Visibility: message.VisibilityPublic,
		Audience:   message.Audience{targetID},
	}
	if res, err := callerPen.Write(context.Background(), eventEnv); err != nil || !res.Accepted() {
		t.Fatalf("write event: err=%v accepted=%v", err, res.Accepted())
	}
	home.HandleCancelUpstreamForTest(ch, callerID, eventID)
	stillBlocked("non-request")

	// (The empty-audience branch is a defensive guard: a legitimately-written request
	// always carries an audience, so it is unreachable via the normal write path.)

	// Happy path: the authentic author revokes its own in-flight request — the home
	// reverse-resolves the target from the request's audience and cancels its reqCtx.
	home.HandleCancelUpstreamForTest(ch, callerID, reqID)
	select {
	case <-cell.cancelled:
	case <-time.After(10 * time.Second):
		t.Fatal("happy path: upstream cancel never reached the target's reqCtx")
	}
}

// TestCancelUpstream_CrossWireAcrossReconnect (DoD §5 half): the caller-side cancel
// arrives at the home as a KindCancelRequest frame up the caller's own stream, the
// home reverse-resolves the target + validates the sender, and reaches the target's
// reqCtx — AND it still works on a SECOND connection after the first link tears down
// (the daemon-side forwarder holds the CURRENT Dialer, never a captured dead one).
// Drives the full additive path over a real httptest+websocket wire: Dialer frame →
// port readLoop onCancelRequest → Home.handleCancelUpstream → Home.CancelRequest.
func TestCancelUpstream_CrossWireAcrossReconnect(t *testing.T) {
	ch := newClosureHome(t)

	targetID := actor.ActorID("agent:up-wire-target")
	callerID := actor.ActorID("agent:up-wire-caller")

	// Home-hosted blocking target.
	cell := &cancelBlockingCell{started: make(chan struct{}), cancelled: make(chan struct{})}
	registerActor(t, ch, &targetID, actor.KindAgent)
	var err error
	targetID, err = home.SpawnForTest(ch, targetID, actor.KindAgent, platform.CapsFactory(func(actorcaps.Caps) actorrt.Actor {
		return cell
	}))
	if err != nil {
		t.Fatalf("spawn target: %v", err)
	}

	// The caller authors its in-flight request to the target (via a home pen; the
	// daemon then attaches under the same id — the request already lives in truth).
	// KindAgent, not KindHuman: a human is恒 home-hosted (期12 S3.5 attach
	// gate rejects a daemon declaring one) — this test's subject is the
	// cross-wire cancel plumbing, which any daemon-hostable kind exercises.
	callerPen := spawnWithPen(t, ch, &callerID, actor.KindAgent)
	reqID := writeRequest(t, callerPen, targetID, "up.wire.probe", nil)

	select {
	case <-cell.started:
	case <-time.After(10 * time.Second):
		t.Fatal("target never entered Receive on the request")
	}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ch.ServeAttach(w, r, "daemon-up")
	}))
	defer srv.Close()
	wsURL := "ws" + srv.URL[4:]

	decls := []link.Declaration{{ActorID: callerID, Kind: actor.KindAgent, Binding: actor.BindingEmbedded}}

	// First connection: attach the caller's stream, then let it tear down.
	d1, err := link.Dial(context.Background(), wsURL, "daemon-up", decls, link.DialConfig{}, nil)
	if err != nil {
		t.Fatalf("Dial d1: %v", err)
	}
	if _, err := d1.OpenStream(context.Background(), callerID, 0, func(*message.Envelope) error { return nil }, func(message.ID) {}); err != nil {
		t.Fatalf("OpenStream d1: %v", err)
	}
	d1.Start()
	_ = d1.Close() // drop the first link — a captured-first-Dialer forwarder would now be dead.

	// Second connection: reattach the same caller id on a FRESH Dialer and send the
	// upstream cancel there. It must still reach the home.
	d2, err := link.Dial(context.Background(), wsURL, "daemon-up", decls, link.DialConfig{}, nil)
	if err != nil {
		t.Fatalf("Dial d2: %v", err)
	}
	defer func() { _ = d2.Close() }()
	if _, err := d2.OpenStream(context.Background(), callerID, 0, func(*message.Envelope) error { return nil }, func(message.ID) {}); err != nil {
		t.Fatalf("OpenStream d2: %v", err)
	}
	d2.Start()

	// Retry the upstream send until the stream is live and the home reacts (the
	// reattach handshake is async; the frame is best-effort so a pre-attach send is
	// a silent no-op).
	deadline := time.After(10 * time.Second)
	for {
		d2.SendCancelRequest(callerID, reqID)
		select {
		case <-cell.cancelled:
			return
		case <-time.After(100 * time.Millisecond):
		case <-deadline:
			t.Fatal("upstream cancel on the reconnected link never reached the target's reqCtx")
		}
	}
}
