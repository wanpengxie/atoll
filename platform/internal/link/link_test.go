package link_test

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"github.com/wanpengxie/atoll/lib/introspect"
	"github.com/wanpengxie/atoll/platform/internal/link"
	"github.com/wanpengxie/atoll/platform/internal/presence"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/runtime/actorrt"
	"github.com/wanpengxie/atoll/runtime/harness"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

const testChannelID = channel.ID("test-channel")

type memberPresenceRegistry struct{}

func (memberPresenceRegistry) Lookup(_ context.Context, id actor.ActorID) (storespec.Record, bool, error) {
	return storespec.Record{ID: id}, true, nil
}
func (memberPresenceRegistry) Exists(context.Context, actor.ActorID) (bool, error) {
	return true, nil
}
func (memberPresenceRegistry) ListActive(context.Context) ([]storespec.Record, error) {
	return nil, nil
}

// --- stubs ---

// stubMinter is the test substrate mint machine: Mint welds (id, chID) onto a stubPen
// that mimics the real boundPen's fail-fast inject — so a remote cell's emit
// (relayed up empty by the proxy pen) is welded with the connection's
// authenticated bound id exactly as the production Minter would. A self-reported
// identity on the relayed envelope is rejected fail-fast, proving the host welds
// authorship and never trusts the wire's self-report.
type stubMinter struct {
	mu      sync.Mutex
	writes  []*message.Envelope
	nextSeq int64
}

func (s *stubMinter) Mint(id actor.ActorID, kind actor.Kind, chID channel.ID) harness.Pen {
	return &stubPen{minter: s, id: id, kind: kind, chID: chID}
}

func (s *stubMinter) record(env *message.Envelope) harness.WriteResult {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.nextSeq++
	cp := *env
	s.writes = append(s.writes, &cp)
	return harness.WriteResult{MessageID: env.ID, Seq: s.nextSeq}
}

func (s *stubMinter) all() []*message.Envelope {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]*message.Envelope, len(s.writes))
	copy(out, s.writes)
	return out
}

// stubPen is the welded Pen a stubMinter hands out: it fail-fasts on a self-
// reported identity (matching the real boundPen) and otherwise injects the
// welded (id, chID) before recording the write.
type stubPen struct {
	minter *stubMinter
	id     actor.ActorID
	kind   actor.Kind
	chID   channel.ID
}

func (p *stubPen) Write(_ context.Context, env *message.Envelope) (harness.WriteResult, error) {
	if env.Sender.ID != "" || env.ChannelID != "" {
		return harness.WriteResult{
			RejectReason: harness.HarnessIdentityNotCallerSettable,
			RejectDetail: "sender.id/channel_id are substrate-injected, not caller-settable",
		}, nil
	}
	env.Sender.ID = p.id
	env.Sender.Kind = p.kind
	env.ChannelID = p.chID
	return p.minter.record(env), nil
}

type stubMembership struct {
	mu   sync.Mutex
	adds []storespec.MemberActorAdd
}

func (s *stubMembership) Deregister(context.Context, actor.ActorID, int64) error { return nil }
func (s *stubMembership) Admit(context.Context, actor.Kind, string, int64) (actor.ActorID, error) {
	return "", nil
}
func (s *stubMembership) EnsureSystemActor(context.Context, int64) error { return nil }
func (s *stubMembership) ApplyMemberTransitions(_ context.Context, adds []storespec.MemberActorAdd, _ []storespec.MemberActorRemove) error {
	s.mu.Lock()
	s.adds = append(s.adds, adds...)
	s.mu.Unlock()
	return nil
}

func (s *stubMembership) getAdds() []storespec.MemberActorAdd {
	s.mu.Lock()
	defer s.mu.Unlock()
	cp := make([]storespec.MemberActorAdd, len(s.adds))
	copy(cp, s.adds)
	return cp
}

// echoCell responds to each request via its injected pen. It leaves
// Sender.ID/ChannelID empty (substrate-injected): the welded pen stamps the
// cell's own identity. A non-empty self-report would be rejected fail-fast — the
// cell does not author its own identity.
type echoCell struct{ w harness.Pen }

func (e *echoCell) Receive(ctx context.Context, env *message.Envelope) error {
	if env.Kind != message.KindRequest {
		return nil
	}
	_, _ = e.w.Write(ctx, &message.Envelope{
		ID:       message.ID("resp-" + string(env.ID)),
		Kind:     message.KindResponse,
		Type:     env.Type,
		ParentID: env.ID,
		Audience: message.Audience{env.Sender.ID},
	})
	return nil
}

type panicCell struct{}

func (panicCell) Receive(context.Context, *message.Envelope) error { panic("boom") }

// blockingCell blocks in Receive until its own per-request cancel fires,
// signalling entry on started and the cancel outcome on cancelled. It is the
// cross-wire cancel probe: the request occupies the cell goroutine, and the only
// way out is the occupant's RequestCanceller hook being invoked OFF the goroutine
// — proving the home's KindCancel reached the daemon and one-hop-delivered to this
// occupant (期10 S5: the cell's built-in reqCtx machine was retired; the occupant
// owns the request-cancel disposition, mirroring the actorbase engine).
type blockingCell struct {
	started   chan struct{}
	cancelled chan struct{}
	mu        sync.Mutex
	cancel    context.CancelFunc
}

func (b *blockingCell) Receive(ctx context.Context, env *message.Envelope) error {
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

func (b *blockingCell) CancelRequest(_ message.ID) {
	b.mu.Lock()
	c := b.cancel
	b.mu.Unlock()
	if c != nil {
		c()
	}
}

// --- daemon rig ---

// daemonHost is the test-local daemon side: an actorrt.Runtime (the cells) plus
// the per-actor downHandler the link installs so a dead cell closes its own
// stream. It mirrors exactly what platform.RunCompute wires inline — cell
// running is the kernel, the only daemon glue is the down watcher.
type daemonHost struct {
	rt   *actorrt.Runtime
	del  actorrt.Deliverer
	mu   sync.Mutex
	down map[actor.ActorID]func(cause string)
}

func newDaemonHost() *daemonHost {
	rt, del := actorrt.New(actorrt.Config{})
	h := &daemonHost{rt: rt, del: del, down: map[actor.ActorID]func(cause string){}}
	rt.WatchDown(h)
	return h
}

func (h *daemonHost) OnDown(_ context.Context, id actor.ActorID, _ actorrt.Incarnation, cause error) {
	h.mu.Lock()
	handler := h.down[id]
	h.mu.Unlock()
	if handler != nil {
		msg := ""
		if cause != nil {
			msg = cause.Error()
		}
		handler(msg)
	}
}

func (h *daemonHost) Install(id actor.ActorID, impl actorrt.Actor, downHandler func(cause string)) {
	h.mu.Lock()
	h.down[id] = downHandler
	h.mu.Unlock()
	_, _, _ = h.rt.SpawnIfAbsent(id, actor.KindAgent, func(actorrt.Incarnation) actorrt.Actor { return impl })
}

func (h *daemonHost) Dispatch(target actor.ActorID, env *message.Envelope) error {
	_, err := h.del.Deliver([]actor.ActorID{target}, env)
	return err
}

func (h *daemonHost) CancelRequest(target actor.ActorID, requestID message.ID) {
	h.rt.CancelRequest(target, requestID)
}

func (h *daemonHost) Stop() { h.rt.StopAll() }

// --- home rig ---

type homeRig struct {
	acc        *link.Acceptor
	rt         *actorrt.Runtime
	deliver    actorrt.Deliverer
	minter     *stubMinter
	membership *stubMembership
	srv        *httptest.Server

	mu         sync.Mutex
	downActors []actor.ActorID
}

func newHomeRig(t *testing.T, leasePing, leaseTTL time.Duration) *homeRig {
	t.Helper()
	rt, del := actorrt.New(actorrt.Config{Parent: context.Background()})
	r := &homeRig{
		rt:         rt,
		deliver:    del,
		minter:     &stubMinter{},
		membership: &stubMembership{},
	}
	rt.WatchDown(watcherFunc(func(_ context.Context, id actor.ActorID, _ actorrt.Incarnation, _ error) {
		r.mu.Lock()
		r.downActors = append(r.downActors, id)
		r.mu.Unlock()
	}))
	r.acc = link.NewAcceptor(link.Config{
		Minter:     r.minter,
		Runtime:    rt,
		Membership: r.membership,
		ChannelID:  testChannelID,
		LeasePing:  leasePing,
		LeaseTTL:   leaseTTL,
	})
	r.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		r.acc.Serve(w, req, "daemon-1")
	}))
	t.Cleanup(func() { _ = r.acc.Close(); r.srv.Close() })
	return r
}

func (r *homeRig) wsURL() string { return "ws" + r.srv.URL[4:] }

func (r *homeRig) getDown() []actor.ActorID {
	r.mu.Lock()
	defer r.mu.Unlock()
	cp := make([]actor.ActorID, len(r.downActors))
	copy(cp, r.downActors)
	return cp
}

type watcherFunc func(context.Context, actor.ActorID, actorrt.Incarnation, error)

func (f watcherFunc) OnDown(ctx context.Context, id actor.ActorID, incarnation actorrt.Incarnation, cause error) {
	f(ctx, id, incarnation, cause)
}

// --- tests ---

// TestEndToEnd_AttachDispatchEmit drives a full home↔daemon link: a daemon
// attaches one actor, the home dispatches a request down the actor's stream into
// the daemon cell, and the cell's emit flows UP that same stream to the home
// write gate. Native ipc over a mux stream, zero translation.
func TestEndToEnd_AttachDispatchEmit(t *testing.T) {
	r := newHomeRig(t, 5*time.Second, 30*time.Second)

	const toolID = actor.ActorID("tool:echo")
	d, err := link.Dial(context.Background(), r.wsURL(), "daemon-1",
		[]link.Declaration{{ActorID: toolID, Kind: actor.KindTool, Binding: actor.BindingEmbedded}}, link.DialConfig{}, nil)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer func() { _ = d.Close() }()

	h := newDaemonHost()
	defer h.Stop()

	arms, err := d.OpenStream(toolID, func(env *message.Envelope) error {
		return h.Dispatch(toolID, env)
	}, func(requestID message.ID) { h.CancelRequest(toolID, requestID) })
	if err != nil {
		t.Fatalf("OpenStream: %v", err)
	}
	h.Install(toolID, &echoCell{w: arms.Pen}, nil)
	d.Start()

	// Membership contains the declared active actor.
	if adds := r.membership.getAdds(); len(adds) != 1 || adds[0].ID != toolID {
		t.Fatalf("membership adds = %+v, want one %s", adds, toolID)
	}

	// Home dispatches a request to the actor's port (the daemon stream).
	req := &message.Envelope{
		ID:        "req-1",
		ChannelID: testChannelID,
		Kind:      message.KindRequest,
		Type:      "echo.do",
		Sender:    message.Sender{ID: "user:a", Kind: actor.KindHuman},
		Audience:  message.Audience{toolID},
	}
	if _, err := r.deliver.Deliver([]actor.ActorID{toolID}, req); err != nil {
		t.Fatalf("home deliver: %v", err)
	}

	// The cell's response flows UP to the home write gate.
	deadline := time.Now().Add(10 * time.Second)
	for {
		got := r.minter.all()
		if len(got) >= 1 {
			if got[0].ID != "resp-req-1" || got[0].ParentID != "req-1" {
				t.Fatalf("emitted up = %+v, want resp-req-1/req-1", got[0])
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("cell emit never reached the home writer")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// TestHomeCloseQuietTeardown_NoDownEdge: a home-side Acceptor.Close tears the link
// down GRACEFULLY — its port-hosted actors are quiet-stopped (pointer-guarded, no
// down edge), so an in-flight request never materialises receiver_unavailable.
// Without the quiet handle the port would EOF loud on Close and publish a death
// edge. The request round-trip first proves the port is fully attached (its
// Incarnation retained) before Close, so the handle deterministically covers it.
func TestHomeCloseQuietTeardown_NoDownEdge(t *testing.T) {
	r := newHomeRig(t, 5*time.Second, 30*time.Second)

	const toolID = actor.ActorID("tool:echo")
	d, err := link.Dial(context.Background(), r.wsURL(), "daemon-1",
		[]link.Declaration{{ActorID: toolID, Kind: actor.KindTool, Binding: actor.BindingEmbedded}}, link.DialConfig{}, nil)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer func() { _ = d.Close() }()

	h := newDaemonHost()
	defer h.Stop()

	arms, err := d.OpenStream(toolID, func(env *message.Envelope) error {
		return h.Dispatch(toolID, env)
	}, func(requestID message.ID) { h.CancelRequest(toolID, requestID) })
	if err != nil {
		t.Fatalf("OpenStream: %v", err)
	}
	h.Install(toolID, &echoCell{w: arms.Pen}, nil)
	d.Start()

	req := &message.Envelope{
		ID: "req-1", ChannelID: testChannelID, Kind: message.KindRequest, Type: "echo.do",
		Sender: message.Sender{ID: "user:a", Kind: actor.KindHuman}, Audience: message.Audience{toolID},
	}
	if _, err := r.deliver.Deliver([]actor.ActorID{toolID}, req); err != nil {
		t.Fatalf("home deliver: %v", err)
	}
	deadline := time.Now().Add(10 * time.Second)
	for len(r.minter.all()) == 0 {
		if time.Now().After(deadline) {
			t.Fatal("cell emit never reached the home writer (port not fully attached)")
		}
		time.Sleep(5 * time.Millisecond)
	}

	// Graceful home Close: the port must fall silent — no down edge.
	if err := r.acc.Close(); err != nil {
		t.Fatalf("acc.Close: %v", err)
	}
	if down := r.getDown(); len(down) != 0 {
		t.Fatalf("home Close published down edge(s) %+v — graceful port teardown must be quiet", down)
	}
}

// TestDispatchInOpenStreamWindow_NotDropped pins the step-0 race fix in its
// per-stream form: a deliver the home sends AFTER the daemon opens the stream
// (so the home-side port is live) but BEFORE the daemon installs the cell and
// calls Start must NOT be silently dropped. The fix defers the per-stream read
// loop to Start, so the in-window frame buffers and is dispatched once the cell
// is installed. Before the fix the read loop ran from OpenStream, hit NotHosted,
// and dropped the envelope with zero trace.
func TestDispatchInOpenStreamWindow_NotDropped(t *testing.T) {
	r := newHomeRig(t, 5*time.Second, 30*time.Second)

	const toolID = actor.ActorID("tool:echo")
	d, err := link.Dial(context.Background(), r.wsURL(), "daemon-1",
		[]link.Declaration{{ActorID: toolID, Kind: actor.KindTool, Binding: actor.BindingEmbedded}}, link.DialConfig{}, nil)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer func() { _ = d.Close() }()

	h := newDaemonHost()
	defer h.Stop()

	arms, err := d.OpenStream(toolID, func(env *message.Envelope) error {
		return h.Dispatch(toolID, env)
	}, func(requestID message.ID) { h.CancelRequest(toolID, requestID) })
	if err != nil {
		t.Fatalf("OpenStream: %v", err)
	}

	// The home-side port is live after the handshake. Dispatch NOW — before the
	// cell is installed and before Start. The frame must wait in the daemon's
	// stream buffer, not race a half-built host.
	req := &message.Envelope{
		ID:        "req-window",
		ChannelID: testChannelID,
		Kind:      message.KindRequest,
		Type:      "echo.do",
		Sender:    message.Sender{ID: "user:a", Kind: actor.KindHuman},
		Audience:  message.Audience{toolID},
	}
	if _, err := r.deliver.Deliver([]actor.ActorID{toolID}, req); err != nil {
		t.Fatalf("home deliver: %v", err)
	}
	// Give the frame ample time to cross the wire into the daemon's stream buffer
	// while no read loop is running — this is what makes the OLD code drop it
	// deterministically rather than racily.
	time.Sleep(50 * time.Millisecond)

	h.Install(toolID, &echoCell{w: arms.Pen}, nil)
	d.Start()

	// The buffered in-window request must now be dispatched and answered.
	deadline := time.Now().Add(10 * time.Second)
	for {
		got := r.minter.all()
		if len(got) >= 1 {
			if got[0].ID != "resp-req-window" || got[0].ParentID != "req-window" {
				t.Fatalf("emitted up = %+v, want resp-req-window/req-window", got[0])
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("in-window dispatch was dropped: cell never answered")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// TestPostStartOpenStream_StartStreamDrives pins the S2 three-step arm order for a
// stream opened AFTER Start: OpenStream + Spawn install the cell but its read loop
// stays deferred until the ring calls StartStream. A deliver sent in the window
// between Spawn and StartStream must buffer (not race a stream with no loop), then
// be drained once StartStream runs. This is the per-stream, mid-life form of the
// initial-batch deferral Start gives — the mechanism the S3 reconcile ring rides.
func TestPostStartOpenStream_StartStreamDrives(t *testing.T) {
	r := newHomeRig(t, 5*time.Second, 30*time.Second)

	const initID = actor.ActorID("tool:init")
	const lateID = actor.ActorID("tool:late")
	d, err := link.Dial(context.Background(), r.wsURL(), "daemon-1",
		[]link.Declaration{
			{ActorID: initID, Kind: actor.KindTool, Binding: actor.BindingEmbedded},
			{ActorID: lateID, Kind: actor.KindTool, Binding: actor.BindingEmbedded},
		}, link.DialConfig{}, nil)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer func() { _ = d.Close() }()

	h := newDaemonHost()
	defer h.Stop()

	// Initial batch: open + spawn the first actor, then Start launches its loop.
	armsInit, err := d.OpenStream(initID, func(env *message.Envelope) error {
		return h.Dispatch(initID, env)
	}, func(requestID message.ID) { h.CancelRequest(initID, requestID) })
	if err != nil {
		t.Fatalf("OpenStream init: %v", err)
	}
	h.Install(initID, &echoCell{w: armsInit.Pen}, nil)
	d.Start()

	// Post-Start open: the second actor's stream comes up AFTER Start, so Start's
	// batch snapshot never covered it — its loop is the ring's to launch.
	armsLate, err := d.OpenStream(lateID, func(env *message.Envelope) error {
		return h.Dispatch(lateID, env)
	}, func(requestID message.ID) { h.CancelRequest(lateID, requestID) })
	if err != nil {
		t.Fatalf("OpenStream late: %v", err)
	}
	h.Install(lateID, &echoCell{w: armsLate.Pen}, nil)

	req := &message.Envelope{
		ID: "req-late", ChannelID: testChannelID, Kind: message.KindRequest, Type: "echo.do",
		Sender: message.Sender{ID: "user:a", Kind: actor.KindHuman}, Audience: message.Audience{lateID},
	}
	if _, err := r.deliver.Deliver([]actor.ActorID{lateID}, req); err != nil {
		t.Fatalf("home deliver: %v", err)
	}
	// No read loop yet: the frame must sit in the stream buffer, unanswered.
	time.Sleep(50 * time.Millisecond)
	if got := r.minter.all(); len(got) != 0 {
		t.Fatalf("late stream answered before StartStream %+v — post-Start read loop must be deferred", got)
	}

	// Step three drives the buffered deliver. A second StartStream is a no-op
	// (per-stream loopStarted guard), so Start's batch and a ring launch compose.
	d.StartStream(lateID)
	d.StartStream(lateID)

	deadline := time.Now().Add(10 * time.Second)
	for {
		got := r.minter.all()
		if len(got) >= 1 {
			if got[0].ID != "resp-req-late" || got[0].ParentID != "req-late" {
				t.Fatalf("emitted up = %+v, want resp-req-late/req-late", got[0])
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("StartStream never drove the buffered post-Start deliver")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// TestEndToEnd_CellDeath_ClosesStreamToOnDown proves a daemon cell's abnormal
// death closes its link stream, the home port reads EOF, and the home publishes
// the down edge (the closure trigger). Death is the stream EOF — no
// translated death frame.
func TestEndToEnd_CellDeath_ClosesStreamToOnDown(t *testing.T) {
	r := newHomeRig(t, 5*time.Second, 30*time.Second)

	const toolID = actor.ActorID("tool:doomed")
	d, err := link.Dial(context.Background(), r.wsURL(), "daemon-1",
		[]link.Declaration{{ActorID: toolID, Kind: actor.KindTool, Binding: actor.BindingEmbedded}}, link.DialConfig{}, nil)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer func() { _ = d.Close() }()

	h := newDaemonHost()
	defer h.Stop()

	arms, err := d.OpenStream(toolID, func(env *message.Envelope) error {
		return h.Dispatch(toolID, env)
	}, func(requestID message.ID) { h.CancelRequest(toolID, requestID) })
	if err != nil {
		t.Fatalf("OpenStream: %v", err)
	}
	_ = arms
	h.Install(toolID, panicCell{}, arms.Down)
	d.Start()

	// Dispatch triggers the panic → cell death → downHandler closes the stream →
	// home port EOF → OnDown.
	_ = mustDeliver(t, r, toolID)

	deadline := time.Now().Add(10 * time.Second)
	for {
		if down := r.getDown(); len(down) >= 1 && down[0] == toolID {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("home never observed the cell's down edge over the link")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// TestLease_NoTraffic_ExpiresToDown proves the home's lease judges
// liveness: with the daemon NOT pinging (Start never called) and a short TTL,
// the home tears the link down on TTL — the actor stream EOFs and the home
// publishes down (the lease timeout stands in for the positive signal a
// frozen daemon's TCP connection never produces).
func TestLease_NoTraffic_ExpiresToDown(t *testing.T) {
	r := newHomeRig(t, 20*time.Millisecond, 60*time.Millisecond)

	const toolID = actor.ActorID("tool:frozen")
	d, err := link.Dial(context.Background(), r.wsURL(), "daemon-1",
		[]link.Declaration{{ActorID: toolID, Kind: actor.KindTool, Binding: actor.BindingEmbedded}}, link.DialConfig{}, nil)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer func() { _ = d.Close() }()

	h := newDaemonHost()
	defer h.Stop()

	arms, err := d.OpenStream(toolID, func(env *message.Envelope) error {
		return h.Dispatch(toolID, env)
	}, func(requestID message.ID) { h.CancelRequest(toolID, requestID) })
	if err != nil {
		t.Fatalf("OpenStream: %v", err)
	}
	_ = arms
	h.Install(toolID, &echoCell{w: arms.Pen}, arms.Down)
	// Deliberately do NOT call d.Start(): no idle ping, so no inbound traffic
	// refreshes the home lease. The daemon is "frozen" from the home's view.

	deadline := time.Now().Add(10 * time.Second)
	for {
		if down := r.getDown(); len(down) >= 1 && down[0] == toolID {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("lease never expired the frozen link to down")
		}
		time.Sleep(5 * time.Millisecond)
	}

	// The daemon side also observes the link tearing down.
	select {
	case <-d.Done():
	case <-time.After(10 * time.Second):
		t.Fatal("daemon never saw the link close after lease expiry")
	}
}

// TestLease_KeepAliveAliveButAppFrozen_ExpiresToDown proves the actual
// invariant the two-heartbeat design (linksession.go's linkYamuxConfig doc)
// promises, not a proxy for it: with yamux's OWN keepalive genuinely running —
// fast-forwarded via SetYamuxKeepAliveIntervalForTest so several real
// keepalive round trips land inside the test's TTL window — but the daemon's
// APPLICATION layer producing nothing (Start is never called: no pingLoop, no
// actor traffic), the home's Lease still expires the link on TTL.
//
// This is deliberately NOT TestLease_NoTraffic_ExpiresToDown's shortcut of
// setting TTL (60ms) far below yamux's 30s default keepalive so the keepalive
// simply never gets a chance to fire during the test — that shortcut cannot
// distinguish "the Lease correctly ignores keepalive" from "the test window
// was too short for keepalive to matter either way", so it would NOT have
// caught the 换底 regression this test guards: onRead used to fire on every
// wsByteStream.Read — including yamux's own keepalive ping/pong bytes — so a
// frozen-app-but-live-socket daemon was kept "alive" by keepalive alone, and
// the home's Lease (whose entire job is catching exactly that daemon) never
// expired it.
func TestLease_KeepAliveAliveButAppFrozen_ExpiresToDown(t *testing.T) {
	// yamux's real keepalive fires every 15ms for the life of this test — many
	// round trips complete inside the 200ms TTL window below, so the wire
	// genuinely carries traffic throughout, not merely by the window being too
	// short to notice its absence.
	reset := link.SetYamuxKeepAliveIntervalForTest(15 * time.Millisecond)
	defer reset()

	r := newHomeRig(t, 20*time.Millisecond, 200*time.Millisecond)

	const toolID = actor.ActorID("tool:frozen-app-live-keepalive")
	d, err := link.Dial(context.Background(), r.wsURL(), "daemon-1",
		[]link.Declaration{{ActorID: toolID, Kind: actor.KindTool, Binding: actor.BindingEmbedded}}, link.DialConfig{}, nil)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer func() { _ = d.Close() }()

	h := newDaemonHost()
	defer h.Stop()

	arms, err := d.OpenStream(toolID, func(env *message.Envelope) error {
		return h.Dispatch(toolID, env)
	}, func(requestID message.ID) { h.CancelRequest(toolID, requestID) })
	if err != nil {
		t.Fatalf("OpenStream: %v", err)
	}
	_ = arms
	h.Install(toolID, &echoCell{w: arms.Pen}, arms.Down)
	// Deliberately do NOT call d.Start(): the app-level pingLoop never runs and
	// no actor traffic exists — the daemon's APPLICATION layer is frozen. Its
	// yamux SESSION is nonetheless fully alive and answering keepalive entirely
	// on its own (yamux's newSession launches the keepalive goroutine
	// unconditionally, independent of any application code) — exactly the "app
	// goroutine wedged, yamux read/write loops still scheduled" daemon the
	// Lease exists to catch, reproduced here by simply never starting the app
	// layer rather than by actually wedging a goroutine.
	start := time.Now()

	// Well inside the TTL window, several keepalive round trips have already
	// landed (200ms TTL / 15ms keepalive ≈ 13 ticks by TTL) — confirm the lease
	// has NOT been torn down yet, i.e. this is a real mid-flight observation of
	// live keepalive traffic, not a window too short for it to occur.
	time.Sleep(80 * time.Millisecond)
	if down := r.getDown(); len(down) != 0 {
		t.Fatalf("lease already expired at 80ms (TTL=200ms) despite ongoing keepalive traffic: down=%v", down)
	}

	// The lease still expires once the app-frame silence outlasts the TTL — the
	// regression this test guards would instead have kept refreshing forever
	// off keepalive alone, so this loop would never observe a down and the
	// bounded deadline below is what actually catches it.
	deadline := time.Now().Add(5 * time.Second)
	for {
		if down := r.getDown(); len(down) >= 1 && down[0] == toolID {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("lease never expired the frozen-app link to down, despite yamux keepalive genuinely running — keepalive traffic is refreshing the lease again")
		}
		time.Sleep(5 * time.Millisecond)
	}
	// The expiry should land close to the configured TTL, not near the test's
	// own generous outer deadline — a wide margin here would itself suggest the
	// down edge came from something other than the TTL judgment.
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("lease expiry took implausibly long (%v) for a 200ms TTL — suspect keepalive intermittently refreshed it", elapsed)
	}

	// The daemon side also observes the link tearing down.
	select {
	case <-d.Done():
	case <-time.After(10 * time.Second):
		t.Fatal("daemon never saw the link close after lease expiry")
	}
}

// TestEndToEnd_CancelRequest_CrossWire proves the request-scope of cancel(scope)
// crosses the wire: a daemon cell is parked in Receive on an in-flight request;
// the home calls rt.CancelRequest(actor, requestID) (the same runtime call
// Home.CancelRequest forwards — the substrate mechanism the app's
// caller-abandon trigger drives), which writes a KindCancel frame down that
// actor's stream; the daemon fires the matching reqCtx OFF the cell goroutine,
// so the parked Receive's ctx goes Done. No truth terminal is written — cancel
// is a best-effort interrupt of in-flight work, the caller's closure owns the
// terminal (the home stubWriter records nothing).
func TestEndToEnd_CancelRequest_CrossWire(t *testing.T) {
	r := newHomeRig(t, 5*time.Second, 30*time.Second)

	const toolID = actor.ActorID("tool:blocker")
	d, err := link.Dial(context.Background(), r.wsURL(), "daemon-1",
		[]link.Declaration{{ActorID: toolID, Kind: actor.KindTool, Binding: actor.BindingEmbedded}}, link.DialConfig{}, nil)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer func() { _ = d.Close() }()

	h := newDaemonHost()
	defer h.Stop()

	cell := &blockingCell{started: make(chan struct{}), cancelled: make(chan struct{})}
	arms, err := d.OpenStream(toolID, func(env *message.Envelope) error {
		return h.Dispatch(toolID, env)
	}, func(requestID message.ID) { h.CancelRequest(toolID, requestID) })
	if err != nil {
		t.Fatalf("OpenStream: %v", err)
	}
	_ = arms
	h.Install(toolID, cell, nil)
	d.Start()

	// Home dispatches a request; the cell parks in Receive on it (no ExpiresAt, so
	// the ONLY way out is an explicit cancel — not a deadline).
	const reqID = message.ID("req-cancel-1")
	if _, err := r.deliver.Deliver([]actor.ActorID{toolID}, &message.Envelope{
		ID:        reqID,
		ChannelID: testChannelID,
		Kind:      message.KindRequest,
		Type:      "block.do",
		Sender:    message.Sender{ID: "user:a", Kind: actor.KindHuman},
		Audience:  message.Audience{toolID},
	}); err != nil {
		t.Fatalf("home deliver: %v", err)
	}

	select {
	case <-cell.started:
	case <-time.After(10 * time.Second):
		t.Fatal("cell never entered Receive on the request")
	}

	// The home reaches the request-scope of cancel(scope) across the wire.
	r.rt.CancelRequest(toolID, reqID)

	select {
	case <-cell.cancelled:
	case <-time.After(10 * time.Second):
		t.Fatal("cross-wire KindCancel never cancelled the hosted cell's reqCtx")
	}

	// No truth terminal: cancel is a best-effort interrupt, not a write. The home
	// write gate recorded nothing (the caller's closure owns the terminal).
	if got := r.minter.all(); len(got) != 0 {
		t.Fatalf("cancel wrote truth terminal(s) %+v, want none", got)
	}
}

// TestReattach_FullSetReplace proves the §S-P8 full-set idiom: Reattach
// wholesale-REPLACES the home's declared set, it never merges. A larger
// declared set lets a newly-declared actor's stream open; a smaller one makes a
// previously-declared actor's NEXT stream open fail — the id simply fell out.
func TestReattach_FullSetReplace(t *testing.T) {
	r := newHomeRig(t, 5*time.Second, 30*time.Second)

	const (
		toolA = actor.ActorID("tool:a")
		toolB = actor.ActorID("tool:b")
	)
	d, err := link.Dial(context.Background(), r.wsURL(), "daemon-1",
		[]link.Declaration{{ActorID: toolA, Kind: actor.KindTool}}, link.DialConfig{}, nil)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer func() { _ = d.Close() }()

	noopDispatch := func(*message.Envelope) error { return nil }

	// toolB was never declared: its stream must not open.
	if _, err := d.OpenStream(toolB, noopDispatch, nil); err == nil {
		t.Fatal("OpenStream(toolB) succeeded before any declaration — want rejected")
	}

	// Reattach the FULL set {a, b} (never an increment) — b becomes openable.
	if err := d.Reattach(context.Background(), []link.Declaration{
		{ActorID: toolA, Kind: actor.KindTool},
		{ActorID: toolB, Kind: actor.KindTool},
	}); err != nil {
		t.Fatalf("Reattach (add): %v", err)
	}
	if _, err := d.OpenStream(toolB, noopDispatch, nil); err != nil {
		t.Fatalf("OpenStream(toolB) after Reattach: %v", err)
	}

	// Reattach the REDUCED set {b} only: a's declaration falls out of the
	// wholesale replacement, so a NEW stream open for toolA must now fail.
	if err := d.Reattach(context.Background(), []link.Declaration{
		{ActorID: toolB, Kind: actor.KindTool},
	}); err != nil {
		t.Fatalf("Reattach (shrink): %v", err)
	}
	if _, err := d.OpenStream(toolA, noopDispatch, nil); err == nil {
		t.Fatal("OpenStream(toolA) succeeded after it fell out of the Reattach set — want rejected")
	}
}

// TestReattach_RejectedLeavesPriorSetIntact proves a REJECTED Reattach (the
// reserved system-actor-id guard) surfaces its reason in the returned error and
// does not clobber the link's existing declared set — a previously-declared
// actor's stream still opens normally afterwards.
func TestReattach_RejectedLeavesPriorSetIntact(t *testing.T) {
	r := newHomeRig(t, 5*time.Second, 30*time.Second)

	const toolA = actor.ActorID("tool:a")
	d, err := link.Dial(context.Background(), r.wsURL(), "daemon-1",
		[]link.Declaration{{ActorID: toolA, Kind: actor.KindTool}}, link.DialConfig{}, nil)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer func() { _ = d.Close() }()

	err = d.Reattach(context.Background(), []link.Declaration{
		{ActorID: actor.SystemActorID, Kind: actor.KindSystem},
	})
	if err == nil {
		t.Fatal("Reattach declaring the reserved system actor id unexpectedly succeeded")
	}

	// The prior set (toolA) must still be intact — the rejected Reattach never
	// replaced it.
	if _, err := d.OpenStream(toolA, func(*message.Envelope) error { return nil }, nil); err != nil {
		t.Fatalf("OpenStream(toolA) after a rejected Reattach: %v", err)
	}
}

// TestHardLinkDrop_DownEdgeDecaysDevicePresence pins the ABNORMAL half of the
// link-death lifecycle (the counterpart of TestHomeCloseQuietTeardown): a raw
// transport-level drop of the daemon's /compute connection (kill -9 / network
// partition — NOT a graceful KindDetach) makes the home port read a hard error,
// publish the LOUD down edge, and that edge decays the actor's folded device
// presence to unknown (the L3 link-death cascade backstop). This coverage lives
// HERE, at the mechanism layer, because since S1 a graceful ctx-cancel detaches
// quiet — the only trigger for the decay is the raw drop, which app-level tests
// cannot produce without reaching under the transport.
func TestHardLinkDrop_DownEdgeDecaysDevicePresence(t *testing.T) {
	rt, del := actorrt.New(actorrt.Config{Parent: context.Background()})
	r := &homeRig{
		rt:         rt,
		deliver:    del,
		minter:     &stubMinter{},
		membership: &stubMembership{},
	}
	rt.WatchDown(watcherFunc(func(_ context.Context, id actor.ActorID, _ actorrt.Incarnation, _ error) {
		r.mu.Lock()
		r.downActors = append(r.downActors, id)
		r.mu.Unlock()
	}))
	// The device-presence fold rides the SAME down edge (registered exactly as
	// platform/home.go wires it): the drop must decay its folded level.
	fold := presence.New(nil, time.Now, []actorrt.ObsKind{actorrt.ObsKind(introspect.ObsDevicePresence)}, nil, 30*time.Second)
	rt.WatchDown(fold)
	r.acc = link.NewAcceptor(link.Config{
		Minter:     r.minter,
		Runtime:    rt,
		Membership: r.membership,
		ChannelID:  testChannelID,
		LeasePing:  5 * time.Second,
		LeaseTTL:   30 * time.Second,
	})
	// httptest's Close/CloseClientConnections never touch a hijacked WS conn, so
	// the raw transport handle is captured via ConnState — closing IT is the hard
	// drop (no frame, no close handshake; the home just stops hearing the daemon).
	var connMu sync.Mutex
	var hijacked net.Conn
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		r.acc.Serve(w, req, "daemon-1")
	}))
	srv.Config.ConnState = func(c net.Conn, state http.ConnState) {
		if state == http.StateHijacked {
			connMu.Lock()
			hijacked = c
			connMu.Unlock()
		}
	}
	srv.Start()
	r.srv = srv
	t.Cleanup(func() { _ = r.acc.Close(); srv.Close() })

	const toolID = actor.ActorID("tool:device")
	d, err := link.Dial(context.Background(), r.wsURL(), "daemon-1",
		[]link.Declaration{{ActorID: toolID, Kind: actor.KindTool, Binding: actor.BindingEmbedded}}, link.DialConfig{}, nil)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer func() { _ = d.Close() }()

	h := newDaemonHost()
	defer h.Stop()

	arms, err := d.OpenStream(toolID, func(env *message.Envelope) error {
		return h.Dispatch(toolID, env)
	}, func(requestID message.ID) { h.CancelRequest(toolID, requestID) })
	if err != nil {
		t.Fatalf("OpenStream: %v", err)
	}
	h.Install(toolID, &echoCell{w: arms.Pen}, nil)
	d.Start()

	// Round-trip a request first: proves the port is fully attached, so the drop
	// below is observed by a LIVE port (not a half-built one).
	if _, err := r.deliver.Deliver([]actor.ActorID{toolID}, &message.Envelope{
		ID: "req-1", ChannelID: testChannelID, Kind: message.KindRequest, Type: "echo.do",
		Sender: message.Sender{ID: "user:a", Kind: actor.KindHuman}, Audience: message.Audience{toolID},
	}); err != nil {
		t.Fatalf("home deliver: %v", err)
	}
	deadline := time.Now().Add(10 * time.Second)
	for len(r.minter.all()) == 0 {
		if time.Now().After(deadline) {
			t.Fatal("cell emit never reached the home writer (port not fully attached)")
		}
		time.Sleep(5 * time.Millisecond)
	}

	// Seed a KNOWN device-presence level for the actor — the thing the drop decays.
	gen, ok := rt.CurrentIncarnation(toolID)
	if !ok {
		t.Fatal("home port has no live incarnation")
	}
	fold.OnObs(context.Background(), toolID,
		gen,
		actorrt.ObsKind(introspect.ObsDevicePresence), actorrt.ObsValue(`{"online":true}`))
	view := presence.NewView(fold, rt, memberPresenceRegistry{})
	snapshot, err := view.Snapshot(context.Background(), toolID)
	if err != nil || snapshot.L3[actorrt.ObsKind(introspect.ObsDevicePresence)].Val == nil {
		t.Fatal("device presence not folded — precondition broken")
	}

	// HARD drop: close the daemon's hijacked /compute conn at the transport.
	connMu.Lock()
	hc := hijacked
	connMu.Unlock()
	if hc == nil {
		t.Fatal("the /compute WS connection was never hijacked (cannot simulate abnormal death)")
	}
	_ = hc.Close()

	// The home must observe the LOUD down edge AND the fold must decay to unknown.
	deadline = time.Now().Add(10 * time.Second)
	for {
		downSeen := false
		for _, id := range r.getDown() {
			if id == toolID {
				downSeen = true
			}
		}
		snapshot, _ := view.Snapshot(context.Background(), toolID)
		_, known := snapshot.L3[actorrt.ObsKind(introspect.ObsDevicePresence)]
		if downSeen && !known {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("hard link drop never cascaded (down edge seen=%v, presence known=%v — want true/false)", downSeen, known)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// TestEndToEnd_DespawnID_CrossWireKill proves DespawnID's remote-kill semantics
// across a real wire — the mechanism period8 S1's Home.Remove step ① composes
// over (rt.DespawnID(id) reaching a daemon-attached identity, not merely
// deleting a local map entry). SetDespawnLocal (the daemon's own
// RunCompute-style wiring, platform/compute.go:287) is installed so a
// KindDespawn frame really ends the remote cell's execution arm; the daemon
// replies KindDetach and the port dies STOPPING (stopDespawn sets `stopping`
// before the wire write, port.go), so the home publishes NO down edge for a
// by-name kill — a despawn is never a death.
func TestEndToEnd_DespawnID_CrossWireKill(t *testing.T) {
	r := newHomeRig(t, 5*time.Second, 30*time.Second)
	h := newDaemonHost()
	defer h.Stop()

	const toolID = actor.ActorID("tool:despawn-target")
	d, err := link.Dial(context.Background(), r.wsURL(), "daemon-1",
		[]link.Declaration{{ActorID: toolID, Kind: actor.KindTool, Binding: actor.BindingEmbedded}}, link.DialConfig{DespawnLocal: func(id actor.ActorID) { h.rt.DespawnID(id) }}, nil)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer func() { _ = d.Close() }()
	// The daemon's own kill wiring: a host KindDespawn frame despawns the local
	// cell in the daemon's runtime (exactly platform/compute.go's RunCompute).

	arms, err := d.OpenStream(toolID, func(env *message.Envelope) error {
		return h.Dispatch(toolID, env)
	}, func(requestID message.ID) { h.CancelRequest(toolID, requestID) })
	if err != nil {
		t.Fatalf("OpenStream: %v", err)
	}
	h.Install(toolID, &echoCell{w: arms.Pen}, arms.Down)
	d.Start()

	// Precondition: the daemon-side cell is actually live before the kill.
	if _, ok := h.rt.Stat(toolID); !ok {
		t.Fatal("daemon cell not live before DespawnID — precondition broken")
	}
	// Barrier: the home-side attach registers the port embodiment asynchronously
	// to Dial returning; DespawnID before that registration lands returns false.
	attachDeadline := time.Now().Add(10 * time.Second)
	for {
		if _, ok := r.rt.Stat(toolID); ok {
			break
		}
		if time.Now().After(attachDeadline) {
			t.Fatal("home runtime never registered the port embodiment — attach lost")
		}
		time.Sleep(5 * time.Millisecond)
	}

	// The home-side kill: exactly Home.Remove step ①.
	if ok := r.rt.DespawnID(toolID); !ok {
		t.Fatal("DespawnID(toolID) = false, want true (a live port embodiment)")
	}

	deadline := time.Now().Add(10 * time.Second)
	for {
		if _, ok := h.rt.Stat(toolID); !ok {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("daemon cell never died — KindDespawn frame never reached the remote")
		}
		time.Sleep(5 * time.Millisecond)
	}

	// The kill is a by-name despawn, not a death: the home must NOT publish a
	// down edge for it (port.stopDespawn marks `stopping` before the wire
	// write, so its own die() path takes the "no down edge" branch).
	for _, id := range r.getDown() {
		if id == toolID {
			t.Fatal("DespawnID materialised a down edge — a by-name kill must be quiet, not a death")
		}
	}
}

// TestKickDaemon_ClosesAllLinksIncludingHalfAttached is S3's DoD (§8.3/§7.4):
// KickDaemon(computeID) tears down EVERY link registered under that id — the
// one fully attached (port live, round-tripped a request) AND one still stuck
// in the half-attach window (TCP/WS connected, daemonID pre-authenticated by
// Serve, but no attach frame sent yet). It proves:
//  1. the quiet edge — no receiver_unavailable / down edge for the killed port;
//  2. fail-closed — the home-side port embodiment is gone from the runtime
//     (any pen still welded to that Incarnation is IsLive()==false, so a
//     leaked writer is rejected rather than authoring on a dead incarnation);
//  3. the half-attach sliver — a link that never got as far as a declared
//     actor is closed by the SAME Kick call (registration precedes the attach
//     frame, closing the T2→T3 window per §S-P21).
func TestKickDaemon_ClosesAllLinksIncludingHalfAttached(t *testing.T) {
	r := newHomeRig(t, 5*time.Second, 30*time.Second)

	const toolID = actor.ActorID("tool:echo")
	d, err := link.Dial(context.Background(), r.wsURL(), "daemon-1",
		[]link.Declaration{{ActorID: toolID, Kind: actor.KindTool, Binding: actor.BindingEmbedded}}, link.DialConfig{}, nil)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer func() { _ = d.Close() }()

	h := newDaemonHost()
	defer h.Stop()

	arms, err := d.OpenStream(toolID, func(env *message.Envelope) error {
		return h.Dispatch(toolID, env)
	}, func(requestID message.ID) { h.CancelRequest(toolID, requestID) })
	if err != nil {
		t.Fatalf("OpenStream: %v", err)
	}
	h.Install(toolID, &echoCell{w: arms.Pen}, nil)
	d.Start()

	// Round-trip first: proves the port is FULLY attached before the kick.
	req := &message.Envelope{
		ID: "req-1", ChannelID: testChannelID, Kind: message.KindRequest, Type: "echo.do",
		Sender: message.Sender{ID: "user:a", Kind: actor.KindHuman}, Audience: message.Audience{toolID},
	}
	if _, err := r.deliver.Deliver([]actor.ActorID{toolID}, req); err != nil {
		t.Fatalf("home deliver: %v", err)
	}
	deadline := time.Now().Add(10 * time.Second)
	for len(r.minter.all()) == 0 {
		if time.Now().After(deadline) {
			t.Fatal("cell emit never reached the home writer (port not fully attached)")
		}
		time.Sleep(5 * time.Millisecond)
	}
	if _, ok := r.rt.Stat(toolID); !ok {
		t.Fatal("home runtime has no live port embodiment before kick — precondition broken")
	}

	// A SECOND connection to the same daemonID, stuck in the half-attach window
	// (Serve has authenticated it as "daemon-1" — runLink has registered it —
	// but it never sends the stream-0 attach frame). Raw gorilla dial, NOT
	// link.Dial (which always completes the attach handshake).
	half, _, err := websocket.DefaultDialer.Dial(r.wsURL(), nil)
	if err != nil {
		t.Fatalf("raw dial (half-attach probe): %v", err)
	}
	defer func() { _ = half.Close() }()
	// Give runLink a moment to actually register the link (registration races
	// the TCP accept — this only needs to win before KickDaemon below, not
	// before some fixed deadline).
	time.Sleep(50 * time.Millisecond)

	n := r.acc.KickDaemon("daemon-1")
	if n != 2 {
		t.Fatalf("KickDaemon = %d, want 2 (one fully attached + one half-attached)", n)
	}

	// IsAttached flips false once the fully-attached link's runLink unwinds.
	deadline = time.Now().Add(10 * time.Second)
	for r.acc.IsAttached("daemon-1") {
		if time.Now().After(deadline) {
			t.Fatal("IsAttached(daemon-1) still true after KickDaemon")
		}
		time.Sleep(5 * time.Millisecond)
	}

	// No down edge for the killed port — kick is a voluntary revocation, not an
	// observed death.
	for _, id := range r.getDown() {
		if id == toolID {
			t.Fatal("KickDaemon materialised a down edge — a kick must be quiet, not a death")
		}
	}

	// Fail-closed: the home-side port embodiment is gone (any writer still
	// welded to that Incarnation is rejected as not-live, ErrWriterNotLive).
	deadline = time.Now().Add(10 * time.Second)
	for {
		if _, ok := r.rt.Stat(toolID); !ok {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("home runtime still reports a live port embodiment after KickDaemon")
		}
		time.Sleep(5 * time.Millisecond)
	}

	// The half-attached raw connection is closed by the SAME Kick call — a read
	// on it must observe the close, not hang.
	_ = half.SetReadDeadline(time.Now().Add(5 * time.Second))
	if _, _, err := half.ReadMessage(); err == nil {
		t.Fatal("half-attached connection still open after KickDaemon — half-attach window not closed")
	}
}

// TestControlDeath_LeavesNoZombieOnlineAccount is G-1's real-path guard (P2-1).
// The generic handler-drop unit test (tra_dod_test.go) only proves a killed
// control worker stops calling its handler; it says nothing about the ACCOUNT.
// This drives the FULL production path — Serve → runLink → handleAttach →
// markAttached + registerLaneLink — then kills the control flow and asserts the
// terminal account state the工单 names (not a handler call count): the daemon
// must not linger "online" (the deferred markDetached ran symmetrically) and its
// lane-relay entry must be gone (deferred deregisterLaneLink ran) — no zombie
// online row, no dead lane session residual. The teardown ordering H-1's bounded
// waitControlWorkers preserves is exactly what keeps this account clean.
func TestControlDeath_LeavesNoZombieOnlineAccount(t *testing.T) {
	r := newHomeRig(t, 5*time.Second, 30*time.Second)

	const toolID = actor.ActorID("tool:zombie-probe")
	d, err := link.Dial(context.Background(), r.wsURL(), "daemon-1",
		[]link.Declaration{{ActorID: toolID, Kind: actor.KindTool, Binding: actor.BindingEmbedded}}, link.DialConfig{}, nil)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer func() { _ = d.Close() }()

	h := newDaemonHost()
	defer h.Stop()

	arms, err := d.OpenStream(toolID, func(env *message.Envelope) error {
		return h.Dispatch(toolID, env)
	}, func(requestID message.ID) { h.CancelRequest(toolID, requestID) })
	if err != nil {
		t.Fatalf("OpenStream: %v", err)
	}
	h.Install(toolID, &echoCell{w: arms.Pen}, nil)
	d.Start()

	// Round-trip a request so the attach is provably COMPLETE — markAttached and
	// registerLaneLink have both run for daemon-1. The online account is now dirty
	// by design; the death below must clean it symmetrically.
	req := &message.Envelope{
		ID: "req-1", ChannelID: testChannelID, Kind: message.KindRequest, Type: "echo.do",
		Sender: message.Sender{ID: "user:a", Kind: actor.KindHuman}, Audience: message.Audience{toolID},
	}
	if _, err := r.deliver.Deliver([]actor.ActorID{toolID}, req); err != nil {
		t.Fatalf("home deliver: %v", err)
	}
	deadline := time.Now().Add(10 * time.Second)
	for len(r.minter.all()) == 0 {
		if time.Now().After(deadline) {
			t.Fatal("cell emit never reached the home writer (attach not complete)")
		}
		time.Sleep(5 * time.Millisecond)
	}
	// Precondition: the account is genuinely dirty before the death.
	if !r.acc.IsAttached("daemon-1") {
		t.Fatal("daemon-1 not online after a completed attach — precondition broken")
	}
	if !r.acc.LaneLinkPresentForTest("daemon-1") {
		t.Fatal("daemon-1 has no lane-relay link after attach — precondition broken")
	}

	// Kill the control flow: the daemon closes its end, the home session dies, and
	// runLink unwinds through the teardown funnel (waitControlWorkers → deferred
	// markDetached + deregisterLaneLink). Any control-flow death funnels the same
	// way; a daemon close is the deterministic trigger.
	if err := d.Close(); err != nil {
		t.Fatalf("daemon Close: %v", err)
	}

	// Terminal account state: daemon-1 gone from the online account (no zombie
	// "still online") AND no residual lane-relay entry.
	deadline = time.Now().Add(10 * time.Second)
	for {
		online := r.acc.IsAttached("daemon-1")
		lane := r.acc.LaneLinkPresentForTest("daemon-1")
		if !online && !lane {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("after control death: IsAttached=%v laneLinkPresent=%v — want both false (zombie online account / dead lane residual)", online, lane)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func mustDeliver(t *testing.T, r *homeRig, target actor.ActorID) error {
	t.Helper()
	_, err := r.deliver.Deliver([]actor.ActorID{target}, &message.Envelope{
		ID:        "kill-1",
		ChannelID: testChannelID,
		Kind:      message.KindRequest,
		Type:      "x.kill",
		Sender:    message.Sender{ID: "user:a", Kind: actor.KindHuman},
		Audience:  message.Audience{target},
	})
	if err != nil {
		t.Fatalf("deliver: %v", err)
	}
	return nil
}
