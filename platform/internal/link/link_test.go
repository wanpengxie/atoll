package link_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/wanpengxie/ActOS/platform/internal/link"
	"github.com/wanpengxie/ActOS/protocol/actor"
	"github.com/wanpengxie/ActOS/protocol/channel"
	"github.com/wanpengxie/ActOS/protocol/message"
	"github.com/wanpengxie/ActOS/runtime/actorrt"
	"github.com/wanpengxie/ActOS/runtime/harness"
	"github.com/wanpengxie/ActOS/runtime/storespec"
)

const testChannelID = channel.ID("test-channel")

// --- stubs ---

// stubMinter is the test substrate铸笔机: Mint welds (id, chID) onto a stubPen
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

func (s *stubMinter) Mint(id actor.ActorID, chID channel.ID) harness.Pen {
	return &stubPen{minter: s, id: id, chID: chID}
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
	env.ChannelID = p.chID
	return p.minter.record(env), nil
}

type stubMembership struct {
	mu   sync.Mutex
	adds []storespec.MemberActorAdd
}

func (s *stubMembership) Insert(context.Context, storespec.Record) error         { return nil }
func (s *stubMembership) Deregister(context.Context, actor.ActorID, int64) error { return nil }
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

// blockingCell blocks in Receive until its (per-request) ctx is cancelled,
// signalling entry on started and the ctx-cancel outcome on cancelled. It is the
// cross-wire cancel probe: the request occupies the cell goroutine, and the only
// way out is the reqCtx going Done — proving the home's KindCancel reached the
// daemon and fired exactly this request's reqCtx OFF the goroutine.
type blockingCell struct {
	started   chan struct{}
	cancelled chan struct{}
}

func (b *blockingCell) Receive(ctx context.Context, env *message.Envelope) error {
	if env.Kind != message.KindRequest {
		return nil
	}
	close(b.started)
	<-ctx.Done()
	close(b.cancelled)
	return nil
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
	rt.WatchPresence(h)
	return h
}

func (h *daemonHost) OnDown(_ context.Context, id actor.ActorID, cause error) {
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
	h.rt.Spawn(id, impl)
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
	rt.WatchPresence(watcherFunc(func(_ context.Context, id actor.ActorID, _ error) {
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

type watcherFunc func(context.Context, actor.ActorID, error)

func (f watcherFunc) OnDown(ctx context.Context, id actor.ActorID, cause error) { f(ctx, id, cause) }

// --- tests ---

// TestEndToEnd_AttachDispatchEmit drives a full home↔daemon link: a daemon
// attaches one actor, the home dispatches a request down the actor's stream into
// the daemon cell, and the cell's emit flows UP that same stream to the home
// write门. Native ipc over a mux stream, zero translation.
func TestEndToEnd_AttachDispatchEmit(t *testing.T) {
	r := newHomeRig(t, 5*time.Second, 30*time.Second)

	const toolID = actor.ActorID("tool:echo")
	d, err := link.Dial(context.Background(), r.wsURL(), "daemon-1",
		[]link.Declaration{{ActorID: toolID, Kind: actor.KindTool, Binding: actor.BindingEmbedded}}, nil)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer func() { _ = d.Close() }()

	if d.ChannelID() != string(testChannelID) {
		t.Fatalf("ChannelID = %q, want %q", d.ChannelID(), testChannelID)
	}

	h := newDaemonHost()
	defer h.Stop()

	pen, _, err := d.OpenStream(toolID, func(env *message.Envelope) error {
		return h.Dispatch(toolID, env)
	}, func(requestID message.ID) { h.CancelRequest(toolID, requestID) })
	if err != nil {
		t.Fatalf("OpenStream: %v", err)
	}
	h.Install(toolID, &echoCell{w: pen}, nil)
	d.Start()

	// Membership registered the declared actor (register/reactivate semantics).
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

	// The cell's response flows UP to the home write门.
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
		[]link.Declaration{{ActorID: toolID, Kind: actor.KindTool, Binding: actor.BindingEmbedded}}, nil)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer func() { _ = d.Close() }()

	h := newDaemonHost()
	defer h.Stop()

	pen, _, err := d.OpenStream(toolID, func(env *message.Envelope) error {
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

	h.Install(toolID, &echoCell{w: pen}, nil)
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

// TestEndToEnd_CellDeath_ClosesStreamToOnDown proves a daemon cell's abnormal
// death closes its link stream, the home port reads EOF, and the home publishes
// the presence-down edge (the closure trigger). Death is the stream EOF — no
// translated death frame.
func TestEndToEnd_CellDeath_ClosesStreamToOnDown(t *testing.T) {
	r := newHomeRig(t, 5*time.Second, 30*time.Second)

	const toolID = actor.ActorID("tool:doomed")
	d, err := link.Dial(context.Background(), r.wsURL(), "daemon-1",
		[]link.Declaration{{ActorID: toolID, Kind: actor.KindTool, Binding: actor.BindingEmbedded}}, nil)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer func() { _ = d.Close() }()

	h := newDaemonHost()
	defer h.Stop()

	pen, downHandler, err := d.OpenStream(toolID, func(env *message.Envelope) error {
		return h.Dispatch(toolID, env)
	}, func(requestID message.ID) { h.CancelRequest(toolID, requestID) })
	if err != nil {
		t.Fatalf("OpenStream: %v", err)
	}
	_ = pen
	h.Install(toolID, panicCell{}, downHandler)
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
			t.Fatal("home never observed the cell's presence-down edge over the link")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// TestLease_NoTraffic_ExpiresToPresenceDown proves the home's lease judges
// liveness: with the daemon NOT pinging (Start never called) and a short TTL,
// the home tears the link down on TTL — the actor stream EOFs and the home
// publishes presence-down (the正面观察 a frozen daemon's TCP never produces).
func TestLease_NoTraffic_ExpiresToPresenceDown(t *testing.T) {
	r := newHomeRig(t, 20*time.Millisecond, 60*time.Millisecond)

	const toolID = actor.ActorID("tool:frozen")
	d, err := link.Dial(context.Background(), r.wsURL(), "daemon-1",
		[]link.Declaration{{ActorID: toolID, Kind: actor.KindTool, Binding: actor.BindingEmbedded}}, nil)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer func() { _ = d.Close() }()

	h := newDaemonHost()
	defer h.Stop()

	pen, downHandler, err := d.OpenStream(toolID, func(env *message.Envelope) error {
		return h.Dispatch(toolID, env)
	}, func(requestID message.ID) { h.CancelRequest(toolID, requestID) })
	if err != nil {
		t.Fatalf("OpenStream: %v", err)
	}
	_ = pen
	h.Install(toolID, &echoCell{w: pen}, downHandler)
	// Deliberately do NOT call d.Start(): no idle ping, so no inbound traffic
	// refreshes the home lease. The daemon is "frozen" from the home's view.

	deadline := time.Now().Add(10 * time.Second)
	for {
		if down := r.getDown(); len(down) >= 1 && down[0] == toolID {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("lease never expired the frozen link to presence-down")
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

// TestEndToEnd_CancelRequest_CrossWire proves the request-scope of cancel(scope)
// crosses the wire: a daemon cell is parked in Receive on an in-flight request;
// the home calls Acceptor.CancelRequest(actor, requestID) (the substrate
// mechanism the app's caller-abandon trigger drives), which writes a KindCancel
// frame down that actor's stream; the daemon fires the matching reqCtx OFF the
// cell goroutine, so the parked Receive's ctx goes Done. No truth terminal is
// written — cancel is a best-effort interrupt of in-flight work, the caller's
// closure owns the terminal (the home stubWriter records nothing).
func TestEndToEnd_CancelRequest_CrossWire(t *testing.T) {
	r := newHomeRig(t, 5*time.Second, 30*time.Second)

	const toolID = actor.ActorID("tool:blocker")
	d, err := link.Dial(context.Background(), r.wsURL(), "daemon-1",
		[]link.Declaration{{ActorID: toolID, Kind: actor.KindTool, Binding: actor.BindingEmbedded}}, nil)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer func() { _ = d.Close() }()

	h := newDaemonHost()
	defer h.Stop()

	cell := &blockingCell{started: make(chan struct{}), cancelled: make(chan struct{})}
	pen, _, err := d.OpenStream(toolID, func(env *message.Envelope) error {
		return h.Dispatch(toolID, env)
	}, func(requestID message.ID) { h.CancelRequest(toolID, requestID) })
	if err != nil {
		t.Fatalf("OpenStream: %v", err)
	}
	_ = pen
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
	r.acc.CancelRequest(toolID, reqID)

	select {
	case <-cell.cancelled:
	case <-time.After(10 * time.Second):
		t.Fatal("cross-wire KindCancel never cancelled the hosted cell's reqCtx")
	}

	// No truth terminal: cancel is a best-effort interrupt, not a write. The home
	// write门 recorded nothing (the caller's closure owns the terminal).
	if got := r.minter.all(); len(got) != 0 {
		t.Fatalf("cancel wrote truth terminal(s) %+v, want none", got)
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
