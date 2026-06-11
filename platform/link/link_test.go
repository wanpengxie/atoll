package link_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/wanpengxie/ActOS/platform/host"
	"github.com/wanpengxie/ActOS/platform/link"
	"github.com/wanpengxie/ActOS/protocol/actor"
	"github.com/wanpengxie/ActOS/protocol/channel"
	"github.com/wanpengxie/ActOS/protocol/message"
	"github.com/wanpengxie/ActOS/runtime/actorrt"
	"github.com/wanpengxie/ActOS/runtime/harness"
	"github.com/wanpengxie/ActOS/runtime/storespec"
)

const testChannelID = channel.ID("test-channel")

// --- stubs ---

type stubWriter struct {
	mu      sync.Mutex
	writes  []*message.Envelope
	nextSeq int64
}

func (s *stubWriter) Write(_ context.Context, env *message.Envelope) (harness.WriteResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.nextSeq++
	cp := *env
	s.writes = append(s.writes, &cp)
	return harness.WriteResult{MessageID: env.ID, Seq: s.nextSeq}, nil
}

func (s *stubWriter) all() []*message.Envelope {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]*message.Envelope, len(s.writes))
	copy(out, s.writes)
	return out
}

type stubMembership struct {
	mu   sync.Mutex
	adds []storespec.MemberActorAdd
}

func (s *stubMembership) Insert(context.Context, storespec.Record) error         { return nil }
func (s *stubMembership) Deregister(context.Context, actor.ActorID, int64) error { return nil }
func (s *stubMembership) ApplyMemberTransitions(_ context.Context, _ channel.ID, adds []storespec.MemberActorAdd, _ []storespec.MemberActorRemove) error {
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

// echoCell responds to each request via its injected pen.
type echoCell struct{ w harness.Writer }

func (e *echoCell) Receive(ctx context.Context, env *message.Envelope) error {
	if env.Kind != message.KindRequest {
		return nil
	}
	_, _ = e.w.Write(ctx, &message.Envelope{
		ID:        message.ID("resp-" + string(env.ID)),
		ChannelID: env.ChannelID,
		Kind:      message.KindResponse,
		Type:      env.Type,
		ParentID:  env.ID,
		Sender:    message.Sender{ID: env.Audience[0], Kind: actor.KindTool},
		Audience:  message.Audience{env.Sender.ID},
	})
	return nil
}

type panicCell struct{}

func (panicCell) Receive(context.Context, *message.Envelope) error { panic("boom") }

// --- home rig ---

type homeRig struct {
	acc        *link.Acceptor
	rt         *actorrt.Runtime
	deliver    actorrt.Deliverer
	writer     *stubWriter
	membership *stubMembership
	srv        *httptest.Server

	mu         sync.Mutex
	downActors []actor.ActorID
}

func newHomeRig(t *testing.T, leasePing, leaseTTL time.Duration) *homeRig {
	t.Helper()
	rt, del, _ := actorrt.New(actorrt.Config{Parent: context.Background()})
	r := &homeRig{
		rt:         rt,
		deliver:    del,
		writer:     &stubWriter{},
		membership: &stubMembership{},
	}
	rt.WatchPresence(watcherFunc(func(_ context.Context, id actor.ActorID, _ error) {
		r.mu.Lock()
		r.downActors = append(r.downActors, id)
		r.mu.Unlock()
	}))
	r.acc = link.NewAcceptor(link.Config{
		Writer:     r.writer,
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

	h := host.New()
	defer h.Stop()

	pen, _, err := d.OpenStream(toolID, func(env *message.Envelope) error {
		return h.Dispatch(toolID, env)
	})
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
	deadline := time.Now().Add(2 * time.Second)
	for {
		got := r.writer.all()
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

	h := host.New()
	defer h.Stop()

	pen, _, err := d.OpenStream(toolID, func(env *message.Envelope) error {
		return h.Dispatch(toolID, env)
	})
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
	deadline := time.Now().Add(2 * time.Second)
	for {
		got := r.writer.all()
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

	h := host.New()
	defer h.Stop()

	pen, downHandler, err := d.OpenStream(toolID, func(env *message.Envelope) error {
		return h.Dispatch(toolID, env)
	})
	if err != nil {
		t.Fatalf("OpenStream: %v", err)
	}
	_ = pen
	h.Install(toolID, panicCell{}, downHandler)
	d.Start()

	// Dispatch triggers the panic → cell death → downHandler closes the stream →
	// home port EOF → OnDown.
	_ = mustDeliver(t, r, toolID)

	deadline := time.Now().Add(2 * time.Second)
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

	h := host.New()
	defer h.Stop()

	pen, downHandler, err := d.OpenStream(toolID, func(env *message.Envelope) error {
		return h.Dispatch(toolID, env)
	})
	if err != nil {
		t.Fatalf("OpenStream: %v", err)
	}
	_ = pen
	h.Install(toolID, &echoCell{w: pen}, downHandler)
	// Deliberately do NOT call d.Start(): no idle ping, so no inbound traffic
	// refreshes the home lease. The daemon is "frozen" from the home's view.

	deadline := time.Now().Add(2 * time.Second)
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
	case <-time.After(time.Second):
		t.Fatal("daemon never saw the link close after lease expiry")
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
