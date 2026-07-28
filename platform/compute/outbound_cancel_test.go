package compute

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/platform/internal/link"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/runtime/actorhost"
)

// OutboundSlot.CancelRequest is the daemon's half of request cancellation: a
// body's Canceller hook (daemonBodyBuilder's actorbase.Hooks) calls it for
// every cancel the actor wants to propagate to the server. It is a HOT PATH
// with no retry, no buffer, and no fallback — and, unlike the five capability
// facades, no test touched it at all.
//
// What it must guarantee is FAIL-CLOSED behaviour: when the physical link is
// not in a state where the cancel can actually be delivered, the call returns
// an error that says so and NOTHING is sent, held, or replayed later. The
// contrast with PublishObs on the same slot is deliberate and is the reason
// this needs its own pinning: an observation is a LEVEL, so the slot carries
// it across a stream gap (TestOutboundLevelObsPublishedBeforeConnectReachesThe
// Channel); a cancel is an EDGE bound to one in-flight request, so replaying it
// onto a later stream would cancel whatever request happens to be running then.

// cancelRecordingProbe is one stream's cancel-side ledger. The shared
// outboundProbe (outbound_test.go) records obs but leaves ActorStreamResource's
// CancelRequest hook unwired, so a cancel-side test needs its own resource.
type cancelRecordingProbe struct {
	mu      sync.Mutex
	seen    []message.ID
	err     error
	done    chan struct{}
	closeOn sync.Once

	// arms carries the five capability arms unchanged: a stream resource with
	// an invalid Arms bundle is rejected before it is ever published, so a
	// cancel-side probe still has to supply them.
	arms outboundProbe
}

func newCancelRecordingProbe() *cancelRecordingProbe {
	return &cancelRecordingProbe{done: make(chan struct{})}
}

func (p *cancelRecordingProbe) record(id message.ID) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.err != nil {
		return p.err
	}
	p.seen = append(p.seen, id)
	return nil
}

func (p *cancelRecordingProbe) cancels() []message.ID {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]message.ID(nil), p.seen...)
}

func (p *cancelRecordingProbe) killStream() {
	p.closeOn.Do(func() { close(p.done) })
}

func (p *cancelRecordingProbe) resource() link.ActorStreamResource {
	return link.ActorStreamResource{
		Arms:          p.arms.arms(),
		Close:         func() error { p.killStream(); return nil },
		Done:          p.done,
		CancelRequest: p.record,
		PublishObs:    func(string, []byte) error { return nil },
	}
}

// newCancelProbeSession builds a session whose every actor stream is backed by
// probe — one probe per session keeps "which stream saw this cancel"
// unambiguous.
func newCancelProbeSession(
	t *testing.T,
	peer string,
	probe *cancelRecordingProbe,
) *link.AuthenticatedLinkSession {
	t.Helper()
	return newOutboundSessionWithOpener(t, peer, func(
		context.Context,
		actor.ActorID,
		actorhost.AttemptKey,
	) (link.ActorStreamResource, error) {
		return probe.resource(), nil
	})
}

// TestOutboundCancelRequestDropsRatherThanQueuesWhenNoStreamIsUp walks one slot
// through the three link states a running daemon actually passes through —
// never connected, connected, connection lost — and asserts the same law each
// time: a cancel either goes out on the live stream NOW or is refused with
// ErrOutboundDisconnected, and a refused cancel leaves no residue that a later
// stream could deliver.
func TestOutboundCancelRequestDropsRatherThanQueuesWhenNoStreamIsUp(t *testing.T) {
	t.Parallel()
	outbound := NewDaemonOutbound(DaemonOutboundConfig{
		PollInterval: 5 * time.Millisecond,
		// A long retry keeps the "stream just died" state stable long enough
		// to assert on it, instead of racing a reopen.
		RetryDelay: time.Hour,
	})
	builds := make(chan outboundBuild)
	host := newOutboundHost(t, outbound, builds, false)
	first := newCancelRecordingProbe()
	second := newCancelRecordingProbe()
	s1 := newCancelProbeSession(t, "server", first)
	s2 := newCancelProbeSession(t, "server", second)
	defer closeOutboundFixture(t, host, outbound, s1, s2)

	id := actor.ActorID("agent:cancel-hot-path")
	key := outboundAttempt(t)
	if err := host.AcceptFullDesired([]actorhost.Desired{outboundDesired(t, id, key)}); err != nil {
		t.Fatal(err)
	}
	build := <-builds
	close(build.release)
	eventuallyOutbound(t, build.input.Current.IsCurrent)
	slot := build.prepared.Slot

	// (1) Body up, no session yet. The daemon is running and the actor may
	// already be cancelling work it started locally.
	if err := slot.CancelRequest("before-connect"); !errors.Is(err, ErrOutboundDisconnected) {
		t.Fatalf("cancel before any session err = %v, want ErrOutboundDisconnected", err)
	}

	// (2) Connected: the cancel reaches the wire, exactly once, unmodified.
	if err := outbound.SetSession(s1); err != nil {
		t.Fatal(err)
	}
	eventuallyOutbound(t, func() bool {
		bundle := slot.arms.Load()
		return bundle != nil && bundle.Session == s1 && bundle.Stream != nil
	})
	if err := slot.CancelRequest("live"); err != nil {
		t.Fatalf("cancel on a live stream: %v", err)
	}
	if got := first.cancels(); len(got) != 1 || got[0] != "live" {
		t.Fatalf("stream saw %v, want exactly the one live cancel (the pre-connect one must never have been queued)", got)
	}

	// (3) Connection lost. The bundle still points at the dead stream until
	// convergence swaps it; the load path must judge the stream, not the
	// pointer.
	first.killStream()
	eventuallyOutbound(t, func() bool {
		bundle := slot.arms.Load()
		return bundle == nil || bundle.Stream == nil || channelClosed(bundle.Stream.Done())
	})
	if err := slot.CancelRequest("after-death"); !errors.Is(err, ErrOutboundDisconnected) {
		t.Fatalf("cancel after stream death err = %v, want ErrOutboundDisconnected", err)
	}
	if got := first.cancels(); len(got) != 1 {
		t.Fatalf("dead stream received more traffic: %v", got)
	}

	// (4) Reconnect. Neither refused cancel may surface on the new stream —
	// they belong to requests that are long over.
	if err := outbound.SetSession(s2); err != nil {
		t.Fatal(err)
	}
	eventuallyOutbound(t, func() bool {
		bundle := slot.arms.Load()
		return bundle != nil && bundle.Session == s2 && bundle.Stream != nil
	})
	if err := slot.CancelRequest("post-reconnect"); err != nil {
		t.Fatalf("cancel after reconnect: %v", err)
	}
	if got := second.cancels(); len(got) != 1 || got[0] != "post-reconnect" {
		t.Fatalf("reconnected stream saw %v, want only the cancel issued after it came up", got)
	}
}

// TestOutboundCancelRequestRefusesAStaleIncarnationAndAClosedSlot pins the two
// fences that are NOT about the wire being down. Both matter because a cancel
// carries a bare message.ID: delivered through the wrong slot it would cancel a
// request the server attributes to a different incarnation of the same actor.
func TestOutboundCancelRequestRefusesAStaleIncarnationAndAClosedSlot(t *testing.T) {
	t.Parallel()
	outbound := NewDaemonOutbound(DaemonOutboundConfig{PollInterval: 5 * time.Millisecond})
	builds := make(chan outboundBuild)
	host := newOutboundHost(t, outbound, builds, false)
	probe := newCancelRecordingProbe()
	session := newCancelProbeSession(t, "server", probe)
	defer closeOutboundFixture(t, host, outbound, session)
	if err := outbound.SetSession(session); err != nil {
		t.Fatal(err)
	}

	id := actor.ActorID("agent:cancel-fenced")
	g1 := outboundAttempt(t)
	if err := host.AcceptFullDesired([]actorhost.Desired{outboundDesired(t, id, g1)}); err != nil {
		t.Fatal(err)
	}
	b1 := <-builds
	close(b1.release)
	eventuallyOutbound(t, func() bool {
		bundle := b1.prepared.Slot.arms.Load()
		return b1.input.Current.IsCurrent() && bundle.Session == session && bundle.Stream != nil
	})
	if err := b1.prepared.Slot.CancelRequest("g1-live"); err != nil {
		t.Fatalf("G1 cancel while current: %v", err)
	}

	// G2 takes over the physical coordinate. G1's body may still be inside a
	// Receive that decides to cancel something.
	g2 := outboundAttempt(t)
	if err := host.AcceptFullDesired([]actorhost.Desired{outboundDesired(t, id, g2)}); err != nil {
		t.Fatal(err)
	}
	b2 := <-builds
	close(b2.release)
	eventuallyOutbound(t, b2.input.Current.IsCurrent)
	eventuallyOutbound(t, func() bool { return !b1.input.Current.IsCurrent() })

	if err := b1.prepared.Slot.CancelRequest("g1-stale"); !errors.Is(err, ErrOutboundNotCurrent) {
		t.Fatalf("superseded G1 cancel err = %v, want ErrOutboundNotCurrent", err)
	}

	// A slot closed out from under a body that the Host still calls current —
	// the daemon shutdown DAG's CloseResidual window — sends nothing either.
	eventuallyOutbound(t, func() bool {
		bundle := b2.prepared.Slot.arms.Load()
		return bundle != nil && bundle.Stream != nil
	})
	if err := outbound.CloseResidual(); err != nil {
		t.Fatal(err)
	}
	if err := b2.prepared.Slot.CancelRequest("g2-after-residual-close"); !errors.Is(err, ErrOutboundClosed) {
		t.Fatalf("closed-slot cancel err = %v, want ErrOutboundClosed", err)
	}

	if got := probe.cancels(); len(got) != 1 || got[0] != "g1-live" {
		t.Fatalf("wire saw %v, want only the cancel issued by the then-current incarnation", got)
	}
}
