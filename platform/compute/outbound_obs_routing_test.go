package compute

import (
	"fmt"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/runtime/actorhost"
	"github.com/wanpengxie/atoll/runtime/actorrt"
)

// DaemonOutbound.publishObs is the daemon's routing step for every observation
// an actor body pushes: daemonHostEvents.OnBodyObs hands it the publisher's
// Incarnation and it must find THE slot minted for that exact body — the slot
// being the body's one stable arm onto the wire.
//
// Routing is the whole job, and the identity it routes by is the Incarnation,
// not the (ActorID, AttemptKey) coordinate. One attempt can own more than one
// Unit: an abandoned build's slot is registered before its build is dropped and
// stays registered until retirement closes it, so for that window two unclosed
// slots carry the same coordinate. The coordinate is therefore a many-to-one
// projection of the slot it must select; the Incarnation is exactly 1:1 with
// it. These tests pin what that scan must produce.

// heldObsKinds reports the observations a slot is holding for its next stream.
// With no session wired, a published observation cannot go out, so the slot
// that received it is exactly the slot holding it — which makes "who was this
// routed to" directly observable without a wire.
func heldObsKinds(slot *OutboundSlot) []actorrt.ObsKind {
	slot.obsMu.Lock()
	defer slot.obsMu.Unlock()
	out := make([]actorrt.ObsKind, 0, len(slot.pendingObs))
	for kind := range slot.pendingObs {
		out = append(out, kind)
	}
	return out
}

func heldObsCount(slot *OutboundSlot) int {
	slot.obsMu.Lock()
	defer slot.obsMu.Unlock()
	return len(slot.pendingObs)
}

// TestPublishObsRoutesByPublisherIncarnation is the positive contract: with
// several live slots present, each observation reaches the slot of the exact
// body that published it, an identity no live slot answers to is carried by
// nobody, and a closed slot is never chosen.
func TestPublishObsRoutesByPublisherIncarnation(t *testing.T) {
	t.Parallel()
	outbound := NewDaemonOutbound(DaemonOutboundConfig{PollInterval: 5 * time.Millisecond})
	builds := make(chan outboundBuild)
	host := newOutboundHost(t, outbound, builds, false)
	defer closeOutboundFixture(t, host, outbound)

	alpha := actor.ActorID("agent:obs-alpha")
	beta := actor.ActorID("agent:obs-beta")
	if err := host.AcceptFullDesired([]actorhost.Desired{
		outboundDesired(t, alpha, outboundAttempt(t)),
		outboundDesired(t, beta, outboundAttempt(t)),
	}); err != nil {
		t.Fatal(err)
	}
	slots := map[actor.ActorID]*OutboundSlot{}
	selves := map[actor.ActorID]actorrt.Incarnation{}
	for range 2 {
		build := <-builds
		close(build.release)
		slots[build.input.ActorID] = build.prepared.Slot
		selves[build.input.ActorID] = build.input.Self
	}
	eventuallyOutbound(t, func() bool {
		return slots[alpha] != nil && slots[beta] != nil
	})

	// The publisher's own identity selects the publisher's own slot.
	outbound.publishObs(selves[alpha], "presence", actorrt.ObsValue(`{"online":true}`))
	if got := heldObsKinds(slots[alpha]); len(got) != 1 || got[0] != "presence" {
		t.Fatalf("alpha slot holds %v, want the observation its own body published", got)
	}
	if n := heldObsCount(slots[beta]); n != 0 {
		t.Fatalf("beta slot received alpha's observation (%d held)", n)
	}

	// An identity no live slot answers to has no arm to carry it. Nothing may
	// absorb it — in particular the scan must not fall back to a coordinate and
	// hand it to some other body.
	outbound.publishObs(actorrt.Incarnation{}, "stranger", actorrt.ObsValue(`{}`))
	if n := heldObsCount(slots[alpha]); n != 1 {
		t.Fatalf("alpha slot took an observation from an identity it does not own (%d held)", n)
	}
	if n := heldObsCount(slots[beta]); n != 0 {
		t.Fatalf("beta slot took an observation from an identity it does not own (%d held)", n)
	}

	// The other body's identity reaches the other body's slot, and only it.
	outbound.publishObs(selves[beta], "beta-presence", actorrt.ObsValue(`{}`))
	if n := heldObsCount(slots[beta]); n != 1 {
		t.Fatalf("beta slot holds %d, want the observation its own body published", n)
	}
	if n := heldObsCount(slots[alpha]); n != 1 {
		t.Fatalf("alpha slot absorbed beta's observation (%d held)", n)
	}

	// A closed slot is not a routing target: its held observations were dropped
	// at Close (the body is gone; a successor mints its own), so handing it more
	// would be writing into a grave.
	if err := slots[beta].Close(); err != nil {
		t.Fatal(err)
	}
	outbound.publishObs(selves[beta], "after-close", actorrt.ObsValue(`{}`))
	if n := heldObsCount(slots[beta]); n != 0 {
		t.Fatalf("closed slot accepted an observation (%d held)", n)
	}
}

// TestPublishObsRoutesToTheLiveIncarnationWhenAnAbandonedBuildSharesItsAttemptKey
// is defect A2's regression guard.
//
// The scenario is an ordinary plan flap, entirely within one AcceptFullDesired
// stream: the row for an actor disappears from one plan snapshot and comes back
// in the next one carrying the SAME AttemptKey (a plan republish, not a new
// incarnation decision). The Host abandons the in-flight build for the
// withdrawn row and starts a fresh one for the restored row. Both builds have
// already called DaemonOutbound.Prepare, so for the length of the second
// build's window there are two registered, unclosed slots whose (ActorID,
// AttemptKey) coordinates are identical — one belonging to the incarnation that
// actually got published, one to the build that lost.
//
// Selecting between them by coordinate is undefined: the scan takes the first
// match and Go randomizes map iteration, so a large share of everything the
// live body publishes used to be handed to the abandoned slot instead — and
// that slot's Close() drops its held observations on the floor, so those pushes
// vanished with no error anywhere (measured 171/200 before the fix). The
// Incarnation the Host forwards separates the two slots exactly, which is why
// it, and not the coordinate, is the routing identity.
func TestPublishObsRoutesToTheLiveIncarnationWhenAnAbandonedBuildSharesItsAttemptKey(t *testing.T) {
	t.Parallel()
	outbound := NewDaemonOutbound(DaemonOutboundConfig{PollInterval: 5 * time.Millisecond})
	builds := make(chan outboundBuild)
	host := newOutboundHost(t, outbound, builds, false)
	defer closeOutboundFixture(t, host, outbound)

	id := actor.ActorID("agent:obs-plan-flap")
	key := outboundAttempt(t)
	desired := []actorhost.Desired{outboundDesired(t, id, key)}

	if err := host.AcceptFullDesired(desired); err != nil {
		t.Fatal(err)
	}
	abandoned := <-builds // held inside the builder: its slot is registered and open
	defer close(abandoned.release)

	// Plan flap: the row is withdrawn and republished unchanged.
	if err := host.AcceptFullDesired(nil); err != nil {
		t.Fatal(err)
	}
	if err := host.AcceptFullDesired(desired); err != nil {
		t.Fatal(err)
	}
	live := <-builds
	close(live.release)
	eventuallyOutbound(t, live.input.Current.IsCurrent)

	if abandoned.prepared.Slot.closed.Load() {
		t.Skip("the abandoned build's slot closed before the window opened")
	}
	outbound.mu.Lock()
	registered := len(outbound.slots)
	outbound.mu.Unlock()
	if registered != 2 {
		t.Fatalf("registered slots = %d, want the two same-coordinate slots this defect needs", registered)
	}
	if abandoned.input.AttemptKey != live.input.AttemptKey {
		t.Fatalf("attempt keys differ (%q vs %q), so the two slots are not ambiguous by coordinate and this test proves nothing",
			abandoned.input.AttemptKey, live.input.AttemptKey)
	}
	if abandoned.input.Self == live.input.Self {
		t.Fatal("the two builds share one Incarnation, so identity cannot separate them")
	}

	const pushes = 200
	for i := range pushes {
		outbound.publishObs(live.input.Self, actorrt.ObsKind(fmt.Sprintf("obs-%03d", i)), actorrt.ObsValue(`{}`))
	}

	onLive := heldObsCount(live.prepared.Slot)
	onAbandoned := heldObsCount(abandoned.prepared.Slot)
	if onLive != pushes || onAbandoned != 0 {
		t.Fatalf(
			"observations from the published body: %d reached its own slot, %d were handed to the abandoned build's slot (want %d/0) — everything on the abandoned slot dies silently at its Close",
			onLive, onAbandoned, pushes,
		)
	}
}
