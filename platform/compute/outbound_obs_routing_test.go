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
// an actor body pushes: daemonHostEvents.OnBodyObs hands it (ActorID,
// AttemptKey, kind, value) and it must find THE slot that belongs to the body
// that published — the slot being the body's one stable arm onto the wire.
//
// Routing is the whole job, and it is done by scanning the slot registry for a
// coordinate match. These tests pin what that scan must produce.

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

// TestPublishObsRoutesByTheFullActorAndAttemptCoordinate is the positive
// contract: with several live slots present, each observation reaches the one
// whose (ActorID, AttemptKey) it carries — neither the actor id alone nor the
// attempt key alone is enough to pick a slot, and a slot that has been closed
// is never chosen even while a matching coordinate is still being published.
func TestPublishObsRoutesByTheFullActorAndAttemptCoordinate(t *testing.T) {
	t.Parallel()
	outbound := NewDaemonOutbound(DaemonOutboundConfig{PollInterval: 5 * time.Millisecond})
	builds := make(chan outboundBuild)
	host := newOutboundHost(t, outbound, builds, false)
	defer closeOutboundFixture(t, host, outbound)

	alpha := actor.ActorID("agent:obs-alpha")
	beta := actor.ActorID("agent:obs-beta")
	alphaKey := outboundAttempt(t)
	betaKey := outboundAttempt(t)
	if err := host.AcceptFullDesired([]actorhost.Desired{
		outboundDesired(t, alpha, alphaKey),
		outboundDesired(t, beta, betaKey),
	}); err != nil {
		t.Fatal(err)
	}
	slots := map[actor.ActorID]*OutboundSlot{}
	for range 2 {
		build := <-builds
		close(build.release)
		slots[build.input.ActorID] = build.prepared.Slot
	}
	eventuallyOutbound(t, func() bool {
		return len(slots) == 2 && slots[alpha] != nil && slots[beta] != nil
	})

	// Right actor, right attempt.
	outbound.publishObs(alpha, alphaKey, "presence", actorrt.ObsValue(`{"online":true}`))
	if got := heldObsKinds(slots[alpha]); len(got) != 1 || got[0] != "presence" {
		t.Fatalf("alpha slot holds %v, want the observation addressed to it", got)
	}
	if n := heldObsCount(slots[beta]); n != 0 {
		t.Fatalf("beta slot received alpha's observation (%d held)", n)
	}

	// Right actor, WRONG attempt: no slot answers to that coordinate, so the
	// observation has no arm to carry it and must not be handed to the other
	// generation of the same actor.
	outbound.publishObs(alpha, betaKey, "misrouted-attempt", actorrt.ObsValue(`{}`))
	if n := heldObsCount(slots[alpha]); n != 1 {
		t.Fatalf("alpha slot took an observation carrying another attempt key (%d held)", n)
	}
	if n := heldObsCount(slots[beta]); n != 0 {
		t.Fatalf("beta slot took an observation carrying another actor id (%d held)", n)
	}

	// Right attempt, WRONG actor: same argument, other axis.
	outbound.publishObs(beta, alphaKey, "misrouted-actor", actorrt.ObsValue(`{}`))
	if heldObsCount(slots[alpha]) != 1 || heldObsCount(slots[beta]) != 0 {
		t.Fatalf("cross-actor observation landed somewhere: alpha=%d beta=%d",
			heldObsCount(slots[alpha]), heldObsCount(slots[beta]))
	}

	// A closed slot is not a routing target: its held observations were
	// dropped at Close (the body is gone; a successor mints its own), so
	// handing it more would be writing into a grave.
	if err := slots[beta].Close(); err != nil {
		t.Fatal(err)
	}
	outbound.publishObs(beta, betaKey, "after-close", actorrt.ObsValue(`{}`))
	if n := heldObsCount(slots[beta]); n != 0 {
		t.Fatalf("closed slot accepted an observation (%d held)", n)
	}
}

// TestPublishObsRoutesToTheLiveIncarnationWhenAnAbandonedBuildSharesItsAttemptKey
// is defect A2's reproduction.
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
// publishObs picks between them by scanning d.slots and taking the first match.
// Go randomizes map iteration, so roughly half of everything the live body
// publishes is handed to the abandoned slot instead — and that slot's Close()
// drops its held observations on the floor, so those pushes vanish with no
// error anywhere. The upstream half of the same defect is
// daemonHostEvents.OnBodyObs (compute.go:50) discarding the actorrt.Incarnation
// the Host took the trouble to forward: with it, the two slots would be
// distinguishable; without it, the coordinate genuinely is ambiguous and no
// scan order can fix it.
func TestPublishObsRoutesToTheLiveIncarnationWhenAnAbandonedBuildSharesItsAttemptKey(t *testing.T) {
	t.Skip("known defect A2: publishObs picks a slot by (ActorID, AttemptKey) alone, so an abandoned build's slot sharing that coordinate steals the live body's observations (measured 171/200) and drops them at Close")
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

	const pushes = 200
	for i := range pushes {
		outbound.publishObs(id, key, actorrt.ObsKind(fmt.Sprintf("obs-%03d", i)), actorrt.ObsValue(`{}`))
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
