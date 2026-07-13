package home

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/lib/actorcaps"
	"github.com/wanpengxie/atoll/platform/internal/hostcommon"
	"github.com/wanpengxie/atoll/protocol/access"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/resource"
	"github.com/wanpengxie/atoll/runtime/actorrt"
	"github.com/wanpengxie/atoll/runtime/schedule"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

// TestRemove_AnchorRejected: the system anchor is never removable — it is the
// channel's structural anchor, not a member id.
func TestRemove_AnchorRejected(t *testing.T) {
	ctx := context.Background()
	h := openActivationHome(t, &testDesired{}, newTestBuilder())
	if err := h.Remove(ctx, actor.SystemActorID); !errors.Is(err, ErrRemoveAnchor) {
		t.Fatalf("Remove(SystemActorID) = %v, want ErrRemoveAnchor", err)
	}
	if !live(h, actor.SystemActorID) {
		t.Fatal("system anchor was killed despite the rejection")
	}
}

// spyMembership wraps a real MembershipControlPlane and records, at the
// instant ApplyMemberTransitions(remove) is invoked, whether the target id was
// ALREADY dead — the observable proof that Remove's step ① (DespawnID) ran
// BEFORE step ② (the dereg cascade), not the other way round.
type spyMembership struct {
	storespec.MembershipControlPlane
	h            *Home
	target       actor.ActorID
	mu           sync.Mutex
	deadAtRemove bool
	sawRemove    bool
}

func (s *spyMembership) ApplyMemberTransitions(ctx context.Context, adds []storespec.MemberActorAdd, removes []storespec.MemberActorRemove) error {
	for _, r := range removes {
		if r.ID == s.target {
			s.mu.Lock()
			s.sawRemove = true
			s.deadAtRemove = !live(s.h, s.target)
			s.mu.Unlock()
		}
	}
	return s.MembershipControlPlane.ApplyMemberTransitions(ctx, adds, removes)
}

// TestRemove_OrderSpy_DespawnBeforeDereg proves the composition's step order
// (despawn-first): at the exact moment the dereg-cascade mirror write runs,
// the target's embodiment must already be dead.
func TestRemove_OrderSpy_DespawnBeforeDereg(t *testing.T) {
	ctx := context.Background()
	h := openActivationHome(t, &testDesired{}, newTestBuilder())
	id := admit(t, h, actor.ActorID("agent:order-spy"), actor.KindAgent)
	minted, err := SpawnForTest(h, id, actor.KindAgent, hostcommon.CapsFactory(func(actorcaps.Caps) actorrt.Actor {
		return recordActor{}
	}))
	if err != nil {
		t.Fatalf("Spawn: %v", err)
	}
	id = minted
	if !live(h, id) {
		t.Fatal("precondition: id must be live before Remove")
	}

	spy := &spyMembership{MembershipControlPlane: h.cs.Membership, h: h, target: id}
	h.cs.Membership = spy

	if err := h.Remove(ctx, id); err != nil {
		t.Fatalf("Remove: %v", err)
	}
	if !spy.sawRemove {
		t.Fatal("dereg cascade never ran for the target id")
	}
	if !spy.deadAtRemove {
		t.Fatal("target id was still LIVE when the dereg cascade ran — despawn-first order violated")
	}
	if live(h, id) {
		t.Fatal("id still live after Remove")
	}
}

// TestRemove_Idempotent: a second Remove on an already-removed id is a no-op
// (nil error, no repeat mirror event) — applyMemberRemoveTx's changed=false
// path.
func TestRemove_Idempotent(t *testing.T) {
	ctx := context.Background()
	h := openActivationHome(t, &testDesired{}, newTestBuilder())
	id := admit(t, h, actor.ActorID("agent:idempotent"), actor.KindAgent)
	minted, err := SpawnForTest(h, id, actor.KindAgent, hostcommon.CapsFactory(func(actorcaps.Caps) actorrt.Actor {
		return recordActor{}
	}))
	if err != nil {
		t.Fatalf("Spawn: %v", err)
	}
	id = minted

	if err := h.Remove(ctx, id); err != nil {
		t.Fatalf("first Remove: %v", err)
	}
	before, err := h.View().MaxSeq(ctx)
	if err != nil {
		t.Fatalf("MaxSeq: %v", err)
	}

	if err := h.Remove(ctx, id); err != nil {
		t.Fatalf("second Remove: %v", err)
	}
	after, err := h.View().MaxSeq(ctx)
	if err != nil {
		t.Fatalf("MaxSeq: %v", err)
	}
	if after != before {
		t.Fatalf("second Remove appended truth (seq %d -> %d) — dereg cascade is not idempotent", before, after)
	}
}

// TestRemove_CascadeClearsState proves the dereg cascade (actors.go, same tx
// as the mirror) clears the identity's actor-scoped state: a value written
// before Remove must NOT survive under a fresh admission of the SAME id.
func TestRemove_CascadeClearsState(t *testing.T) {
	ctx := context.Background()
	h := openActivationHome(t, &testDesired{}, newTestBuilder())
	id := admit(t, h, actor.ActorID("agent:cascade"), actor.KindAgent)
	var caps1 actorcaps.Caps
	minted, err := SpawnForTest(h, id, actor.KindAgent, hostcommon.CapsFactory(func(c actorcaps.Caps) actorrt.Actor {
		caps1 = c
		return recordActor{caps: c}
	}))
	if err != nil {
		t.Fatalf("Spawn: %v", err)
	}
	id = minted
	const key = resource.ResourceID("cursor")
	if _, err := caps1.State.Invoke(ctx, access.OpCreate, key, []byte("v1"), nil); err != nil {
		t.Fatalf("state create: %v", err)
	}

	if err := h.Remove(ctx, id); err != nil {
		t.Fatalf("Remove: %v", err)
	}

	// Re-admit mints a fresh identity; state cannot cross the old id boundary.
	newID, err := h.Admit(ctx, actor.KindAgent, "cascade")
	if err != nil {
		t.Fatalf("re-Admit: %v", err)
	}
	if newID == id {
		t.Fatal("re-Admit reused removed actor id")
	}
	id = newID
	var caps2 actorcaps.Caps
	minted, err = SpawnForTest(h, id, actor.KindAgent, hostcommon.CapsFactory(func(c actorcaps.Caps) actorrt.Actor {
		caps2 = c
		return recordActor{caps: c}
	}))
	if err != nil {
		t.Fatalf("re-Spawn: %v", err)
	}
	id = minted
	out, err := caps2.State.Invoke(ctx, access.OpRead, key, nil, nil)
	if err == nil && out.RejectReason == "" {
		t.Fatalf("state survived Remove's dereg cascade: read back %q, want not_found", out.Value)
	}
}

// TestRemove_DueFireRace (DoD ⓐ): a Due identity-timer snapshot in flight
// racing a concurrent Remove must settle to a terminal state with NO live
// embodiment and an inactive registry row — never a resurrected zombie.
func TestRemove_DueFireRace(t *testing.T) {
	ctx := context.Background()
	builder := newTestBuilder()
	id := actor.ActorID("agent:due-race")
	builder.byID[id] = builder.recordFactory(id)
	h := openActivationHome(t, &testDesired{}, builder)

	id = admit(t, h, id, actor.KindAgent)
	if _, err := h.schedMinter.Mint(id).Schedule(ctx, schedule.ScheduleReq{
		Bind: schedule.BindIdentity, FireAt: h.nowMs() - 1, Type: "demo.tick",
	}); err != nil {
		t.Fatalf("Schedule: %v", err)
	}

	// Racing Remove against the engine's in-flight due fire/revive.
	if err := h.Remove(ctx, id); err != nil {
		t.Fatalf("Remove: %v", err)
	}

	deadline := time.Now().Add(5 * time.Second)
	for {
		rec, ok, err := h.cs.Registry.Lookup(ctx, id)
		settled := !live(h, id) && (!ok || !rec.IsActive())
		if err == nil && settled {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("Remove×Due-fire race never settled: live=%v ok=%v active=%v", live(h, id), ok, ok && rec.IsActive())
		}
		time.Sleep(10 * time.Millisecond)
	}
	// Stability window: the engine's own backoff retries must not resurrect it.
	for i := 0; i < 10; i++ {
		time.Sleep(20 * time.Millisecond)
		if live(h, id) {
			t.Fatal("id was resurrected after the race settled inactive")
		}
	}
}

// TestRemove_ReviverStraddle_HumanSlotCleaned (gateway 期 P1, 死 ID 槽复建): a human's
// factoryFor embodies it via EnsureSubjectSlot (装配链 step②). A stale-rec 补臂/EnsureLive
// whose Lookup predated a concurrent Remove can RE-create the dead id's binding slot AFTER
// RemoveSubjectSlot ran; verifyPostBuild's recheckGone branch must级联删槽 (RemoveSubjectSlot
// + Forget), not merely Despawn the cell — else a dead-id slot leaks forever.
func TestRemove_ReviverStraddle_HumanSlotCleaned(t *testing.T) {
	ctx := context.Background()
	h := openActivationHome(t, &testDesired{}, newTestBuilder())
	id := admit(t, h, actor.ActorID("user:ghost"), actor.KindHuman)
	if _, ok := h.SubjectSlotFor(id); ok {
		t.Fatal("precondition: direct admit must not create the slot (factoryFor does)")
	}

	paused := make(chan struct{})
	resume := make(chan struct{})
	var once sync.Once
	h.reviverStraddleHook = func() {
		once.Do(func() { close(paused) })
		<-resume
	}

	reviveErr := make(chan error, 1)
	go func() { reviveErr <- (homeReviver{h: h}).EnsureLive(ctx, id) }()

	select {
	case <-paused:
	case <-time.After(5 * time.Second):
		t.Fatal("reviver never reached the straddle hook")
	}
	// factoryFor ran before the hook → the slot now exists for a still-active member.
	if _, ok := h.SubjectSlotFor(id); !ok {
		t.Fatal("factoryFor should have ensured the human slot before the hook")
	}

	// Remove runs FULLY inside the window: dereg + RemoveSubjectSlot drop the slot.
	if err := h.Remove(ctx, id); err != nil {
		t.Fatalf("Remove during straddle: %v", err)
	}
	if _, ok := h.SubjectSlotFor(id); ok {
		t.Fatal("Remove must drop the slot")
	}
	// Model the concurrent stale-rec factoryFor: it RE-creates the dead id's slot AFTER
	// Remove — the exact resurrection verifyPostBuild's recheckGone must undo.
	h.EnsureSubjectSlot(id)
	if _, ok := h.SubjectSlotFor(id); !ok {
		t.Fatal("modelled stale-rec factoryFor should have re-created the slot")
	}

	// Resume the build: verifyPostBuild sees the id gone and must self-undo BOTH the cell
	// AND the resurrected slot.
	close(resume)
	var rejected schedule.ReviveRejected
	select {
	case err := <-reviveErr:
		if !errors.As(err, &rejected) {
			t.Fatalf("EnsureLive post-Remove = %v, want ReviveRejected (self-undo)", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("reviver never returned after resume")
	}
	if live(h, id) {
		t.Fatal("post-build recheck failed to Despawn the straddling cell")
	}
	if _, ok := h.SubjectSlotFor(id); ok {
		t.Fatal("recheckGone must级联删槽 the resurrected dead-id slot (死 ID 槽复建 fix)")
	}
}

// TestRemove_ReviverStraddle_SelfUndo (DoD ⓑ, S-P20's other half): the
// reviver is parked BETWEEN its Lookup (which passed) and SpawnIfAbsent via
// the test-only straddle hook; Remove runs to completion FULLY inside that
// window; the reviver is then released to finish its build — its OWN
// post-build recheck must self-undo (Despawn the just-built incarnation) and
// report ReviveRejected{not_a_member}, leaving no live embodiment behind.
func TestRemove_ReviverStraddle_SelfUndo(t *testing.T) {
	ctx := context.Background()
	builder := newTestBuilder()
	id := actor.ActorID("agent:straddle")
	builder.byID[id] = builder.recordFactory(id)
	h := openActivationHome(t, &testDesired{}, builder)

	id = admit(t, h, id, actor.KindAgent)

	paused := make(chan struct{})
	resume := make(chan struct{})
	var once sync.Once
	h.reviverStraddleHook = func() {
		once.Do(func() { close(paused) })
		<-resume
	}

	reviveErr := make(chan error, 1)
	go func() { reviveErr <- (homeReviver{h: h}).EnsureLive(ctx, id) }()

	select {
	case <-paused:
	case <-time.After(5 * time.Second):
		t.Fatal("reviver never reached the straddle hook")
	}

	// Remove runs to completion FULLY inside the reviver's straddle window.
	if err := h.Remove(ctx, id); err != nil {
		t.Fatalf("Remove during straddle: %v", err)
	}
	if live(h, id) {
		t.Fatal("id live immediately after Remove, inside the straddle window")
	}

	close(resume)
	var rejected schedule.ReviveRejected
	select {
	case err := <-reviveErr:
		if !errors.As(err, &rejected) {
			t.Fatalf("EnsureLive post-Remove build = %v, want ReviveRejected (self-undo)", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("reviver never returned after resume")
	}
	if live(h, id) {
		t.Fatal("reviver's post-build recheck failed to self-undo the straddling build")
	}
}
