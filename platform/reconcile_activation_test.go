package platform

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/lib/actorcaps"
	"github.com/wanpengxie/atoll/protocol/access"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/protocol/resource"
	"github.com/wanpengxie/atoll/runtime/actorrt"
	"github.com/wanpengxie/atoll/runtime/schedule"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

const activationTestChannelID = channel.ID("test-activation")

// recordActor is a no-op cell that captures the caps bundle its factory was
// handed — the reconcile/revive tests read it back to prove the welded caps.
type recordActor struct{ caps actorcaps.Caps }

func (recordActor) Receive(context.Context, *message.Envelope) error { return nil }

// testDesired is an in-memory, swappable DesiredSource (the intent half the app
// assembly root injects for real, over channel_actors).
type testDesired struct {
	mu      sync.Mutex
	members []actorrt.DesiredMember
}

func (d *testDesired) set(ms ...actorrt.DesiredMember) {
	d.mu.Lock()
	d.members = ms
	d.mu.Unlock()
}

func (d *testDesired) Members(context.Context) ([]actorrt.DesiredMember, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	out := make([]actorrt.DesiredMember, len(d.members))
	copy(out, d.members)
	return out, nil
}

// testBuilder is the platform CapsFactoryBuilder double: id-keyed for activation,
// class-keyed for fork. Each factory records the caps it received.
type testBuilder struct {
	mu      sync.Mutex
	byID    map[actor.ActorID]ActorFactory
	byClass map[string]ActorFactory
	seen    map[actor.ActorID]actorcaps.Caps
}

func newTestBuilder() *testBuilder {
	return &testBuilder{
		byID:    map[actor.ActorID]ActorFactory{},
		byClass: map[string]ActorFactory{},
		seen:    map[actor.ActorID]actorcaps.Caps{},
	}
}

// recordFactory returns an ActorFactory that records the caps it received and
// returns a recordActor. Registered on byID (activation) and/or byClass
// (fork). Built over CapsFactory — platform's own test seam over the whole
// caps bundle (ActorFactory's doc).
func (b *testBuilder) recordFactory(id actor.ActorID) ActorFactory {
	return CapsFactory(func(c actorcaps.Caps) actorrt.Actor {
		b.mu.Lock()
		b.seen[id] = c
		b.mu.Unlock()
		return recordActor{caps: c}
	})
}

func (b *testBuilder) capsFor(id actor.ActorID) (actorcaps.Caps, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	c, ok := b.seen[id]
	return c, ok
}

func (b *testBuilder) Lookup(id actor.ActorID) (ActorFactory, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	f, ok := b.byID[id]
	return f, ok
}

func (b *testBuilder) LookupByClass(class string) (ActorFactory, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	f, ok := b.byClass[class]
	return f, ok
}

func openActivationHome(t *testing.T, desired actorrt.DesiredSource, builder CapsFactoryBuilder) *Home {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "activation.sqlite")
	// A long ReconcileInterval keeps the ticker out of the way — these tests drive
	// reconcileActivation synchronously for determinism.
	h, err := Open(HomeConfig{
		ChannelID:         activationTestChannelID,
		DBPath:            dbPath,
		ReconcileInterval: time.Hour,
		Desired:           desired,
		Builder:           builder,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = h.Close() })
	return h
}

func live(h *Home, id actor.ActorID) bool {
	_, ok := h.channel.Cells().Stat(id)
	return ok
}

// admit seeds durable membership for id (desired = intent ∩ membership: the ring
// only embodies an intent row whose户籍 landed). Tests that inject a组合域
// DesiredMember must first Admit the id, exactly as the real introduce path does.
func admit(t *testing.T, h *Home, id actor.ActorID, kind actor.Kind) {
	t.Helper()
	if err := h.Admit(context.Background(), id, kind); err != nil {
		t.Fatalf("admit %s: %v", id, err)
	}
}

// Test 1 — desired absent member → Builder revives it (eager activation) and the
// revived incarnation carries a usable State cap (create+read round-trips).
func TestReconcileActivation_RevivesAbsentDesiredMemberWithState(t *testing.T) {
	ctx := context.Background()
	desired := &testDesired{}
	builder := newTestBuilder()
	const id = actor.ActorID("agent:always")
	builder.byID[id] = builder.recordFactory(id)

	h := openActivationHome(t, desired, builder)
	admit(t, h, id, actor.KindAgent)

	if live(h, id) {
		t.Fatal("member live before it was ever desired")
	}
	desired.set(actorrt.DesiredMember{ID: id, Kind: actor.KindAgent, Lifecycle: actorrt.LifecycleAlwaysOn})
	h.reconcileActivation(ctx)

	if !live(h, id) {
		t.Fatal("desired always-on member was NOT revived by reconcile")
	}
	caps, ok := builder.capsFor(id)
	if !ok {
		t.Fatal("builder factory never ran — member not built through the caps seam")
	}
	if caps.Pen == nil || caps.Access == nil || caps.State == nil || caps.Schedule == nil || caps.Spawn == nil {
		t.Fatalf("revived member got an incomplete caps bundle: %+v", caps)
	}
	// state 读回: the revived incarnation's own actor-scoped state door round-trips
	// (proves the State cap is wired end to end, not merely non-nil).
	const key = resource.ResourceID("cursor")
	if _, err := caps.State.Invoke(ctx, access.OpCreate, key, []byte("v1"), nil); err != nil {
		t.Fatalf("revived member State create: %v", err)
	}
	out, err := caps.State.Invoke(ctx, access.OpRead, key, nil, nil)
	if err != nil {
		t.Fatalf("revived member State read: %v", err)
	}
	if string(out.Value) != "v1" {
		t.Fatalf("State read back %q, want v1", out.Value)
	}
}

// Test 2 — a member this ring previously minted, no longer desired, is deactivated
// via DespawnID (the prevEagerDesired − currentDesired 削 arm).
func TestReconcileActivation_DespawnsNoLongerDesired(t *testing.T) {
	ctx := context.Background()
	desired := &testDesired{}
	builder := newTestBuilder()
	const id = actor.ActorID("agent:transient")
	builder.byID[id] = builder.recordFactory(id)

	h := openActivationHome(t, desired, builder)
	admit(t, h, id, actor.KindAgent)

	desired.set(actorrt.DesiredMember{ID: id, Kind: actor.KindAgent, Lifecycle: actorrt.LifecycleAlwaysOn})
	h.reconcileActivation(ctx)
	if !live(h, id) {
		t.Fatal("member not revived on first tick")
	}

	desired.set() // no longer desired
	h.reconcileActivation(ctx)
	if live(h, id) {
		t.Fatal("no-longer-desired member was NOT deactivated (DespawnID 削 arm)")
	}
}

// Test 3 — 反误杀: live actors OUTSIDE the eager-managed set (the intrinsic system
// cell, an admitted human, a fork child) survive a reconcile tick that does not
// desire them. Only prevEagerDesired − currentDesired is ever DespawnID'd, and
// none of these ever enter prevEagerDesired.
func TestReconcileActivation_DoesNotKillUnmanagedLiveActors(t *testing.T) {
	ctx := context.Background()
	desired := &testDesired{}
	builder := newTestBuilder()
	const parent = actor.ActorID("agent:always")
	builder.byID[parent] = builder.recordFactory(parent)
	builder.byClass["worker"] = builder.recordFactory("agent:always/w1")

	h := openActivationHome(t, desired, builder)
	admit(t, h, parent, actor.KindAgent)

	// A human, admitted as a real cell (durable member) — now a MANAGED member (its
	// Admit puts it in the user域 desired set; it survives here because it is
	// desired+live, no longer because it is a "protected category").
	const human = actor.ActorID("user:alice")
	if err := h.Spawn(ctx, human, actor.KindHuman, CapsFactory(func(actorcaps.Caps) actorrt.Actor {
		return recordActor{}
	})); err != nil {
		t.Fatalf("Spawn human: %v", err)
	}

	// The eager-managed parent, revived by reconcile.
	desired.set(actorrt.DesiredMember{ID: parent, Kind: actor.KindAgent, Lifecycle: actorrt.LifecycleAlwaysOn})
	h.reconcileActivation(ctx)
	if !live(h, parent) {
		t.Fatal("parent not revived")
	}
	// A fork child owned by the parent's incarnation (ephemeral, never a durable
	// member, never desired).
	caps, ok := builder.capsFor(parent)
	if !ok {
		t.Fatal("parent caps not captured")
	}
	childID, err := caps.Spawn.Fork(actorrt.ForkSpec{Kind: actor.KindTool, Class: "worker", NameHint: "w1"})
	if err != nil {
		t.Fatalf("Fork child: %v", err)
	}
	if !live(h, childID) {
		t.Fatal("fork child not live after Fork")
	}

	// Reconcile again with the SAME desired set (only parent). system/human/fork
	// child are all live and NONE are in prevEagerDesired, so the 削 arm must touch
	// none of them.
	h.reconcileActivation(ctx)

	if !live(h, actor.SystemActorID) {
		t.Fatal("intrinsic system cell was killed by reconcile (反误杀 broken)")
	}
	if !live(h, human) {
		t.Fatal("admitted human was killed by reconcile (反误杀 broken)")
	}
	if !live(h, childID) {
		t.Fatal("fork child was killed by reconcile (反误杀 broken)")
	}
	if !live(h, parent) {
		t.Fatal("eager-managed parent was killed while still desired")
	}
}

// Test 4 — boot-order: an identity timer whose author has NO live incarnation
// fires by reviving the author first (the Reviver seam), THEN appending — with no
// reconcile tick involved at all (the wake path is self-sufficient from the first
// instant, before any eager reconcile).
func TestReconcileActivation_IdentityTimerFireRevivesThenAppends(t *testing.T) {
	ctx := context.Background()
	desired := &testDesired{} // deliberately empty: no eager reconcile for this author
	builder := newTestBuilder()
	const author = actor.ActorID("agent:sleeper")
	builder.byID[author] = builder.recordFactory(author)

	h := openActivationHome(t, desired, builder)

	// Seed durable membership (no cell) so the FireSink can resolve the author's
	// kind and the harness accepts the fired envelope — but leave it UN-embodied.
	if err := h.Spawn(ctx, author, actor.KindAgent, ActorFactory{}); err != nil {
		t.Fatalf("seed membership: %v", err)
	}
	if live(h, author) {
		t.Fatal("author already live before the timer fired — precondition broken")
	}

	// Schedule a past-due identity timer directly through the schedule minter (the
	// same handle a caps-injected cell would hold). The engine — already Started in
	// Open, Reviver wired — must revive the author and append the fire.
	handle := h.schedMinter.Mint(author)
	id, err := handle.Schedule(ctx, schedule.ScheduleReq{
		Bind: schedule.BindIdentity, FireAt: h.nowMs() - 1, Type: "demo.tick",
	})
	if err != nil {
		t.Fatalf("Schedule: %v", err)
	}

	wantID := message.ID("timer:" + string(id))
	deadline := time.Now().Add(5 * time.Second)
	for {
		rows, rerr := h.View().ReadAfterSeq(ctx, 0, 1000)
		if rerr != nil {
			t.Fatalf("ReadAfterSeq: %v", rerr)
		}
		found := false
		for _, r := range rows {
			if r.Envelope.ID == wantID {
				found = true
				if r.Envelope.Sender.ID != author {
					t.Fatalf("fire sender = %q, want %q (pen-welded author)", r.Envelope.Sender.ID, author)
				}
			}
		}
		if found {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("identity timer fire never landed in truth (Reviver-then-append boot path broken)")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !live(h, author) {
		t.Fatal("author not live after fire — the Reviver did not activate an embodiment before append")
	}
}

// Test 5 — F1 regression: an identity-timer fire for an ALREADY-LIVE author must
// succeed even when NO Builder is wired. EnsureLive checks liveness BEFORE the
// builder gate — the builder is only needed to activate an ABSENT author. Before
// the fix a live author + nil builder returned ReviveRejected{no_builder}, poison-
// deleting a legitimate live author's timer row.
func TestReviver_LiveAuthorWithNilBuilderIsNotPoisoned(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "revive-nobuilder.sqlite")
	// Desired + Builder deliberately nil: a legal nil-builder home.
	h, err := Open(HomeConfig{
		ChannelID:         activationTestChannelID,
		DBPath:            dbPath,
		ReconcileInterval: time.Hour,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = h.Close() })

	const author = actor.ActorID("agent:live-nobuilder")
	if err := h.Spawn(ctx, author, actor.KindAgent, CapsFactory(func(actorcaps.Caps) actorrt.Actor {
		return recordActor{}
	})); err != nil {
		t.Fatalf("Spawn live author: %v", err)
	}
	if !live(h, author) {
		t.Fatal("author not live after Spawn — precondition broken")
	}

	// Live author, nil builder → idempotent no-op, NOT a ReviveRejected poison.
	if err := (homeReviver{h: h}).EnsureLive(ctx, author); err != nil {
		t.Fatalf("EnsureLive(live author, nil builder) = %v, want nil (must not poison a live author's timer)", err)
	}

	// Contrast: an ABSENT author with nil builder is still a ReviveRejected — the
	// builder gate correctly guards the activation (absent) path.
	var rejected schedule.ReviveRejected
	if err := (homeReviver{h: h}).EnsureLive(ctx, actor.ActorID("agent:absent")); !errors.As(err, &rejected) {
		t.Fatalf("EnsureLive(absent author, nil builder) = %v, want ReviveRejected", err)
	}
}

// Test 6 — F2 teardown ordering: Close quiesces schedule PRODUCERS (cells) before
// the schedule engine, and completes without deadlock even with an in-memory
// (incarnation-bind) timer parked in the engine. Regression guard for the reorder
// (producers-before-consumer): the old "engine first" order left a window where a
// still-live cell could Schedule() an in-memory timer into a dead run loop.
func TestClose_ProducersBeforeEngineNoDeadlock(t *testing.T) {
	ctx := context.Background()
	desired := &testDesired{}
	builder := newTestBuilder()
	const id = actor.ActorID("agent:sched")
	builder.byID[id] = builder.recordFactory(id)

	dbPath := filepath.Join(t.TempDir(), "close-order.sqlite")
	h, err := Open(HomeConfig{
		ChannelID:         activationTestChannelID,
		DBPath:            dbPath,
		ReconcileInterval: time.Hour,
		Desired:           desired,
		Builder:           builder,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	// NB: Close is called explicitly below (not via t.Cleanup) — a second Close
	// would double-close the engine's stop channel and panic.
	admit(t, h, id, actor.KindAgent)

	// Bring a cell live and park a far-future incarnation-bind timer in the engine's
	// in-memory family (so the engine holds live schedule state at Close time).
	desired.set(actorrt.DesiredMember{ID: id, Kind: actor.KindAgent, Lifecycle: actorrt.LifecycleAlwaysOn})
	h.reconcileActivation(ctx)
	if !live(h, id) {
		t.Fatal("author not revived")
	}
	if _, err := h.schedMinter.Mint(id).Schedule(ctx, schedule.ScheduleReq{
		Bind: schedule.BindIncarnation, FireAt: h.nowMs() + 1_000_000, Type: "demo.tick",
	}); err != nil {
		t.Fatalf("Schedule incarnation timer: %v", err)
	}

	done := make(chan error, 1)
	go func() { done <- h.Close() }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Close: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Close did not complete within 10s — teardown deadlock")
	}
}

// Test 7 — S6 补 filter (§10.13 推导2/7): a desired AlwaysOn member the
// registry already shows attached to a daemon (Host != "") is NOT spawned
// locally by the eager ring — home is not the placement authority for an
// already-attached identity, and double-embodying it would race the daemon's
// own copy.
func TestReconcileActivation_DoesNotSpawnAttachedDesiredMember(t *testing.T) {
	ctx := context.Background()
	desired := &testDesired{}
	builder := newTestBuilder()
	const id = actor.ActorID("agent:attached-desired")
	builder.byID[id] = builder.recordFactory(id)

	h := openActivationHome(t, desired, builder)

	if err := h.Spawn(ctx, id, actor.KindAgent, ActorFactory{}); err != nil {
		t.Fatalf("seed membership: %v", err)
	}
	if err := h.cs.Membership.ApplyMemberTransitions(ctx, []storespec.MemberActorAdd{
		{ID: id, Kind: actor.KindAgent, Host: "daemon-y", At: h.nowMs()},
	}, nil); err != nil {
		t.Fatalf("attach host: %v", err)
	}

	desired.set(actorrt.DesiredMember{ID: id, Kind: actor.KindAgent, Lifecycle: actorrt.LifecycleAlwaysOn})
	h.reconcileActivation(ctx)

	if live(h, id) {
		t.Fatal("attached desired member was spawned locally by home (补 filter not applied — double-embodiment)")
	}
	if _, ok := builder.capsFor(id); ok {
		t.Fatal("builder factory ran for an attached desired member (补 filter not applied)")
	}
}

// Test 8 — S6 削 filter (DoD 6, 反误杀 extended to placement): a member this
// ring previously eager-managed, no longer desired, is NOT evicted if the
// registry now shows it attached to a daemon — a migrated placement fact, not
// identity absence.
func TestReconcileActivation_DoesNotEvictAttachedNoLongerDesiredMember(t *testing.T) {
	ctx := context.Background()
	desired := &testDesired{}
	builder := newTestBuilder()
	const id = actor.ActorID("agent:migrated")
	builder.byID[id] = builder.recordFactory(id)

	h := openActivationHome(t, desired, builder)
	admit(t, h, id, actor.KindAgent)

	desired.set(actorrt.DesiredMember{ID: id, Kind: actor.KindAgent, Lifecycle: actorrt.LifecycleAlwaysOn})
	h.reconcileActivation(ctx)
	if !live(h, id) {
		t.Fatal("member not revived on first tick")
	}

	// Simulate a migration mid-flight: the registry now shows Host attached to
	// a daemon, and the id falls out of desired in the SAME tick.
	if err := h.cs.Membership.ApplyMemberTransitions(ctx, []storespec.MemberActorAdd{
		{ID: id, Kind: actor.KindAgent, Host: "daemon-z", At: h.nowMs()},
	}, nil); err != nil {
		t.Fatalf("attach host: %v", err)
	}
	desired.set() // no longer desired

	h.reconcileActivation(ctx)
	if !live(h, id) {
		t.Fatal("attached member was evicted by reconcile's 削 arm (placement filter not applied — 反误杀 broken)")
	}
}

// Test 9 — S6 Reviver placement gate + DoD 5 (fake-clock driven): an identity
// timer whose author's registry row shows Host attached to a daemon fires
// NEITHER by home (the row must be RETAINED as transient, never poison-
// deleted, never doubly-embodied) NOR silently lost — once the author is no
// longer attached (Host reverts to home), the SAME row revives-then-appends
// on a later tick.
func TestReviver_AttachedHostRetainsThenFiresOnceDetached(t *testing.T) {
	ctx := context.Background()
	desired := &testDesired{} // deliberately empty: only the identity timer drives this author
	builder := newTestBuilder()
	const author = actor.ActorID("agent:wire-flap")
	builder.byID[author] = builder.recordFactory(author)

	clock := newFakeClock(time.UnixMilli(1_000_000))
	dbPath := filepath.Join(t.TempDir(), "revive-attached.sqlite")
	h, err := Open(HomeConfig{
		ChannelID:         activationTestChannelID,
		DBPath:            dbPath,
		ReconcileInterval: time.Hour,
		Desired:           desired,
		Builder:           builder,
		Clock:             clock,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = h.Close() })

	// Seed durable membership, unembodied, then mark it Host-attached — the
	// wire-flap window this test exercises (the author's own daemon holds the
	// live embodiment, home only has the durable row).
	if err := h.Spawn(ctx, author, actor.KindAgent, ActorFactory{}); err != nil {
		t.Fatalf("seed membership: %v", err)
	}
	if err := h.cs.Membership.ApplyMemberTransitions(ctx, []storespec.MemberActorAdd{
		{ID: author, Kind: actor.KindAgent, Host: "daemon-flap", At: h.nowMs()},
	}, nil); err != nil {
		t.Fatalf("attach host: %v", err)
	}

	handle := h.schedMinter.Mint(author)
	id, err := handle.Schedule(ctx, schedule.ScheduleReq{
		Bind: schedule.BindIdentity, FireAt: clock.Now().UnixMilli() - 1, Type: "demo.tick",
	})
	if err != nil {
		t.Fatalf("Schedule: %v", err)
	}
	wantID := message.ID("timer:" + string(id))

	hasFired := func() bool {
		rows, rerr := h.View().ReadAfterSeq(ctx, 0, 1000)
		if rerr != nil {
			t.Fatalf("ReadAfterSeq: %v", rerr)
		}
		for _, r := range rows {
			if r.Envelope.ID == wantID {
				return true
			}
		}
		return false
	}

	// Drive several backoff ticks (schedule.backoffDuration pace) while still
	// attached: the row must be RETAINED — never fired, never poison-deleted,
	// and home must never double-embody the attached author locally.
	for i := 0; i < 5; i++ {
		clock.Advance(2 * time.Second)
		time.Sleep(20 * time.Millisecond) // let the run loop observe the advance
		if hasFired() {
			t.Fatal("attached author's identity timer fired while Host != \"\" (placement gate not applied)")
		}
		if live(h, author) {
			t.Fatal("attached author was revived locally by home (double-embodiment — placement gate not applied)")
		}
	}

	// 重连: the author is no longer attached (Host reverts to home) — the SAME
	// row must revive-then-append on a later tick, never lost.
	if err := h.cs.Membership.ApplyMemberTransitions(ctx, []storespec.MemberActorAdd{
		{ID: author, Kind: actor.KindAgent, Host: "", At: h.nowMs()},
	}, nil); err != nil {
		t.Fatalf("detach host: %v", err)
	}

	deadline := time.Now().Add(5 * time.Second)
	for {
		clock.Advance(2 * time.Second)
		if hasFired() {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("identity timer never fired after Host reverted to home")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !live(h, author) {
		t.Fatal("author not live after fire — the Reviver did not activate an embodiment before append")
	}
}

// Test 10 — 期7 review P1b regression: an ATTACHED author's identity timer on a
// NIL-BUILDER home must classify as transient, never ReviveRejected{no_builder}.
// EnsureLive consults the placement fact BEFORE the builder gate — the builder
// is only load-bearing for activating a HOME-placed absent author, so an
// attached author's wake on a builderless home is S6 transient (row retained,
// retried) rather than a poison verdict that deletes the row.
func TestReviver_AttachedAuthorNilBuilderIsTransientNotPoisoned(t *testing.T) {
	ctx := context.Background()
	clock := newFakeClock(time.UnixMilli(1_000_000))
	dbPath := filepath.Join(t.TempDir(), "revive-attached-nobuilder.sqlite")
	// Desired + Builder deliberately nil: a legal nil-builder home.
	h, err := Open(HomeConfig{
		ChannelID:         activationTestChannelID,
		DBPath:            dbPath,
		ReconcileInterval: time.Hour,
		Clock:             clock,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = h.Close() })

	const author = actor.ActorID("agent:attached-nobuilder")
	if err := h.Spawn(ctx, author, actor.KindAgent, ActorFactory{}); err != nil {
		t.Fatalf("seed membership: %v", err)
	}
	if err := h.cs.Membership.ApplyMemberTransitions(ctx, []storespec.MemberActorAdd{
		{ID: author, Kind: actor.KindAgent, Host: "daemon-x", At: h.nowMs()},
	}, nil); err != nil {
		t.Fatalf("attach host: %v", err)
	}

	// Direct contract check: transient (plain error), NOT ReviveRejected.
	err = (homeReviver{h: h}).EnsureLive(ctx, author)
	if err == nil {
		t.Fatal("EnsureLive(attached author) = nil, want a transient error (home never activates an attached author)")
	}
	var rejected schedule.ReviveRejected
	if errors.As(err, &rejected) {
		t.Fatalf("EnsureLive(attached author, nil builder) = ReviveRejected{%s} — placement must classify BEFORE the builder gate", rejected.Reason)
	}

	// End-to-end: a past-due identity timer fire must RETAIN the row (transient
	// backoff), never fire it and never poison-delete it.
	id, err := h.schedMinter.Mint(author).Schedule(ctx, schedule.ScheduleReq{
		Bind: schedule.BindIdentity, FireAt: clock.Now().UnixMilli() - 1, Type: "demo.tick",
	})
	if err != nil {
		t.Fatalf("Schedule: %v", err)
	}
	wantID := message.ID("timer:" + string(id))
	hasFired := func() bool {
		rows, rerr := h.View().ReadAfterSeq(ctx, 0, 1000)
		if rerr != nil {
			t.Fatalf("ReadAfterSeq: %v", rerr)
		}
		for _, r := range rows {
			if r.Envelope.ID == wantID {
				return true
			}
		}
		return false
	}
	for i := 0; i < 5; i++ {
		clock.Advance(2 * time.Second)
		time.Sleep(20 * time.Millisecond) // let the run loop observe the advance
		if hasFired() {
			t.Fatal("attached author's identity timer fired on a nil-builder home while Host != \"\"")
		}
		if live(h, author) {
			t.Fatal("attached author was revived locally by a nil-builder home (double-embodiment)")
		}
	}

	// Retention proof (the raw TimerStore is deliberately unreachable here): bring
	// the author home and LIVE — Spawn re-adds the row with Host "" and mints a
	// cell, so EnsureLive's already-live fast path clears the gate. The SAME
	// timer must now fire: a no_builder poison during the attached window would
	// have deleted the row and this fire could never land.
	if err := h.Spawn(ctx, author, actor.KindAgent, CapsFactory(func(actorcaps.Caps) actorrt.Actor {
		return recordActor{}
	})); err != nil {
		t.Fatalf("Spawn author home: %v", err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for !hasFired() {
		clock.Advance(2 * time.Second)
		if time.Now().After(deadline) {
			t.Fatal("retained timer never fired once the author came home live — the attached window poison-deleted it")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// Test 11 — G10 fire × 并发 dereg 竞态: a durable author deregistered AFTER a fire
// was snapshotted as due but BEFORE it lands must be REJECTED at both timer seams.
// Registry.Lookup deliberately returns soft-deregistered rows (Exists/grant
// resolution consumers need them), so BOTH fireSink.Append and homeReviver.EnsureLive
// must screen rec.IsActive() themselves — else the in-flight fire would append truth
// authored by a gone member (Append) or resurrect a zombie cell (EnsureLive) for an
// identity the dereg cascade already tore down.
func TestFireAndRevive_RejectDeregisteredAuthor(t *testing.T) {
	ctx := context.Background()
	desired := &testDesired{}
	builder := newTestBuilder()
	const author = actor.ActorID("agent:dereged")
	builder.byID[author] = builder.recordFactory(author)

	h := openActivationHome(t, desired, builder)

	// Seed durable membership (unembodied), then soft-deregister it — the registry
	// row survives with DeregisteredAt != 0, exactly the in-flight-race snapshot.
	if err := h.Spawn(ctx, author, actor.KindAgent, ActorFactory{}); err != nil {
		t.Fatalf("seed membership: %v", err)
	}
	if err := h.cs.Membership.ApplyMemberTransitions(ctx, nil, []storespec.MemberActorRemove{
		{ID: author, At: h.nowMs()},
	}); err != nil {
		t.Fatalf("deregister author: %v", err)
	}
	// Precondition: Lookup still resolves the row (soft-dereg), but as inactive.
	rec, ok, err := h.cs.Registry.Lookup(ctx, author)
	if err != nil {
		t.Fatalf("Lookup: %v", err)
	}
	if !ok || rec.IsActive() {
		t.Fatalf("precondition broken: want a resolvable-but-inactive row, got ok=%v active=%v", ok, rec.IsActive())
	}

	// Reviver seam: a dereg'd author is the deterministic unrevivable class, NOT a
	// transient error and NOT a silent revive.
	var revRejected schedule.ReviveRejected
	if rerr := (homeReviver{h: h}).EnsureLive(ctx, author); !errors.As(rerr, &revRejected) {
		t.Fatalf("EnsureLive(deregistered author) = %v, want ReviveRejected (zombie-cell resurrection guard)", rerr)
	}
	if live(h, author) {
		t.Fatal("deregistered author was revived into a live cell (G10 zombie-cell resurrection)")
	}

	// FireSink seam: appending a fire for a dereg'd author must be a deterministic
	// FireRejected (disposed), never a false nil that lets zombie truth land.
	sink := fireSink{minter: h.minter, registry: h.cs.Registry, rt: h.channel.Cells(), chID: activationTestChannelID}
	// The IsActive guard rejects before pen.Write, so a minimal envelope suffices.
	env := &message.Envelope{
		ID:       message.ID("timer:test-dereg"),
		Kind:     message.KindEvent,
		Type:     "demo.tick",
		Payload:  []byte("{}"),
		Audience: message.Audience{author},
	}
	var fireRejected schedule.FireRejected
	if ferr := sink.Append(ctx, author, env); !errors.As(ferr, &fireRejected) {
		t.Fatalf("Append(deregistered author) = %v, want FireRejected (zombie-truth guard)", ferr)
	}
}

// Test 12 — G11: an incarnation-bind timer on a FORK CHILD fires successfully,
// with its kind resolved from the LIVE embodiments table (the incarnation-level
// oracle) rather than the durable registry — a fork child is never a durable
// member, so a registry-only fireSink would reject every fork-child fire as
// author_not_member. The fired envelope's Sender.Kind must be the child's
// ForkSpec.Kind (the pen is minted with it, exactly as an admission Spawn's
// author would carry).
func TestFireSink_ForkChildIncarnationFire_KindFromLiveEmbodiment(t *testing.T) {
	ctx := context.Background()
	desired := &testDesired{}
	builder := newTestBuilder()
	const parent = actor.ActorID("agent:fork-parent")
	builder.byID[parent] = builder.recordFactory(parent)
	const childNameHint = "w1"
	builder.byClass["worker"] = builder.recordFactory(parent + "/" + actor.ActorID(childNameHint))

	h := openActivationHome(t, desired, builder)
	admit(t, h, parent, actor.KindAgent)

	desired.set(actorrt.DesiredMember{ID: parent, Kind: actor.KindAgent, Lifecycle: actorrt.LifecycleAlwaysOn})
	h.reconcileActivation(ctx)
	if !live(h, parent) {
		t.Fatal("parent not revived")
	}
	parentCaps, ok := builder.capsFor(parent)
	if !ok {
		t.Fatal("parent caps not captured")
	}
	childID, err := parentCaps.Spawn.Fork(actorrt.ForkSpec{Kind: actor.KindTool, Class: "worker", NameHint: childNameHint})
	if err != nil {
		t.Fatalf("Fork child: %v", err)
	}
	childCaps, ok := builder.capsFor(childID)
	if !ok {
		t.Fatal("fork child caps not captured")
	}

	id, err := childCaps.Schedule.Schedule(ctx, schedule.ScheduleReq{
		Bind: schedule.BindIncarnation, FireAt: h.nowMs() - 1, Type: "demo.tick",
	})
	if err != nil {
		t.Fatalf("Schedule (incarnation-bind, fork child): %v", err)
	}
	wantID := message.ID("timer:" + string(id))

	deadline := time.Now().Add(5 * time.Second)
	for {
		rows, rerr := h.View().ReadAfterSeq(ctx, 0, 1000)
		if rerr != nil {
			t.Fatalf("ReadAfterSeq: %v", rerr)
		}
		found := false
		for _, r := range rows {
			if r.Envelope.ID == wantID {
				found = true
				if r.Envelope.Sender.ID != childID {
					t.Fatalf("fire sender = %q, want fork child %q", r.Envelope.Sender.ID, childID)
				}
				if r.Envelope.Sender.Kind != actor.KindTool {
					t.Fatalf("fire sender kind = %q, want %q (resolved from the LIVE embodiments table, not the durable registry — a fork child has no registry row)", r.Envelope.Sender.Kind, actor.KindTool)
				}
			}
		}
		if found {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("fork-child incarnation-bind fire never landed in truth (G11 fireSink double-oracle broken)")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// Test 13 — G11 death race: an incarnation-bind fire whose author (a fork
// child) dies BETWEEN the engine's IsLive gate (schedule/engine.go's fireDue,
// outside fireSink) and fireSink's own kind resolution must be a quiet,
// deterministic drop — never a dead-write (truth authored by a departed fork
// child) and never a resurrection. fireSink.Append is exercised DIRECTLY
// (the injection hook: killing the child then calling Append simulates
// "IsLive passed a moment ago, died before the sink's own lookup ran" without
// racing the real engine goroutine) — a fork child is never a durable member,
// so the registry fallback also returns !ok, and the drop is
// author_not_member, symmetric with G10's identity-family race.
func TestFireSink_ForkChildDeathRace_QuietDrop(t *testing.T) {
	ctx := context.Background()
	desired := &testDesired{}
	builder := newTestBuilder()
	const parent = actor.ActorID("agent:fork-death-parent")
	builder.byID[parent] = builder.recordFactory(parent)
	const childNameHint = "w1"
	builder.byClass["worker"] = builder.recordFactory(parent + "/" + actor.ActorID(childNameHint))

	h := openActivationHome(t, desired, builder)
	admit(t, h, parent, actor.KindAgent)

	desired.set(actorrt.DesiredMember{ID: parent, Kind: actor.KindAgent, Lifecycle: actorrt.LifecycleAlwaysOn})
	h.reconcileActivation(ctx)
	parentCaps, ok := builder.capsFor(parent)
	if !ok {
		t.Fatal("parent caps not captured")
	}
	childID, err := parentCaps.Spawn.Fork(actorrt.ForkSpec{Kind: actor.KindTool, Class: "worker", NameHint: childNameHint})
	if err != nil {
		t.Fatalf("Fork child: %v", err)
	}
	if !live(h, childID) {
		t.Fatal("fork child not live after Fork")
	}

	// The race: the child dies (rt.DespawnID) — the SAME live-embodiment slot
	// fireSink.Append is about to consult is now empty, AND the child was never
	// a durable member (fork children carry no registry row), so both oracles
	// come up empty.
	h.channel.Cells().DespawnID(childID)
	if live(h, childID) {
		t.Fatal("fork child still live after DespawnID — precondition broken")
	}

	sink := fireSink{minter: h.minter, registry: h.cs.Registry, rt: h.channel.Cells(), chID: activationTestChannelID}
	env := &message.Envelope{
		ID:       message.ID("timer:test-fork-death-race"),
		Kind:     message.KindEvent,
		Type:     "demo.tick",
		Payload:  []byte("{}"),
		Audience: message.Audience{childID},
	}
	var fireRejected schedule.FireRejected
	if ferr := sink.Append(ctx, childID, env); !errors.As(ferr, &fireRejected) {
		t.Fatalf("Append(dead fork child) = %v, want FireRejected (death-race quiet drop)", ferr)
	}

	rows, rerr := h.View().ReadAfterSeq(ctx, 0, 1000)
	if rerr != nil {
		t.Fatalf("ReadAfterSeq: %v", rerr)
	}
	for _, r := range rows {
		if r.Envelope.ID == env.ID {
			t.Fatal("a dead-race fire landed in truth — G11 death-race guard broken (dead-write)")
		}
	}
}
