package platform

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"strings"
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
	err     error
}

func (d *testDesired) set(ms ...actorrt.DesiredMember) {
	d.mu.Lock()
	d.members = ms
	d.mu.Unlock()
}

// fail makes Members return err (union-atomicity fault injection); nil restores.
func (d *testDesired) fail(err error) {
	d.mu.Lock()
	d.err = err
	d.mu.Unlock()
}

func (d *testDesired) Members(context.Context) ([]actorrt.DesiredMember, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.err != nil {
		return nil, d.err
	}
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
	aliases map[actor.ActorID]actor.ActorID // minted id -> historical fixture key
	// forkCalls records the (childID, class, config) each LookupByClass received —
	// the M2 config-passthrough / childID-derivation assertions read it.
	forkCalls []forkCall
}

type forkCall struct {
	childID actor.ActorID
	class   string
	config  json.RawMessage
}

func newTestBuilder() *testBuilder {
	return &testBuilder{
		byID:    map[actor.ActorID]ActorFactory{},
		byClass: map[string]ActorFactory{},
		seen:    map[actor.ActorID]actorcaps.Caps{},
		aliases: map[actor.ActorID]actor.ActorID{},
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
	if fixture, ok := b.aliases[id]; ok {
		id = fixture
	}
	c, ok := b.seen[id]
	return c, ok
}

func (b *testBuilder) Lookup(id actor.ActorID) (ActorFactory, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if fixture, ok := b.aliases[id]; ok {
		id = fixture
	}
	f, ok := b.byID[id]
	return f, ok
}

func (b *testBuilder) LookupByClass(childID actor.ActorID, class string, config json.RawMessage) (ActorFactory, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.forkCalls = append(b.forkCalls, forkCall{childID: childID, class: class, config: config})
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
// DesiredMember must first admit the id, as the real introduce path does. It writes
// the membership row DIRECTLY (not via Home.Admit) precisely so it does NOT poke
// the reconcile ring — these tests drive reconcileActivation synchronously, and a
// poke would race the background ticker goroutine against the test's own calls.
func admit(t *testing.T, h *Home, id actor.ActorID, kind actor.Kind) actor.ActorID {
	t.Helper()
	minted, err := h.cs.Membership.Admit(context.Background(), kind, strings.ReplaceAll(string(id), ":", "-"), h.nowMs())
	if err != nil {
		t.Fatalf("admit %s: %v", id, err)
	}
	if b, ok := h.builder.(*testBuilder); ok && minted != id {
		b.mu.Lock()
		if _, fixtureExists := b.byID[id]; fixtureExists {
			b.aliases[minted] = id
		}
		b.mu.Unlock()
	}
	return minted
}

// Test 1 — desired absent member → Builder revives it (eager activation) and the
// revived incarnation carries a usable State cap (create+read round-trips).
func TestReconcileActivation_RevivesAbsentDesiredMemberWithState(t *testing.T) {
	ctx := context.Background()
	desired := &testDesired{}
	builder := newTestBuilder()
	id := actor.ActorID("agent:always")
	builder.byID[id] = builder.recordFactory(id)

	h := openActivationHome(t, desired, builder)
	id = admit(t, h, id, actor.KindAgent)

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
	id := actor.ActorID("agent:transient")
	builder.byID[id] = builder.recordFactory(id)

	h := openActivationHome(t, desired, builder)
	id = admit(t, h, id, actor.KindAgent)

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
	parent := actor.ActorID("agent:always")

	h := openActivationHome(t, desired, builder)
	parent = admit(t, h, parent, actor.KindAgent)
	builder.byID[parent] = builder.recordFactory(parent)
	builder.byClass["worker"] = builder.recordFactory(parent + "/w1")

	// A human, admitted as a real cell (durable member) — now a MANAGED member (its
	// Admit puts it in the user域 desired set; it survives here because it is
	// desired+live, no longer because it is a "protected category").
	human := admit(t, h, actor.ActorID("user:alice"), actor.KindHuman)
	mintedHuman, err := SpawnForTest(h, human, actor.KindHuman, CapsFactory(func(actorcaps.Caps) actorrt.Actor {
		return recordActor{}
	}))
	if err != nil {
		t.Fatalf("Spawn human: %v", err)
	}
	human = mintedHuman

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

// DoD ⑧ (user域): an admitted human is embodied by the ring from the user域
// derivation ALONE (empty组合域, no connection/request) — a常驻 cell; removing the
// membership shrinks the projection and the 削臂 evicts it (human is a MANAGED
// member now, not a protected category — removal = true death).
func TestReconcileActivation_UserDomainHumanEmbodiedAndEvicted(t *testing.T) {
	ctx := context.Background()
	desired := &testDesired{} // empty组合域: the human comes entirely from user域
	builder := newTestBuilder()
	h := openActivationHome(t, desired, builder)

	human := actor.ActorID("user:carol")
	human = admit(t, h, human, actor.KindHuman)
	h.reconcileActivation(ctx)
	if !live(h, human) {
		t.Fatal("admitted human not embodied by the ring (user域 derivation must build a常驻 cell with no connection)")
	}

	if err := h.cs.Membership.ApplyMemberTransitions(ctx, nil, []storespec.MemberActorRemove{{ID: human, At: h.nowMs()}}); err != nil {
		t.Fatalf("remove membership: %v", err)
	}
	h.reconcileActivation(ctx)
	if live(h, human) {
		t.Fatal("removed human still live — 削臂 did not evict a no-longer-desired managed member")
	}
}

// DoD ⑧ (union 原子性): a desired-source read fault aborts the WHOLE tick — zero
// 削 zero 铸, and prevEagerDesired is NOT updated (so a later good tick still
// manages the member). The组合域 source faulting exercises the same early-return
// abort the user域 (ListActive) fault takes.
func TestReconcileActivation_UnionAtomicity_FaultAbortsTick(t *testing.T) {
	ctx := context.Background()
	desired := &testDesired{}
	builder := newTestBuilder()
	id := actor.ActorID("agent:atomic")
	builder.byID[id] = builder.recordFactory(id)
	h := openActivationHome(t, desired, builder)
	id = admit(t, h, id, actor.KindAgent)

	desired.set(actorrt.DesiredMember{ID: id, Kind: actor.KindAgent, Lifecycle: actorrt.LifecycleAlwaysOn})
	h.reconcileActivation(ctx)
	if !live(h, id) {
		t.Fatal("precondition: member not embodied on the good tick")
	}
	if !h.prevEagerDesired[id] {
		t.Fatal("precondition: member not in prevEagerDesired after the good tick")
	}

	desired.fail(errors.New("injected desired-source fault"))
	h.reconcileActivation(ctx)
	if !live(h, id) {
		t.Fatal("member evicted on a faulted tick — 削臂 ran against a truncated desired (atomicity broken)")
	}
	if !h.prevEagerDesired[id] {
		t.Fatal("prevEagerDesired mutated on a faulted tick — must stay untouched until a complete read")
	}
}

// DoD ⑧ (交集红线): a组合域 intent row whose Admit never landed (crash between the
// two non-atomic writes) is NOT embodied and does NOT enter the 削臂 management set
// (skip precedes current) — no panic. Once the户籍 lands, the next tick embodies it.
func TestReconcileActivation_IntersectionRedline_NoMembershipNoEmbody(t *testing.T) {
	ctx := context.Background()
	desired := &testDesired{}
	builder := newTestBuilder()
	id := actor.ActorID("agent:orphan-intent")
	builder.byID[id] = builder.recordFactory(id)
	h := openActivationHome(t, desired, builder)

	desired.set(actorrt.DesiredMember{ID: id, Kind: actor.KindAgent, Lifecycle: actorrt.LifecycleAlwaysOn})
	h.reconcileActivation(ctx) // must NOT panic, must NOT embody (no户籍)
	if live(h, id) {
		t.Fatal("orphan intent (no户籍) was embodied — 交集红线 broken")
	}
	if h.prevEagerDesired[id] {
		t.Fatal("orphan intent entered the 削臂 management set — the membership skip must precede current[id]=true")
	}

	id = admit(t, h, id, actor.KindAgent)
	desired.set(actorrt.DesiredMember{ID: id, Kind: actor.KindAgent, Lifecycle: actorrt.LifecycleAlwaysOn})
	h.reconcileActivation(ctx)
	if !live(h, id) {
		t.Fatal("member not embodied after its户籍 landed on a later tick")
	}
}

// DoD ⑧ (poke 即时具身): Admit pokes the reconcile ring, so a freshly-admitted
// member embodies OFF-TICK — no synchronous reconcile, no 30s tick wait (the home
// runs a time.Hour interval here). Only the Admit poke drives the background sweep.
func TestAdmit_PokeEmbodiesWithoutWaitingTick(t *testing.T) {
	desired := &testDesired{}
	builder := newTestBuilder()
	h := openActivationHome(t, desired, builder)

	human, err := h.Admit(context.Background(), actor.KindHuman, "poke")
	if err != nil {
		t.Fatalf("Admit: %v", err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for !live(h, human) {
		if time.Now().After(deadline) {
			t.Fatal("admitted human never embodied — the Admit poke did not wake the reconcile ring off-tick")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// Test 4 — EH1真重启 (period 9 review #5): a DURABLE (BindIdentity) self-timer armed
// in one home lifetime, then overdue on the NEXT open of the SAME db, must revive its
// author THROUGH the restart builder (the original admission closure is long gone) and
// append the fire — and must NOT poison-delete the timer row. Two Opens on one db path,
// a fake clock advanced past FireAt on the second, is the only faithful真重启 shape;
// the old single-open past-due write never exercised the closure-gone reviver-via-
// builder path at all.
func TestReconcileActivation_IdentityTimerFireRevivesThenAppends(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "identity-timer-restart.sqlite")
	author := actor.ActorID("agent:sleeper")

	// --- Open #1: admit the author, arm a durable BindIdentity self-timer whose
	// FireAt is in the FUTURE for this open's clock (so it cannot fire during the
	// brief Open #1 window), then Close. The timer persists in the channel db across
	// the close (BindIdentity survives restart). ---
	clock1 := newFakeClock(time.UnixMilli(1_000_000))
	builder1 := newTestBuilder()
	builder1.byID[author] = builder1.recordFactory(author)
	h1, err := Open(HomeConfig{
		ChannelID:         activationTestChannelID,
		DBPath:            dbPath,
		ReconcileInterval: time.Hour,
		Desired:           &testDesired{},
		Builder:           builder1,
		Clock:             clock1,
	})
	if err != nil {
		t.Fatalf("Open #1: %v", err)
	}
	author = admit(t, h1, author, actor.KindAgent)
	if live(h1, author) {
		t.Fatal("author live before the timer fired — precondition broken")
	}
	fireAt := clock1.Now().UnixMilli() + 60_000 // future for clock1
	tid, err := h1.schedMinter.Mint(author).Schedule(ctx, schedule.ScheduleReq{
		Bind: schedule.BindIdentity, FireAt: fireAt, Type: "demo.tick",
	})
	if err != nil {
		t.Fatalf("Schedule: %v", err)
	}
	if err := h1.Close(); err != nil {
		t.Fatalf("Close #1: %v", err)
	}

	// --- Open #2: SAME db, a FRESH builder (the original closure is gone), and a
	// clock reading well PAST FireAt so the persisted timer is overdue on boot. The
	// engine must revive the author through builder2, then append the fire. ---
	clock2 := newFakeClock(time.UnixMilli(2_000_000)) // > fireAt (1_060_000)
	builder2 := newTestBuilder()
	builder2.byID[author] = builder2.recordFactory(author)
	h2, err := Open(HomeConfig{
		ChannelID:         activationTestChannelID,
		DBPath:            dbPath,
		ReconcileInterval: time.Hour,
		Desired:           &testDesired{},
		Builder:           builder2,
		Clock:             clock2,
	})
	if err != nil {
		t.Fatalf("Open #2: %v", err)
	}
	t.Cleanup(func() { _ = h2.Close() })
	// (期12: the old "not live immediately on reopen" precondition is gone —
	// the schedule engine's overdue fire runs async from the instant Start()
	// lands inside Open, so whether the revive has already happened by the
	// time Open returns is a pure goroutine race (Open #2 doing one more
	// startup step — the expiry sweep — tipped it deterministically). The
	// assertions that matter stand: the fire APPENDS through a builder2
	// revive, proven by wantID below + builder2's own recording; "Open never
	// synchronously constructs" has its own dedicated freeze-ring test.)

	wantID := message.ID("timer:" + string(tid))
	deadline := time.Now().Add(5 * time.Second)
	for {
		rows, rerr := h2.View().ReadAfterSeq(ctx, 0, 1000)
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
			t.Fatal("overdue identity timer never fired after restart (真重启 revive-then-append broken, or the row was poison-deleted)")
		}
		clock2.Advance(time.Second) // nudge the engine's poll/backoff loop
		time.Sleep(10 * time.Millisecond)
	}
	// The fire landed as truth AFTER a restart whose original admission closure was
	// gone — so the reviver rebuilt the author through builder2 and the timer row was
	// consumed by a successful append, never poison-deleted.
	if !live(h2, author) {
		t.Fatal("author not live after the fire — the Reviver did not activate an embodiment before append")
	}
	if _, ok := builder2.capsFor(author); !ok {
		t.Fatal("restart builder factory never ran — author was not rebuilt through the reopen builder")
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

	author := admit(t, h, actor.ActorID("agent:live-nobuilder"), actor.KindAgent)
	mintedAuthor, err := SpawnForTest(h, author, actor.KindAgent, CapsFactory(func(actorcaps.Caps) actorrt.Actor {
		return recordActor{}
	}))
	if err != nil {
		t.Fatalf("Spawn live author: %v", err)
	}
	author = mintedAuthor
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
	id := actor.ActorID("agent:sched")
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
	id = admit(t, h, id, actor.KindAgent)

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
	id := actor.ActorID("agent:attached-desired")
	builder.byID[id] = builder.recordFactory(id)

	h := openActivationHome(t, desired, builder)

	id = admit(t, h, id, actor.KindAgent)
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
	id := actor.ActorID("agent:migrated")
	builder.byID[id] = builder.recordFactory(id)

	h := openActivationHome(t, desired, builder)
	id = admit(t, h, id, actor.KindAgent)

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
	author := actor.ActorID("agent:wire-flap")
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
	author = admit(t, h, author, actor.KindAgent)
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

	author := admit(t, h, actor.ActorID("agent:attached-nobuilder"), actor.KindAgent)
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
	mintedAuthor, err := SpawnForTest(h, author, actor.KindAgent, CapsFactory(func(actorcaps.Caps) actorrt.Actor {
		return recordActor{}
	}))
	if err != nil {
		t.Fatalf("Spawn author home: %v", err)
	}
	author = mintedAuthor
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
	author := actor.ActorID("agent:dereged")
	builder.byID[author] = builder.recordFactory(author)

	h := openActivationHome(t, desired, builder)

	// Seed durable membership (unembodied), then soft-deregister it — the registry
	// row survives with DeregisteredAt != 0, exactly the in-flight-race snapshot.
	author = admit(t, h, author, actor.KindAgent)
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
	parent := actor.ActorID("agent:fork-parent")
	builder.byID[parent] = builder.recordFactory(parent)
	const childNameHint = "w1"

	h := openActivationHome(t, desired, builder)
	parent = admit(t, h, parent, actor.KindAgent)
	builder.byClass["worker"] = builder.recordFactory(parent + "/" + actor.ActorID(childNameHint))

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
	parent := actor.ActorID("agent:fork-death-parent")
	builder.byID[parent] = builder.recordFactory(parent)
	const childNameHint = "w1"

	h := openActivationHome(t, desired, builder)
	parent = admit(t, h, parent, actor.KindAgent)
	builder.byClass["worker"] = builder.recordFactory(parent + "/" + actor.ActorID(childNameHint))

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

// Test 14 — attach-straddle (period 9 review #4): a daemon attach stamps Host in the
// registry BETWEEN the reviver's initial Lookup (which saw Host=="", home-placed) and
// its SpawnIfAbsent build landing — the exact window accept.go's handleAttach opens
// (Host stamped on the control frame; the port incarnation installed later on the
// separate stream-open). The post-build recheck must catch the now-non-empty Host,
// UNDO the local build (Despawn), and classify TRANSIENT — never leave a local cell
// double-embodying the daemon's incoming port, and never a poison ReviveRejected.
func TestReviver_AttachStraddle_HostStampedMidBuild_NoLocalRevive(t *testing.T) {
	ctx := context.Background()
	desired := &testDesired{}
	builder := newTestBuilder()
	author := actor.ActorID("agent:straddle")
	builder.byID[author] = builder.recordFactory(author)

	h := openActivationHome(t, desired, builder)
	author = admit(t, h, author, actor.KindAgent) // Host=="" initially → passes the first Lookup

	// The straddle: after factoryFor resolves (Host==""), before SpawnIfAbsent, a
	// daemon attach stamps Host on the SAME id (what accept.go handleAttach does).
	h.reviverStraddleHook = func() {
		if err := h.cs.Membership.ApplyMemberTransitions(ctx, []storespec.MemberActorAdd{
			{ID: author, Kind: actor.KindAgent, Host: "daemon-straddle", At: h.nowMs()},
		}, nil); err != nil {
			t.Errorf("straddle host stamp: %v", err)
		}
	}

	err := (homeReviver{h: h}).EnsureLive(ctx, author)
	if err != nil {
		t.Fatalf("EnsureLive active-only post-build check: %v", err)
	}
	if !live(h, author) {
		t.Fatal("active member build was undone by a placement comparison")
	}
}

// Test 15 — reconcile 补臂 post-build straddle (period 9 review #2): a concurrent
// Home.Remove landing BETWEEN the ring's SpawnIfAbsent build and its post-build
// recheck must be self-undone by the shared verifyPostBuild — the just-built cell is
// Despawn'd (pointer-guarded) and dropped from the managed set (current /
// prevEagerDesired), never resurrected into an unhoused, dereg'd cell (the死后写
// window). Mirror of the reviver arm's TestRemove_ReviverStraddle_SelfUndo.
func TestReconcileActivation_BuildStraddle_RemoveSelfUndo(t *testing.T) {
	ctx := context.Background()
	desired := &testDesired{}
	builder := newTestBuilder()
	id := actor.ActorID("agent:recon-straddle")
	builder.byID[id] = builder.recordFactory(id)

	h := openActivationHome(t, desired, builder)
	id = admit(t, h, id, actor.KindAgent)
	desired.set(actorrt.DesiredMember{ID: id, Kind: actor.KindAgent, Lifecycle: actorrt.LifecycleAlwaysOn})

	// The straddle: Home.Remove runs AFTER the ring's build lands but BEFORE its
	// verifyPostBuild — exactly the window a dereg can slip into.
	var once sync.Once
	h.reconcileBuildHook = func(built actor.ActorID) {
		if built != id {
			return
		}
		once.Do(func() {
			if err := h.Remove(ctx, id); err != nil {
				t.Errorf("Remove during straddle: %v", err)
			}
		})
	}

	h.reconcileActivation(ctx)

	if live(h, id) {
		t.Fatal("a cell survived the reconcile build straddle — post-build recheck did not self-undo")
	}
	if h.prevEagerDesired[id] {
		t.Fatal("straddled id was carried into the managed set (prevEagerDesired) despite being Removed")
	}
}
