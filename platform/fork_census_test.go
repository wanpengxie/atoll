package platform

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/lib/actorbase"
	"github.com/wanpengxie/atoll/protocol/access"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/resource"
	"github.com/wanpengxie/atoll/runtime/actorrt"
	"github.com/wanpengxie/atoll/runtime/resourcespec"
)

// fork_census_test.go is S4's acceptance: the fork子代户籍轴 (spec §4.1/§4.4). A
// fork child is an incarnation-level citizen — no durable name分, so its private
// state is per-incarnation memory (dies with the world, zero inheritance across a
// same-named转世 = EH2 root-cure), it cannot create channel-scoped (durable)
// resources (门 IsMember 拒), it has no BindIdentity入口 (structural), and it dies
// when its parent does (ownership cascade). Plus M2's non-blocking spawn carries
// the parent's opaque config through to the domain build table.

// TestFork_ConfigAndChildID_Passthrough — M2 non-blocking spawn: Fork returns the
// derived childID immediately and threads (childID, class, config) verbatim into
// the domain build table (compositionBuilder.LookupByClass → registry.Build). This
// is the fork half of InstanceSpec.Config's injection (spec §4.2, P1-1: childID is
// derived BEFORE the lookup, because the domain build needs it).
func TestFork_ConfigAndChildID_Passthrough(t *testing.T) {
	ctx := context.Background()
	desired := &testDesired{}
	builder := newTestBuilder()
	parent := actor.ActorID("agent:fork-cfg-parent")
	const nameHint = "cfg1"
	builder.byID[parent] = builder.recordFactory(parent)

	h := openActivationHome(t, desired, builder)
	parent = admit(t, h, parent, actor.KindAgent)
	childID := parent + "/" + actor.ActorID(nameHint)
	builder.byClass["worker"] = builder.recordFactory(childID)
	desired.set(actorrt.DesiredMember{ID: parent, Kind: actor.KindAgent, Lifecycle: actorrt.LifecycleAlwaysOn})
	h.reconcileActivation(ctx)
	parentCaps, ok := builder.capsFor(parent)
	if !ok {
		t.Fatal("parent caps not captured")
	}

	cfg := json.RawMessage(`{"seed":"abc"}`)
	got, err := parentCaps.Spawn.Fork(actorrt.ForkSpec{Kind: actor.KindTool, Class: "worker", NameHint: nameHint, Config: cfg})
	if err != nil {
		t.Fatalf("Fork: %v", err)
	}
	if got != childID {
		t.Fatalf("Fork returned childID %q, want %q (parentID + \"/\" + nameHint)", got, childID)
	}
	if len(builder.forkCalls) != 1 {
		t.Fatalf("builder saw %d fork lookups, want 1", len(builder.forkCalls))
	}
	fc := builder.forkCalls[0]
	if fc.childID != childID {
		t.Fatalf("LookupByClass childID = %q, want %q (derived BEFORE lookup, P1-1)", fc.childID, childID)
	}
	if fc.class != "worker" {
		t.Fatalf("LookupByClass class = %q, want worker", fc.class)
	}
	if string(fc.config) != string(cfg) {
		t.Fatalf("LookupByClass config = %q, want %q (parent's委托 passed through verbatim)", fc.config, cfg)
	}
}

// TestFork_MemoryStateEvaporates_ZeroInheritance — 户籍轴断言①: a fork child's
// private state is per-incarnation MEMORY. A child writes state, dies; a NEW child
// under the SAME name reads nothing (structural EH2 root-cure — the store is not
// keyed globally, it IS the instance). AND the parent's own DURABLE state is
// untouched by the child's memory writes/death.
func TestFork_MemoryStateEvaporates_ZeroInheritance(t *testing.T) {
	ctx := context.Background()
	desired := &testDesired{}
	builder := newTestBuilder()
	parent := actor.ActorID("agent:fork-state-parent")
	const nameHint = "w1"
	builder.byID[parent] = builder.recordFactory(parent)

	h := openActivationHome(t, desired, builder)
	parent = admit(t, h, parent, actor.KindAgent)
	childID := parent + "/" + actor.ActorID(nameHint)
	builder.byClass["worker"] = builder.recordFactory(childID)
	desired.set(actorrt.DesiredMember{ID: parent, Kind: actor.KindAgent, Lifecycle: actorrt.LifecycleAlwaysOn})
	h.reconcileActivation(ctx)
	parentCaps, ok := builder.capsFor(parent)
	if !ok {
		t.Fatal("parent caps not captured")
	}

	// Parent writes its own durable state.
	const pkey = resource.ResourceID("parent-cursor")
	if _, err := parentCaps.State.Invoke(ctx, access.OpCreate, pkey, []byte("pv"), nil); err != nil {
		t.Fatalf("parent durable State create: %v", err)
	}

	// Fork child A, write child state, read it back.
	const ckey = resource.ResourceID("child-cursor")
	if _, err := parentCaps.Spawn.Fork(actorrt.ForkSpec{Kind: actor.KindTool, Class: "worker", NameHint: nameHint}); err != nil {
		t.Fatalf("Fork child A: %v", err)
	}
	childCapsA, ok := builder.capsFor(childID)
	if !ok {
		t.Fatal("child A caps not captured")
	}
	if _, err := childCapsA.State.Invoke(ctx, access.OpCreate, ckey, []byte("v1"), nil); err != nil {
		t.Fatalf("child A memory State create: %v", err)
	}
	outA, err := childCapsA.State.Invoke(ctx, access.OpRead, ckey, nil, nil)
	if err != nil {
		t.Fatalf("child A memory State read: %v", err)
	}
	if string(outA.Value) != "v1" {
		t.Fatalf("child A State read back %q, want v1", outA.Value)
	}

	// Child A dies — its memory store must evaporate with the incarnation.
	if err := parentCaps.Spawn.Despawn(childID); err != nil {
		t.Fatalf("Despawn child A: %v", err)
	}
	if live(h, childID) {
		t.Fatal("child A still live after Despawn — precondition broken")
	}

	// Fork child B under the SAME name → a fresh empty memory store. It must NOT
	// inherit child A's state (EH2 root-cure: zero global keying).
	if _, err := parentCaps.Spawn.Fork(actorrt.ForkSpec{Kind: actor.KindTool, Class: "worker", NameHint: nameHint}); err != nil {
		t.Fatalf("Fork child B (same name): %v", err)
	}
	childCapsB, ok := builder.capsFor(childID)
	if !ok {
		t.Fatal("child B caps not captured")
	}
	outB, err := childCapsB.State.Invoke(ctx, access.OpRead, ckey, nil, nil)
	if err != nil {
		t.Fatalf("child B memory State read: %v", err)
	}
	if outB.Accepted() && outB.Found {
		t.Fatalf("child B inherited child A's state (value=%q) — same-named转世 must inherit NOTHING (EH2 not root-cured)", outB.Value)
	}
	if outB.RejectReason != access.ResourceNotFound {
		t.Fatalf("child B State read verdict = %q, want resource_not_found (fresh empty per-incarnation store)", outB.RejectReason)
	}

	// Parent's durable state is untouched by the child's memory writes and death.
	outP, err := parentCaps.State.Invoke(ctx, access.OpRead, pkey, nil, nil)
	if err != nil {
		t.Fatalf("parent durable State read: %v", err)
	}
	if string(outP.Value) != "pv" {
		t.Fatalf("parent durable state read back %q, want pv (fork children must not touch the durable backend)", outP.Value)
	}
	// And the parent's durable backend never held the child's key (a memory-backed
	// child arm proves it structurally; verify the parent cannot read it either).
	crossread, err := parentCaps.State.Invoke(ctx, access.OpRead, ckey, nil, nil)
	if err != nil {
		t.Fatalf("parent cross-read: %v", err)
	}
	if crossread.Accepted() && crossread.Found {
		t.Fatalf("child memory state leaked into the parent's durable locus (value=%q)", crossread.Value)
	}
}

// TestFork_ChildDurableCreateDenied — 户籍轴断言②: a fork child cannot create a
// channel-scoped (durable) resource — it is not a channel member, so the门's
// IsMember check denies OpCreate (the orphan-resource source is closed at the door,
// not a capability gap). The正道 for durable output is父授 workspace (spec §4.1).
func TestFork_ChildDurableCreateDenied(t *testing.T) {
	ctx := context.Background()
	desired := &testDesired{}
	builder := newTestBuilder()
	parent := actor.ActorID("agent:fork-orphan-parent")
	const nameHint = "w1"
	builder.byID[parent] = builder.recordFactory(parent)

	h := openActivationHome(t, desired, builder)
	parent = admit(t, h, parent, actor.KindAgent)
	childID := parent + "/" + actor.ActorID(nameHint)
	builder.byClass["worker"] = builder.recordFactory(childID)
	desired.set(actorrt.DesiredMember{ID: parent, Kind: actor.KindAgent, Lifecycle: actorrt.LifecycleAlwaysOn})
	h.reconcileActivation(ctx)
	parentCaps, ok := builder.capsFor(parent)
	if !ok {
		t.Fatal("parent caps not captured")
	}

	if _, err := parentCaps.Spawn.Fork(actorrt.ForkSpec{Kind: actor.KindTool, Class: "worker", NameHint: nameHint}); err != nil {
		t.Fatalf("Fork child: %v", err)
	}
	childCaps, ok := builder.capsFor(childID)
	if !ok {
		t.Fatal("child caps not captured")
	}

	// The child is NOT a member (fork is not an introduce门). A channel-scoped
	// create must be denied at the door's IsMember check.
	out, err := childCaps.Access.Create(ctx, resource.ResourceID("res:child-orphan"), resourcespec.CreateSpec{Kind: resourcespec.KindKV}, []byte("{}"))
	if err != nil {
		t.Fatalf("child durable create: %v", err)
	}
	if out.Accepted() {
		t.Fatal("fork child created a channel-scoped resource — the门 must deny a non-member's create (orphan-resource source)")
	}
	if out.RejectReason != access.AccessDenied {
		t.Fatalf("child create verdict = %q, want access_denied (IsMember 拒)", out.RejectReason)
	}

	// Contrast: the parent IS a member and CAN create — proves the denial is the
	// child's non-membership, not a broken door.
	pout, err := parentCaps.Access.Create(ctx, resource.ResourceID("res:parent-owned"), resourcespec.CreateSpec{Kind: resourcespec.KindKV}, []byte("{}"))
	if err != nil {
		t.Fatalf("parent durable create: %v", err)
	}
	if !pout.Accepted() {
		t.Fatalf("parent (a member) create was denied: %q — the door itself is broken", pout.RejectReason)
	}
}

// TestFork_ParentDeathCascadesChild — 户籍轴断言④: a fork child's lifetime is ≤ its
// parent's incarnation. When the parent dies, the still-live child is
// signal-cascaded to death too (r.owned ownership edge). Regression of既有 cascade.
func TestFork_ParentDeathCascadesChild(t *testing.T) {
	ctx := context.Background()
	desired := &testDesired{}
	builder := newTestBuilder()
	parent := actor.ActorID("agent:fork-cascade-parent")
	const nameHint = "w1"
	builder.byID[parent] = builder.recordFactory(parent)

	h := openActivationHome(t, desired, builder)
	parent = admit(t, h, parent, actor.KindAgent)
	childID := parent + "/" + actor.ActorID(nameHint)
	builder.byClass["worker"] = builder.recordFactory(childID)
	// Parent is NOT desired-always-on here: we admit + spawn via reconcile once,
	// then despawn it directly and assert the child dies too WITHOUT the ring
	// reviving the parent (ReconcileInterval is 1h; no tick fires).
	desired.set(actorrt.DesiredMember{ID: parent, Kind: actor.KindAgent, Lifecycle: actorrt.LifecycleAlwaysOn})
	h.reconcileActivation(ctx)
	parentCaps, ok := builder.capsFor(parent)
	if !ok {
		t.Fatal("parent caps not captured")
	}
	if _, err := parentCaps.Spawn.Fork(actorrt.ForkSpec{Kind: actor.KindTool, Class: "worker", NameHint: nameHint}); err != nil {
		t.Fatalf("Fork child: %v", err)
	}
	if !live(h, childID) {
		t.Fatal("child not live after Fork")
	}

	h.channel.Cells().DespawnID(parent)
	if live(h, parent) {
		t.Fatal("parent still live after DespawnID (live bit flips in the despawn critical section)")
	}
	// The ownership cascade runs when the parent cell's goroutine EXITS (onExit →
	// removeIf → signal each owned child), which the escort drives asynchronously —
	// poll for the child to become not-live rather than asserting synchronously.
	deadline := time.Now().Add(5 * time.Second)
	for live(h, childID) {
		if time.Now().After(deadline) {
			t.Fatal("child still live after parent death — ownership cascade (父死子亡) broken")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// 户籍轴断言③ — no BindIdentity入口 — is a COMPILE-LEVEL fact, asserted here by the
// absence of any such verb: actorbase.Sys exposes only After (which hardcodes
// schedule.BindIncarnation in the engine) — there is no Sys.BindIdentity method, so
// a Proc (child or not) has no入口 to a durable timer. The following reference to
// the only timer verb documents that surface; if a BindIdentity verb were ever
// added to Sys, this file would still compile but the assertion's premise (kept in
// the comment + the fact that After is the sole timer verb) would need revisiting.
var _ = func(sys actorbase.Sys) { _ = sys.After }
