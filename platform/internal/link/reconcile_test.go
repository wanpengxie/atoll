package link_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/platform/internal/link"
	"github.com/wanpengxie/atoll/protocol/access"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/protocol/resource"
	"github.com/wanpengxie/atoll/runtime"
	"github.com/wanpengxie/atoll/runtime/actorrt"
)

// storeHomeRig is the S5b counterpart to homeRig: it wires the Acceptor over a
// REAL per-channel sqlite (runtime.OpenChannel) instead of the stub membership,
// so it exercises the actual Host-column Registry read face and the actual
// applyMemberRemoveTx cascade (state/timers) — not a fake that could drift from
// substrate truth.
type storeHomeRig struct {
	acc     *link.Acceptor
	rt      *actorrt.Runtime
	deliver actorrt.Deliverer
	cs      *runtime.ChannelStores
	srv     *httptest.Server
}

func newStoreHomeRig(t *testing.T) *storeHomeRig {
	return newStoreHomeRigWithObs(t, nil)
}

func newStoreHomeRigWithObs(t *testing.T, obsWatcher actorrt.ObsWatcher) *storeHomeRig {
	t.Helper()
	ctx := context.Background()
	cs, err := runtime.OpenChannel(ctx, testChannelID, filepath.Join(t.TempDir(), "ch.sqlite"), runtime.OpenChannelOptions{})
	if err != nil {
		t.Fatalf("OpenChannel: %v", err)
	}
	t.Cleanup(func() { _ = cs.Close() })

	rt, del := actorrt.New(actorrt.Config{Parent: context.Background()})
	if obsWatcher != nil {
		rt.WatchObsAll(obsWatcher)
	}
	r := &storeHomeRig{rt: rt, deliver: del, cs: cs}
	r.acc = link.NewAcceptor(link.Config{
		Minter:     &stubMinter{},
		Runtime:    rt,
		Membership: cs.Membership,
		Registry:   cs.Registry,
		ChannelID:  testChannelID,
		LeasePing:  5 * time.Second,
		LeaseTTL:   30 * time.Second,
	})
	r.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		r.acc.Serve(w, req, "daemon-1")
	}))
	t.Cleanup(func() { _ = r.acc.Close(); r.srv.Close() })
	return r
}

func (r *storeHomeRig) wsURL() string { return "ws" + r.srv.URL[4:] }

// admit seeds durable membership for ids (Host="" neutral rows). Since the
// membrane law (v1.8 问①) stopped attach from minting membership, a declared id
// must be an existing active member for the attach to stamp its Host — this stands
// in for the introduce door the raw link rig bypasses.
func (r *storeHomeRig) admit(t *testing.T, ids ...actor.ActorID) []actor.ActorID {
	t.Helper()
	out := make([]actor.ActorID, 0, len(ids))
	for _, id := range ids {
		minted, err := r.cs.Membership.Admit(context.Background(), actor.KindTool, strings.ReplaceAll(string(id), ":", "-"), time.Now().UnixMilli())
		if err != nil {
			t.Fatalf("admit: %v", err)
		}
		out = append(out, minted)
	}
	return out
}

// deliverProbe drives one request at id straight off the rig's Runtime and
// returns the per-audience Outcome — the despawn-first probe: a despawned
// actor's port is gone from the Runtime's addressing map entirely, so this
// reports NotHosted rather than dispatching anywhere.
func (r *storeHomeRig) deliverProbe(id actor.ActorID) (actorrt.Outcome, error) {
	res, err := r.deliver.Deliver([]actor.ActorID{id}, &message.Envelope{
		ID:        "probe-1",
		ChannelID: testChannelID,
		Kind:      message.KindRequest,
		Type:      "probe.do",
		Sender:    message.Sender{ID: "user:a", Kind: actor.KindHuman},
		Audience:  message.Audience{id},
	})
	if err != nil {
		return 0, err
	}
	return res.Per[id], nil
}

// TestAttach_DeclarationWithoutMembership_NotMinted proves the membrane law
// (v1.8 问①): a daemon declaring an id with NO active户籍 does NOT mint a
// membership row — "a daemon may attach" is not "a daemon may任命 members". The
// admitted id in the same declaration IS stamped (只盖 Host); the un-admitted one
// is refused/skipped, leaving zero new rows.
func TestAttach_DeclarationWithoutMembership_NotMinted(t *testing.T) {
	ctx := context.Background()
	r := newStoreHomeRig(t)

	var (
		member = actor.ActorID("tool:member")
		orphan = actor.ActorID("tool:orphan")
	)
	member = r.admit(t, member)[0] // orphan is deliberately NOT admitted

	d, err := link.Dial(ctx, r.wsURL(), "daemon-1", []link.Declaration{
		{ActorID: member, Kind: actor.KindTool, Binding: actor.BindingEmbedded},
		{ActorID: orphan, Kind: actor.KindTool, Binding: actor.BindingEmbedded},
	}, link.DialConfig{}, nil)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer func() { _ = d.Close() }()

	// The admitted member's Host is stamped by attach.
	deadline := time.Now().Add(5 * time.Second)
	for {
		rec, ok, _ := r.cs.Registry.Lookup(ctx, member)
		if ok && rec.Host == "daemon-1" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("admitted member never got Host=daemon-1 (只盖 Host arm broken)")
		}
		time.Sleep(5 * time.Millisecond)
	}
	// The un-admitted orphan has NO membership row — attach never minted one.
	if _, ok, err := r.cs.Registry.Lookup(ctx, orphan); err != nil {
		t.Fatalf("Lookup(orphan): %v", err)
	} else if ok {
		t.Fatal("attach minted membership for an un-admitted declaration (膜律 问① broken — 绝不铸行)")
	}
}

// TestAttach_OrphanDeclaration_NotInAllowSet proves the 问① allow-set gate: an
// orphan declaration (no active户籍) is dropped not only from the membership write
// but from the allowed/kinds allow-set too, so the daemon CANNOT OpenStream a
// welded pen for it (resolve rejects → the home closes the stream → the handshake
// ack read fails). The admitted member's stream opens fine, proving the gate is
// per-declaration and not a wholesale reject.
func TestAttach_OrphanDeclaration_NotInAllowSet(t *testing.T) {
	ctx := context.Background()
	r := newStoreHomeRig(t)

	var (
		member = actor.ActorID("tool:member")
		orphan = actor.ActorID("tool:orphan")
	)
	member = r.admit(t, member)[0] // orphan deliberately NOT admitted

	d, err := link.Dial(ctx, r.wsURL(), "daemon-1", []link.Declaration{
		{ActorID: member, Kind: actor.KindTool, Binding: actor.BindingEmbedded},
		{ActorID: orphan, Kind: actor.KindTool, Binding: actor.BindingEmbedded},
	}, link.DialConfig{}, nil)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer func() { _ = d.Close() }()

	// The admitted member's stream opens (it is in the allow-set).
	if _, err := d.OpenStream(member, func(*message.Envelope) error { return nil }, func(message.ID) {}); err != nil {
		t.Fatalf("OpenStream(member) must succeed (admitted): %v", err)
	}
	// The orphan's stream is refused: resolve rejects the undeclared id, the home
	// closes the stream, and OpenStream's handshake-ack read fails. Before the fix
	// the orphan sat in the allow-set and this would succeed → a welded pen for a
	// non-member (truth forgery surface).
	if _, err := d.OpenStream(orphan, func(*message.Envelope) error { return nil }, func(message.ID) {}); err == nil {
		t.Fatal("OpenStream(orphan) must fail — an un-admitted declaration is NOT in the allow-set (问① gate broken)")
	}

	// Doubly: the orphan minted no membership row (allow-set and membership share the
	// one Lookup verdict).
	if _, ok, err := r.cs.Registry.Lookup(ctx, orphan); err != nil {
		t.Fatalf("Lookup(orphan): %v", err)
	} else if ok {
		t.Fatal("orphan declaration minted a membership row (膜律 问① broken)")
	}
}

// TestReattach_HostReconcile_DespawnsAndDeregistersFallenOut proves the S5
// attach-time reconciliation (spec §3-S5, forward §10.13 推导7): a Reattach
// declaring a SMALLER set than the compute currently hosts (Host==computeID in
// the real Registry) despawns the fallen-out actor's port FIRST (guarded by the
// per-link Incarnation pointer retained at onOpen) and THEN deregisters its
// membership row — in that order, so the removal transaction never runs while
// the embodiment is still live.
//
// Despawn-first is observed via NotHosted (the home Runtime no longer routes to
// the actor at all — the guarded Despawn matched and evicted the embodiment);
// dereg is observed via the real Registry row's DeregisteredAt; the same-tx
// state cascade (§10.12 row 6) is observed via a pre-seeded actor-scoped value
// disappearing (cascade-cleared, not merely orphaned) — proving this reconcile
// path drives the SAME applyMemberRemoveTx used everywhere else, not a
// parallel dereg mechanism that would skip the cascade.
func TestReattach_HostReconcile_DespawnsAndDeregistersFallenOut(t *testing.T) {
	ctx := context.Background()
	r := newStoreHomeRig(t)

	var (
		toolA = actor.ActorID("tool:a")
		toolB = actor.ActorID("tool:b")
	)
	minted := r.admit(t, toolA, toolB)
	toolA, toolB = minted[0], minted[1]

	d, err := link.Dial(ctx, r.wsURL(), "daemon-1", []link.Declaration{
		{ActorID: toolA, Kind: actor.KindTool, Binding: actor.BindingEmbedded},
		{ActorID: toolB, Kind: actor.KindTool, Binding: actor.BindingEmbedded},
	}, link.DialConfig{}, nil)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer func() { _ = d.Close() }()

	noopDispatch := func(*message.Envelope) error { return nil }
	if _, err := d.OpenStream(toolA, noopDispatch, nil); err != nil {
		t.Fatalf("OpenStream(a): %v", err)
	}
	if _, err := d.OpenStream(toolB, noopDispatch, nil); err != nil {
		t.Fatalf("OpenStream(b): %v", err)
	}
	d.Start()

	// Wait for both ports to attach (home-side onOpen retains the Incarnation
	// asynchronously) — poll via the real Registry until both rows show up
	// Host-owned by this compute.
	deadline := time.Now().Add(10 * time.Second)
	for {
		recA, okA, _ := r.cs.Registry.Lookup(ctx, toolA)
		recB, okB, _ := r.cs.Registry.Lookup(ctx, toolB)
		if okA && okB && recA.Host == "daemon-1" && recB.Host == "daemon-1" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("actors never registered Host=daemon-1")
		}
		time.Sleep(5 * time.Millisecond)
	}

	// Seed actor-scoped state for toolA — the cascade probe.
	hState := r.cs.Access.MintState(toolA)
	const stateID = resource.ResourceID("cursor")
	if out, err := hState.Invoke(ctx, access.OpCreate, stateID, []byte("v1"), nil); err != nil || !out.Accepted() {
		t.Fatalf("seed actor state: out=%+v err=%v", out, err)
	}

	// Shrink the declared set to {b} only — toolA falls out.
	if err := d.Reattach(ctx, []link.Declaration{
		{ActorID: toolB, Kind: actor.KindTool, Binding: actor.BindingEmbedded},
	}); err != nil {
		t.Fatalf("Reattach (shrink): %v", err)
	}

	// Despawn-first ORDERING invariant (not just "both eventually happen"):
	// sample hosted-status and Registry-active-status TOGETHER on every tick and
	// fail the instant a sample shows the FORBIDDEN intermediate state — the
	// Registry row already deregistered while the port is still hosted. Two
	// independent poll loops (one for NotHosted, then one for dereg) would NOT
	// catch a reordering regression: each loop only waits for its own condition
	// to eventually go true, so it passes even if dereg raced ahead of despawn.
	// Sampling both facts on the same tick is what actually pins the order.
	deadline = time.Now().Add(10 * time.Second)
	var sawNotHosted, sawDeregistered bool
	for {
		outcome, err := r.deliverProbe(toolA)
		if err != nil {
			t.Fatalf("deliverProbe(toolA): %v", err)
		}
		hosted := outcome != actorrt.NotHosted
		rec, ok, err := r.cs.Registry.Lookup(ctx, toolA)
		if err != nil {
			t.Fatalf("Lookup(toolA): %v", err)
		}
		deregistered := ok && !rec.IsActive()
		if deregistered && hosted {
			t.Fatalf("toolA Registry row deregistered while the port is STILL hosted (outcome=%v) — dereg ran ahead of (or decoupled from) despawn", outcome)
		}
		if !hosted {
			sawNotHosted = true
		}
		if deregistered {
			sawDeregistered = true
		}
		if sawNotHosted && sawDeregistered {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("shrink Reattach never reached terminal state: notHosted=%v deregistered=%v", sawNotHosted, sawDeregistered)
		}
		time.Sleep(2 * time.Millisecond)
	}

	// toolB (still declared) stays active and untouched.
	recB, okB, err := r.cs.Registry.Lookup(ctx, toolB)
	if err != nil || !okB || !recB.IsActive() {
		t.Fatalf("toolB Lookup = %+v ok=%v err=%v, want active", recB, okB, err)
	}

	// Same-tx cascade: toolA's actor-scoped state is cascade-cleared, not merely
	// orphaned — a fresh read for the SAME id resolves resource_not_found.
	out, err := hState.Invoke(ctx, access.OpRead, stateID, nil, nil)
	if err != nil {
		t.Fatalf("state read after dereg: %v", err)
	}
	if out.Accepted() {
		t.Fatalf("state read after dereg = accepted %+v, want cascade-cleared (resource_not_found)", out)
	}
}

// countingObsWatcher proves the one population subscription remains single
// across declaration churn and same-name reattachment.
type countingObsWatcher struct {
	mu    sync.Mutex
	calls map[actor.ActorID]int
}

func newCountingObsWatcher() *countingObsWatcher {
	return &countingObsWatcher{calls: map[actor.ActorID]int{}}
}

func (w *countingObsWatcher) OnObs(_ context.Context, id actor.ActorID, _ actorrt.Incarnation, _ actorrt.ObsKind, _ actorrt.ObsValue) {
	w.mu.Lock()
	w.calls[id]++
	w.mu.Unlock()
}

func (w *countingObsWatcher) count(id actor.ActorID) int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.calls[id]
}

// TestReattach_HostReconcile_PopulationObsStaysSingle proves attach/dereg churn
// performs no subscription bookkeeping and each publish still arrives once.
func TestReattach_HostReconcile_PopulationObsStaysSingle(t *testing.T) {
	ctx := context.Background()
	obs := newCountingObsWatcher()
	r := newStoreHomeRigWithObs(t, obs)

	toolA := actor.ActorID("tool:obs-a")
	newID, err := r.cs.Membership.Admit(ctx, actor.KindTool, "obs-a", time.Now().UnixMilli())
	if err != nil {
		t.Fatalf("re-admit: %v", err)
	}
	if newID == toolA {
		t.Fatal("re-admit reused removed id")
	}
	toolA = newID
	d, err := link.Dial(ctx, r.wsURL(), "daemon-1",
		[]link.Declaration{{ActorID: toolA, Kind: actor.KindTool, Binding: actor.BindingEmbedded}}, link.DialConfig{}, nil)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer func() { _ = d.Close() }()

	noopDispatch := func(*message.Envelope) error { return nil }
	if _, err := d.OpenStream(toolA, noopDispatch, nil); err != nil {
		t.Fatalf("OpenStream(a): %v", err)
	}
	d.Start()

	deadline := time.Now().Add(10 * time.Second)
	for {
		rec, ok, _ := r.cs.Registry.Lookup(ctx, toolA)
		if ok && rec.Host == "daemon-1" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("toolA never registered Host=daemon-1")
		}
		time.Sleep(5 * time.Millisecond)
	}

	d.SendObs(toolA, "probe.kind", []byte("v1"))
	deadline = time.Now().Add(5 * time.Second)
	for obs.count(toolA) < 1 {
		if time.Now().After(deadline) {
			t.Fatal("obs watcher never observed the first publish")
		}
		time.Sleep(5 * time.Millisecond)
	}

	// Shrink the declared set to {} — toolA falls out without touching obs
	// subscription state.
	if err := d.Reattach(ctx, nil); err != nil {
		t.Fatalf("Reattach (drop toolA): %v", err)
	}
	deadline = time.Now().Add(10 * time.Second)
	for {
		rec, ok, _ := r.cs.Registry.Lookup(ctx, toolA)
		if ok && !rec.IsActive() {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("toolA never deregistered after shrink Reattach")
		}
		time.Sleep(5 * time.Millisecond)
	}

	// Re-admit toolA (the introduce door re-runs: a deregistered id needs its户籍
	// back before attach may stamp Host — membrane law, v1.8 问①), then re-declare
	// it on the SAME link (a fresh embodiment, same id).
	oldID := toolA
	toolA, err = r.cs.Membership.Admit(ctx, actor.KindTool, "obs-a", time.Now().UnixMilli())
	if err != nil {
		t.Fatalf("re-admit: %v", err)
	}
	if toolA == oldID {
		t.Fatal("re-admit reused removed id")
	}
	if err := d.Reattach(ctx, []link.Declaration{
		{ActorID: toolA, Kind: actor.KindTool, Binding: actor.BindingEmbedded},
	}); err != nil {
		t.Fatalf("Reattach (re-add toolA): %v", err)
	}
	if _, err := d.OpenStream(toolA, noopDispatch, nil); err != nil {
		t.Fatalf("OpenStream(a) after re-attach: %v", err)
	}

	deadline = time.Now().Add(10 * time.Second)
	for {
		rec, ok, _ := r.cs.Registry.Lookup(ctx, toolA)
		if ok && rec.Host == "daemon-1" && rec.IsActive() {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("toolA never re-registered Host=daemon-1")
		}
		time.Sleep(5 * time.Millisecond)
	}

	before := obs.count(toolA)
	d.SendObs(toolA, "probe.kind", []byte("v2"))
	deadline = time.Now().Add(5 * time.Second)
	for obs.count(toolA) == before {
		if time.Now().After(deadline) {
			t.Fatal("obs watcher never observed the post-reattach publish")
		}
		time.Sleep(5 * time.Millisecond)
	}
	// Settle before asserting the count stayed at exactly one new call.
	time.Sleep(50 * time.Millisecond)
	if got := obs.count(toolA) - before; got != 1 {
		t.Fatalf("post-reattach publish observed %d times, want exactly 1 population delivery", got)
	}
}

// TestAttach_HumanDeclaration_Rejected proves the ontological gate (期12
// S3.5, 主题A A2): a human is恒 home-hosted (三层律), so a daemon declaring a
// KindHuman id — judged on the REGISTRY's kind, not the daemon's self-report
// — is dropped: no Host stamp, and the daemon cannot OpenStream a welded pen
// for it. A sibling tool declaration on the same attach is unaffected
// (per-declaration gate, not a wholesale reject).
func TestAttach_HumanDeclaration_Rejected(t *testing.T) {
	ctx := context.Background()
	r := newStoreHomeRig(t)

	var (
		toolID  = actor.ActorID("tool:member")
		humanID = actor.ActorID("user:alice")
	)
	toolID = r.admit(t, toolID)[0]
	// Admit the human with its TRUE registry kind.
	var err error
	humanID, err = r.cs.Membership.Admit(ctx, actor.KindHuman, "alice", time.Now().UnixMilli())
	if err != nil {
		t.Fatalf("admit human: %v", err)
	}

	// The daemon lies about the kind (self-reports tool) — the gate must
	// judge on rec.Kind and still reject.
	d, err := link.Dial(ctx, r.wsURL(), "daemon-1", []link.Declaration{
		{ActorID: toolID, Kind: actor.KindTool, Binding: actor.BindingEmbedded},
		{ActorID: humanID, Kind: actor.KindTool, Binding: actor.BindingEmbedded},
	}, link.DialConfig{}, nil)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer func() { _ = d.Close() }()

	// Sibling tool gets its Host stamp (attach succeeded, gate is per-decl).
	deadline := time.Now().Add(5 * time.Second)
	for {
		rec, ok, _ := r.cs.Registry.Lookup(ctx, toolID)
		if ok && rec.Host == "daemon-1" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("sibling tool never got Host=daemon-1")
		}
		time.Sleep(5 * time.Millisecond)
	}
	// The human row is untouched: kind preserved, no Host claim.
	rec, ok, err := r.cs.Registry.Lookup(ctx, humanID)
	if err != nil || !ok {
		t.Fatalf("Lookup(human): ok=%v err=%v", ok, err)
	}
	if rec.Kind != actor.KindHuman || rec.Host == "daemon-1" {
		t.Fatalf("human row = kind %q host %q — attach stamped a daemon host onto a human (本体非法声明放行)", rec.Kind, rec.Host)
	}
	// And the daemon cannot open a welded-pen stream for the human id.
	if _, err := d.OpenStream(humanID, func(*message.Envelope) error { return nil }, func(message.ID) {}); err == nil {
		t.Fatal("OpenStream(human) must fail — a human declaration is never in a daemon's allow-set")
	}
}
