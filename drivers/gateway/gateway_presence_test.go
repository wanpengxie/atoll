package gateway

// 在场对账圈 (spec §3.2 收敛对象乙) tests: cross-channel isolation (T6 消融锚),
// multi-tab device account (three edges), convergence-from-any-dirty-state,
// Remove→re-Admit 换值差集, 出生握手 interleave, and the 关站清账收窄 contract.

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/platform/subjectgate"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
)

// observeSlot registers a counting observer on slot and returns (edges, level): the
// number of PresenceUpdate callbacks and the last delivered level+Live.
type slotObs struct {
	mu    sync.Mutex
	edges []subjectgate.PresenceUpdate
}

func (o *slotObs) fn(u subjectgate.PresenceUpdate) {
	o.mu.Lock()
	o.edges = append(o.edges, u)
	o.mu.Unlock()
}
func (o *slotObs) snapshot() []subjectgate.PresenceUpdate {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]subjectgate.PresenceUpdate(nil), o.edges...)
}
func (o *slotObs) count() int { o.mu.Lock(); defer o.mu.Unlock(); return len(o.edges) }

// TestCrossChannelPresenceIsolated (DoD-2, T6 消融钉锚): ONE principal on TWO channels
// (two Homes) with two devices. Both channels resolve online. Revoking channel c1
// (drop its route) → c1's slot goes offline while c2's slot in场账 is UNTOUCHED (分毫
// 不动) — the假轴 that made a per-identity slot shared across channels is gone, so a
// c1 revocation can never误杀 c2's testimony (this格 is what old
// TestCrossChannelArmsIsolated tested against a生产捏不出的 geometry).
func TestCrossChannelPresenceIsolated(t *testing.T) {
	clk := newClock()
	res := newResolver()
	g := newTestGateway(t, Config{Resolver: res}, settings{clock: clk})

	const principal = "alice"
	h1, id1 := openHome(t, channel.ID("c1"), principal)
	h2, id2 := openHome(t, channel.ID("c2"), principal)
	res.set(principal, []Route{
		memberRoute("c1", h1, id1, clk.now()),
		memberRoute("c2", h2, id2, clk.now()),
	}, nil, nil)

	// Two devices for alice.
	s1, _ := g.Attach(principal, nil)
	s2, _ := g.Attach(principal, nil)
	defer s1.Close()
	defer s2.Close()

	slot1, ok1 := h1.SubjectSlotFor(id1)
	slot2, ok2 := h2.SubjectSlotFor(id2)
	if !ok1 || !ok2 {
		t.Fatalf("member slots must exist after Admit: c1=%v c2=%v", ok1, ok2)
	}
	obs1, obs2 := &slotObs{}, &slotObs{}
	slot1.RegisterObserver("o1", obs1.fn)
	slot2.RegisterObserver("o2", obs2.fn)

	// One 圈: both channels online.
	g.presenceReconcile()
	waitOnline := func(o *slotObs, ch string) {
		es := o.snapshot()
		if len(es) == 0 || !es[len(es)-1].Live || es[len(es)-1].Level != subjectgate.LevelOnline {
			t.Fatalf("%s expected online after first 圈, got %+v", ch, es)
		}
	}
	waitOnline(obs1, "c1")
	waitOnline(obs2, "c2")
	c2EdgesBefore := obs2.count()

	// Revoke c1 only (drop its route). Another 圈.
	res.set(principal, []Route{memberRoute("c2", h2, id2, clk.now())}, nil, nil)
	g.presenceReconcile()

	// c1 → offline (撤证: an offline LEVEL edge, still a live testimony; Live=false is
	// reserved for epoch-supersede/Forget).
	e1 := obs1.snapshot()
	if e1[len(e1)-1].Level != subjectgate.LevelOffline {
		t.Fatalf("c1 must go offline after revocation, got %+v", e1[len(e1)-1])
	}
	// c2 → 分毫不动: no new edge delivered (idempotent online is a no-op).
	if got := obs2.count(); got != c2EdgesBefore {
		t.Fatalf("revoking c1 must NOT touch c2's presence account (T6 消融): c2 edges %d→%d", c2EdgesBefore, got)
	}
	if _, _, _, set := slot2.Snapshot(); !set {
		t.Fatal("c2 slot testimony must survive a c1 revocation")
	}
	// coverage account: c1 dropped, c2 retained.
	if _, ok := g.coverage[covKey{principal: principal, channel: "c1"}]; ok {
		t.Fatal("c1 coverage must be dropped after revocation")
	}
	if _, ok := g.coverage[covKey{principal: principal, channel: "c2"}]; !ok {
		t.Fatal("c2 coverage must survive")
	}
}

// TestT6RealRemoveCascadeIsolated (DoD-2/T6 消融钉锚): the canonical T6
// anchor driven through the Bundle SysOp revoke path — the channel's cascade must
// torn c1's slot down (RemoveSubjectSlot: a later SubjectSlotFor lookup on the SAME id
// misses), and c2's slot/presence account must be UNTOUCHED — proving the T6-era bug
// ("一人两频道共享同一 per-identity 槽") cannot recur: a c1 removal's cascade has no
// path to c2's slot because they were never the same registry entry to begin with.
// Convergence is driven by the REAL membership-change poke (openHomeWired), no manual
// g.presenceReconcile() call.
func TestT6RealRemoveCascadeIsolated(t *testing.T) {
	clk := newClock()
	res := newResolver()
	g := newTestGateway(t, Config{Resolver: res}, settings{clock: clk})
	const principal = "wren"
	h1, id1 := openDeclaredAgentHomeWired(t, channel.ID("c1"), principal, g)
	h2, id2 := openDeclaredAgentHomeWired(t, channel.ID("c2"), principal, g)
	res.set(principal, []Route{
		memberRoute("c1", h1, id1, clk.now()),
		memberRoute("c2", h2, id2, clk.now()),
	}, nil, nil)

	s, _ := g.Attach(principal, nil)
	defer s.Close()
	g.Start()
	s.StartFeed()

	slot2, ok2 := h2.SubjectSlotFor(id2)
	if !ok2 {
		t.Fatal("c2 slot must exist after Admit")
	}
	obs2 := &slotObs{}
	slot2.RegisterObserver("o2", obs2.fn)
	g.kickPresence()
	waitFor(t, func() bool {
		lvl, _, _, set := slot2.Snapshot()
		return set && lvl == subjectgate.LevelOnline
	}, "c2 must reach online before the c1 Remove")
	c2EdgesBefore := obs2.count()

	// REAL Remove on c1's subject (the fake resolver's answer is updated too — in
	// production the resolver IS the membership truth; here it stands in for it, but
	// the actual cascade + poke below are real, not hand-driven).
	res.set(principal, []Route{memberRoute("c2", h2, id2, clk.now())}, nil, nil)
	if err := h1.Remove(context.Background(), id1); err != nil {
		t.Fatalf("Remove: %v", err)
	}

	// c1 槽级联销毁: RemoveSubjectSlot ran — a lookup on the SAME id now misses.
	waitFor(t, func() bool {
		_, ok := h1.SubjectSlotFor(id1)
		return !ok
	}, "Remove must cascade RemoveSubjectSlot on c1's slot")

	// c2 分毫不动: no new edge, still online, coverage untouched.
	if got := obs2.count(); got != c2EdgesBefore {
		t.Fatalf("a c1 Remove must not touch c2's presence account: c2 edges %d→%d", c2EdgesBefore, got)
	}
	if lvl, _, _, set := slot2.Snapshot(); !set || lvl != subjectgate.LevelOnline {
		t.Fatalf("c2 slot testimony must survive a c1 Remove: set=%v lvl=%v", set, lvl)
	}
}

// TestMultiTabPresenceThreeEdges (DoD-3): same channel, two devices — any one断连 → 账
// 恒在线; 末出 → offline; 重连首入 → online (per-人 device account three edges). Driven
// through addDevice/removeDevice + one 圈 each.
func TestMultiTabPresenceThreeEdges(t *testing.T) {
	clk := newClock()
	res := newResolver()
	g := newTestGateway(t, Config{Resolver: res}, settings{clock: clk})

	const principal = "bob"
	h, id := openHome(t, channel.ID("c"), principal)
	res.set(principal, []Route{memberRoute("c", h, id, clk.now())}, nil, nil)
	slot, _ := h.SubjectSlotFor(id)
	obs := &slotObs{}
	slot.RegisterObserver("o", obs.fn)

	s1, _ := g.Attach(principal, nil)
	s2, _ := g.Attach(principal, nil)
	g.presenceReconcile()
	last := func() subjectgate.PresenceUpdate { es := obs.snapshot(); return es[len(es)-1] }
	if !last().Live || last().Level != subjectgate.LevelOnline {
		t.Fatalf("two devices → online, got %+v", last())
	}

	// One device drops: 账恒在线 (the other device holds the account online).
	s1.Close()
	g.presenceReconcile()
	if !last().Live || last().Level != subjectgate.LevelOnline {
		t.Fatalf("one device dropping must keep the account online, got %+v", last())
	}

	// Last device out: offline.
	s2.Close()
	g.presenceReconcile()
	if last().Level != subjectgate.LevelOffline {
		t.Fatalf("last device out → offline, got %+v", last())
	}

	// Reconnect 首入 → online again.
	s3, _ := g.Attach(principal, nil)
	defer s3.Close()
	g.presenceReconcile()
	if !last().Live || last().Level != subjectgate.LevelOnline {
		t.Fatalf("reconnect 首入 → online, got %+v", last())
	}
}

// TestPresenceConvergesFromAnyDirty (DoD-7④ 在场收敛性三则): manufacture three kinds of
// dirty account state, then ONE 圈 converges each to desired (reconcile consumes no
// events — it 整算 from current truth, so旧离线压新在线/删槽后写/漏边沿 all self-heal).
func TestPresenceConvergesFromAnyDirty(t *testing.T) {
	clk := newClock()
	res := newResolver()
	g := newTestGateway(t, Config{Resolver: res}, settings{clock: clk})

	const principal = "carol"
	h, id := openHome(t, channel.ID("c"), principal)
	res.set(principal, []Route{memberRoute("c", h, id, clk.now())}, nil, nil)
	slot, _ := h.SubjectSlotFor(id)
	s, _ := g.Attach(principal, nil)
	defer s.Close()
	key := covKey{principal: principal, channel: "c"}

	// (a) 漏边沿: device present, coverage empty, slot未发布 → 圈 publishes online.
	g.presenceReconcile()
	if lvl, _, _, set := slot.Snapshot(); !set || lvl != subjectgate.LevelOnline {
		t.Fatalf("(a) missed-edge dirty must converge to online, got set=%v lvl=%v", set, lvl)
	}

	// (b) 旧离线压新在线: force a stale offline onto the slot (as if an out-of-order
	// write landed), same epoch → next 圈 restores online.
	slot.PublishCurrent(g.epoch, subjectgate.LevelOffline)
	g.presenceReconcile()
	if lvl, _, _, set := slot.Snapshot(); !set || lvl != subjectgate.LevelOnline {
		t.Fatalf("(b) stale-offline-over-online must converge back to online, got set=%v lvl=%v", set, lvl)
	}

	// (c) 删槽后写入: point coverage at a DANGLING slot whose subject was removed from
	// the Home (SubjectSlotFor now misses) → 圈 sees the key not in desired → publishes
	// offline to the dangling slot and销账. Model: keep the device but drop the route so
	// desired excludes it; coverage still references the (now offline-bound) slot.
	res.set(principal, nil, nil, nil) // no eligibility → desired empty
	g.presenceReconcile()
	if _, ok := g.coverage[key]; ok {
		t.Fatal("(c) a key absent from desired must be销账 from coverage")
	}
	if lvl, _, _, set := slot.Snapshot(); !set || lvl != subjectgate.LevelOffline {
		t.Fatalf("(c) a dropped key must publish offline to its old slot, got set=%v lvl=%v", set, lvl)
	}
}

// TestPresenceTickConvergesWithoutPoke is the periodic half of DoD-6/7④: after the
// real presence loop has drained the attach poke and is waiting, a membership route is
// added without an OnMembershipChange wire. Only the injected PresenceTick alarm can
// wake the loop; advancing the clock proves that timer path publishes online.
func TestPresenceTickConvergesWithoutPoke(t *testing.T) {
	clk := newClock()
	res := newResolver()
	const tick = 7 * time.Second
	g := newTestGateway(t, Config{Resolver: res}, settings{clock: clk, presenceTick: tick})
	const principal = "presence-tick"
	res.set(principal, nil, nil, nil)
	s, _ := g.Attach(principal, nil)
	defer s.Close()
	g.Start()
	defer g.Close()

	// Initial circle + the already-buffered attach poke circle must both finish, leaving
	// no prompt edge that could falsely receive credit for the convergence below.
	waitFor(t, func() bool { return res.callCount() >= 2 && clk.armCount() >= 2 },
		"presence loop did not drain its initial attach poke")
	baselineCalls := res.callCount()

	h, id := openHome(t, channel.ID("late-presence"), principal) // no poke wire
	res.set(principal, []Route{memberRoute("late-presence", h, id, clk.now())}, nil, nil)
	slot, _ := h.SubjectSlotFor(id)
	clk.advance(tick)
	waitFor(t, func() bool {
		level, _, _, set := slot.Snapshot()
		return set && level == subjectgate.LevelOnline && res.callCount() > baselineCalls
	}, "PresenceTick did not drive the no-poke online convergence")
}

// TestPresenceRebindNewSlot (DoD-7⑧) changes the resolver's Bundle-scoped
// subject binding and verifies one reconcile withdraws the old testimony and
// publishes the new one.
func TestPresenceRebindNewSlot(t *testing.T) {
	clk := newClock()
	res := newResolver()
	g := newTestGateway(t, Config{Resolver: res}, settings{clock: clk})

	const principal = "dave"
	h1, id1 := openHomeWired(t, channel.ID("c"), principal, g)
	res.set(principal, []Route{memberRoute("c", h1, id1, clk.now())}, nil, nil)
	slotOld, _ := h1.SubjectSlotFor(id1)
	obsOld := &slotObs{}
	slotOld.RegisterObserver("old", obsOld.fn)

	s, _ := g.Attach(principal, nil)
	defer s.Close()
	g.Start()
	defer g.Close()
	waitFor(t, func() bool {
		lvl, _, _, set := slotOld.Snapshot()
		return set && lvl == subjectgate.LevelOnline
	}, "old slot never became online before rebind")

	id2 := actor.ActorID("gateway-rebound-subject")
	slotNew := h1.EnsureSubjectSlot(id2)
	if slotNew == slotOld {
		t.Fatal("a fresh admit must mint a distinct slot")
	}
	obsNew := &slotObs{}
	slotNew.RegisterObserver("new", obsNew.fn)
	res.set(principal, []Route{memberRoute("c", h1, id2, clk.now())}, nil, nil)
	// Issue the public timely hint used after a directory/membership commit.
	g.Poke(principal)
	waitFor(t, func() bool {
		lvl, _, _, set := slotNew.Snapshot()
		return set && lvl == subjectgate.LevelOnline
	}, "Remove→re-Admit poke did not converge presence to the new slot")
	// 撤旧: old slot offline.
	eo := obsOld.snapshot()
	if eo[len(eo)-1].Level != subjectgate.LevelOffline {
		t.Fatalf("换值 must撤 the old slot's online, got %+v", eo[len(eo)-1])
	}
	// 向新补: new slot online.
	if lvl, _, _, set := slotNew.Snapshot(); !set || lvl != subjectgate.LevelOnline {
		t.Fatalf("换值 must补 the new slot online, got set=%v lvl=%v", set, lvl)
	}
	// coverage is loop-private. Join the loop before white-box inspection so the
	// assertion preserves that production ownership rule under -race.
	g.presenceCancel()
	g.presenceWG.Wait()
	ce := g.coverage[covKey{principal: principal, channel: "c"}]
	if ce == nil || ce.slot != slotNew {
		t.Fatalf("coverage value must track the new slotRef")
	}
}

// TestBirthHandshakeInterleave (DoD-7⑦): the presence 圈's publish and a cell's
// RegisterObserver race arbitrarily — after both, the observer's account必达 desired
// (online), never stuck at a stale/missing value. RegisterObserver delivers the current
// value as its first callback UNDER the slot lock (spec §3.2 出生握手 原子化), so no
// interleaving loses the online edge. Run many iterations under -race.
func TestBirthHandshakeInterleave(t *testing.T) {
	clk := newClock()
	res := newResolver()
	g := newTestGateway(t, Config{Resolver: res}, settings{clock: clk})
	const principal = "erin"
	h, id := openHome(t, channel.ID("c"), principal)
	res.set(principal, []Route{memberRoute("c", h, id, clk.now())}, nil, nil)
	s, _ := g.Attach(principal, nil)
	defer s.Close()
	slot, _ := h.SubjectSlotFor(id)

	for iter := 0; iter < 200; iter++ {
		obs := &slotObs{}
		var wg sync.WaitGroup
		wg.Add(2)
		go func() { defer wg.Done(); g.presenceReconcile() }()
		go func() { defer wg.Done(); slot.RegisterObserver("cell", obs.fn) }()
		wg.Wait()
		// NO extra corrective 圈 here (六轮终审 P1-5: the original test ran ONE MORE
		// g.presenceReconcile() after the race, which would silently repair any edge the
		// interleave actually lost — masking exactly the bug this test exists to catch).
		// The出生握手原子化 guarantee (spec §3.2) is that RegisterObserver's FIRST callback,
		// delivered under the slot lock, already reflects whatever value is current at
		// the instant it registers — no matter how the two goroutines above interleaved —
		// so the assertion must hold on the observer's state immediately after wg.Wait(),
		// with no help from a follow-up round.
		es := obs.snapshot()
		if len(es) == 0 || !es[len(es)-1].Live || es[len(es)-1].Level != subjectgate.LevelOnline {
			t.Fatalf("iter %d: birth handshake lost the online edge (no corrective 圈 masking it), got %+v", iter, es)
		}
		slot.RemoveObserver("cell")
	}
}

// TestPresenceFailedChannelPreservesCoverage (P2-9, 六轮终审): a per-channel failure
// on a round where the WHOLE snapshot otherwise succeeds must not force
// an immediate false-offline — the same principle N2 already applies to a whole-
// snapshot failure, extended per-channel. Before the fix, presenceReconcile discarded
// the `failed` return value entirely, so a single transient per-channel query error
// looked identical to "confirmed gone" and published offline immediately.
func TestPresenceFailedChannelPreservesCoverage(t *testing.T) {
	clk := newClock()
	res := newResolver()
	g := newTestGateway(t, Config{Resolver: res}, settings{clock: clk})
	const principal = "yara"
	h, id := openHome(t, channel.ID("c"), principal)
	res.set(principal, []Route{memberRoute("c", h, id, clk.now())}, nil, nil)
	s, _ := g.Attach(principal, nil)
	defer s.Close()

	g.presenceReconcile()
	slot, _ := h.SubjectSlotFor(id)
	if lvl, _, _, set := slot.Snapshot(); !set || lvl != subjectgate.LevelOnline {
		t.Fatalf("channel must be online after the first 圈, got set=%v lvl=%v", set, lvl)
	}
	obs := &slotObs{}
	slot.RegisterObserver("o", obs.fn)
	edgesBefore := obs.count()

	// A per-channel failure THIS round (whole snapshot succeeds — routes=nil, err=nil —
	// but this channel is reported as a query failure, not confirmed absent).
	res.set(principal, nil, []channel.ID{"c"}, nil)
	g.presenceReconcile()

	if got := obs.count(); got != edgesBefore {
		t.Fatalf("a per-channel failure must NOT force an offline edge: edges %d→%d", edgesBefore, got)
	}
	if lvl, _, _, set := slot.Snapshot(); !set || lvl != subjectgate.LevelOnline {
		t.Fatalf("a per-channel failure must preserve existing online coverage, got set=%v lvl=%v", set, lvl)
	}
	if _, ok := g.coverage[covKey{principal: principal, channel: "c"}]; !ok {
		t.Fatal("a per-channel failure must not drop the coverage entry")
	}
}

// TestCloseCleanupNarrow (DoD-6 关站清账收窄形, spec §3.2 v0.7): Close ForgetEpochs ONLY
// the coverage's still-online slots at THIS gateway's epoch (→ unknown); a slot bearing
// a FOREIGN epoch's testimony is untouched (异代不动, CAS), and an offline残值 (a slot no
// longer in coverage) is NOT cleared — it is a生前目击 of离场, content恒真.
func TestCloseCleanupNarrow(t *testing.T) {
	clk := newClock()
	res := newResolver()
	g := newTestGateway(t, Config{Resolver: res}, settings{epoch: 100, clock: clk})

	const principal = "frank"
	h, id := openHome(t, channel.ID("c"), principal)
	res.set(principal, []Route{memberRoute("c", h, id, clk.now())}, nil, nil)
	s, _ := g.Attach(principal, nil)
	g.presenceReconcile() // this-epoch online coverage for (frank,c)
	onlineSlot, _ := h.SubjectSlotFor(id)

	// A foreign-epoch slot parked in coverage (异代): its testimony is at a HIGHER epoch,
	// so Close's ForgetEpoch(100) is a CAS no-op. (A real admitted slot, published at a
	// foreign epoch directly.)
	fid, _ := h.Admit(context.Background(), "human", "ghost")
	foreign, _ := h.SubjectSlotFor(fid)
	foreign.PublishCurrent(999, subjectgate.LevelOnline)
	g.coverage[covKey{principal: "ghost", channel: "c"}] = &covEntry{slot: foreign}

	// An offline残值 slot: it went offline and was销账 from coverage (not present at Close).
	rid, _ := h.Admit(context.Background(), "human", "residual")
	residual, _ := h.SubjectSlotFor(rid)
	residual.PublishCurrent(100, subjectgate.LevelOnline)
	residual.PublishCurrent(100, subjectgate.LevelOffline)

	_ = s
	if err := g.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// this-epoch online slot → forgotten (unknown).
	if _, _, _, set := onlineSlot.Snapshot(); set {
		t.Fatal("Close must ForgetEpoch this-epoch online coverage slots (→ unknown)")
	}
	// foreign-epoch slot → untouched (异代不动).
	if lvl, ep, _, set := foreign.Snapshot(); !set || ep != 999 || lvl != subjectgate.LevelOnline {
		t.Fatalf("Close must NOT touch a foreign-epoch slot (CAS): set=%v ep=%d lvl=%v", set, ep, lvl)
	}
	// offline残值 → NOT cleared (content恒真, advisory语义内合法).
	if lvl, _, _, set := residual.Snapshot(); !set || lvl != subjectgate.LevelOffline {
		t.Fatalf("Close must NOT clear an offline残值 (not in coverage): set=%v lvl=%v", set, lvl)
	}
}
