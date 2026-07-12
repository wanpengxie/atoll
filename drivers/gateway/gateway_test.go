package gateway

import (
	"context"
	"sync"
	"testing"

	"github.com/wanpengxie/atoll/platform"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
)

// newSession is a bare session bound to a subject (nil slot — presence writes are
// skipped, the十字 bookkeeping is what these tests exercise; the slot-side presence
// dedup is covered by subjectgate slot_test).
func (g *Gateway) newBareSession(subj actor.ActorID) *Session {
	return &Session{gw: g, subjectID: subj, isMember: true}
}

// mustArm ensures the (subject, channel) arm and returns entry+arm (test helper; the
// gateway is never closed here so the error is always nil).
func (g *Gateway) mustArm(t *testing.T, chID channel.ID, subj actor.ActorID) (*userEntry, *channelArm) {
	t.Helper()
	e, a, err := g.ensureArm(nil, chID, subj, nil)
	if err != nil {
		t.Fatalf("ensureArm: %v", err)
	}
	return e, a
}

// TestUserEntryLifecycle: an entry is born on the first device and retired on the
// last (§5.6 出生=首设备 / 死=末设备出) — zero residual after (DoD-11 认证失败/退场
// 零残账 invariant on the会话账).
func TestUserEntryLifecycle(t *testing.T) {
	g := New(Config{})
	subj := actor.ActorID("user:alice")

	if _, ok := g.entryFor(subj); ok {
		t.Fatal("no entry should exist before any device (zero residual)")
	}
	e, _ := g.mustArm(t, channel.ID("c"), subj)
	s1 := g.newBareSession(subj)
	s2 := g.newBareSession(subj)
	g.addDevice(e, s1)
	g.addDevice(e, s2)
	if got, ok := g.entryFor(subj); !ok || len(got.devices) != 2 {
		t.Fatalf("entry should hold 2 devices, got ok=%v n=%d", ok, len(got.devices))
	}

	g.removeDevice(e, s1)
	if _, ok := g.entryFor(subj); !ok {
		t.Fatal("entry must survive while one device remains")
	}
	g.removeDevice(e, s2)
	if _, ok := g.entryFor(subj); ok {
		t.Fatal("entry must be retired (zero residual) on the last device out")
	}
}

// TestUserEntryStaleTeardownMissesSuccessor: 旧件晚删摘不掉新件 (DoD-11) — a teardown
// from a superseded entry can never touch the successor's account. Retire entry A,
// let entry B take the subject, then replay A's device teardown: B is untouched.
func TestUserEntryStaleTeardownMissesSuccessor(t *testing.T) {
	g := New(Config{})
	subj := actor.ActorID("user:bob")

	entryA, _ := g.mustArm(t, channel.ID("c"), subj)
	sA := g.newBareSession(subj)
	g.addDevice(entryA, sA)
	g.removeDevice(entryA, sA) // A retired (last device out)

	entryB, _ := g.mustArm(t, channel.ID("c"), subj)
	if entryB == entryA {
		t.Fatal("a re-attach after retirement must mint a NEW entry")
	}
	sB := g.newBareSession(subj)
	g.addDevice(entryB, sB)

	// Replay A's (already-gone) teardown: it must be a no-op against B.
	g.removeDevice(entryA, sA)
	got, ok := g.entryFor(subj)
	if !ok || got != entryB || len(got.devices) != 1 {
		t.Fatalf("stale teardown摘掉了新件: ok=%v isB=%v n=%d", ok, got == entryB, len(got.devices))
	}
}

// TestCrossChannelArmsIsolated (P0-1): one subject attached to two channels holds
// two DISTINCT arms under a single entry — a revocation on channel c1 seals ONLY
// c1's arm (仅此一臂死), never误杀 c2's (跨频道 arm 串线 fix). Presence stays per-
// identity: the device set aggregates across both arms.
func TestCrossChannelArmsIsolated(t *testing.T) {
	hub := NewRevocationHub()
	g := New(Config{Revocation: hub})
	if err := g.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	subj := actor.ActorID("user:multi")
	e1, arm1 := g.mustArm(t, channel.ID("c1"), subj)
	e2, arm2 := g.mustArm(t, channel.ID("c2"), subj)
	if e1 != e2 {
		t.Fatal("same subject must share ONE user entry across channels")
	}
	if arm1 == arm2 {
		t.Fatal("distinct channels must own DISTINCT arms (串线 fix)")
	}
	g.addDevice(e1, g.newBareSession(subj))
	g.addDevice(e1, g.newBareSession(subj))
	if got, _ := g.entryFor(subj); len(got.devices) != 2 || len(got.arms) != 2 {
		t.Fatalf("entry should hold 2 devices + 2 arms, got dev=%d arms=%d", len(got.devices), len(got.arms))
	}

	// Revoke channel c1 only.
	hub.Emit(channel.ID("c1"), subj)
	if !arm1.isSealed() {
		t.Fatal("revoking c1 must seal c1's arm")
	}
	if arm2.isSealed() {
		t.Fatal("revoking c1 must NOT seal c2's arm (误杀他臂)")
	}
	// c1's arm is dropped from the entry (a fresh attach rebinds); c2 survives.
	if got, _ := g.entryFor(subj); got.arms[channel.ID("c1")] != nil || got.arms[channel.ID("c2")] != arm2 {
		t.Fatal("revocation must drop only c1's arm from the entry")
	}
}

// TestGatewayCloseSealsArms: Close seals every live arm across all entries/channels
// (关站序: gateway 先静默 before Home). After Close the arms are sealed so no session
// can drive the closing Home.
func TestGatewayCloseSealsArms(t *testing.T) {
	g := New(Config{})
	e, arm := g.mustArm(t, channel.ID("c"), actor.ActorID("user:carol"))
	g.addDevice(e, g.newBareSession("user:carol"))
	if arm.isSealed() {
		t.Fatal("arm should be live before Close")
	}
	if err := g.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if !arm.isSealed() {
		t.Fatal("Close must seal every live arm (关站序)")
	}
}

// TestGatewayCloseRejectsAttach (P0-4 straddle): after Close, ensureArm (the
// gateway-lock inner half of Attach) refuses with ErrGatewayClosed — a
// still-arriving connection never mints a session that could touch a closing Home.
func TestGatewayCloseRejectsAttach(t *testing.T) {
	g := New(Config{})
	if err := g.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if _, _, err := g.ensureArm(nil, channel.ID("c"), actor.ActorID("user:late"), nil); err != ErrGatewayClosed {
		t.Fatalf("ensureArm after Close want ErrGatewayClosed, got %v", err)
	}
}

// TestGatewayCloseAttachStraddle (P0-4): Close and concurrent ensureArm race under
// the same lock — every ensureArm either succeeds fully (before closed) or refuses
// with ErrGatewayClosed (after), and once Close has returned every subsequent
// ensureArm refuses. No panic, no half-open entry.
func TestGatewayCloseAttachStraddle(t *testing.T) {
	for iter := 0; iter < 50; iter++ {
		g := New(Config{})
		var wg sync.WaitGroup
		for i := 0; i < 8; i++ {
			wg.Add(1)
			go func(n int) {
				defer wg.Done()
				subj := actor.ActorID(actor.ActorID(rune('a' + n)))
				_, _, err := g.ensureArm(nil, channel.ID("c"), subj, nil)
				if err != nil && err != ErrGatewayClosed {
					t.Errorf("unexpected ensureArm err: %v", err)
				}
			}(i)
		}
		wg.Add(1)
		go func() { defer wg.Done(); _ = g.Close() }()
		wg.Wait()
		// After Close has returned, every ensureArm must refuse.
		if _, _, err := g.ensureArm(nil, channel.ID("c"), actor.ActorID("z"), nil); err != ErrGatewayClosed {
			t.Fatalf("post-Close ensureArm want ErrGatewayClosed, got %v", err)
		}
	}
}

// TestGatewayCloseOrder (DoD-9 gateway全序断言): 关站期帧得 closed 不 panic. After
// Close has sealed the arms, a business frame driven upstream through a member
// session bound to that arm must return a sealed (stale_binding) error frame —
// gracefully closed, never a panic — because the共享世代 admission gate refuses
// every business frame on a sealed arm (gateway 先静默 before Home).
func TestGatewayCloseOrder(t *testing.T) {
	g := New(Config{})
	subj := actor.ActorID("user:erin")
	e, arm := g.mustArm(t, channel.ID("c"), subj)
	// A member session bound to this subject's arm (the arm is what Close seals).
	s := &Session{gw: g, subjectID: subj, isMember: true, arm: arm}
	g.addDevice(e, s)

	if err := g.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Drive a business frame after the关站: it must return a sealed error, not panic.
	f, _ := platform.NewFrame(platform.FrameSubmit, s.BindingGen(), "ref-late", platform.SubmitPayload{
		MsgType: "human.message", Audience: []string{"tool:kimi"}, Payload: []byte(`{}`),
	})
	got := s.Upstream(context.Background(), f)
	if got.Type != platform.FrameError {
		t.Fatalf("frame after Close should be an error frame, got %q", got.Type)
	}
	var p platform.ErrorPayload
	if err := got.DecodePayload(&p); err != nil {
		t.Fatalf("decode error payload: %v", err)
	}
	if p.Code != platform.CodeStaleBinding {
		t.Fatalf("frame after Close should be stale_binding (sealed), got %q", p.Code)
	}
}

// TestRevocationSealsArm: a revocation (membership/ACL emit → RevocationSource)
// seals the subject's arm (DoD-5 两触发源 both land here via the hub).
func TestRevocationSealsArm(t *testing.T) {
	hub := NewRevocationHub()
	g := New(Config{Revocation: hub})
	if err := g.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	subj := actor.ActorID("user:dave")
	e, arm := g.mustArm(t, channel.ID("c"), subj)
	g.addDevice(e, g.newBareSession(subj))

	hub.Emit(channel.ID("c"), subj)
	if !arm.isSealed() {
		t.Fatal("a revocation emit must seal the subject's arm")
	}
	// An unknown subject is a harmless no-op.
	hub.Emit(channel.ID("c"), actor.ActorID("user:nobody"))
}

// TestUpstreamStaleGenRefused (P0-2): a business frame carrying a STALE binding_gen
// (an earlier binding, after a rebind advanced the arm世代) is refused stale_binding
// by the共享世代 admission gate — a真投 stale-世代帧, not a counter assertion.
func TestUpstreamStaleGenRefused(t *testing.T) {
	g := New(Config{})
	subj := actor.ActorID("user:frank")
	e, arm := g.mustArm(t, channel.ID("c"), subj)
	s := &Session{gw: g, subjectID: subj, isMember: true, arm: arm}
	g.addDevice(e, s)

	g1 := arm.nextGen() // first binding
	g2 := arm.nextGen() // rebind advances the世代
	if g1 == g2 {
		t.Fatal("rebind must advance the binding generation")
	}
	// A frame stamped with the STALE g1 while the arm is at g2.
	stale, _ := platform.NewFrame(platform.FrameSubmit, g1, "ref-stale", platform.SubmitPayload{
		MsgType: "human.message", Audience: []string{"tool:kimi"}, Payload: []byte(`{}`),
	})
	got := s.Upstream(context.Background(), stale)
	var p platform.ErrorPayload
	if got.Type != platform.FrameError || got.DecodePayload(&p) != nil || p.Code != platform.CodeStaleBinding {
		t.Fatalf("stale-gen frame must be refused stale_binding, got type=%q code=%q", got.Type, p.Code)
	}
	// The CURRENT gen g2 passes the世代 gate (the Session would then reach the slot);
	// the stale g1 does not. Asserted at the gate to avoid driving a nil test slot.
	if ok, _ := arm.admitUpstream(g2); !ok {
		t.Fatal("current-gen frame must pass the世代 gate")
	}
}

// TestDetachSealRefusesCoArmDevice (P0-3): detach seals the shared (subject,channel)
// arm; a co-arm device's subsequent business frame is refused stale_binding (同臂
// 其他设备 seal 后 Upstream 必拒). Detaching produces NO receipt frame (empty).
func TestDetachSealRefusesCoArmDevice(t *testing.T) {
	g := New(Config{})
	subj := actor.ActorID("user:grace")
	e, arm := g.mustArm(t, channel.ID("c"), subj)
	arm.nextGen()
	// Two devices sharing the one arm.
	sA := &Session{gw: g, subjectID: subj, chID: channel.ID("c"), isMember: true, arm: arm, lane: newLane(newCursor(nil))}
	sB := &Session{gw: g, subjectID: subj, chID: channel.ID("c"), isMember: true, arm: arm, lane: newLane(newCursor(nil))}
	sA.ctx, sA.cancel = context.WithCancel(arm.context())
	sB.ctx, sB.cancel = context.WithCancel(arm.context())
	g.addDevice(e, sA)
	g.addDevice(e, sB)

	// Device A detaches → seals the shared arm, drops it, returns an empty frame.
	detach, _ := platform.NewFrame(platform.FrameDetach, arm.currentGen(), "ref-detach", platform.DetachPayload{ChannelID: "c"})
	resp := sA.Upstream(context.Background(), detach)
	if resp.Type != "" {
		t.Fatalf("detach must return NO frame (表② '—'), got type %q", resp.Type)
	}
	if !arm.isSealed() {
		t.Fatal("detach must seal the shared arm")
	}
	// Co-arm device B's business frame is now refused stale_binding (sealed).
	f, _ := platform.NewFrame(platform.FrameSubmit, arm.currentGen(), "ref-b", platform.SubmitPayload{
		MsgType: "human.message", Audience: []string{"tool:kimi"}, Payload: []byte(`{}`),
	})
	got := sB.Upstream(context.Background(), f)
	var p platform.ErrorPayload
	if got.Type != platform.FrameError || got.DecodePayload(&p) != nil || p.Code != platform.CodeStaleBinding {
		t.Fatalf("co-arm device after seal must get stale_binding, got type=%q code=%q", got.Type, p.Code)
	}
}
