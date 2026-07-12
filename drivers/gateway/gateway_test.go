package gateway

import (
	"testing"

	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
)

// newSession is a bare session bound to a subject (nil slot — presence writes are
// skipped, the十字 bookkeeping is what these tests exercise; the slot-side presence
// dedup is covered by subjectgate slot_test).
func (g *Gateway) newBareSession(subj actor.ActorID) *Session {
	return &Session{gw: g, subjectID: subj, isMember: true}
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
	e := g.ensureEntry(nil, channel.ID("c"), subj, nil)
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

	entryA := g.ensureEntry(nil, channel.ID("c"), subj, nil)
	sA := g.newBareSession(subj)
	g.addDevice(entryA, sA)
	g.removeDevice(entryA, sA) // A retired (last device out)

	entryB := g.ensureEntry(nil, channel.ID("c"), subj, nil)
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

// TestGatewayCloseSealsArms: Close seals every live arm (关站序: gateway 先静默 before
// Home). After Close the arms are sealed so no session can drive the closing Home.
func TestGatewayCloseSealsArms(t *testing.T) {
	g := New(Config{})
	e := g.ensureEntry(nil, channel.ID("c"), actor.ActorID("user:carol"), nil)
	g.addDevice(e, g.newBareSession("user:carol"))
	if e.arm.isSealed() {
		t.Fatal("arm should be live before Close")
	}
	if err := g.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if !e.arm.isSealed() {
		t.Fatal("Close must seal every live arm (关站序)")
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
	e := g.ensureEntry(nil, channel.ID("c"), subj, nil)
	g.addDevice(e, g.newBareSession(subj))

	hub.Emit(channel.ID("c"), subj)
	if !e.arm.isSealed() {
		t.Fatal("a revocation emit must seal the subject's arm")
	}
	// An unknown subject is a harmless no-op.
	hub.Emit(channel.ID("c"), actor.ActorID("user:nobody"))
}
