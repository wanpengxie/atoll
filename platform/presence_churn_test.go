package platform

import (
	"testing"

	"github.com/wanpengxie/atoll/lib/introspect"
	"github.com/wanpengxie/atoll/platform/subjectgate"
)

// presence_churn_test.go is the platform-layer presence CHURN family (DoD-4): it
// drives the REAL cell self-report policy (wirePresenceSelfReport + publishPresence)
// against a REAL slot across incarnation churn, epoch失效 and layer-2 rebind. The
// slot-mechanic invariants (edgeSeq dedup, new-epoch revoke-then-snapshot,
// independence, observer-pointer removal, Forget-folds-unknown) are ALSO pinned at
// the slot layer in platform/subjectgate/slot_test.go; this family asserts
// they hold as the human cell OBSERVES them — i.e. what the person's device actually
// self-reports. The 真路径 online-after-attach (real gateway + ws + cell) is the e2e
// TestGatewayFrames; 多设备首入末出 is the gateway TestUserEntry* family.

// obsLevels decodes a fakeSys's captured device-presence obs into online/offline
// booleans (one per PublishObs).
func obsLevels(t *testing.T, fs *fakeSys) []bool {
	t.Helper()
	out := make([]bool, 0, len(fs.obs))
	for _, raw := range fs.obs {
		p, ok := introspect.ParseDevicePresence(raw)
		if !ok {
			t.Fatalf("undecodable device-presence obs: %q", raw)
		}
		out = append(out, p.Online)
	}
	return out
}

// TestPresenceChurnSnapshotSelfReportOnIncarnation: the slot outlives incarnations,
// so a FRESH cell born after a level was published reads the snapshot at Start and
// self-reports it (换代槽快照自报) — no new edge required.
func TestPresenceChurnSnapshotSelfReportOnIncarnation(t *testing.T) {
	slot := subjectgate.NewRegistry().EnsureSlot("human:alice")

	// incarnation 1 wires and observes an online edge.
	fs1 := &fakeSys{}
	tok1 := wirePresenceSelfReport(fs1, slot)
	slot.PublishLevel(1, 1, subjectgate.LevelOnline)
	if got := obsLevels(t, fs1); len(got) != 1 || !got[0] {
		t.Fatalf("incarnation 1 obs = %v, want [online]", got)
	}
	// incarnation 1 dies (its observer摘除 by ITS token).
	slot.RemoveObserver(tok1)

	// incarnation 2 is born AFTER the online was published: it must read the
	// snapshot at Start and self-report online with zero new edges.
	fs2 := &fakeSys{}
	_ = wirePresenceSelfReport(fs2, slot)
	if got := obsLevels(t, fs2); len(got) != 1 || !got[0] {
		t.Fatalf("incarnation 2 snapshot self-report = %v, want [online]", got)
	}
}

// TestPresenceChurnOldObserverCannotUnmountNew: a stale cell's RemoveObserver(oldToken)
// can never摘 a newer incarnation's registration (旧观察者摘不掉新登记) — the tokens
// differ, so a late teardown of the dead cell leaves the live one observing.
func TestPresenceChurnOldObserverCannotUnmountNew(t *testing.T) {
	slot := subjectgate.NewRegistry().EnsureSlot("human:alice")
	fs1 := &fakeSys{}
	tok1 := wirePresenceSelfReport(fs1, slot)
	fs2 := &fakeSys{}
	_ = wirePresenceSelfReport(fs2, slot)

	// The stale incarnation tears its own observer down by its own token.
	slot.RemoveObserver(tok1)

	// A fresh edge must still reach incarnation 2.
	slot.PublishLevel(1, 1, subjectgate.LevelOnline)
	if got := obsLevels(t, fs2); len(got) != 1 || !got[0] {
		t.Fatalf("live incarnation obs = %v, want [online] (stale RemoveObserver must not have unmounted it)", got)
	}
	// And it must NOT reach the dead one.
	if got := obsLevels(t, fs1); len(got) != 0 {
		t.Fatalf("dead incarnation obs = %v, want [] (its own token unmounted it)", got)
	}
}

// TestPresenceChurnForgetNotSelfRetracted: a revocation (Forget on gateway Close /
// epoch teardown) folds the slot to unknown but the cell does NOT self-retract via
// PublishObs — the证词账清洁 is the容器 owner's, not the cell's (gateway Close→Forget→
// unknown 无行, only positive edges self-reported).
func TestPresenceChurnForgetNotSelfRetracted(t *testing.T) {
	slot := subjectgate.NewRegistry().EnsureSlot("human:alice")
	fs := &fakeSys{}
	_ = wirePresenceSelfReport(fs, slot)
	slot.PublishLevel(1, 1, subjectgate.LevelOnline)

	before := len(fs.obs)
	slot.Forget() // revocation (Live=false) — the cell must not self-report anything.
	if after := len(fs.obs); after != before {
		t.Fatalf("cell self-reported on Forget: obs grew %d→%d (revocation is the owner's account, not a self-retract)", before, after)
	}
	if got := obsLevels(t, fs); len(got) != 1 || !got[0] {
		t.Fatalf("obs = %v, want exactly [online] (the earlier positive edge, unretracted)", got)
	}
	// Snapshot folded back to unknown (无行) — a fresh incarnation says nothing.
	if _, _, _, present := slot.Snapshot(); present {
		t.Fatal("slot still present after Forget — must fold to unknown")
	}
	fresh := &fakeSys{}
	_ = wirePresenceSelfReport(fresh, slot)
	if got := obsLevels(t, fresh); len(got) != 0 {
		t.Fatalf("post-Forget incarnation obs = %v, want [] (unknown 诚实默认)", got)
	}
}

// TestPresenceChurnPureRebindNoPresence: a pure layer-2 rebind (SetBinding) produces
// ZERO presence self-report — the独立性不变式 (level's only writer is PublishLevel,
// never co-written with a rebind; build spec §2 pair B×E) as the cell observes it.
func TestPresenceChurnPureRebindNoPresence(t *testing.T) {
	slot := subjectgate.NewRegistry().EnsureSlot("human:alice")
	fs := &fakeSys{}
	_ = wirePresenceSelfReport(fs, slot)
	slot.SetBinding(2)
	slot.SetBinding(3)
	if got := obsLevels(t, fs); len(got) != 0 {
		t.Fatalf("pure rebind produced presence obs %v, want [] (level绝不随层2联写)", got)
	}
}

// TestPresenceChurnEdgeSeqAndEpoch: as the cell observes them — a same-epoch
// duplicate/reorder is dropped (no self-report), and a new (greater) epoch revokes
// the old testimony (Live=false, NOT self-reported) then delivers the new level
// (self-reported).
func TestPresenceChurnEdgeSeqAndEpoch(t *testing.T) {
	slot := subjectgate.NewRegistry().EnsureSlot("human:alice")
	fs := &fakeSys{}
	_ = wirePresenceSelfReport(fs, slot)

	slot.PublishLevel(1, 2, subjectgate.LevelOnline) // online
	slot.PublishLevel(1, 2, subjectgate.LevelOffline) // same epoch, edgeSeq not increasing → dropped
	slot.PublishLevel(1, 1, subjectgate.LevelOffline) // same epoch, lower edgeSeq → dropped
	if got := obsLevels(t, fs); len(got) != 1 || !got[0] {
		t.Fatalf("after dup/reorder obs = %v, want [online] (dedup)", got)
	}

	// New epoch offline: the observer sees revoke(old, Live=false, skipped) then
	// deliver(offline, Live=true, self-reported).
	slot.PublishLevel(2, 1, subjectgate.LevelOffline)
	got := obsLevels(t, fs)
	if len(got) != 2 || !got[0] || got[1] {
		t.Fatalf("after new-epoch offline obs = %v, want [online, offline] (revoke not self-reported, new level is)", got)
	}
}
