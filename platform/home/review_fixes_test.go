package home

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/lib/introspect"
	"github.com/wanpengxie/atoll/platform/internal/presence"
	"github.com/wanpengxie/atoll/protocol/actor"
	channelpkg "github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/runtime/actorrt"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

// (债②已落：期12 humancell 重建后，human-door 机器的测试住
// platform/humandoor_test.go——跨 incarnation/冻结环/presence straddle/
// resource face 真集成；旧 humanCaller 形随 behavior.Caller 拆删不再重建。)

// openWhiteboxHome assembles a Home reachable白盒 (this file is package platform),
// so a test can stamp Host directly via cs.Membership and read cs.Registry — the
// placement facts no public verb exposes.
func openWhiteboxHome(t *testing.T) *Home {
	t.Helper()
	h, err := Open(Config{CompositionResolver: emptyCompositionResolver{}, DaemonAuthority: allowTestDaemonAuthority{},
		ChannelID: channelpkg.ID("test-review-fixes"),
		DBPath:    filepath.Join(t.TempDir(), "home.sqlite"),
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = h.Close() })
	return h
}

// TestAdmit_ReAdmitPreservesHost pins the Admit no-op fix (#3): a re-Admit of an
// ALREADY-active member (an idempotent introduce retry) must NOT reset a
// daemon-stamped Host back to "" — placement authority is the attach/plan path's,
// never Admit's. Before the fix, Admit unconditionally applied a Host="" row, and
// applyMemberAddTx's host-diff UPDATE clobbered the live placement.
func TestAdmit_ReAdmitPreservesHost(t *testing.T) {
	h := openWhiteboxHome(t)
	ctx := context.Background()
	principal := "rev"

	// Genesis Admit → active row, Host="".
	id, err := h.Admit(ctx, actor.KindAgent, principal)
	if err != nil {
		t.Fatalf("Admit genesis: %v", err)
	}
	// Stamp a daemon Host (what an attach does): active row + host-diff → UPDATE host.
	if err := h.cs.Membership.ApplyMemberTransitions(ctx, []storespec.MemberActorAdd{{
		ID: id, Kind: actor.KindAgent, Host: "daemon-1", At: h.nowMs(),
	}}, nil); err != nil {
		t.Fatalf("stamp host: %v", err)
	}
	if rec, ok, _ := h.cs.Registry.Lookup(ctx, id); !ok || rec.Host != "daemon-1" {
		t.Fatalf("precondition: Host not stamped, rec=%+v ok=%v", rec, ok)
	}

	// Idempotent re-Admit — must be a pure no-op, Host untouched.
	reAdmitted, err := h.Admit(ctx, actor.KindAgent, principal)
	if err != nil {
		t.Fatalf("re-Admit: %v", err)
	}
	if reAdmitted != id {
		t.Fatalf("idempotent re-Admit id=%q want %q", reAdmitted, id)
	}
	rec, ok, err := h.cs.Registry.Lookup(ctx, id)
	if err != nil || !ok {
		t.Fatalf("Lookup after re-Admit: ok=%v err=%v", ok, err)
	}
	if rec.Host != "daemon-1" {
		t.Fatalf("re-Admit clobbered Host to %q, want daemon-1 preserved (#3)", rec.Host)
	}
}

func installControlledPresenceFold(h *Home, now *time.Time) {
	h.reconcileStop()
	<-h.reconcileDone
	h.nowMs = func() int64 { return now.UnixMilli() }
	h.presenceFold = presence.New(nil, func() time.Time { return *now },
		[]actorrt.ObsKind{actorrt.ObsKind(introspect.ObsDevicePresence)}, time.Second)
}

func TestNoFactoryWarningStateIsPerHome(t *testing.T) {
	const id actor.ActorID = "agent:missing-factory"
	var first, second Home
	if !first.firstNoFactoryWarning(id) || first.firstNoFactoryWarning(id) {
		t.Fatal("first Home did not preserve one warning per continuous edge")
	}
	if !second.firstNoFactoryWarning(id) {
		t.Fatal("second Home inherited another Home's no-factory edge state")
	}
	first.clearNoFactoryWarned(id)
	if !first.firstNoFactoryWarning(id) {
		t.Fatal("cleared no-factory edge did not become warnable again")
	}
}

// TestPresenceSweep_ClearsBypassDeregOrphan covers the reconciliation backstop
// used after declaration reconciliation deregisters membership without calling Home.Remove.
func TestPresenceSweep_ClearsBypassDeregOrphan(t *testing.T) {
	h := openWhiteboxHome(t)
	ctx := context.Background()
	now := time.Unix(100, 0)
	installControlledPresenceFold(h, &now)
	id, err := h.Admit(ctx, actor.KindTool, "sweep-orphan")
	if err != nil {
		t.Fatal(err)
	}
	h.presenceFold.OnObs(ctx, id, actorrt.Incarnation{}, actorrt.ObsKind(introspect.ObsDevicePresence), introspect.MarshalDevicePresence(true))
	if got := h.PresenceSweptCount(); got != 0 {
		t.Fatalf("initial swept count = %d", got)
	}
	if err := h.cs.Membership.ApplyMemberTransitions(ctx, nil, []storespec.MemberActorRemove{{ID: id, At: now.UnixMilli()}}); err != nil {
		t.Fatalf("bypass dereg: %v", err)
	}
	now = now.Add(2 * time.Second)
	h.sweepPresence(ctx)
	if got := h.PresenceSweptCount(); got == 0 {
		t.Fatal("successful sweep did not expose a non-zero clear count")
	}
	snapshot, err := h.View().Snapshot(ctx, id)
	if err != nil || len(snapshot.L3) != 0 {
		t.Fatalf("orphan after sweep: snapshot=%+v err=%v", snapshot, err)
	}
}

func TestRemoveSnapshotAndReadmitHaveNoPriorTestimony(t *testing.T) {
	h := openWhiteboxHome(t)
	ctx := context.Background()
	now := time.Unix(100, 0)
	installControlledPresenceFold(h, &now)
	id, err := h.Admit(ctx, actor.KindAgent, "presence-life")
	if err != nil {
		t.Fatal(err)
	}
	h.presenceFold.OnObs(ctx, id, actorrt.Incarnation{}, actorrt.ObsKind(introspect.ObsDevicePresence), introspect.MarshalDevicePresence(true))
	if err := h.Remove(ctx, id); err != nil {
		t.Fatal(err)
	}
	snapshot, err := h.View().Snapshot(ctx, id)
	if err != nil || snapshot.Member || snapshot.L1Present || len(snapshot.L3) != 0 {
		t.Fatalf("snapshot after Remove = %+v err=%v", snapshot, err)
	}
	readmittedID, err := h.Admit(ctx, actor.KindAgent, "presence-life")
	if err != nil {
		t.Fatalf("re-Admit: %v", err)
	}
	// Deregistration is irreversible: a same-principal re-Admit must mint a
	// fresh identity, never resurrect the removed id.
	if readmittedID == id {
		t.Fatalf("re-Admit reused removed id %q", id)
	}
	snapshot, err = h.View().Snapshot(ctx, readmittedID)
	if err != nil || !snapshot.Member || snapshot.L1Present || len(snapshot.L3) != 0 {
		t.Fatalf("snapshot after same-principal re-Admit = %+v err=%v", snapshot, err)
	}
}

type listFailRegistry struct {
	storespec.Registry
}

func (listFailRegistry) ListActive(context.Context) ([]storespec.Record, error) {
	return nil, errors.New("forced ListActive failure")
}

func TestSweepPresence_ListActiveFailureSkipsWholePass(t *testing.T) {
	h := openWhiteboxHome(t)
	ctx := context.Background()
	now := time.Unix(100, 0)
	installControlledPresenceFold(h, &now)
	id, err := h.Admit(ctx, actor.KindAgent, "list-fail-orphan")
	if err != nil {
		t.Fatal(err)
	}
	h.presenceFold.OnObs(ctx, id, actorrt.Incarnation{}, actorrt.ObsKind(introspect.ObsDevicePresence), introspect.MarshalDevicePresence(true))
	now = now.Add(2 * time.Second)
	original := h.cs.Registry
	h.cs.Registry = listFailRegistry{Registry: original}
	h.sweepPresence(ctx)
	if got := h.PresenceSweptCount(); got != 0 {
		t.Fatalf("failed pass changed swept count to %d", got)
	}
	h.cs.Registry = original
	snapshot, err := h.View().Snapshot(ctx, id)
	if err != nil || len(snapshot.L3) != 1 {
		t.Fatalf("failed pass did not preserve testimony: snapshot=%+v err=%v", snapshot, err)
	}
}

func TestViewTestimonyAgeMsUsesHomeClockAndClampsNegative(t *testing.T) {
	h := openWhiteboxHome(t)
	now := time.UnixMilli(1_250)
	h.nowMs = func() int64 { return now.UnixMilli() }
	if got := h.View().TestimonyAgeMs(1_000); got != 250 {
		t.Fatalf("age = %d, want 250", got)
	}
	if got := h.View().TestimonyAgeMs(2_000); got != 0 {
		t.Fatalf("future receipt age = %d, want 0", got)
	}
}
