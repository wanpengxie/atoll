package home

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/lib/introspect"
	"github.com/wanpengxie/atoll/platform/internal/presence"
	"github.com/wanpengxie/atoll/protocol/actor"
	channelpkg "github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/runtime/actorrt"
	"github.com/wanpengxie/atoll/runtime/harness"
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
	h, err := Open(Config{CompositionResolver: emptyCompositionResolver{},
		ChannelID: channelpkg.ID("test-review-fixes"),
		DBPath:    filepath.Join(t.TempDir(), "home.sqlite"), Bootstrap: true,
		IntroductionResolver: fixedIntroductionResolver{kind: actor.KindAgent},
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = h.closeInternal("test") })
	return h
}

func TestChannelOwnerGenesisIdempotencyAndProtection(t *testing.T) {
	t.Run("neutral principal cannot be upgraded", func(t *testing.T) {
		h := openWhiteboxHome(t)
		neutral, err := admitThroughSysOp(h, context.Background(), actor.KindHuman, "same-principal")
		if err != nil {
			t.Fatal(err)
		}
		if _, err := h.admitChannelOwner(context.Background(), "same-principal"); err == nil {
			t.Fatal("owner admission accepted a pre-existing neutral row")
		}
		row, ok, err := h.controlIndex.LookupActive(context.Background(), neutral)
		if err != nil || !ok || row.Role != storespec.RoleNone {
			t.Fatalf("neutral row changed = (%+v,%v,%v)", row, ok, err)
		}
	})

	t.Run("owner retry and ordinary admit preserve owner", func(t *testing.T) {
		h := openWhiteboxHome(t)
		owner, err := h.admitChannelOwner(context.Background(), "owner-principal")
		if err != nil {
			t.Fatal(err)
		}
		retry, err := h.admitChannelOwner(context.Background(), "owner-principal")
		if err != nil || retry != owner {
			t.Fatalf("owner retry = (%q,%v)", retry, err)
		}
		ordinary, err := admitThroughSysOp(h, context.Background(), actor.KindHuman, "owner-principal")
		if err != nil || ordinary != owner {
			t.Fatalf("ordinary retry = (%q,%v)", ordinary, err)
		}
		row, _, _ := h.controlIndex.LookupActive(context.Background(), owner)
		if row.Role != storespec.RoleOwner {
			t.Fatalf("ordinary retry downgraded role to %q", row.Role)
		}
		err = removeThroughSysOp(h, context.Background(), owner)
		var opErr *channelpkg.OperationError
		if !errors.As(err, &opErr) || opErr.Code != channelpkg.ErrCodeProtectedActor {
			t.Fatalf("Remove owner err=%v, want protected_actor", err)
		}
		if _, ok, _ := h.controlIndex.LookupActive(context.Background(), owner); !ok {
			t.Fatal("protected owner disappeared")
		}
	})
}

func TestAdmitIsHumanOnlyAndIdempotentlyPublishesAuthority(t *testing.T) {
	h := openWhiteboxHome(t)
	ctx := context.Background()
	principal := "rev"
	id, err := admitThroughSysOp(h, ctx, actor.KindHuman, principal)
	if err != nil {
		t.Fatalf("Admit genesis: %v", err)
	}
	reAdmitted, err := admitThroughSysOp(h, ctx, actor.KindHuman, principal)
	if err != nil {
		t.Fatalf("re-Admit: %v", err)
	}
	if reAdmitted != id {
		t.Fatalf("idempotent re-Admit id=%q want %q", reAdmitted, id)
	}
	rec, ok, err := h.cs.Authority.LookupActive(ctx, id)
	if err != nil || !ok {
		t.Fatalf("Lookup after re-Admit: ok=%v err=%v", ok, err)
	}
	if rec.Class != "human" || rec.CurrentDeclVersion != 1 || rec.Placement != storespec.NewServerPlacement() {
		t.Fatalf("authority row = %+v", rec)
	}
}

func TestRealPensFenceAppliedAndEndedDeclaredAndRunIdentities(t *testing.T) {
	h := openWhiteboxHome(t)
	ctx := context.Background()
	declared, err := h.declare(ctx, DeclareRequest{
		SourceDeclID: "source:pen-gate", Kind: actor.KindAgent,
		Class: "pen-gate", Placement: storespec.NewServerPlacement(), TIdle: int64((time.Hour) / time.Millisecond),
		CreatedAt: time.Now().UnixMilli(),
	})
	if err != nil {
		t.Fatal(err)
	}
	writeEvent := func(pen harness.Pen, id string) harness.WriteResult {
		t.Helper()
		now := time.Now().UnixMilli()
		res, writeErr := pen.Write(ctx, &message.Envelope{
			ID: message.ID(id), Kind: message.KindEvent, Type: "gate.probe",
			Audience: message.Audience{actor.SystemActorID}, Visibility: message.VisibilitySystem,
			TS: now, TSReceived: now,
		})
		if writeErr != nil {
			t.Fatalf("write %s: %v", id, writeErr)
		}
		return res
	}

	v1 := h.minter.Mint(declared.Row.ID, declared.Row.Kind, h.channelID, 1)
	if res := writeEvent(v1, "declared-v1-before-apply"); !res.Accepted() {
		t.Fatalf("v1 before apply rejected: %+v", res)
	}
	configV2 := json.RawMessage(`{"version":2}`)
	updated, err := h.declare(ctx, DeclareRequest{
		SourceDeclID: declared.Row.SourceDeclID, Kind: declared.Row.Kind, Class: declared.Row.Class,
		Config: &configV2, Placement: declared.Row.Placement, TIdle: declared.Row.TIdle.Milliseconds(),
		CreatedAt: time.Now().UnixMilli(),
	})
	if err != nil || !updated.ConfigUpdated || updated.Row.CurrentDeclVersion != 2 {
		t.Fatalf("atomic declaration sync = %+v err=%v", updated, err)
	}
	if res := writeEvent(v1, "declared-v1-after-apply"); res.RejectReason != harness.HarnessAuthorVersionStale {
		t.Fatalf("old declared pen after apply=%+v", res)
	}
	v2 := h.minter.Mint(declared.Row.ID, declared.Row.Kind, h.channelID, updated.Row.CurrentDeclVersion)
	if res := writeEvent(v2, "declared-v2-before-end"); !res.Accepted() {
		t.Fatalf("current declared pen rejected: %+v", res)
	}
	if err := removeThroughSysOp(h, ctx, declared.Row.ID); err != nil {
		t.Fatal(err)
	}
	if res := writeEvent(v2, "declared-v2-after-end"); res.RejectReason != harness.HarnessAuthorNotMember {
		t.Fatalf("ended declared pen=%+v", res)
	}

	parent, err := admitThroughSysOp(h, ctx, actor.KindHuman, "pen-gate-parent")
	if err != nil {
		t.Fatal(err)
	}
	child, err := h.forkAdmission(ctx, parent, 1, actorrt.ForkSpec{
		Kind: actor.KindAgent, Class: "pen-gate-child",
	}, "pen-gate-child")
	if err != nil {
		t.Fatal(err)
	}
	runPen := h.minter.Mint(child, actor.KindAgent, h.channelID, 1)
	if res := writeEvent(runPen, "run-v1-before-end"); !res.Accepted() {
		t.Fatalf("run pen rejected: %+v", res)
	}
	if err := (lifecycleEndHandle{home: h, author: storespec.AuthorStamp{ID: parent, BirthVersion: 1}}).End(ctx, child, "pen-gate-end"); err != nil {
		t.Fatal(err)
	}
	if res := writeEvent(runPen, "run-v1-after-end"); res.RejectReason != harness.HarnessAuthorNotMember {
		t.Fatalf("ended run pen=%+v", res)
	}

	// Unrelated declaration churn must never stale the system anchor's welded v1.
	if res := writeEvent(h.systemPen, "system-v1-after-declaration-churn"); !res.Accepted() {
		t.Fatalf("system pen lost v1 authority: %+v", res)
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

func TestRemoveSnapshotAndReadmitHaveNoPriorTestimony(t *testing.T) {
	h := openWhiteboxHome(t)
	ctx := context.Background()
	now := time.Unix(100, 0)
	installControlledPresenceFold(h, &now)
	id, err := admitThroughSysOp(h, ctx, actor.KindHuman, "presence-life")
	if err != nil {
		t.Fatal(err)
	}
	h.presenceFold.OnObs(ctx, id, actorrt.Incarnation{}, actorrt.ObsKind(introspect.ObsDevicePresence), introspect.MarshalDevicePresence(true))
	if err := removeThroughSysOp(h, ctx, id); err != nil {
		t.Fatal(err)
	}
	snapshot, err := h.View().Snapshot(ctx, id)
	if err != nil || snapshot.Member || snapshot.L1Present || len(snapshot.L3) != 0 {
		t.Fatalf("snapshot after Remove = %+v err=%v", snapshot, err)
	}
	readmittedID, err := admitThroughSysOp(h, ctx, actor.KindHuman, "presence-life")
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
