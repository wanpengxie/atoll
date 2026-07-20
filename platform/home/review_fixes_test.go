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
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = h.Close() })
	return h
}

func TestChannelOwnerGenesisIdempotencyAndProtection(t *testing.T) {
	t.Run("neutral principal cannot be upgraded", func(t *testing.T) {
		h := openWhiteboxHome(t)
		neutral, err := h.Admit(context.Background(), actor.KindHuman, "same-principal")
		if err != nil {
			t.Fatal(err)
		}
		if _, err := h.AdmitChannelOwner(context.Background(), "same-principal"); err == nil {
			t.Fatal("owner admission accepted a pre-existing neutral row")
		}
		row, ok, err := h.ActiveActor(context.Background(), neutral)
		if err != nil || !ok || row.Role != storespec.RoleNone {
			t.Fatalf("neutral row changed = (%+v,%v,%v)", row, ok, err)
		}
	})

	t.Run("owner retry and ordinary admit preserve owner", func(t *testing.T) {
		h := openWhiteboxHome(t)
		owner, err := h.AdmitChannelOwner(context.Background(), "owner-principal")
		if err != nil {
			t.Fatal(err)
		}
		retry, err := h.AdmitChannelOwner(context.Background(), "owner-principal")
		if err != nil || retry != owner {
			t.Fatalf("owner retry = (%q,%v)", retry, err)
		}
		ordinary, err := h.Admit(context.Background(), actor.KindHuman, "owner-principal")
		if err != nil || ordinary != owner {
			t.Fatalf("ordinary retry = (%q,%v)", ordinary, err)
		}
		row, _, _ := h.ActiveActor(context.Background(), owner)
		if row.Role != storespec.RoleOwner {
			t.Fatalf("ordinary retry downgraded role to %q", row.Role)
		}
		if err := h.Remove(context.Background(), owner); !errors.Is(err, storespec.ErrChannelOwnerProtected) {
			t.Fatalf("Remove owner err=%v, want protected sentinel", err)
		}
		if _, ok, _ := h.ActiveActor(context.Background(), owner); !ok {
			t.Fatal("protected owner disappeared")
		}
	})
}

func TestAdmitIsHumanOnlyAndIdempotentlyPublishesAuthority(t *testing.T) {
	h := openWhiteboxHome(t)
	ctx := context.Background()
	principal := "rev"
	if _, err := h.Admit(ctx, actor.KindAgent, principal); !errors.Is(err, ErrAdmitKind) {
		t.Fatalf("non-human Admit err=%v, want ErrAdmitKind", err)
	}
	id, err := h.Admit(ctx, actor.KindHuman, principal)
	if err != nil {
		t.Fatalf("Admit genesis: %v", err)
	}
	reAdmitted, err := h.Admit(ctx, actor.KindHuman, principal)
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

func TestDeclarationEditApplyPublishesCurrentAndKeepsLatestDistinct(t *testing.T) {
	h := openWhiteboxHome(t)
	ctx := context.Background()
	id := actor.ActorID("agent:decl-verbs:1")
	in := storespec.AdmitBundle{
		ID: id, Kind: actor.KindAgent, Principal: "decl-verbs", Binding: actor.BindingRuntimeInboundViaRelay,
		Class: "agent.v1", Placement: storespec.NewServerPlacement(), SourceDeclID: "source-v1", CreatedAt: 1,
	}
	if _, err := h.cs.DeclAdmission.AdmitDeclared(ctx, in); err != nil {
		t.Fatal(err)
	}
	row, ok, err := h.cs.Declared.LookupDeclaredActive(ctx, id)
	if err != nil || !ok {
		t.Fatalf("lookup admitted: ok=%v err=%v", ok, err)
	}
	if !h.controlIndex.UpsertBatch([]controlEntry{{Row: row, World: storespec.WorldDurable}}) {
		t.Fatal("publish admitted row")
	}
	if h.liveness.AdmitIdentity(id) != transitionApplied {
		t.Fatal("publish parent liveness")
	}
	child, err := h.forkAdmission(ctx, id, 1, actorrt.ForkSpec{Kind: actor.KindAgent, Class: "version-gated-child"}, "version-gated-child")
	if err != nil {
		t.Fatal(err)
	}
	oldEnd := lifecycleEndHandle{home: h, author: storespec.AuthorStamp{ID: id, BirthVersion: 1}}
	edited, err := h.EditDeclaration(ctx, storespec.DeclEditBundle{
		ActorID: id, Class: "agent.v2", Config: nil, Placement: storespec.NewServerPlacement(),
		SourceDeclID: "source-v2", CreatedAt: 2,
	})
	if err != nil || edited.CurrentDeclVersion != 2 || edited.Config != nil {
		t.Fatalf("edit = %+v err=%v", edited, err)
	}
	current, latest, err := h.DeclarationVersions(ctx, id)
	if err != nil || current.CurrentDeclVersion != 1 || latest.CurrentDeclVersion != 2 {
		t.Fatalf("versions before apply current=%+v latest=%+v err=%v", current, latest, err)
	}
	if verdict, err := h.controlIndex.CheckAuthor(ctx, storespec.AuthorStamp{ID: id, BirthVersion: 2}); err != nil || verdict != storespec.AuthorVersionStale {
		t.Fatalf("edited version acquired authority before apply: verdict=%v err=%v", verdict, err)
	}
	applied, err := h.ApplyDeclaration(ctx, id, 2)
	if err != nil || applied.CurrentDeclVersion != 2 {
		t.Fatalf("apply = %+v err=%v", applied, err)
	}
	if verdict, err := h.controlIndex.CheckAuthor(ctx, storespec.AuthorStamp{ID: id, BirthVersion: 2}); err != nil || verdict != storespec.AuthorOK {
		t.Fatalf("apply was not immediately published: verdict=%v err=%v", verdict, err)
	}
	if err := oldEnd.End(ctx, child, "stale-parent"); !errors.Is(err, ErrEndVersionStale) {
		t.Fatalf("old lifecycle handle crossed apply gate: %v", err)
	}
	if _, active, err := h.controlIndex.LookupActive(ctx, child); err != nil || !active {
		t.Fatalf("stale lifecycle handle ended child: active=%v err=%v", active, err)
	}
	if _, err := h.ApplyDeclaration(ctx, id, 1); !errors.Is(err, ErrApplyVersionRegress) {
		t.Fatalf("regress err=%v", err)
	}
	if _, err := h.ApplyDeclaration(ctx, id, 3); !errors.Is(err, ErrApplyVersionNotFound) {
		t.Fatalf("missing err=%v", err)
	}
	if err := h.Remove(ctx, id); err != nil {
		t.Fatal(err)
	}
	if _, err := h.ApplyDeclaration(ctx, id, 2); !errors.Is(err, ErrApplyActorEnded) {
		t.Fatalf("ended err=%v", err)
	}
}

func TestSystemDeclarationApplyIsForbidden(t *testing.T) {
	h := openWhiteboxHome(t)
	if _, err := h.ApplyDeclaration(context.Background(), actor.SystemActorID, 2); !errors.Is(err, ErrApplySystemForbidden) {
		t.Fatalf("system apply err=%v", err)
	}
}

func TestRealPensFenceAppliedAndEndedDeclaredAndRunIdentities(t *testing.T) {
	h := openWhiteboxHome(t)
	ctx := context.Background()
	declared, err := h.Declare(ctx, DeclareRequest{
		SourceDeclID: "source:pen-gate", Principal: "pen-gate-declared", Kind: actor.KindAgent,
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
	edited, err := h.EditDeclaration(ctx, storespec.DeclEditBundle{
		ActorID: declared.Row.ID, Class: declared.Row.Class, Placement: declared.Row.Placement,
		TIdle: declared.Row.TIdle, SourceDeclID: declared.Row.SourceDeclID, CreatedAt: time.Now().UnixMilli(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h.ApplyDeclaration(ctx, declared.Row.ID, edited.CurrentDeclVersion); err != nil {
		t.Fatal(err)
	}
	if res := writeEvent(v1, "declared-v1-after-apply"); res.RejectReason != harness.HarnessAuthorVersionStale {
		t.Fatalf("old declared pen after apply=%+v", res)
	}
	v2 := h.minter.Mint(declared.Row.ID, declared.Row.Kind, h.channelID, edited.CurrentDeclVersion)
	if res := writeEvent(v2, "declared-v2-before-end"); !res.Accepted() {
		t.Fatalf("current declared pen rejected: %+v", res)
	}
	if err := h.Remove(ctx, declared.Row.ID); err != nil {
		t.Fatal(err)
	}
	if res := writeEvent(v2, "declared-v2-after-end"); res.RejectReason != harness.HarnessAuthorNotMember {
		t.Fatalf("ended declared pen=%+v", res)
	}

	parent, err := h.Admit(ctx, actor.KindHuman, "pen-gate-parent")
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
	id, err := h.Admit(ctx, actor.KindHuman, "presence-life")
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
	readmittedID, err := h.Admit(ctx, actor.KindHuman, "presence-life")
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
