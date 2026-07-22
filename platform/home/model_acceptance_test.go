package home

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/lib/actorbase"
	"github.com/wanpengxie/atoll/platform"
	"github.com/wanpengxie/atoll/protocol/access"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/protocol/resource"
	"github.com/wanpengxie/atoll/runtime/accessdoor"
	"github.com/wanpengxie/atoll/runtime/actorrt"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

type acceptanceBuild struct {
	id     actor.ActorID
	class  string
	config json.RawMessage
}

type acceptanceResolver struct {
	mu     sync.Mutex
	builds []acceptanceBuild
}

func (r *acceptanceResolver) ResolveDeclaration(context.Context, channel.ID, string) (channel.DeclarationFacts, error) {
	return channel.DeclarationFacts{}, channel.ErrDeclarationNotFound
}

func (r *acceptanceResolver) ClassKind(context.Context, string) (actor.Kind, bool, error) {
	return actor.KindAgent, true, nil
}

func (r *acceptanceResolver) DaemonFacts(context.Context, string) (channel.DaemonFacts, error) {
	return channel.DaemonFacts{}, nil
}

func (r *acceptanceResolver) BuildClass(_ channel.ID, id actor.ActorID, class string, config json.RawMessage) (platform.ActorFactory, bool) {
	r.mu.Lock()
	r.builds = append(r.builds, acceptanceBuild{id: id, class: class, config: append(json.RawMessage(nil), config...)})
	r.mu.Unlock()
	return platform.ActorFactory{Proc: actorbase.Def{New: func() (actorbase.Proc, error) {
		return func(sys actorbase.Sys) error {
			<-sys.Life().Done()
			return sys.Life().Err()
		}, nil
	}}}, true
}

func (r *acceptanceResolver) count(id actor.ActorID, class string, nilConfig bool) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	n := 0
	for _, build := range r.builds {
		if build.id == id && build.class == class && (!nilConfig || build.config == nil) {
			n++
		}
	}
	return n
}

func openAcceptanceHome(t *testing.T, dbPath string, chID channel.ID, resolver CompositionResolver, interval time.Duration) *Home {
	t.Helper()
	introduction, ok := resolver.(IntroductionResolver)
	if !ok {
		introduction = fixedIntroductionResolver{kind: actor.KindAgent}
	}
	h, err := Open(Config{
		ChannelID: chID, DBPath: dbPath, CompositionResolver: resolver, IntroductionResolver: introduction,
		ReconcileInterval: interval, Bootstrap: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	return h
}

func TestBootPublishesHumanSystemAndCompositionRowsInOneControlShape(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "channel.sqlite")
	resolver := &acceptanceResolver{}
	h1 := openAcceptanceHome(t, dbPath, "three-births", resolver, time.Hour)
	humanID, err := h1.admit(ctx, actor.KindHuman, "three-birth-human")
	if err != nil {
		t.Fatal(err)
	}
	config := json.RawMessage(`{"shape":"complete"}`)
	decl, err := h1.declare(ctx, DeclareRequest{
		SourceDeclID: "decl:three-births", Kind: actor.KindAgent, Class: "composition-probe", Config: &config,
		Placement: storespec.NewServerPlacement(), TIdle: 321, CreatedAt: time.Now().UnixMilli(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := h1.closeInternal("test"); err != nil {
		t.Fatal(err)
	}

	h2 := openAcceptanceHome(t, dbPath, "three-births", resolver, time.Hour)
	t.Cleanup(func() { _ = h2.closeInternal("test") })
	cases := []struct {
		id        actor.ActorID
		kind      actor.Kind
		class     string
		tIdle     time.Duration
		source    string
		config    string
		principal string
	}{
		{actor.SystemActorID, actor.KindSystem, "system", 0, "", "", ""},
		{humanID, actor.KindHuman, "human", 0, "", "", "three-birth-human"},
		{decl.Row.ID, actor.KindAgent, "composition-probe", 321 * time.Millisecond, "decl:three-births", string(config), ""},
	}
	for _, tc := range cases {
		row, ok, err := h2.controlIndex.LookupActive(ctx, tc.id)
		if err != nil || !ok {
			t.Fatalf("boot lookup %s=(%+v,%v,%v)", tc.id, row, ok, err)
		}
		if row.Kind != tc.kind || row.Class != tc.class || row.CurrentDeclVersion != 1 ||
			row.Placement != storespec.NewServerPlacement() || row.TIdle != tc.tIdle ||
			row.SourceDeclID != tc.source || string(row.Config) != tc.config || row.Principal != tc.principal {
			t.Fatalf("boot row %s=%+v", tc.id, row)
		}
		if verdict, err := h2.controlIndex.CheckAuthor(ctx, storespec.AuthorStamp{ID: tc.id, BirthVersion: 1}); err != nil || verdict != storespec.AuthorOK {
			t.Fatalf("gate %s=(%v,%v)", tc.id, verdict, err)
		}
	}
}

func TestEmptyConfigSurvivesAdmissionEditBootAndForkFactoryBuild(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "channel.sqlite")
	resolver := &acceptanceResolver{}
	h1 := openAcceptanceHome(t, dbPath, "empty-config-chain", resolver, 5*time.Millisecond)
	decl, err := h1.declare(ctx, DeclareRequest{
		SourceDeclID: "decl:empty", Kind: actor.KindAgent,
		Class: "declared-empty", Config: nil, Placement: storespec.NewServerPlacement(), CreatedAt: time.Now().UnixMilli(),
	})
	if err != nil {
		t.Fatal(err)
	}
	waitHomeCondition(t, func() bool { return resolver.count(decl.Row.ID, "declared-empty", true) >= 1 })
	synced, err := h1.opEntry.applyResolvedDeclaration(ctx, decl.Row.ID, decl.Row.SourceDeclID, "declared-empty-v2", nil)
	if err != nil || synced.Status != storespec.DeclarationApplied || synced.Version != 2 {
		t.Fatalf("empty atomic sync=%+v err=%v", synced, err)
	}
	waitHomeCondition(t, func() bool { return resolver.count(decl.Row.ID, "declared-empty-v2", true) >= 1 })

	parent, err := h1.admit(ctx, actor.KindHuman, "empty-fork-parent")
	if err != nil {
		t.Fatal(err)
	}
	child, err := h1.forkAdmission(ctx, parent, 1, actorrt.ForkSpec{
		Kind: actor.KindAgent, Class: "fork-empty", Config: nil,
	}, "empty-fork")
	if err != nil {
		t.Fatal(err)
	}
	if err := (homeReviver{h: h1}).EnsureLive(ctx, child); err != nil {
		t.Fatal(err)
	}
	if resolver.count(child, "fork-empty", true) != 1 {
		t.Fatal("empty ForkSpec.Config did not reach LookupByClass unchanged")
	}
	row, ok, _ := h1.controlIndex.LookupActive(ctx, child)
	if !ok || row.Config != nil {
		t.Fatalf("fork row config=%q ok=%v", row.Config, ok)
	}
	if err := h1.closeInternal("test"); err != nil {
		t.Fatal(err)
	}

	h2 := openAcceptanceHome(t, dbPath, "empty-config-chain", resolver, 5*time.Millisecond)
	t.Cleanup(func() { _ = h2.closeInternal("test") })
	row, ok, err = h2.controlIndex.LookupActive(ctx, decl.Row.ID)
	if err != nil || !ok || row.Config != nil || row.CurrentDeclVersion != 2 || row.Class != "declared-empty-v2" {
		t.Fatalf("boot empty config=(%+v,%v,%v)", row, ok, err)
	}
	waitHomeCondition(t, func() bool { return resolver.count(decl.Row.ID, "declared-empty-v2", true) >= 2 })
}

func TestStateLifetimeSplitsDurableIdentityFromHomeSessionRun(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "channel.sqlite")
	resolver := &acceptanceResolver{}
	h1 := openAcceptanceHome(t, dbPath, "state-two-layers", resolver, time.Hour)
	declared, err := h1.admit(ctx, actor.KindHuman, "state-durable")
	if err != nil {
		t.Fatal(err)
	}
	durable, err := h1.stateHandles.Resolve(ctx, storespec.AuthorStamp{ID: declared, BirthVersion: 1})
	if err != nil {
		t.Fatal(err)
	}
	if out, err := durable.Invoke(ctx, access.OpCreate, resource.ResourceID("value"), []byte("durable"), nil); err != nil || !out.Accepted() {
		t.Fatalf("durable create=(%+v,%v)", out, err)
	}
	child, err := h1.forkAdmission(ctx, declared, 1, actorrt.ForkSpec{Kind: actor.KindAgent, Class: "state-run"}, "state-run")
	if err != nil {
		t.Fatal(err)
	}
	run, err := h1.stateHandles.Resolve(ctx, storespec.AuthorStamp{ID: child, BirthVersion: 1})
	if err != nil {
		t.Fatal(err)
	}
	if out, err := run.Invoke(ctx, access.OpCreate, resource.ResourceID("value"), []byte("run"), nil); err != nil || !out.Accepted() {
		t.Fatalf("run create=(%+v,%v)", out, err)
	}
	// Resolving for a successor embodiment returns the same Home-session run handle.
	runSuccessor, _ := h1.stateHandles.Resolve(ctx, storespec.AuthorStamp{ID: child, BirthVersion: 1})
	if out, err := runSuccessor.Invoke(ctx, access.OpRead, resource.ResourceID("value"), nil, nil); err != nil || string(out.Value) != "run" {
		t.Fatalf("run successor read=(%+v,%v)", out, err)
	}
	ended, err := h1.forkAdmission(ctx, declared, 1, actorrt.ForkSpec{Kind: actor.KindAgent, Class: "state-ended"}, "state-ended")
	if err != nil {
		t.Fatal(err)
	}
	if err := (lifecycleEndHandle{home: h1, author: storespec.AuthorStamp{ID: declared, BirthVersion: 1}}).End(ctx, ended, "state-test"); err != nil {
		t.Fatal(err)
	}
	if _, err := h1.stateHandles.Resolve(ctx, storespec.AuthorStamp{ID: ended, BirthVersion: 1}); !errors.Is(err, accessdoor.ErrStateHandleUnavailable) {
		t.Fatalf("ended run State=%v", err)
	}
	if err := h1.closeInternal("test"); err != nil {
		t.Fatal(err)
	}

	h2 := openAcceptanceHome(t, dbPath, "state-two-layers", resolver, time.Hour)
	t.Cleanup(func() { _ = h2.closeInternal("test") })
	durable2, err := h2.stateHandles.Resolve(ctx, storespec.AuthorStamp{ID: declared, BirthVersion: 1})
	if err != nil {
		t.Fatal(err)
	}
	if out, err := durable2.Invoke(ctx, access.OpRead, resource.ResourceID("value"), nil, nil); err != nil || string(out.Value) != "durable" {
		t.Fatalf("durable restart read=(%+v,%v)", out, err)
	}
	if _, err := h2.stateHandles.Resolve(ctx, storespec.AuthorStamp{ID: child, BirthVersion: 1}); !errors.Is(err, accessdoor.ErrStateHandleUnavailable) {
		t.Fatalf("run State crossed Home restart: %v", err)
	}
}

func TestBootDropsRunIdentityAndClosesItsOpenRequestOnlyAfterRestart(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "channel.sqlite")
	resolver := &acceptanceResolver{}
	h1 := openAcceptanceHome(t, dbPath, "boot-run-closure", resolver, 5*time.Millisecond)
	parent, err := h1.admit(ctx, actor.KindHuman, "boot-parent")
	if err != nil {
		t.Fatal(err)
	}
	child, err := h1.forkAdmission(ctx, parent, 1, actorrt.ForkSpec{Kind: actor.KindAgent, Class: "nonreplying"}, "boot-child")
	if err != nil {
		t.Fatal(err)
	}
	expires := time.Now().Add(time.Hour).UnixMilli()
	now := time.Now().UnixMilli()
	write, err := h1.systemPen.Write(ctx, &message.Envelope{
		ID: "boot-open-request", Kind: message.KindRequest, Type: "stay.open",
		Audience: message.Audience{child}, ExpiresAt: &expires, Visibility: message.VisibilitySystem,
		TS: now, TSReceived: now,
	})
	if err != nil || !write.Accepted() {
		t.Fatalf("open request write=(%+v,%v)", write, err)
	}
	waitHomeCondition(t, func() bool {
		rows, qerr := h1.cs.Query.OpenRequestsForActor(ctx, child)
		return qerr == nil && len(rows) == 1
	})
	// Several closure passes while the run identity is active must not poison it.
	time.Sleep(25 * time.Millisecond)
	if rows, err := h1.cs.Query.OpenRequestsForActor(ctx, child); err != nil || len(rows) != 1 {
		t.Fatalf("live fork request was closed: rows=%d err=%v", len(rows), err)
	}
	if err := h1.closeInternal("test"); err != nil {
		t.Fatal(err)
	}

	h2 := openAcceptanceHome(t, dbPath, "boot-run-closure", resolver, 5*time.Millisecond)
	t.Cleanup(func() { _ = h2.closeInternal("test") })
	if _, ok, _ := h2.controlIndex.LookupActive(ctx, child); ok {
		t.Fatal("previous Home run identity survived boot")
	}
	if ever, err := h2.cs.DurableHistory.ExistsEver(ctx, child); err != nil || ever {
		t.Fatalf("fork left durable identity row=(%v,%v)", ever, err)
	}
	waitHomeCondition(t, func() bool {
		rows, qerr := h2.cs.Query.OpenRequestsForActor(ctx, child)
		return qerr == nil && len(rows) == 0
	})
}

func TestEnsureTicketFromPriorHomeSessionCannotAttach(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "channel.sqlite")
	resolver := &acceptanceResolver{}
	placement, _ := storespec.NewDaemonPlacement("daemon-ticket")
	h1 := openAcceptanceHome(t, dbPath, "ticket-restart", resolver, time.Hour)
	decl, err := h1.declare(ctx, DeclareRequest{
		SourceDeclID: "decl:ticket", Kind: actor.KindAgent,
		Class: "ticket-worker", Placement: placement, CreatedAt: time.Now().UnixMilli(),
	})
	if err != nil {
		t.Fatal(err)
	}
	_, _ = h1.liveness.AcceptDelivery(decl.Row.ID, &message.Envelope{Kind: message.KindRequest})
	h1.reconcileDaemonIntent(ctx)
	oldPlan, err := h1.planForDaemon(ctx, "daemon-ticket")
	if err != nil || len(oldPlan) != 1 || oldPlan[0].EnsureTicket == "" {
		t.Fatalf("old plan=%+v err=%v", oldPlan, err)
	}
	oldTicket := EnsureTicket(oldPlan[0].EnsureTicket)
	if err := h1.closeInternal("test"); err != nil {
		t.Fatal(err)
	}

	h2 := openAcceptanceHome(t, dbPath, "ticket-restart", resolver, time.Hour)
	t.Cleanup(func() { _ = h2.closeInternal("test") })
	_, _ = h2.liveness.AcceptDelivery(decl.Row.ID, &message.Envelope{Kind: message.KindRequest})
	h2.reconcileDaemonIntent(ctx)
	newPlan, err := h2.planForDaemon(ctx, "daemon-ticket")
	if err != nil || len(newPlan) != 1 || newPlan[0].EnsureTicket == "" || newPlan[0].EnsureTicket == string(oldTicket) {
		t.Fatalf("new plan=%+v old=%q err=%v", newPlan, oldTicket, err)
	}
	carrier := &testCarrier{}
	if got := h2.liveness.Attach(decl.Row.ID, oldTicket, 1, noInc, carrier); got != transitionStaleTicket {
		t.Fatalf("old-session attach=%v, want stale ticket", got)
	}
	if got := h2.liveness.Attach(decl.Row.ID, EnsureTicket(newPlan[0].EnsureTicket), 1, noInc, carrier); got != transitionApplied {
		t.Fatalf("current-session attach=%v", got)
	}
}

func TestBootConvergesDurableDeclarationCommittedBeforeMemoryPublication(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "channel.sqlite")
	resolver := &acceptanceResolver{}
	h1 := openAcceptanceHome(t, dbPath, "commit-before-publish", resolver, time.Hour)
	admitted, err := h1.cs.DeclAdmission.AdmitDeclared(ctx, storespec.AdmitBundle{
		ID: "agent:crash-window", Kind: actor.KindAgent,
		Class: "crash-window-v1", Placement: storespec.NewServerPlacement(),
		SourceDeclID: "decl:crash-window", CreatedAt: time.Now().UnixMilli(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, visible, _ := h1.controlIndex.LookupActive(ctx, admitted.ID); visible {
		t.Fatal("test precondition: direct durable bundle unexpectedly published in memory")
	}
	// Model a crash after the atomic current+history transaction commits but
	// before Home can consume its advisory effects and publish the projection.
	meta, err := systemMeta("ifin:v1:crash-window", struct{ Class string }{"crash-window-v2"})
	if err != nil {
		t.Fatal(err)
	}
	synced, err := h1.cs.DeclarationSync.ApplyResolvedDeclaration(ctx, storespec.DeclarationSyncTx{
		SysOpMeta: meta, ActorID: admitted.ID, DeclID: "decl:crash-window", Class: "crash-window-v2",
	})
	if err != nil || synced.Status != storespec.DeclarationApplied || synced.Version != 2 {
		t.Fatalf("durable atomic sync=(%+v,%v)", synced, err)
	}
	if err := h1.closeInternal("test"); err != nil {
		t.Fatal(err)
	}

	h2 := openAcceptanceHome(t, dbPath, "commit-before-publish", resolver, time.Hour)
	t.Cleanup(func() { _ = h2.closeInternal("test") })
	row, visible, err := h2.controlIndex.LookupActive(ctx, admitted.ID)
	if err != nil || !visible || row.CurrentDeclVersion != 2 || row.Class != "crash-window-v2" || row.Config != nil {
		t.Fatalf("boot convergence=(%+v,%v,%v)", row, visible, err)
	}
	if verdict, err := h2.controlIndex.CheckAuthor(ctx, storespec.AuthorStamp{ID: admitted.ID, BirthVersion: 2}); err != nil || verdict != storespec.AuthorOK {
		t.Fatalf("boot-published gate=(%v,%v)", verdict, err)
	}
}

func TestForkWakeConvergesOnLevelSweepWithoutInlineBuildOrPoke(t *testing.T) {
	ctx := context.Background()
	resolver := &acceptanceResolver{}
	h := openAcceptanceHome(t, filepath.Join(t.TempDir(), "channel.sqlite"), "accelerators-off", resolver, 10*time.Millisecond)
	t.Cleanup(func() { _ = h.closeInternal("test") })
	h.disablePoke.Store(true)
	h.disableForkInlineActivation.Store(true)
	parent, err := h.admit(ctx, actor.KindHuman, "accelerator-parent")
	if err != nil {
		t.Fatal(err)
	}
	waitHomeCondition(t, func() bool {
		_, live := h.channel.Cells().CurrentIncarnation(parent)
		return live
	})
	parentInc, _ := h.channel.Cells().CurrentIncarnation(parent)
	lifecycle := newSpawnHandle(h, parentInc, 1, h.channel.Cells())
	child, err := lifecycle.Fork(ctx, actorrt.ForkSpec{
		Kind: actor.KindAgent, Class: "accelerator-child",
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, live := h.channel.Cells().CurrentIncarnation(child); live {
		t.Fatal("fork admission used an inline build accelerator")
	}
	now := time.Now().UnixMilli()
	expires := time.Now().Add(time.Minute).UnixMilli()
	res, err := h.systemPen.Write(ctx, &message.Envelope{
		ID: "accelerator-off-request", Kind: message.KindRequest, Type: "work",
		Audience: message.Audience{child}, Visibility: message.VisibilitySystem,
		TS: now, TSReceived: now, ExpiresAt: &expires,
	})
	if err != nil || !res.Accepted() {
		t.Fatalf("request=(%+v,%v)", res, err)
	}
	waitHomeCondition(t, func() bool {
		_, live := h.channel.Cells().CurrentIncarnation(child)
		return live
	})
}
