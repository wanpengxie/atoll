package home

import (
	"context"
	"errors"
	"log/slog"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/lib/actorcaps"
	"github.com/wanpengxie/atoll/platform"
	"github.com/wanpengxie/atoll/platform/internal/hostcommon"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/runtime/actorrt"
	"github.com/wanpengxie/atoll/runtime/schedule"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

// ---------------------------------------------------------------------------
// T0 characterization fixtures (purity v3 B1 §2 T0).
//
// This file pins the PRE-extraction behavior of reconcileActivation (环, table
// §1.6) and homeReviver.EnsureLive (reviver, table §1.7) — one test per table
// cell (word × {current/return, log, control flow, side effect}) plus the
// three §1.9 cancel-interaction matrix cells. T1 extracts activateOne from the
// SAME behavior these tests lock down; T2 flips ONLY the three reviver matrix
// cells (each such test says so explicitly) and adds the regression suite.
// ---------------------------------------------------------------------------

// recordingHandler is a minimal slog.Handler double that captures every
// record so a test can assert exact log-site names (the 环/reviver tables'
// "日志" column) without depending on log formatting.
type recordingHandler struct {
	mu      sync.Mutex
	records []slog.Record
}

func (h *recordingHandler) Enabled(context.Context, slog.Level) bool { return true }
func (h *recordingHandler) Handle(_ context.Context, r slog.Record) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.records = append(h.records, r.Clone())
	return nil
}
func (h *recordingHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *recordingHandler) WithGroup(string) slog.Handler      { return h }

func (h *recordingHandler) has(msg string) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, r := range h.records {
		if r.Message == msg {
			return true
		}
	}
	return false
}

func (h *recordingHandler) count() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.records)
}

// reset drops every record captured so far — used after Open (which itself
// logs platform.home.ready and friends) so a "no log" assertion measures
// only the action under test, not home construction.
func (h *recordingHandler) reset() {
	h.mu.Lock()
	h.records = nil
	h.mu.Unlock()
}

func newRecordingLogger() (*slog.Logger, *recordingHandler) {
	rh := &recordingHandler{}
	return slog.New(rh), rh
}

// toggleCtx is a context.Context whose Err()/Done() flip from nil/open to
// context.Canceled/closed exactly once cancel() is called — used to land a
// cancellation inside a SPECIFIC internal window (e.g. verifyPostBuild's own
// ctx check) via a test hook, without racing a background goroutine.
type toggleCtx struct {
	context.Context
	mu   sync.Mutex
	done chan struct{}
	err  error
}

func newToggleCtx() *toggleCtx {
	return &toggleCtx{Context: context.Background(), done: make(chan struct{})}
}

func (c *toggleCtx) cancel() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.err == nil {
		c.err = context.Canceled
		close(c.done)
	}
}

func (c *toggleCtx) Done() <-chan struct{} { return c.done }
func (c *toggleCtx) Err() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.err
}

// openCharHome opens a home wired the same way openActivationHome does
// (long ReconcileInterval — the tests drive reconcileActivation/EnsureLive
// synchronously) but with a caller-supplied logger, so log-site assertions
// can be made.
func openCharHome(t *testing.T, desired actorrt.DesiredSource, builder CapsFactoryBuilder, logger *slog.Logger) *Home {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "activation-char.sqlite")
	h, err := Open(Config{
		ChannelID:         activationTestChannelID,
		DBPath:            dbPath,
		ReconcileInterval: time.Hour,
		Desired:           desired,
		Builder:           builder,
		Logger:            logger,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = h.Close() })
	return h
}

// blockingFactory returns a CapsFactory that signals startedCh once entered
// and then blocks on release — the seam a real (non-hook) SpawnIfAbsent CAS
// race needs: the build genuinely runs, outside any lock (runtime.go's own
// documented discipline), so a second, concurrent SpawnIfAbsent for the SAME
// id can win the race while this one is still building.
func blockingFactory(startedCh chan<- struct{}, release <-chan struct{}) platform.ActorFactory {
	var once sync.Once
	return hostcommon.CapsFactory(func(actorcaps.Caps) actorrt.Actor {
		once.Do(func() { close(startedCh) })
		<-release
		return recordActor{}
	})
}

// panicFactory deterministically produces an actorrt.BuildFailure (recovered
// panic) — the "BuildFailed" word's trigger, shared by every ring/reviver
// BuildFailed characterization test below.
func panicFactory() platform.ActorFactory {
	return hostcommon.CapsFactory(func(actorcaps.Caps) actorrt.Actor {
		panic("char test: build boom")
	})
}

// ===========================================================================
// 环 (reconcileActivation) — table §1.6, one test per word.
// ===========================================================================

// Embodied (actual[] fast path): current stays true, backoff account clears,
// no log, continue.
func TestCharRing_Embodied_FastPath_ClearsBackoffKeepsCurrent(t *testing.T) {
	ctx := context.Background()
	desired := &testDesired{}
	builder := newTestBuilder()
	id := actor.ActorID("agent:char-embodied")
	builder.byID[id] = builder.recordFactory(id)

	logger, rh := newRecordingLogger()
	h := openCharHome(t, desired, builder, logger)
	id = admit(t, h, id, actor.KindAgent)

	// Bring it live OUTSIDE reconcileActivation (simulates it having been
	// minted earlier), then seed a stale backoff account to prove the fast
	// path clears it.
	if _, built, err := h.channel.Cells().SpawnIfAbsent(id, actor.KindAgent, func(inc actorrt.Incarnation) actorrt.Actor {
		return hostcommon.Build(h.buildCaps(id, actor.KindAgent, inc), h.hooks(), builder.recordFactory(id))
	}); err != nil || !built {
		t.Fatalf("pre-spawn: built=%v err=%v", built, err)
	}
	h.recordBuildFailure(id, time.Now())
	if backoffEntry(t, h, id).failures == 0 {
		t.Fatal("precondition: backoff account not seeded")
	}

	rh.reset()

	desired.set(actorrt.DesiredMember{ID: id, Kind: actor.KindAgent, Lifecycle: actorrt.LifecycleAlwaysOn})
	h.reconcileActivation(ctx)

	if !live(h, id) {
		t.Fatal("embodied member must remain live")
	}
	if !h.prevEagerDesired[id] {
		t.Fatal("current must stay true for the fast-path word")
	}
	if e := backoffEntry(t, h, id); e.failures != 0 {
		t.Fatalf("fast path must clear the backoff account, got %+v", e)
	}
	if rh.count() != 0 {
		t.Fatalf("fast path must not log, got %d records", rh.count())
	}
}

// AlreadyLive (⑤ CAS loser, real interleave): current stays true, backoff
// account is NOT cleared (§1.4 現状: 环翻译器 CAS 输家均不清账), continue.
func TestCharRing_AlreadyLive_CASLoser_DoesNotClearBackoff(t *testing.T) {
	ctx := context.Background()
	desired := &testDesired{}
	builder := newTestBuilder()
	id := actor.ActorID("agent:char-cas-loser")

	started := make(chan struct{})
	release := make(chan struct{})
	builder.byID[id] = blockingFactory(started, release)

	h := openCharHome(t, desired, builder, nil)
	id = admit(t, h, id, actor.KindAgent)
	// Seed an already-ELAPSED backoff window: recordBuildFailure stamps
	// off the passed-in `now`, so back-dating it means backoffGate (which
	// compares against real wall-clock now) reports NOT held — the build
	// must actually be attempted for this to exercise the CAS-loser branch
	// rather than the earlier BackoffHeld gate.
	h.recordBuildFailure(id, time.Now().Add(-time.Hour))
	if backoffEntry(t, h, id).failures == 0 {
		t.Fatal("precondition: backoff account not seeded")
	}

	desired.set(actorrt.DesiredMember{ID: id, Kind: actor.KindAgent, Lifecycle: actorrt.LifecycleAlwaysOn})

	done := make(chan struct{})
	go func() {
		defer close(done)
		h.reconcileActivation(ctx)
	}()

	<-started // reconcileActivation's own build is now genuinely in flight, outside the lock
	if _, built, err := h.channel.Cells().SpawnIfAbsent(id, actor.KindAgent, func(inc actorrt.Incarnation) actorrt.Actor {
		return hostcommon.Build(h.buildCaps(id, actor.KindAgent, inc), h.hooks(), builder.recordFactory(id))
	}); err != nil || !built {
		t.Fatalf("racer spawn: built=%v err=%v", built, err)
	}
	close(release) // let reconcileActivation's own build finish and lose the CAS
	<-done

	if !live(h, id) {
		t.Fatal("id must be live (the racer's build won)")
	}
	if !h.prevEagerDesired[id] {
		t.Fatal("current must stay true for the CAS-loser word")
	}
	if e := backoffEntry(t, h, id); e.failures == 0 {
		t.Fatal("CAS-loser continue arm must NOT clear the backoff account (§1.4 现状)")
	}
}

// Attached: current stays true, no log, continue, no side effect (builder
// never consulted).
func TestCharRing_Attached_KeepsCurrent_NoBuildNoLog(t *testing.T) {
	ctx := context.Background()
	desired := &testDesired{}
	builder := newTestBuilder()
	id := actor.ActorID("agent:char-attached")
	builder.byID[id] = builder.recordFactory(id)

	logger, rh := newRecordingLogger()
	h := openCharHome(t, desired, builder, logger)
	id = admit(t, h, id, actor.KindAgent)
	if err := h.cs.Membership.ApplyMemberTransitions(ctx, []storespec.MemberActorAdd{
		{ID: id, Kind: actor.KindAgent, Host: "daemon-char", At: h.nowMs()},
	}, nil); err != nil {
		t.Fatalf("attach host: %v", err)
	}

	rh.reset()
	desired.set(actorrt.DesiredMember{ID: id, Kind: actor.KindAgent, Lifecycle: actorrt.LifecycleAlwaysOn})
	h.reconcileActivation(ctx)

	if live(h, id) {
		t.Fatal("attached member must not be spawned locally")
	}
	if !h.prevEagerDesired[id] {
		t.Fatal("current must stay true for Attached")
	}
	if _, ok := builder.capsFor(id); ok {
		t.Fatal("builder must never be consulted for an attached member")
	}
	if rh.count() != 0 {
		t.Fatalf("Attached must not log, got %d records", rh.count())
	}
}

// BackoffHeld: current deletes, no log, continue, no build attempt.
func TestCharRing_BackoffHeld_DropsCurrent_NoBuildAttempt(t *testing.T) {
	ctx := context.Background()
	desired := &testDesired{}
	builder := newTestBuilder()
	id := actor.ActorID("agent:char-backoff-held")
	var builds int
	builder.byID[id] = hostcommon.CapsFactory(func(actorcaps.Caps) actorrt.Actor {
		builds++
		return recordActor{}
	})

	logger, rh := newRecordingLogger()
	h := openCharHome(t, desired, builder, logger)
	id = admit(t, h, id, actor.KindAgent)
	h.recordBuildFailure(id, time.Now()) // future-dated backoff window

	rh.reset()
	desired.set(actorrt.DesiredMember{ID: id, Kind: actor.KindAgent, Lifecycle: actorrt.LifecycleAlwaysOn})
	h.reconcileActivation(ctx)

	if live(h, id) {
		t.Fatal("backoff-held member must not embody this tick")
	}
	if h.prevEagerDesired[id] {
		t.Fatal("current must be dropped for BackoffHeld")
	}
	if builds != 0 {
		t.Fatalf("BackoffHeld must skip the build attempt entirely, got %d builds", builds)
	}
	if rh.count() != 0 {
		t.Fatalf("BackoffHeld must not log, got %d records", rh.count())
	}
}

// NoFactory: current stays true, warn log, continue.
func TestCharRing_NoFactory_KeepsCurrent_WarnLog(t *testing.T) {
	ctx := context.Background()
	desired := &testDesired{}
	builder := newTestBuilder() // deliberately no byID[id] entry
	id := actor.ActorID("agent:char-no-factory")

	logger, rh := newRecordingLogger()
	h := openCharHome(t, desired, builder, logger)
	id = admit(t, h, id, actor.KindAgent)

	desired.set(actorrt.DesiredMember{ID: id, Kind: actor.KindAgent, Lifecycle: actorrt.LifecycleAlwaysOn})
	h.reconcileActivation(ctx)

	if live(h, id) {
		t.Fatal("no-factory member must not embody")
	}
	if !h.prevEagerDesired[id] {
		t.Fatal("current must stay true for NoFactory")
	}
	if !rh.has("platform.reconcile.no_factory") {
		t.Fatal("NoFactory must warn-log platform.reconcile.no_factory")
	}
}

// Sealed: info log, whole-tick abort (prevEagerDesired left byte-for-byte as
// it was BEFORE this tick — the return happens before the commit line).
func TestCharRing_Sealed_InfoLog_AbortsWholeTick(t *testing.T) {
	ctx := context.Background()
	desired := &testDesired{}
	builder := newTestBuilder()
	id := actor.ActorID("agent:char-sealed")
	builder.byID[id] = builder.recordFactory(id)

	logger, rh := newRecordingLogger()
	h := openCharHome(t, desired, builder, logger)
	id = admit(t, h, id, actor.KindAgent)

	baseline := map[actor.ActorID]bool{"sentinel-untouched": true}
	h.prevEagerDesired = baseline

	h.channel.Cells().Seal()
	desired.set(actorrt.DesiredMember{ID: id, Kind: actor.KindAgent, Lifecycle: actorrt.LifecycleAlwaysOn})
	h.reconcileActivation(ctx)

	if !rh.has("platform.reconcile.runtime_sealed") {
		t.Fatal("Sealed must info-log platform.reconcile.runtime_sealed")
	}
	if len(h.prevEagerDesired) != 1 || !h.prevEagerDesired["sentinel-untouched"] {
		t.Fatalf("Sealed must abort the WHOLE tick — prevEagerDesired must stay exactly the pre-tick baseline, got %v", h.prevEagerDesired)
	}
}

// BuildFailed: error log, backoff account recorded, current deletes,
// continue (paired with an already-live id2 to prove the tick does NOT
// abort — 现状 vs the Sealed/Cancelled "整环 return" words).
func TestCharRing_BuildFailed_ErrorLog_RecordsBackoff_ContinuesTick(t *testing.T) {
	ctx := context.Background()
	desired := &testDesired{}
	builder := newTestBuilder()
	id := actor.ActorID("agent:char-build-failed")
	builder.byID[id] = panicFactory()
	id2 := actor.ActorID("agent:char-build-failed-sibling")
	builder.byID[id2] = builder.recordFactory(id2)

	logger, rh := newRecordingLogger()
	h := openCharHome(t, desired, builder, logger)
	id = admit(t, h, id, actor.KindAgent)
	id2 = admit(t, h, id2, actor.KindAgent)

	// id2 pre-embodied so it takes the actual[] fast path regardless of the
	// map-iteration order the loop visits id/id2 in — the fast path's own
	// outcome (current[id2]=true) does not depend on id's outcome, so its
	// survival into prevEagerDesired proves the tick did NOT abort.
	if _, built, err := h.channel.Cells().SpawnIfAbsent(id2, actor.KindAgent, func(inc actorrt.Incarnation) actorrt.Actor {
		return hostcommon.Build(h.buildCaps(id2, actor.KindAgent, inc), h.hooks(), builder.recordFactory(id2))
	}); err != nil || !built {
		t.Fatalf("pre-spawn id2: built=%v err=%v", built, err)
	}

	desired.set(
		actorrt.DesiredMember{ID: id, Kind: actor.KindAgent, Lifecycle: actorrt.LifecycleAlwaysOn},
		actorrt.DesiredMember{ID: id2, Kind: actor.KindAgent, Lifecycle: actorrt.LifecycleAlwaysOn},
	)
	h.reconcileActivation(ctx)

	if live(h, id) {
		t.Fatal("build-failed member must not be live")
	}
	if h.prevEagerDesired[id] {
		t.Fatal("current must be dropped for BuildFailed")
	}
	if !rh.has("platform.reconcile.build_failed") {
		t.Fatal("BuildFailed must error-log platform.reconcile.build_failed")
	}
	if e := backoffEntry(t, h, id); e.failures != 1 {
		t.Fatalf("BuildFailed must record one backoff step, got %+v", e)
	}
	if !h.prevEagerDesired[id2] {
		t.Fatal("BuildFailed must NOT abort the whole tick — the sibling fast-path member must still land in current")
	}
}

// Cancelled: whole-tick abort (prevEagerDesired unchanged from the pre-tick
// baseline) and the just-built cell is despawned — the ring's OWN ctx gate
// right after SpawnIfAbsent, distinct from verifyPostBuild's internal gate
// (RecheckFault, below).
func TestCharRing_Cancelled_DespawnsBuiltCell_AbortsWholeTick(t *testing.T) {
	desired := &testDesired{}
	builder := newTestBuilder()
	id := actor.ActorID("agent:char-cancelled")

	tctx := newToggleCtx()
	builder.byID[id] = hostcommon.CapsFactory(func(actorcaps.Caps) actorrt.Actor {
		tctx.cancel() // simulate a cancel landing DURING the build
		return recordActor{}
	})

	h := openCharHome(t, desired, builder, nil)
	id = admit(t, h, id, actor.KindAgent)

	baseline := map[actor.ActorID]bool{"sentinel-untouched": true}
	h.prevEagerDesired = baseline

	desired.set(actorrt.DesiredMember{ID: id, Kind: actor.KindAgent, Lifecycle: actorrt.LifecycleAlwaysOn})
	h.reconcileActivation(tctx)

	if live(h, id) {
		t.Fatal("a cancelled-mid-build cell must be despawned, never left live")
	}
	if len(h.prevEagerDesired) != 1 || !h.prevEagerDesired["sentinel-untouched"] {
		t.Fatalf("Cancelled must abort the WHOLE tick — prevEagerDesired must stay exactly the pre-tick baseline, got %v", h.prevEagerDesired)
	}
}

// RecheckFault: verifyPostBuild's OWN ctx gate (distinct window from the
// ring's outer Cancelled gate, P1-B) — despawns the cell, drops current, and
// (unlike Sealed/Cancelled) does NOT abort the whole tick.
func TestCharRing_RecheckFault_DespawnsCell_DropsCurrent_DoesNotAbort(t *testing.T) {
	desired := &testDesired{}
	builder := newTestBuilder()
	id := actor.ActorID("agent:char-recheck-fault")
	builder.byID[id] = builder.recordFactory(id)

	tctx := newToggleCtx()
	h := openCharHome(t, desired, builder, nil)
	id = admit(t, h, id, actor.KindAgent)
	// Fires AFTER the ring's own post-SpawnIfAbsent ctx check has already
	// passed (build just landed), BEFORE verifyPostBuild's Lookup — lands the
	// cancel inside verifyPostBuild's own ctx gate exactly.
	h.reconcileBuildHook = func(actor.ActorID) { tctx.cancel() }

	desired.set(actorrt.DesiredMember{ID: id, Kind: actor.KindAgent, Lifecycle: actorrt.LifecycleAlwaysOn})
	h.reconcileActivation(tctx)

	if live(h, id) {
		t.Fatal("RecheckFault must despawn the freshly built cell")
	}
	if h.prevEagerDesired[id] {
		t.Fatal("RecheckFault must drop current for this id")
	}
}

// RecheckGone: verifyPostBuild's own not-a-member outcome — despawns the
// cell, drops current, and cascades RemoveSubjectSlot (§0.5 相邻旧账
// clarification: the registry-Membership removal is applied DIRECTLY here
// — bypassing Home.Remove — so the assertion isolates verifyPostBuild's OWN
// cascade call rather than Remove's own (Remove already calls
// RemoveSubjectSlot/Forget itself, which would otherwise mask this).
// presenceFold.Forget has no exported existence read, so per §0.5's
// explicit downgrade allowance that half is characterized only via the
// Despawn assertion above, not independently re-asserted.
func TestCharRing_RecheckGone_DespawnsCell_DropsCurrent_CascadesSubjectSlot(t *testing.T) {
	ctx := context.Background()
	desired := &testDesired{}
	h := openCharHome(t, desired, newTestBuilder(), nil)

	human := admit(t, h, actor.ActorID("user:char-gone"), actor.KindHuman)
	h.reconcileBuildHook = func(built actor.ActorID) {
		if built != human {
			return
		}
		if err := h.cs.Membership.ApplyMemberTransitions(ctx, nil, []storespec.MemberActorRemove{
			{ID: human, At: h.nowMs()},
		}); err != nil {
			t.Errorf("direct membership remove: %v", err)
		}
	}

	h.reconcileActivation(ctx) // user域: no desired.set needed, admit alone seeds it

	if live(h, human) {
		t.Fatal("RecheckGone must despawn the straddled cell")
	}
	if h.prevEagerDesired[human] {
		t.Fatal("RecheckGone must drop current for this id")
	}
	if _, ok := h.SubjectSlotFor(human); ok {
		t.Fatal("RecheckGone must cascade RemoveSubjectSlot (verifyPostBuild's own cascade, §0.5 dissolved account)")
	}
}

// §1.9 环全矩阵零变 anchor: cancel landing during a build that ALSO fails
// hits the ring's OWN ctx gate first (built=false, so buildErr is never even
// inspected) — the ring's cancel-interaction behavior is untouched by the
// extraction (unlike the reviver's three flipped cells below).
func TestCharRingMatrix_CancelledAndBuildFailed_CtxGateWinsBeforeBuildErr(t *testing.T) {
	desired := &testDesired{}
	builder := newTestBuilder()
	id := actor.ActorID("agent:char-matrix-cancel-buildfail")

	tctx := newToggleCtx()
	builder.byID[id] = hostcommon.CapsFactory(func(actorcaps.Caps) actorrt.Actor {
		tctx.cancel()
		panic("char test: cancelled build also fails")
	})

	logger, rh := newRecordingLogger()
	h := openCharHome(t, desired, builder, logger)
	id = admit(t, h, id, actor.KindAgent)
	desired.set(actorrt.DesiredMember{ID: id, Kind: actor.KindAgent, Lifecycle: actorrt.LifecycleAlwaysOn})

	h.reconcileActivation(tctx)

	if live(h, id) {
		t.Fatal("member must not be live")
	}
	if h.prevEagerDesired[id] {
		t.Fatal("current must not carry the id (ctx gate wins, not the BuildFailed continue arm)")
	}
	if e := backoffEntry(t, h, id); e.failures != 0 {
		t.Fatalf("ctx gate must win BEFORE buildErr classification — no backoff account written, got %+v", e)
	}
	if rh.has("platform.reconcile.build_failed") {
		t.Fatal("ctx gate must win BEFORE buildErr classification — no build_failed log")
	}
}

// ===========================================================================
// reviver (homeReviver.EnsureLive) — table §1.7, one test per word.
// ===========================================================================

// Embodied (build succeeds, recheck OK): nil, clearReviveBackoff.
func TestCharReviver_Embodied_ClearsBackoffReturnsNil(t *testing.T) {
	ctx := context.Background()
	desired := &testDesired{}
	builder := newTestBuilder()
	id := actor.ActorID("agent:char-rv-embodied")
	builder.byID[id] = builder.recordFactory(id)

	h := openCharHome(t, desired, builder, nil)
	id = admit(t, h, id, actor.KindAgent)
	h.recordBuildFailure(id, time.Now().Add(-time.Hour)) // already-elapsed window — must actually build

	if err := (homeReviver{h: h}).EnsureLive(ctx, id); err != nil {
		t.Fatalf("EnsureLive: %v", err)
	}
	if !live(h, id) {
		t.Fatal("Embodied must be live")
	}
	if e := backoffEntry(t, h, id); e.failures != 0 {
		t.Fatalf("Embodied must clear the backoff account, got %+v", e)
	}
}

// AlreadyLive (⑤ CAS loser, real interleave): nil, clearReviveBackoff.
func TestCharReviver_AlreadyLive_CASLoser_ClearsBackoffReturnsNil(t *testing.T) {
	ctx := context.Background()
	desired := &testDesired{}
	builder := newTestBuilder()
	id := actor.ActorID("agent:char-rv-cas-loser")

	started := make(chan struct{})
	release := make(chan struct{})
	builder.byID[id] = blockingFactory(started, release)

	h := openCharHome(t, desired, builder, nil)
	id = admit(t, h, id, actor.KindAgent)
	h.recordBuildFailure(id, time.Now().Add(-time.Hour)) // already-elapsed window — must actually build

	errCh := make(chan error, 1)
	go func() { errCh <- (homeReviver{h: h}).EnsureLive(ctx, id) }()

	<-started
	if _, built, err := h.channel.Cells().SpawnIfAbsent(id, actor.KindAgent, func(inc actorrt.Incarnation) actorrt.Actor {
		return hostcommon.Build(h.buildCaps(id, actor.KindAgent, inc), h.hooks(), builder.recordFactory(id))
	}); err != nil || !built {
		t.Fatalf("racer spawn: built=%v err=%v", built, err)
	}
	close(release)
	if err := <-errCh; err != nil {
		t.Fatalf("EnsureLive (CAS loser): %v", err)
	}

	if !live(h, id) {
		t.Fatal("id must be live (the racer's build won)")
	}
	if e := backoffEntry(t, h, id); e.failures != 0 {
		t.Fatalf("AlreadyLive must clear the backoff account, got %+v", e)
	}
}

// NotMember: ReviveRejected{not_a_member}, no log, backoff account untouched.
func TestCharReviver_NotMember_RejectedNoLogNoBackoffTouch(t *testing.T) {
	ctx := context.Background()
	desired := &testDesired{}
	builder := newTestBuilder()
	id := actor.ActorID("agent:char-rv-not-member")
	builder.byID[id] = builder.recordFactory(id)

	logger, rh := newRecordingLogger()
	h := openCharHome(t, desired, builder, logger)
	id = admit(t, h, id, actor.KindAgent)
	if err := h.cs.Membership.ApplyMemberTransitions(ctx, nil, []storespec.MemberActorRemove{{ID: id, At: h.nowMs()}}); err != nil {
		t.Fatalf("deregister: %v", err)
	}

	rh.reset()
	var rejected schedule.ReviveRejected
	err := (homeReviver{h: h}).EnsureLive(ctx, id)
	if !errors.As(err, &rejected) || rejected.Reason != "not_a_member" {
		t.Fatalf("EnsureLive(dereg'd) = %v, want ReviveRejected{not_a_member}", err)
	}
	if live(h, id) {
		t.Fatal("NotMember must not revive")
	}
	if rh.count() != 0 {
		t.Fatalf("NotMember must not log, got %d records", rh.count())
	}
	if e := backoffEntry(t, h, id); e.failures != 0 {
		t.Fatalf("NotMember must not touch the backoff account, got %+v", e)
	}
}

// NoFactory (no_builder split): nil-builder home, absent author.
func TestCharReviver_NoFactory_NoBuilder_Rejected(t *testing.T) {
	ctx := context.Background()
	h := openCharHome(t, &testDesired{}, nil, nil) // nil builder — legal
	id := admit(t, h, actor.ActorID("agent:char-rv-no-builder"), actor.KindAgent)

	var rejected schedule.ReviveRejected
	err := (homeReviver{h: h}).EnsureLive(ctx, id)
	if !errors.As(err, &rejected) || rejected.Reason != "no_builder" {
		t.Fatalf("EnsureLive(nil-builder, absent) = %v, want ReviveRejected{no_builder}", err)
	}
}

// NoFactory (class_not_found split): wired builder, but the id has no entry.
func TestCharReviver_NoFactory_ClassNotFound_Rejected(t *testing.T) {
	ctx := context.Background()
	builder := newTestBuilder() // deliberately no byID[id] entry
	h := openCharHome(t, &testDesired{}, builder, nil)
	id := admit(t, h, actor.ActorID("agent:char-rv-class-not-found"), actor.KindAgent)

	var rejected schedule.ReviveRejected
	err := (homeReviver{h: h}).EnsureLive(ctx, id)
	if !errors.As(err, &rejected) || rejected.Reason != "class_not_found" {
		t.Fatalf("EnsureLive(wired builder, no entry) = %v, want ReviveRejected{class_not_found}", err)
	}
}

// Attached: transient error, logReviveAttached (throttled) fires.
func TestCharReviver_Attached_TransientWithThrottledLog(t *testing.T) {
	ctx := context.Background()
	builder := newTestBuilder()
	id := actor.ActorID("agent:char-rv-attached")
	builder.byID[id] = builder.recordFactory(id)

	logger, rh := newRecordingLogger()
	h := openCharHome(t, &testDesired{}, builder, logger)
	id = admit(t, h, id, actor.KindAgent)
	if err := h.cs.Membership.ApplyMemberTransitions(ctx, []storespec.MemberActorAdd{
		{ID: id, Kind: actor.KindAgent, Host: "daemon-char-rv", At: h.nowMs()},
	}, nil); err != nil {
		t.Fatalf("attach host: %v", err)
	}

	var rejected schedule.ReviveRejected
	err := (homeReviver{h: h}).EnsureLive(ctx, id)
	if err == nil || errors.As(err, &rejected) {
		t.Fatalf("EnsureLive(attached) = %v, want a plain transient error", err)
	}
	if !rh.has("platform.revive.attached") {
		t.Fatal("Attached must log platform.revive.attached (throttled)")
	}
}

// BackoffHeld: transient error, no log, no build attempt.
func TestCharReviver_BackoffHeld_TransientNoLog_NoBuildAttempt(t *testing.T) {
	ctx := context.Background()
	builder := newTestBuilder()
	id := actor.ActorID("agent:char-rv-backoff-held")
	var builds int
	builder.byID[id] = hostcommon.CapsFactory(func(actorcaps.Caps) actorrt.Actor {
		builds++
		return recordActor{}
	})

	logger, rh := newRecordingLogger()
	h := openCharHome(t, &testDesired{}, builder, logger)
	id = admit(t, h, id, actor.KindAgent)
	h.recordBuildFailure(id, time.Now())

	rh.reset()
	var rejected schedule.ReviveRejected
	err := (homeReviver{h: h}).EnsureLive(ctx, id)
	if err == nil || errors.As(err, &rejected) {
		t.Fatalf("EnsureLive(backoff held) = %v, want a plain transient error", err)
	}
	if builds != 0 {
		t.Fatalf("BackoffHeld must skip the build attempt entirely, got %d builds", builds)
	}
	if rh.count() != 0 {
		t.Fatalf("BackoffHeld must not log, got %d records", rh.count())
	}
}

// Sealed: transient error (the raw buildErr), info log.
func TestCharReviver_Sealed_InfoLogTransient(t *testing.T) {
	ctx := context.Background()
	builder := newTestBuilder()
	id := actor.ActorID("agent:char-rv-sealed")
	builder.byID[id] = builder.recordFactory(id)

	logger, rh := newRecordingLogger()
	h := openCharHome(t, &testDesired{}, builder, logger)
	id = admit(t, h, id, actor.KindAgent)
	h.channel.Cells().Seal()

	var rejected schedule.ReviveRejected
	err := (homeReviver{h: h}).EnsureLive(ctx, id)
	if err == nil || errors.As(err, &rejected) || !errors.Is(err, actorrt.ErrRuntimeSealed) {
		t.Fatalf("EnsureLive(sealed) = %v, want the raw ErrRuntimeSealed (transient, not rejected)", err)
	}
	if !rh.has("platform.revive.runtime_sealed") {
		t.Fatal("Sealed must info-log platform.revive.runtime_sealed")
	}
}

// BuildFailed: transient error, error log, backoff account recorded.
func TestCharReviver_BuildFailed_ErrorLog_RecordsBackoff_Transient(t *testing.T) {
	ctx := context.Background()
	builder := newTestBuilder()
	id := actor.ActorID("agent:char-rv-build-failed")
	builder.byID[id] = panicFactory()

	logger, rh := newRecordingLogger()
	h := openCharHome(t, &testDesired{}, builder, logger)
	id = admit(t, h, id, actor.KindAgent)

	var rejected schedule.ReviveRejected
	err := (homeReviver{h: h}).EnsureLive(ctx, id)
	if err == nil || errors.As(err, &rejected) {
		t.Fatalf("EnsureLive(build failed) = %v, want a plain transient error", err)
	}
	if !rh.has("platform.revive.build_failed") {
		t.Fatal("BuildFailed must error-log platform.revive.build_failed")
	}
	if e := backoffEntry(t, h, id); e.failures != 1 {
		t.Fatalf("BuildFailed must record one backoff step, got %+v", e)
	}
}

// RecheckFault: verifyPostBuild's OWN ctx gate, reached via
// reviverStraddleHook (fires before SpawnIfAbsent; nothing rechecks ctx
// between the build landing and verifyPostBuild's own Lookup, so the
// cancellation is still in effect when it runs) — transient, current wording
// is the SAME "post-build recheck" wrap the RecheckGone/Sealed cases don't
// share (T1/T2 will split the Cancelled wording out of this branch per §1.9②;
// this test pins the PRE-split text).
func TestCharReviver_RecheckFault_TransientPostBuildRecheckWrap(t *testing.T) {
	builder := newTestBuilder()
	id := actor.ActorID("agent:char-rv-recheck-fault")
	builder.byID[id] = builder.recordFactory(id)

	tctx := newToggleCtx()
	h := openCharHome(t, &testDesired{}, builder, nil)
	id = admit(t, h, id, actor.KindAgent)
	h.reviverStraddleHook = func() { tctx.cancel() }

	var rejected schedule.ReviveRejected
	err := (homeReviver{h: h}).EnsureLive(tctx, id)
	if err == nil || errors.As(err, &rejected) {
		t.Fatalf("EnsureLive(recheck fault) = %v, want a plain transient error", err)
	}
	if live(h, id) {
		t.Fatal("RecheckFault must despawn the freshly built cell")
	}
}

// RecheckGone: ReviveRejected{not_a_member}, cascades RemoveSubjectSlot
// (same isolation rationale as the ring's mirror test above — the membership
// removal is applied DIRECTLY, bypassing Home.Remove's own cascade call).
func TestCharReviver_RecheckGone_Rejected_CascadesSubjectSlot(t *testing.T) {
	ctx := context.Background()
	h := openCharHome(t, &testDesired{}, newTestBuilder(), nil)
	human := admit(t, h, actor.ActorID("user:char-rv-gone"), actor.KindHuman)
	h.reviverStraddleHook = func() {
		if err := h.cs.Membership.ApplyMemberTransitions(ctx, nil, []storespec.MemberActorRemove{
			{ID: human, At: h.nowMs()},
		}); err != nil {
			t.Errorf("direct membership remove: %v", err)
		}
	}

	var rejected schedule.ReviveRejected
	err := (homeReviver{h: h}).EnsureLive(ctx, human)
	if !errors.As(err, &rejected) || rejected.Reason != "not_a_member" {
		t.Fatalf("EnsureLive(recheck gone) = %v, want ReviveRejected{not_a_member}", err)
	}
	if live(h, human) {
		t.Fatal("RecheckGone must despawn the straddled cell")
	}
	if _, ok := h.SubjectSlotFor(human); ok {
		t.Fatal("RecheckGone must cascade RemoveSubjectSlot")
	}
}

// ===========================================================================
// §1.9 reviver 取消交叉矩阵 — POST-T1 behavior, all three cells (spec §1.9
// 申报臂: flipped by T1's extraction, since activateOne's shared ctx gate now
// applies to the reviver's path too — see the core's ctx 闸 comment). A prior
// revision of these three tests pinned the PRE-extraction "现状" (buildErr/
// sealed/CAS-loser win over a same-window cancel); T1 flips all three
// uniformly to `actCancelled` per spec §1.9①, replacing those assertions
// with this file's own diff record.
// ===========================================================================

// 取消∧建造失败 (spec §1.9①, flipped by T1): the core's ctx gate now wins
// BEFORE buildErr classification — no backoff account write, no build_failed
// log, a Cancelled transient instead of the raw buildErr.
func TestCharReviverMatrix_CancelledAndBuildFailed_FlippedToCancelled(t *testing.T) {
	builder := newTestBuilder()
	id := actor.ActorID("agent:char-rv-matrix-buildfail")
	builder.byID[id] = panicFactory()

	logger, rh := newRecordingLogger()
	h := openCharHome(t, &testDesired{}, builder, logger)
	id = admit(t, h, id, actor.KindAgent)

	// Cancel via reviverStraddleHook — AFTER the membership Lookup/backoffGate/
	// factoryFor sequence (which itself honors ctx and would short-circuit into
	// a DIFFERENT transient wrap if cancelled before it even starts), landing
	// the cancel in the exact "post-spawn" window §1.9 describes.
	tctx := newToggleCtx()
	h.reviverStraddleHook = func() { tctx.cancel() }

	err := (homeReviver{h: h}).EnsureLive(tctx, id)
	if err == nil {
		t.Fatal("want the Cancelled transient, got nil")
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("spec §1.9①: cancelled∧build-failed must now return the Cancelled wrap, got %v", err)
	}
	if rh.has("platform.revive.build_failed") {
		t.Fatal("spec §1.9①: cancelled∧build-failed must NOT error-log build_failed anymore — the ctx gate wins first")
	}
	if e := backoffEntry(t, h, id); e.failures != 0 {
		t.Fatalf("spec §1.9①: cancelled∧build-failed must NOT record a backoff step anymore, got %+v", e)
	}
}

// 取消∧Sealed (spec §1.9①, flipped by T1): the core's ctx gate wins before
// the buildErr is even inspected — no info log, a Cancelled transient instead
// of the raw ErrRuntimeSealed.
func TestCharReviverMatrix_CancelledAndSealed_FlippedToCancelled(t *testing.T) {
	builder := newTestBuilder()
	id := actor.ActorID("agent:char-rv-matrix-sealed")
	builder.byID[id] = builder.recordFactory(id)

	logger, rh := newRecordingLogger()
	h := openCharHome(t, &testDesired{}, builder, logger)
	id = admit(t, h, id, actor.KindAgent)
	h.channel.Cells().Seal()

	tctx := newToggleCtx()
	h.reviverStraddleHook = func() { tctx.cancel() } // see rationale above

	err := (homeReviver{h: h}).EnsureLive(tctx, id)
	if errors.Is(err, actorrt.ErrRuntimeSealed) {
		t.Fatalf("spec §1.9①: cancelled∧sealed must NOT return the raw ErrRuntimeSealed anymore, got %v", err)
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("spec §1.9①: cancelled∧sealed must now return the Cancelled wrap, got %v", err)
	}
	if rh.has("platform.revive.runtime_sealed") {
		t.Fatal("spec §1.9①: cancelled∧sealed must NOT info-log runtime_sealed anymore — the ctx gate wins first")
	}
}

// 取消∧CAS 输家 (spec §1.9①, flipped by T1): the core's ctx gate now applies
// to the !built branch too — no backoff clear, a Cancelled transient instead
// of nil.
func TestCharReviverMatrix_CancelledAndCASLoser_FlippedToCancelled(t *testing.T) {
	builder := newTestBuilder()
	id := actor.ActorID("agent:char-rv-matrix-casloser")

	started := make(chan struct{})
	release := make(chan struct{})
	builder.byID[id] = blockingFactory(started, release)

	h := openCharHome(t, &testDesired{}, builder, nil)
	id = admit(t, h, id, actor.KindAgent)
	h.recordBuildFailure(id, time.Now().Add(-time.Hour)) // already-elapsed window — must actually build

	tctx := newToggleCtx()
	errCh := make(chan error, 1)
	go func() { errCh <- (homeReviver{h: h}).EnsureLive(tctx, id) }()

	<-started
	if _, built, err := h.channel.Cells().SpawnIfAbsent(id, actor.KindAgent, func(inc actorrt.Incarnation) actorrt.Actor {
		return hostcommon.Build(h.buildCaps(id, actor.KindAgent, inc), h.hooks(), builder.recordFactory(id))
	}); err != nil || !built {
		t.Fatalf("racer spawn: built=%v err=%v", built, err)
	}
	tctx.cancel() // cancel BEFORE releasing — the core's ctx gate now consults it on this branch too
	close(release)

	err := <-errCh
	if err == nil {
		t.Fatal("spec §1.9①: cancelled∧CAS-loser must now return the Cancelled transient, not nil")
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("spec §1.9①: cancelled∧CAS-loser must return the Cancelled wrap, got %v", err)
	}
	if e := backoffEntry(t, h, id); e.failures == 0 {
		t.Fatalf("spec §1.9①: cancelled∧CAS-loser must NOT clear the backoff account anymore, got %+v", e)
	}
}
