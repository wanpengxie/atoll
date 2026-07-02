package runtime

import (
	"context"
	"errors"
	"log/slog"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/runtime/actorrt"
	"github.com/wanpengxie/atoll/runtime/harness"
	"github.com/wanpengxie/atoll/runtime/schedule"
	"github.com/wanpengxie/atoll/runtime/storespec"
	"github.com/wanpengxie/atoll/runtime/timerspec"
)

// This file is the runtime-package-root INTEGRATION counterpart to
// runtime/schedule's own in-package unit tests (engine_test.go): those drive
// the engine with fakeStore/fakeFireSink/fakeReviver injected directly; this
// file drives the WHOLE time axis assembled by OpenChannel + OpenScheduler
// over a real per-channel sqlite (runtime/internal/store), a real harness
// write chain (runtime/harness, self-assembled here exactly the way the
// platform FireSink wiring will — since a runtime-tree TEST file is not
// subject to the harness-construction-confined-to-platform archtest wall,
// which excludes _test.go files by construction), and a real
// *actorrt.Runtime as both the LivenessProbe and the Reviver's
// SpawnIfAbsent backend. Only the Clock is fake — schedule.Engine.New
// requires an injected Clock precisely so a test never falls back to the
// wall clock, keeping firing correctness deterministic and testable.
//
// Each TestTimerSlice<N>_* traces to a numbered vertical-slice list over a
// decision table. Two deliberate scope boundaries, both spec-mandated:
//
//   - Slice 1 (and every other slice) asserts truth via cs.Query.ReadAfterSeq
//     — NEVER mailbox delivery. pump/fanout live in platform/internal and are
//     unreachable from the runtime tree (the engine does not hold a
//     Deliverer — its whole job stops at append); mailbox-reaches-actor is
//     the platform integration test's job, not this one's.
//   - The FireSink tri-state contract is proven against the REAL harness
//     chain wherever the reject/dup path is a genuine harness verdict
//     (duplicate messages.id, reserved-type rejection); synthetic Go errors
//     (flakyFireSink) are used ONLY to drive the transient/backoff PACING
//     paths, which the contract explicitly allows to be "any real Go error"
//     — not a harness-specific shape.
//
// The determinism discipline has no dedicated test slice: it is the
// cross-cutting discipline every slice below already satisfies by
// construction — FIRING correctness never depends on wall-clock time, only
// on fakeClock.Advance; the handful of time.Sleep/waitFor calls are
// test-synchronization polling for the run-loop goroutine's asynchronous
// effects to become observable (mirrors schedule/fakes_test.go's own
// waitFor doc), never something a timing assertion depends on.

// scheduleTestChannelID is this file's fixed channel scope — every test opens
// its own tempdir/db (openScheduleChannel), so a shared constant id across
// files causes no collision (separate sqlite files, never the same registry).
const scheduleTestChannelID = channel.ID("c-timer")

func openScheduleChannel(t *testing.T) *ChannelStores {
	t.Helper()
	dir := t.TempDir()
	cs, err := OpenChannel(context.Background(), scheduleTestChannelID,
		filepath.Join(dir, "channel.sqlite"), OpenChannelOptions{})
	if err != nil {
		t.Fatalf("OpenChannel: %v", err)
	}
	t.Cleanup(func() { _ = cs.Close() })
	return cs
}

// newScheduleRuntime builds a real *actorrt.Runtime — the engine's
// LivenessProbe (CurrentIncarnation/IsLive) AND the backend testReviver
// drives via SpawnIfAbsent, the real activation seam, not a fake.
func newScheduleRuntime(t *testing.T) *actorrt.Runtime {
	t.Helper()
	rt, _ := actorrt.New(actorrt.Config{Parent: context.Background()})
	t.Cleanup(rt.StopAll)
	return rt
}

// ---------------------------------------------------------------------
// realFireSink — the runtime-tree self-assembled realization of the
// platform FireSink design ("mint-a-pen-per-fire" + the tri-state
// WriteResult translation). This IS the reference implementation the
// (deferred) platform wiring will copy; building it here is this suite's
// whole reason to exist as an INTEGRATION test rather than another layer of
// engine_test.go's fakes.
// ---------------------------------------------------------------------

// The id-duplicate reject is now a closed-set member
// (harness.HarnessIDDuplicateConflict, canonical string in storespec) — this
// sink compares against the shared symbol, exactly like the real platform
// FireSink will.
const harnessIDDuplicateConflict = harness.HarnessIDDuplicateConflict

type realFireSink struct {
	minter harness.Minter
	chID   channel.ID

	mu    sync.Mutex
	calls int
}

func newRealFireSink(t *testing.T, cs *ChannelStores) *realFireSink {
	t.Helper()
	minter, err := harness.New(harness.Deps{ChannelID: scheduleTestChannelID, Log: cs.Log})
	if err != nil {
		t.Fatalf("harness.New: %v", err)
	}
	return &realFireSink{minter: minter, chID: scheduleTestChannelID}
}

// Append mints a fresh Pen per call (Mint is cheap) and translates
// harness.WriteResult into the FireSink tri-state contract: a naive
// `_, err := pen.Write(...); return err` would swallow a deterministic
// reject into a false nil and let the engine silently drop the fire — that
// failure mode is the entire reason this translation exists.
func (s *realFireSink) Append(ctx context.Context, author actor.ActorID, env *message.Envelope) error {
	s.mu.Lock()
	s.calls++
	s.mu.Unlock()

	pen := s.minter.Mint(author, actor.KindAgent, s.chID)
	res, err := pen.Write(ctx, env)
	if err != nil {
		return err // transient: a genuine Go error, engine leaves the row for retry.
	}
	if res.Accepted() {
		return nil
	}
	if res.RejectReason == harnessIDDuplicateConflict && res.MessageID == env.ID {
		return schedule.ErrDuplicateFire
	}
	return schedule.FireRejected{Reason: string(res.RejectReason), Detail: res.RejectDetail}
}

func (s *realFireSink) callCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls
}

var _ schedule.FireSink = (*realFireSink)(nil)

// flakyFireSink wraps a real FireSink and can be told to fail the next N
// Append calls (or fail indefinitely) with a synthetic transient Go error
// before delegating — the deterministic way to drive the engine's
// leave-the-row-and-retry / backoff-pacing paths without corrupting a real
// store. The tri-state contract explicitly allows "a genuine Go error" here
// — passed through as-is; it does not have to originate from the real
// harness the way the dup/reject paths must.
type flakyFireSink struct {
	inner schedule.FireSink

	mu         sync.Mutex
	failLeft   int
	alwaysFail bool
	calls      int
}

func (f *flakyFireSink) failNext(n int) {
	f.mu.Lock()
	f.failLeft = n
	f.mu.Unlock()
}

// setAlwaysFail flips the permanent-failure switch. Used (rather than a
// one-shot failLeft counter) wherever a test needs to observe "the row is
// STILL retained after a bounded real-time window" without a race against a
// stray wake token slipping an early retry through before the observation —
// while alwaysFail holds, EVERY retry (whatever triggers it) deterministically
// fails, so there is no timing window in which the row could have already
// been deleted by a lucky early success.
func (f *flakyFireSink) setAlwaysFail(v bool) {
	f.mu.Lock()
	f.alwaysFail = v
	f.mu.Unlock()
}

func (f *flakyFireSink) Append(ctx context.Context, author actor.ActorID, env *message.Envelope) error {
	f.mu.Lock()
	f.calls++
	fail := f.alwaysFail
	if !fail && f.failLeft > 0 {
		f.failLeft--
		fail = true
	}
	f.mu.Unlock()
	if fail {
		return errors.New("flakyFireSink: injected transient failure")
	}
	return f.inner.Append(ctx, author, env)
}

func (f *flakyFireSink) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

var _ schedule.FireSink = (*flakyFireSink)(nil)

// ---------------------------------------------------------------------
// testReviver — schedule.Reviver wired to a REAL *actorrt.Runtime via
// SpawnIfAbsent, the real activation seam, with a tiny test factory
// standing in for the deferred platform builder registry.
// ---------------------------------------------------------------------

type stubTimerActor struct{}

func (stubTimerActor) Receive(context.Context, *message.Envelope) error { return nil }

type testReviver struct {
	rt *actorrt.Runtime

	mu      sync.Mutex
	calls   []actor.ActorID
	failFor map[actor.ActorID]int
}

func newTestReviver(rt *actorrt.Runtime) *testReviver {
	return &testReviver{rt: rt, failFor: make(map[actor.ActorID]int)}
}

// failNextFor scripts the next n EnsureLive(id) calls to fail before the
// (idempotent, SpawnIfAbsent-backed) real activation is allowed to succeed.
func (r *testReviver) failNextFor(id actor.ActorID, n int) {
	r.mu.Lock()
	r.failFor[id] = n
	r.mu.Unlock()
}

// allowSucceedFor clears any remaining scripted failure count for id — the
// NEXT EnsureLive(id) call (and every one after it) succeeds. Paired with
// failNextFor's large-count form to observe "retained through a bounded
// real-time window" WITHOUT a race against an early retry slipping through:
// while the count stays large, every retry (however it is triggered)
// deterministically fails, so there is no timing window in which a lucky
// early success could already have fired.
func (r *testReviver) allowSucceedFor(id actor.ActorID) {
	r.mu.Lock()
	r.failFor[id] = 0
	r.mu.Unlock()
}

func (r *testReviver) callCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.calls)
}

func (r *testReviver) EnsureLive(ctx context.Context, id actor.ActorID) error {
	r.mu.Lock()
	r.calls = append(r.calls, id)
	if n := r.failFor[id]; n > 0 {
		r.failFor[id] = n - 1
		r.mu.Unlock()
		return errors.New("testReviver: injected revive failure")
	}
	r.mu.Unlock()
	// SpawnIfAbsent semantics: idempotent no-op for an already-live author
	// (EnsureLive MUST be idempotent) — the real CAS mint, not a fake
	// standing in for it.
	r.rt.SpawnIfAbsent(id, func(actorrt.Incarnation) actorrt.Actor { return stubTimerActor{} })
	return nil
}

var _ schedule.Reviver = (*testReviver)(nil)

// ---------------------------------------------------------------------
// fakeClock — the runtime-package-root twin of schedule/fakes_test.go's own
// fakeClock (package-private to that test binary, so this different package
// needs its own copy of the same semantics to satisfy schedule.Clock
// deterministically, zero wall-clock dependence for firing correctness).
// ---------------------------------------------------------------------

type fakeAlarm struct {
	mu       sync.Mutex
	deadline time.Time
	ch       chan time.Time
	fired    bool
	stopped  bool
}

func (a *fakeAlarm) C() <-chan time.Time { return a.ch }

func (a *fakeAlarm) Stop() bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.fired || a.stopped {
		return false
	}
	a.stopped = true
	return true
}

func (a *fakeAlarm) markDueIfPending(now time.Time) bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.stopped || a.fired || a.deadline.After(now) {
		return false
	}
	a.fired = true
	return true
}

func (a *fakeAlarm) settled() bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.stopped || a.fired
}

type fakeClock struct {
	mu      sync.Mutex
	now     time.Time
	pending []*fakeAlarm
}

func newFakeClock(start time.Time) *fakeClock { return &fakeClock{now: start} }

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

// NewAlarm mirrors clock.go's ABSOLUTE-deadline contract: a deadline already
// <= now fires at once (a stale re-arm racing a just-advanced clock must not
// wait forever for a future Advance nobody calls again).
func (c *fakeClock) NewAlarm(deadline time.Time) schedule.Timer {
	c.mu.Lock()
	defer c.mu.Unlock()
	a := &fakeAlarm{deadline: deadline, ch: make(chan time.Time, 1)}
	if !deadline.After(c.now) {
		a.fired = true
		a.ch <- c.now
	} else {
		c.pending = append(c.pending, a)
	}
	return a
}

// Advance moves the clock forward by d and fires every still-pending alarm
// whose deadline is now due (non-blocking send into each alarm's own
// buffered channel), mirroring real timers all firing once their deadline
// passes.
func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(d)
	now := c.now
	var due, remaining []*fakeAlarm
	for _, a := range c.pending {
		if a.markDueIfPending(now) {
			due = append(due, a)
		} else if !a.settled() {
			remaining = append(remaining, a)
		}
	}
	c.pending = remaining
	c.mu.Unlock()
	for _, a := range due {
		a.ch <- now
	}
}

var _ schedule.Clock = (*fakeClock)(nil)

// ---------------------------------------------------------------------
// logCapture — a minimal in-memory slog.Handler for asserting the "loud
// disposal log" actually fired (obs/log plane, not truth).
// ---------------------------------------------------------------------

type logCapture struct {
	mu      sync.Mutex
	records []slog.Record
}

func (c *logCapture) Enabled(context.Context, slog.Level) bool { return true }

func (c *logCapture) Handle(_ context.Context, r slog.Record) error {
	c.mu.Lock()
	c.records = append(c.records, r.Clone())
	c.mu.Unlock()
	return nil
}

func (c *logCapture) WithAttrs([]slog.Attr) slog.Handler { return c }
func (c *logCapture) WithGroup(string) slog.Handler      { return c }

func (c *logCapture) hasMessage(substr string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, r := range c.records {
		if strings.Contains(r.Message, substr) {
			return true
		}
	}
	return false
}

var _ slog.Handler = (*logCapture)(nil)

// ---------------------------------------------------------------------
// Shared read/poll helpers.
// ---------------------------------------------------------------------

// farFutureMs bounds storeRowCount's Due() scan without depending on
// wall-clock time (this file's tests only ever use fakeClock instants near
// 1_000_000ms, so any far-future sentinel safely dominates every FireAt used
// below while staying well inside sqlite's signed 64-bit INTEGER range).
const farFutureMs = int64(1) << 61

func storeRowCount(t *testing.T, cs *ChannelStores) int {
	t.Helper()
	rows, err := cs.timers.Due(context.Background(), farFutureMs, 10000)
	if err != nil {
		t.Fatalf("timers.Due: %v", err)
	}
	return len(rows)
}

func readAllTruth(t *testing.T, cs *ChannelStores) []storespec.StoredRow {
	t.Helper()
	rows, err := cs.Query.ReadAfterSeq(context.Background(), 0, 10000)
	if err != nil {
		t.Fatalf("ReadAfterSeq: %v", err)
	}
	return rows
}

func findByID(rows []storespec.StoredRow, id message.ID) (storespec.StoredRow, bool) {
	for _, r := range rows {
		if r.Envelope.ID == id {
			return r, true
		}
	}
	return storespec.StoredRow{}, false
}

func fireMsgID(id schedule.TimerID) message.ID { return message.ID("timer:" + string(id)) }

func waitFor(t *testing.T, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		if cond() {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("condition not met within %s", timeout)
		}
		time.Sleep(time.Millisecond)
	}
}

// advanceUntil nudges the fake clock forward repeatedly (rather than one big
// jump) so a stale alarm left over from an already-superseded schedule
// (Cancel-then-reschedule to the same instant) cannot race a single Advance
// into missing the fresh alarm the engine re-arms afterwards.
func advanceUntil(t *testing.T, clock *fakeClock, step time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		if cond() {
			return
		}
		clock.Advance(step)
		if time.Now().After(deadline) {
			t.Fatalf("advanceUntil: condition not met within the deadline")
		}
		time.Sleep(time.Millisecond)
	}
}

// waitStable polls val until it stops changing for a quiet window — used
// where a coalesced wake token can legitimately cause one extra bounded
// transition before a loop's steady state holds.
func waitStable(t *testing.T, val func() int, quiet time.Duration) int {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	last := val()
	lastChange := time.Now()
	for {
		time.Sleep(time.Millisecond)
		cur := val()
		if cur != last {
			last = cur
			lastChange = time.Now()
		}
		if time.Since(lastChange) >= quiet {
			return last
		}
		if time.Now().After(deadline) {
			t.Fatalf("value never stabilized (last=%d)", last)
		}
	}
}

// ---------------------------------------------------------------------
// Slice 1 — basic fire: truth lands in log, every envelope field checked
// against the STORED (post-harness-normalized) row.
// ---------------------------------------------------------------------

func TestTimerSlice1_BasicFireTruthFields(t *testing.T) {
	ctx := context.Background()
	cs := openScheduleChannel(t)
	sink := newRealFireSink(t, cs)
	rt := newScheduleRuntime(t)
	clock := newFakeClock(time.UnixMilli(1_000_000))
	revive := newTestReviver(rt)

	minter, engine, err := OpenScheduler(cs, schedule.AssemblyDeps{Fire: sink, Host: rt, Revive: revive, Clock: clock})
	if err != nil {
		t.Fatalf("OpenScheduler: %v", err)
	}
	engine.Start()
	t.Cleanup(engine.Close)

	const author = actor.ActorID("timer-author-1")
	handle := minter.Mint(author)

	fireAt := clock.Now().Add(time.Hour).UnixMilli()
	id, err := handle.Schedule(ctx, schedule.ScheduleReq{
		Bind: schedule.BindIdentity, FireAt: fireAt, Type: "demo.tick",
		Payload: []byte(`{"k":"v"}`), CorrelationID: "corr-slice1",
	})
	if err != nil {
		t.Fatalf("Schedule: %v", err)
	}

	clock.Advance(time.Hour)

	wantID := fireMsgID(id)
	var row storespec.StoredRow
	waitFor(t, 2*time.Second, func() bool {
		var ok bool
		row, ok = findByID(readAllTruth(t, cs), wantID)
		return ok
	})

	env := row.Envelope
	if env.Sender.ID != author {
		t.Fatalf("sender.id = %q, want %q (pen-welded)", env.Sender.ID, author)
	}
	if env.Sender.Kind != actor.KindAgent {
		t.Fatalf("sender.kind = %q, want %q (pen-welded from Mint)", env.Sender.Kind, actor.KindAgent)
	}
	if env.ChannelID != scheduleTestChannelID {
		t.Fatalf("channel_id = %q, want %q (pen-welded)", env.ChannelID, scheduleTestChannelID)
	}
	if env.Kind != message.KindEvent {
		t.Fatalf("kind = %q, want event (拍点 8.3)", env.Kind)
	}
	if len(env.Audience) != 1 || env.Audience[0] != author {
		t.Fatalf("audience = %v, want [%s] (self-targeted)", env.Audience, author)
	}
	if env.Visibility != message.VisibilityPublic {
		t.Fatalf("visibility = %q, want public (StepNormalize default)", env.Visibility)
	}
	if env.Type != "demo.tick" {
		t.Fatalf("type = %q, want demo.tick", env.Type)
	}
	if string(env.Payload) != `{"k":"v"}` {
		t.Fatalf("payload = %q, want the scheduled payload", env.Payload)
	}
	if string(env.CorrelationID) != "corr-slice1" {
		t.Fatalf("correlation_id = %q, want corr-slice1 (红线❺ 继承)", env.CorrelationID)
	}
	if env.TS != fireAt {
		t.Fatalf("ts = %d, want %d (engine-injected clock at fire, never the pen)", env.TS, fireAt)
	}
	if env.ParentID != "" {
		t.Fatalf("parent_id = %q, want empty (fire is not a reply)", env.ParentID)
	}
	if env.ExpiresAt != nil {
		t.Fatalf("expires_at = %v, want nil (event carries no request-expiry semantics)", env.ExpiresAt)
	}

	// Deliberately NOT asserted: mailbox delivery — pump/fanout live in
	// platform/internal, unreachable from the runtime tree; this slice's job
	// stops at truth landing in the log.
}

// ---------------------------------------------------------------------
// Slice 2 — self-targeted: ScheduleReq has no target field at compile time;
// the runtime correlate is that TWO independent authors' fires are each
// unconditionally self-addressed, never cross-wired.
// ---------------------------------------------------------------------

func TestTimerSlice2_SelfTargetedStructural(t *testing.T) {
	ctx := context.Background()
	cs := openScheduleChannel(t)
	sink := newRealFireSink(t, cs)
	rt := newScheduleRuntime(t)
	clock := newFakeClock(time.UnixMilli(1_000_000))
	revive := newTestReviver(rt)

	minter, engine, err := OpenScheduler(cs, schedule.AssemblyDeps{Fire: sink, Host: rt, Revive: revive, Clock: clock})
	if err != nil {
		t.Fatalf("OpenScheduler: %v", err)
	}
	engine.Start()
	t.Cleanup(engine.Close)

	const authorA = actor.ActorID("author-a")
	const authorB = actor.ActorID("author-b")
	hA := minter.Mint(authorA)
	hB := minter.Mint(authorB)

	fireAt := clock.Now().Add(time.Minute).UnixMilli()
	idA, err := hA.Schedule(ctx, schedule.ScheduleReq{Bind: schedule.BindIdentity, FireAt: fireAt, Type: "t"})
	if err != nil {
		t.Fatalf("Schedule A: %v", err)
	}
	idB, err := hB.Schedule(ctx, schedule.ScheduleReq{Bind: schedule.BindIdentity, FireAt: fireAt, Type: "t"})
	if err != nil {
		t.Fatalf("Schedule B: %v", err)
	}

	clock.Advance(time.Minute)

	wantA, wantB := fireMsgID(idA), fireMsgID(idB)
	var rowA, rowB storespec.StoredRow
	waitFor(t, 2*time.Second, func() bool {
		rows := readAllTruth(t, cs)
		var okA, okB bool
		rowA, okA = findByID(rows, wantA)
		rowB, okB = findByID(rows, wantB)
		return okA && okB
	})

	if rowA.Envelope.Sender.ID != authorA || len(rowA.Envelope.Audience) != 1 || rowA.Envelope.Audience[0] != authorA {
		t.Fatalf("A's fire not self-addressed: sender=%q audience=%v", rowA.Envelope.Sender.ID, rowA.Envelope.Audience)
	}
	if rowB.Envelope.Sender.ID != authorB || len(rowB.Envelope.Audience) != 1 || rowB.Envelope.Audience[0] != authorB {
		t.Fatalf("B's fire not self-addressed: sender=%q audience=%v", rowB.Envelope.Sender.ID, rowB.Envelope.Audience)
	}
}

// ---------------------------------------------------------------------
// Slice 3 / 3b — incarnation-bind drop (pointer-level ABA guard) + the
// attach seam's structural bottom (no live embodiment → ErrBadSchedule).
// ---------------------------------------------------------------------

func TestTimerSlice3_IncarnationDropsOnDeathEvenWithLiveSuccessor(t *testing.T) {
	ctx := context.Background()
	cs := openScheduleChannel(t)
	sink := newRealFireSink(t, cs)
	rt := newScheduleRuntime(t)
	rt.Spawn("author-1", func(actorrt.Incarnation) actorrt.Actor { return stubTimerActor{} })
	clock := newFakeClock(time.UnixMilli(1_000_000))
	revive := newTestReviver(rt)

	minter, engine, err := OpenScheduler(cs, schedule.AssemblyDeps{Fire: sink, Host: rt, Revive: revive, Clock: clock})
	if err != nil {
		t.Fatalf("OpenScheduler: %v", err)
	}
	engine.Start()
	t.Cleanup(engine.Close)

	handle := minter.Mint("author-1")
	fireAt := clock.Now().Add(time.Hour).UnixMilli()
	id, err := handle.Schedule(ctx, schedule.ScheduleReq{Bind: schedule.BindIncarnation, FireAt: fireAt, Type: "demo.retry"})
	if err != nil {
		t.Fatalf("Schedule: %v", err)
	}

	// Structural assertion: an incarnation-bind timer is NEVER a row, at any
	// point in its life — not merely absent after the drop.
	if n := storeRowCount(t, cs); n != 0 {
		t.Fatalf("bind=incarnation created %d durable rows, want 0 (never persisted, structure IS the bind)", n)
	}

	// Predecessor dies, a SAME-ID successor takes over (respawn) — the
	// successor being live must NOT rescue the predecessor's timer (pointer
	// identity, not id identity, is the drop check).
	rt.DespawnID("author-1")
	rt.Spawn("author-1", func(actorrt.Incarnation) actorrt.Actor { return stubTimerActor{} })

	clock.Advance(time.Hour)
	// Bounded real-time window for the (non-)fire to settle, then assert it
	// never happened.
	time.Sleep(50 * time.Millisecond)

	if _, ok := findByID(readAllTruth(t, cs), fireMsgID(id)); ok {
		t.Fatal("dead-embodiment incarnation-bind timer fired despite a live SAME-ID successor")
	}
	if n := storeRowCount(t, cs); n != 0 {
		t.Fatalf("dead incarnation-bind timer leaked into the durable store: %d rows", n)
	}
}

func TestTimerSlice3b_AttachNoLiveEmbodiment(t *testing.T) {
	ctx := context.Background()
	cs := openScheduleChannel(t)
	sink := newRealFireSink(t, cs)
	rt := newScheduleRuntime(t)
	clock := newFakeClock(time.UnixMilli(1_000_000))
	revive := newTestReviver(rt)

	minter, engine, err := OpenScheduler(cs, schedule.AssemblyDeps{Fire: sink, Host: rt, Revive: revive, Clock: clock})
	if err != nil {
		t.Fatalf("OpenScheduler: %v", err)
	}
	engine.Start()
	t.Cleanup(engine.Close)

	handle := minter.Mint("ghost")
	_, err = handle.Schedule(ctx, schedule.ScheduleReq{Bind: schedule.BindIncarnation, FireAt: clock.Now().Add(time.Hour).UnixMilli(), Type: "t"})
	if !errors.Is(err, schedule.ErrBadSchedule) {
		t.Fatalf("Schedule(incarnation, no live embodiment): err=%v, want ErrBadSchedule", err)
	}
}

// ---------------------------------------------------------------------
// Slice 4 — restart: incarnation-bind vanishes physically (a FRESH Engine +
// a FRESH *actorrt.Runtime over the SAME durable store), identity-bind
// survives and fires late (a late timer still fires as scheduled, no
// fast-forward).
// ---------------------------------------------------------------------

func TestTimerSlice4_RestartBatchDropVsIdentitySurvive(t *testing.T) {
	ctx := context.Background()
	cs := openScheduleChannel(t) // the durable store outlives the "restart" below
	clock := newFakeClock(time.UnixMilli(1_000_000))
	fireAt := clock.Now().Add(2 * time.Hour).UnixMilli()

	// --- pre-restart process ---
	rt1 := newScheduleRuntime(t)
	rt1.Spawn("author-inc", func(actorrt.Incarnation) actorrt.Actor { return stubTimerActor{} })
	sink1 := newRealFireSink(t, cs)
	revive1 := newTestReviver(rt1)

	minter1, engine1, err := OpenScheduler(cs, schedule.AssemblyDeps{Fire: sink1, Host: rt1, Revive: revive1, Clock: clock})
	if err != nil {
		t.Fatalf("OpenScheduler (pre-restart): %v", err)
	}
	engine1.Start()

	idIdentity, err := minter1.Mint("author-identity").Schedule(ctx, schedule.ScheduleReq{
		Bind: schedule.BindIdentity, FireAt: fireAt, Type: "demo.identity",
	})
	if err != nil {
		t.Fatalf("Schedule identity: %v", err)
	}
	idIncarnation, err := minter1.Mint("author-inc").Schedule(ctx, schedule.ScheduleReq{
		Bind: schedule.BindIncarnation, FireAt: fireAt, Type: "demo.inc",
	})
	if err != nil {
		t.Fatalf("Schedule incarnation: %v", err)
	}
	if n := storeRowCount(t, cs); n != 1 {
		t.Fatalf("durable row count pre-restart = %d, want 1 (only the identity-bind timer)", n)
	}

	engine1.Close() // simulates the crash/restart boundary — BEFORE fireAt

	// --- post-restart process: fresh Engine (empty mem) AND fresh Runtime
	// (no live embodiments) — a real process restart wipes both in-memory
	// stores; only the sqlite-backed timers table survives.
	rt2 := newScheduleRuntime(t)
	sink2 := newRealFireSink(t, cs)
	revive2 := newTestReviver(rt2)

	_, engine2, err := OpenScheduler(cs, schedule.AssemblyDeps{Fire: sink2, Host: rt2, Revive: revive2, Clock: clock})
	if err != nil {
		t.Fatalf("OpenScheduler (post-restart): %v", err)
	}
	engine2.Start()
	t.Cleanup(engine2.Close)

	clock.Advance(2 * time.Hour) // now well past fireAt — the sleeping identity row is late

	waitFor(t, 2*time.Second, func() bool {
		_, ok := findByID(readAllTruth(t, cs), fireMsgID(idIdentity))
		return ok
	})
	time.Sleep(50 * time.Millisecond) // bounded window for the (non-)fire of the vanished entry

	if _, ok := findByID(readAllTruth(t, cs), fireMsgID(idIncarnation)); ok {
		t.Fatal("an incarnation-bind timer scheduled BEFORE restart fired AFTER restart — it should have vanished with the old process (v1.1 历史校准)")
	}
	if n := storeRowCount(t, cs); n != 0 {
		t.Fatalf("durable row count post-fire = %d, want 0 (identity row deleted on fire)", n)
	}
}

// ---------------------------------------------------------------------
// Slice 5 — crash-window idempotency + the FireSink tri-state contract:
// dup / deterministic-reject / transient, each proven against the REAL
// harness chain except the pure-pacing transient case.
// ---------------------------------------------------------------------

func TestTimerSlice5_CrashIdempotencyAndFireSinkTriState(t *testing.T) {
	t.Run("duplicate_append_maps_to_ErrDuplicateFire_real_harness", func(t *testing.T) {
		ctx := context.Background()
		cs := openScheduleChannel(t)
		sink := newRealFireSink(t, cs)
		const author = actor.ActorID("author-dup")

		mkEnv := func() *message.Envelope {
			return &message.Envelope{
				ID: "timer:dup-1", TS: 1_000_000, Kind: message.KindEvent,
				Type: "demo.tick", Payload: []byte("{}"), Audience: message.Audience{author},
			}
		}
		if err := sink.Append(ctx, author, mkEnv()); err != nil {
			t.Fatalf("first Append: unexpected err (want a landed fire): %v", err)
		}
		// A FRESH envelope value (same id) — reusing the first pointer would
		// carry the pen-welded Sender/ChannelID from call 1 and trip the
		// UNRELATED HarnessIdentityNotCallerSettable guard, not the UNIQUE path
		// this subtest is proving.
		err := sink.Append(ctx, author, mkEnv())
		if !errors.Is(err, schedule.ErrDuplicateFire) {
			t.Fatalf("second Append (same id, real messages.id UNIQUE hit): err=%v, want ErrDuplicateFire", err)
		}
	})

	t.Run("deterministic_reject_maps_to_FireRejected_real_harness", func(t *testing.T) {
		ctx := context.Background()
		cs := openScheduleChannel(t)
		sink := newRealFireSink(t, cs)
		const author = actor.ActorID("author-reserved")

		env := &message.Envelope{
			ID: "timer:reserved-1", TS: 1_000_000, Kind: message.KindEvent,
			Type: actor.ReservedSystemChannelCreated, Payload: []byte("{}"),
			Audience: message.Audience{author},
		}
		err := sink.Append(ctx, author, env)
		var rejected schedule.FireRejected
		if !errors.As(err, &rejected) {
			t.Fatalf("Append(reserved type, non-system sender): err=%v, want FireRejected (naive `_,err:=Write();return err` would return nil here — the exact silent-loss bug this contract exists to prevent)", err)
		}
		if rejected.Reason != string(harness.HarnessReservedTypeUnauthorizedSender) {
			t.Fatalf("FireRejected.Reason = %q, want %q", rejected.Reason, harness.HarnessReservedTypeUnauthorizedSender)
		}
	})

	t.Run("engine_poison_row_disposal_real_harness", func(t *testing.T) {
		ctx := context.Background()
		cs := openScheduleChannel(t)
		sink := newRealFireSink(t, cs)
		rt := newScheduleRuntime(t)
		clock := newFakeClock(time.UnixMilli(1_000_000))
		revive := newTestReviver(rt)
		capture := &logCapture{}

		_, engine, err := OpenScheduler(cs, schedule.AssemblyDeps{
			Fire: sink, Host: rt, Revive: revive, Clock: clock, Logger: slog.New(capture),
		})
		if err != nil {
			t.Fatalf("OpenScheduler: %v", err)
		}

		// Simulate a rule that evolved during a durable timer's sleep: insert a
		// row DIRECTLY (bypassing Schedule's own ingress guard, which would
		// refuse a reserved-prefixed Type at the door) — a row that was legal
		// to accept once, now deterministically rejected by the harness's
		// reserved-namespace authority (ingress already blocks the main
		// entrypoint for a NEW Schedule; this is the leaked-through fallback
		// case the disposal path exists for).
		const author = actor.ActorID("author-poison")
		const poisonID = timerspec.TimerID("poison-1")
		if err := cs.timers.Insert(ctx, timerspec.TimerRow{
			ID: poisonID, AuthorID: author, FireAt: clock.Now().UnixMilli() - 1,
			Type: actor.ReservedSystemChannelCreated, CreatedAt: clock.Now().UnixMilli(),
		}); err != nil {
			t.Fatalf("direct timers.Insert: %v", err)
		}

		engine.Start()
		t.Cleanup(engine.Close)

		wantID := fireMsgID(timerspec.TimerID(poisonID))
		waitFor(t, 2*time.Second, func() bool { return storeRowCount(t, cs) == 0 })
		if _, ok := findByID(readAllTruth(t, cs), wantID); ok {
			t.Fatal("a poison row's fire made it into truth — the harness reject leaked past disposal")
		}
		if !capture.hasMessage("schedule.fire_rejected_dropped") {
			t.Fatal("poison-row disposal did not emit the loud obs-plane log (拍点 8.8)")
		}
	})

	t.Run("transient_error_leaves_row_for_retry", func(t *testing.T) {
		ctx := context.Background()
		cs := openScheduleChannel(t)
		flaky := &flakyFireSink{inner: newRealFireSink(t, cs), alwaysFail: true}
		rt := newScheduleRuntime(t)
		clock := newFakeClock(time.UnixMilli(1_000_000))
		revive := newTestReviver(rt)

		minter, engine, err := OpenScheduler(cs, schedule.AssemblyDeps{Fire: flaky, Host: rt, Revive: revive, Clock: clock})
		if err != nil {
			t.Fatalf("OpenScheduler: %v", err)
		}
		engine.Start()
		t.Cleanup(engine.Close)

		handle := minter.Mint("author-1")
		id, err := handle.Schedule(ctx, schedule.ScheduleReq{
			Bind: schedule.BindIdentity, FireAt: clock.Now().UnixMilli() - 1, Type: "demo.due",
		})
		if err != nil {
			t.Fatalf("Schedule: %v", err)
		}

		// alwaysFail holds through this whole window: whichever retry the
		// engine attempts (the fresh wake, a stray coalesced-wake slip, or a
		// backoff alarm) is GUARANTEED to fail, so there is no race in which
		// the row could already be gone by the time this checks it.
		waitFor(t, 2*time.Second, func() bool { return flaky.callCount() >= 1 })
		time.Sleep(30 * time.Millisecond)
		if n := storeRowCount(t, cs); n != 1 {
			t.Fatalf("row count after a transient failure = %d, want 1 (at-least-once retention)", n)
		}

		// Let the retry succeed against the REAL sink, then advance the clock
		// past whatever real backoff was armed.
		flaky.setAlwaysFail(false)
		clock.Advance(5 * time.Second)
		waitFor(t, 2*time.Second, func() bool {
			_, ok := findByID(readAllTruth(t, cs), fireMsgID(id))
			return ok
		})
	})
}

// ---------------------------------------------------------------------
// Slice 6 — Cancel tri-state (a single Cancel entrypoint shared by both
// binds): pending cancels, already-fired is a silent no-op, a non-owner's
// Cancel never leaks existence.
// ---------------------------------------------------------------------

func TestTimerSlice6_CancelTriState(t *testing.T) {
	ctx := context.Background()
	cs := openScheduleChannel(t)
	sink := newRealFireSink(t, cs)
	rt := newScheduleRuntime(t)
	rt.Spawn("author-1", func(actorrt.Incarnation) actorrt.Actor { return stubTimerActor{} })
	clock := newFakeClock(time.UnixMilli(1_000_000))
	revive := newTestReviver(rt)

	minter, engine, err := OpenScheduler(cs, schedule.AssemblyDeps{Fire: sink, Host: rt, Revive: revive, Clock: clock})
	if err != nil {
		t.Fatalf("OpenScheduler: %v", err)
	}
	engine.Start()
	t.Cleanup(engine.Close)

	h1 := minter.Mint("author-1")
	h2 := minter.Mint("author-2")

	// Pending identity timer: cancel prevents the fire.
	idIdentity, err := h1.Schedule(ctx, schedule.ScheduleReq{Bind: schedule.BindIdentity, FireAt: clock.Now().Add(time.Hour).UnixMilli(), Type: "t"})
	if err != nil {
		t.Fatalf("Schedule: %v", err)
	}
	if err := h1.Cancel(ctx, idIdentity); err != nil {
		t.Fatalf("Cancel: %v", err)
	}
	if storeRowCount(t, cs) != 0 {
		t.Fatal("Cancel left the identity row in place")
	}

	// Pending incarnation timer: cancel prevents the fire too.
	idInc, err := h1.Schedule(ctx, schedule.ScheduleReq{Bind: schedule.BindIncarnation, FireAt: clock.Now().Add(time.Hour).UnixMilli(), Type: "t"})
	if err != nil {
		t.Fatalf("Schedule: %v", err)
	}
	if err := h1.Cancel(ctx, idInc); err != nil {
		t.Fatalf("Cancel: %v", err)
	}

	// Non-owner cancel: existed=false, silent, no leak — h2 cannot cancel h1's timer.
	idForeign, err := h1.Schedule(ctx, schedule.ScheduleReq{Bind: schedule.BindIdentity, FireAt: clock.Now().Add(time.Hour).UnixMilli(), Type: "t"})
	if err != nil {
		t.Fatalf("Schedule: %v", err)
	}
	if err := h2.Cancel(ctx, idForeign); err != nil {
		t.Fatalf("Cancel (non-owner): %v", err)
	}
	if storeRowCount(t, cs) != 1 {
		t.Fatal("a non-owner Cancel deleted someone else's timer")
	}
	if err := h1.Cancel(ctx, idForeign); err != nil { // cleanup
		t.Fatalf("Cancel (owner cleanup): %v", err)
	}

	// Already-fired: Cancel is a silent no-op (fired truth is not retractable).
	idFired, err := h1.Schedule(ctx, schedule.ScheduleReq{Bind: schedule.BindIdentity, FireAt: clock.Now().UnixMilli() - 1, Type: "t"})
	if err != nil {
		t.Fatalf("Schedule: %v", err)
	}
	waitFor(t, 2*time.Second, func() bool {
		_, ok := findByID(readAllTruth(t, cs), fireMsgID(idFired))
		return ok
	})
	if err := h1.Cancel(ctx, idFired); err != nil {
		t.Fatalf("Cancel (already fired): %v", err)
	}

	// Nothing left pending should ever fire from the cancelled entries.
	clock.Advance(2 * time.Hour)
	time.Sleep(30 * time.Millisecond)
	if sink.callCount() != 1 {
		t.Fatalf("sink.callCount() = %d after cancels, want 1 (only idFired)", sink.callCount())
	}
}

// ---------------------------------------------------------------------
// Slice 7 — dereg cascading clear: identity rows clear in the SAME tx as
// the registry flip, both dereg entry points asserted.
// ---------------------------------------------------------------------

func TestTimerSlice7_DeregCascadeClear(t *testing.T) {
	ctx := context.Background()

	run := func(t *testing.T, dereg func(t *testing.T, cs *ChannelStores, id actor.ActorID)) {
		cs := openScheduleChannel(t)
		sink := newRealFireSink(t, cs)
		rt := newScheduleRuntime(t)
		clock := newFakeClock(time.UnixMilli(1_000_000))
		revive := newTestReviver(rt)

		minter, engine, err := OpenScheduler(cs, schedule.AssemblyDeps{Fire: sink, Host: rt, Revive: revive, Clock: clock})
		if err != nil {
			t.Fatalf("OpenScheduler: %v", err)
		}
		engine.Start()
		t.Cleanup(engine.Close)

		const author = actor.ActorID("A")
		seedMember(t, cs, author)
		handle := minter.Mint(author)

		id, err := handle.Schedule(ctx, schedule.ScheduleReq{
			Bind: schedule.BindIdentity, FireAt: clock.Now().Add(time.Hour).UnixMilli(), Type: "t",
		})
		if err != nil {
			t.Fatalf("Schedule: %v", err)
		}
		if n := storeRowCount(t, cs); n != 1 {
			t.Fatalf("row count pre-dereg = %d, want 1", n)
		}

		dereg(t, cs, author)

		if n := storeRowCount(t, cs); n != 0 {
			t.Fatalf("timers row count after dereg = %d, want 0 (cascaded清 same tx, §10.12 row 6)", n)
		}

		clock.Advance(2 * time.Hour)
		time.Sleep(50 * time.Millisecond)

		if _, ok := findByID(readAllTruth(t, cs), fireMsgID(id)); ok {
			t.Fatal("a cascade-cleared timer fired anyway")
		}
	}

	t.Run("Deregister path", func(t *testing.T) {
		run(t, func(t *testing.T, cs *ChannelStores, id actor.ActorID) {
			if err := cs.Membership.Deregister(ctx, id, 100); err != nil {
				t.Fatalf("Deregister: %v", err)
			}
		})
	})

	t.Run("ApplyMemberTransitions removes path", func(t *testing.T) {
		run(t, func(t *testing.T, cs *ChannelStores, id actor.ActorID) {
			if err := cs.Membership.ApplyMemberTransitions(ctx, nil,
				[]storespec.MemberActorRemove{{ID: id, At: 100}}); err != nil {
				t.Fatalf("ApplyMemberTransitions: %v", err)
			}
		})
	})
}

// ---------------------------------------------------------------------
// Slice 8 — the Revive seam: wake-first ordering against a REAL
// SpawnIfAbsent-backed activation, retry-on-failure, exactly-once truth.
// ---------------------------------------------------------------------

func TestTimerSlice8_ReviveSeamWakeFirstOrdering(t *testing.T) {
	ctx := context.Background()
	cs := openScheduleChannel(t)
	sink := newRealFireSink(t, cs)
	rt := newScheduleRuntime(t)
	clock := newFakeClock(time.UnixMilli(1_000_000))
	revive := newTestReviver(rt)

	const author = actor.ActorID("sleepy-author")
	// A large count, not a single failNextFor(author, 1): while it stays
	// large, EVERY retry (the fresh wake, a stray coalesced-wake slip, a
	// backoff alarm) deterministically fails — no race in which an early
	// retry could already have succeeded by the time the "still retained"
	// checks below observe it.
	revive.failNextFor(author, 1_000_000)

	minter, engine, err := OpenScheduler(cs, schedule.AssemblyDeps{Fire: sink, Host: rt, Revive: revive, Clock: clock})
	if err != nil {
		t.Fatalf("OpenScheduler: %v", err)
	}
	engine.Start()
	t.Cleanup(engine.Close)

	if _, live := rt.Stat(author); live {
		t.Fatal("author already live before Schedule — test precondition broken")
	}

	handle := minter.Mint(author)
	id, err := handle.Schedule(ctx, schedule.ScheduleReq{Bind: schedule.BindIdentity, FireAt: clock.Now().UnixMilli() - 1, Type: "demo.wake"})
	if err != nil {
		t.Fatalf("Schedule: %v", err)
	}

	// While Revive keeps failing, append must NEVER be attempted (wake-first
	// ordering) and the row stays.
	waitFor(t, 2*time.Second, func() bool { return revive.callCount() >= 1 })
	time.Sleep(30 * time.Millisecond)
	if sink.callCount() != 0 {
		t.Fatalf("Append called %d times before EnsureLive ever succeeded, want 0 (revive gates append)", sink.callCount())
	}
	if storeRowCount(t, cs) != 1 {
		t.Fatal("row deleted despite a failing Revive, want retained (at-least-once)")
	}

	// Revive is allowed to succeed — a REAL live embodiment is minted via
	// SpawnIfAbsent, THEN (and only then) fire lands, exactly once. A
	// comfortably-large Advance clears whatever real backoff the engine
	// armed, regardless of its exact value.
	revive.allowSucceedFor(author)
	clock.Advance(5 * time.Second)
	waitFor(t, 2*time.Second, func() bool {
		_, ok := findByID(readAllTruth(t, cs), fireMsgID(id))
		return ok
	})
	if _, live := rt.Stat(author); !live {
		t.Fatal("author not live after a successful Revive — the SpawnIfAbsent seam did not activate an embodiment")
	}
	count := 0
	for _, r := range readAllTruth(t, cs) {
		if r.Envelope.ID == fireMsgID(id) {
			count++
		}
	}
	if count != 1 {
		t.Fatalf("fire message appears %d times in truth, want exactly 1 (确定性 ID 兜底重试)", count)
	}
	if storeRowCount(t, cs) != 0 {
		t.Fatal("row not deleted after a successful fire")
	}
}

// ---------------------------------------------------------------------
// Slice 9 — ErrBadSchedule matrix + past FireAt legal/immediate.
// ---------------------------------------------------------------------

func TestTimerSlice9_ErrBadScheduleMatrix(t *testing.T) {
	ctx := context.Background()
	cs := openScheduleChannel(t)
	sink := newRealFireSink(t, cs)
	rt := newScheduleRuntime(t)
	rt.Spawn("author-1", func(actorrt.Incarnation) actorrt.Actor { return stubTimerActor{} })
	clock := newFakeClock(time.UnixMilli(1_000_000))
	revive := newTestReviver(rt)

	minter, engine, err := OpenScheduler(cs, schedule.AssemblyDeps{Fire: sink, Host: rt, Revive: revive, Clock: clock})
	if err != nil {
		t.Fatalf("OpenScheduler: %v", err)
	}
	engine.Start()
	t.Cleanup(engine.Close)
	handle := minter.Mint("author-1")

	cases := []struct {
		name    string
		req     schedule.ScheduleReq
		wantErr bool
	}{
		{"bind outside closed set", schedule.ScheduleReq{Bind: "bogus", FireAt: 2_000_000, Type: "t"}, true},
		{"FireAt zero", schedule.ScheduleReq{Bind: schedule.BindIdentity, FireAt: 0, Type: "t"}, true},
		{"FireAt negative", schedule.ScheduleReq{Bind: schedule.BindIdentity, FireAt: -1, Type: "t"}, true},
		{"Type empty", schedule.ScheduleReq{Bind: schedule.BindIdentity, FireAt: 2_000_000, Type: ""}, true},
		{"Type reserved prefix", schedule.ScheduleReq{Bind: schedule.BindIdentity, FireAt: 2_000_000, Type: "system.internal"}, true},
		{"future identity is legal", schedule.ScheduleReq{Bind: schedule.BindIdentity, FireAt: 2_000_000, Type: "t"}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := handle.Schedule(ctx, tc.req)
			if tc.wantErr && !errors.Is(err, schedule.ErrBadSchedule) {
				t.Fatalf("Schedule(%+v): err=%v, want ErrBadSchedule", tc.req, err)
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("Schedule(%+v): unexpected err=%v", tc.req, err)
			}
		})
	}

	// past FireAt is legal and fires immediately — no threshold (refusing it
	// would make "a millisecond before vs after the deadline" two different
	// behaviours).
	id, err := handle.Schedule(ctx, schedule.ScheduleReq{Bind: schedule.BindIdentity, FireAt: 1, Type: "t"})
	if err != nil {
		t.Fatalf("Schedule(past FireAt): %v", err)
	}
	waitFor(t, 2*time.Second, func() bool {
		_, ok := findByID(readAllTruth(t, cs), fireMsgID(id))
		return ok
	})
}

// ---------------------------------------------------------------------
// Slice 11 — -race: concurrent Schedule/Cancel against a ticking run loop.
// Run with `go test -race`.
// ---------------------------------------------------------------------

func TestTimerSlice11_ConcurrentScheduleCancelRace(t *testing.T) {
	ctx := context.Background()
	cs := openScheduleChannel(t)
	sink := newRealFireSink(t, cs)
	rt := newScheduleRuntime(t)
	clock := newFakeClock(time.UnixMilli(1_000_000))
	revive := newTestReviver(rt)

	minter, engine, err := OpenScheduler(cs, schedule.AssemblyDeps{Fire: sink, Host: rt, Revive: revive, Clock: clock})
	if err != nil {
		t.Fatalf("OpenScheduler: %v", err)
	}
	engine.Start()
	t.Cleanup(engine.Close)
	handle := minter.Mint("author-1")

	// A keeper timer EARLIER than every churn timer, never cancelled: the
	// semantic half of this slice — under concurrent Schedule/Cancel churn
	// constantly re-arming the alarm, the earliest deadline must still fire
	// (no wake/alarm-rearm window may swallow it), not merely "no data race".
	keeperID, err := handle.Schedule(ctx, schedule.ScheduleReq{
		Bind: schedule.BindIdentity, FireAt: clock.Now().Add(10 * time.Second).UnixMilli(), Type: "keeper.tick",
	})
	if err != nil {
		t.Fatalf("Schedule keeper: %v", err)
	}

	const n = 50
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < n; i++ {
			id, err := handle.Schedule(ctx, schedule.ScheduleReq{
				Bind: schedule.BindIdentity, FireAt: clock.Now().Add(time.Duration(i+1) * time.Minute).UnixMilli(), Type: "t",
			})
			if err != nil {
				t.Errorf("Schedule: %v", err)
				return
			}
			_ = handle.Cancel(ctx, id)
		}
	}()

	// Concurrently advance the clock so the run loop is actively re-arming
	// and re-evaluating its due set while Schedule/Cancel race it.
	for i := 0; i < n; i++ {
		clock.Advance(30 * time.Second)
		time.Sleep(time.Millisecond)
	}
	<-done

	// The keeper's deadline passed on the very first Advance — its fire truth
	// must have landed despite the churn (lost-wake assertion, not just -race).
	wantID := fireMsgID(keeperID)
	waitFor(t, 2*time.Second, func() bool {
		_, ok := findByID(readAllTruth(t, cs), wantID)
		return ok
	})
}

// ---------------------------------------------------------------------
// Slice 12 — TimerID never reused: a re-Schedule after Cancel gets a NEW id,
// and only the new id's fire lands (the cancelled id's messages.id UNIQUE
// row must never be resurrected).
// ---------------------------------------------------------------------

func TestTimerSlice12_TimerIDNeverReused(t *testing.T) {
	ctx := context.Background()
	cs := openScheduleChannel(t)
	sink := newRealFireSink(t, cs)
	rt := newScheduleRuntime(t)
	clock := newFakeClock(time.UnixMilli(1_000_000))
	revive := newTestReviver(rt)

	minter, engine, err := OpenScheduler(cs, schedule.AssemblyDeps{Fire: sink, Host: rt, Revive: revive, Clock: clock})
	if err != nil {
		t.Fatalf("OpenScheduler: %v", err)
	}
	engine.Start()
	t.Cleanup(engine.Close)
	handle := minter.Mint("author-1")

	req := schedule.ScheduleReq{Bind: schedule.BindIdentity, FireAt: clock.Now().Add(time.Hour).UnixMilli(), Type: "t"}
	id1, err := handle.Schedule(ctx, req)
	if err != nil {
		t.Fatalf("Schedule 1: %v", err)
	}
	if err := handle.Cancel(ctx, id1); err != nil {
		t.Fatalf("Cancel: %v", err)
	}
	id2, err := handle.Schedule(ctx, req)
	if err != nil {
		t.Fatalf("Schedule 2: %v", err)
	}
	if id1 == id2 {
		t.Fatalf("re-Schedule after Cancel reused TimerID %q", id1)
	}

	advanceUntil(t, clock, time.Minute, func() bool {
		_, ok := findByID(readAllTruth(t, cs), fireMsgID(id2))
		return ok
	})
	if _, ok := findByID(readAllTruth(t, cs), fireMsgID(id1)); ok {
		t.Fatal("the CANCELLED id1's fire message appeared in truth")
	}
}

// ---------------------------------------------------------------------
// Slice 13 — backoff is bounded, never a busy loop.
// ---------------------------------------------------------------------

func TestTimerSlice13_BackoffBounded(t *testing.T) {
	ctx := context.Background()
	cs := openScheduleChannel(t)
	flaky := &flakyFireSink{inner: newRealFireSink(t, cs), alwaysFail: true}
	rt := newScheduleRuntime(t)
	clock := newFakeClock(time.UnixMilli(1_000_000))
	revive := newTestReviver(rt)

	minter, engine, err := OpenScheduler(cs, schedule.AssemblyDeps{Fire: flaky, Host: rt, Revive: revive, Clock: clock})
	if err != nil {
		t.Fatalf("OpenScheduler: %v", err)
	}
	engine.Start()
	t.Cleanup(engine.Close)

	_, err = minter.Mint("author-1").Schedule(ctx, schedule.ScheduleReq{
		Bind: schedule.BindIdentity, FireAt: clock.Now().UnixMilli() - 1, Type: "demo.due",
	})
	if err != nil {
		t.Fatalf("Schedule: %v", err)
	}

	// A freshly-Scheduled already-due item races its OWN wake token against
	// the run loop's discovery of it: the coalesced wake channel may still
	// hold that one token by the time the first failed attempt arms its
	// backoff alarm, letting one extra attempt slip through before the
	// backoff genuinely holds — a bounded, single-token artifact, never an
	// unbounded spin. Settle past that race before asserting no-busy-loop.
	settled := waitStable(t, flaky.callCount, 100*time.Millisecond)
	if settled < 1 {
		t.Fatalf("flaky.callCount() settled at %d, want >= 1", settled)
	}

	// No wake source remains (settled means the race above is over): a
	// bounded real-time window MUST see zero further attempts if the loop is
	// correctly blocked on the alarm rather than spinning.
	time.Sleep(30 * time.Millisecond)
	if flaky.callCount() != settled {
		t.Fatalf("flaky.callCount() = %d after settling at %d, want unchanged (engine must not busy-spin)", flaky.callCount(), settled)
	}

	// Advancing the clock past whatever real backoff duration was armed
	// unblocks exactly one more attempt.
	clock.Advance(10 * time.Second)
	waitFor(t, 2*time.Second, func() bool { return flaky.callCount() == settled+1 })
	if n := storeRowCount(t, cs); n != 1 {
		t.Fatalf("row count after repeated transient failures = %d, want 1 (at-least-once, still pending)", n)
	}
}
