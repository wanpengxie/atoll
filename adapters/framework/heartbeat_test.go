package framework

import (
	"context"
	"encoding/json"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/wanpengxie/ActOS/kernel/actor"
	"github.com/wanpengxie/ActOS/kernel/adapter"
	"github.com/wanpengxie/ActOS/kernel/message"
)

// newHeartbeatPolicy builds a bare timerPolicy backed by a real wall clock so
// time.AfterFunc fire timing matches the deadlines we register. The fallback
// counts F3 fires; it never actually writes (correlation is nil → fire short-
// circuits after the fallback attempt, which is all these tests observe).
func newHeartbeatPolicy(t *testing.T) (*timerPolicy, *atomic.Int32) {
	t.Helper()
	var fires atomic.Int32
	p := newTimerPolicy("xhs", nil, NoopLogger{}, NewMemoryMetrics(), time.Now, "channel:test", nil)
	p.bindFallback(func(context.Context, adapter.CorrelationKey, json.RawMessage, adapter.RespondOptions) (adapter.RespondResult, error) {
		fires.Add(1)
		return adapter.RespondResult{MessageID: "fallback"}, nil
	})
	t.Cleanup(p.shutdown)
	return p, &fires
}

func (p *timerPolicy) armedFor(id adapter.CorrelationKey) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	_, ok := p.timers[id]
	return ok
}

// TestProvisionalReArmDeadlineEstWait: a provisional carrying est_wait_ms sets
// the re-arm deadline to now + est_wait_ms (§6.2 first bullet).
func TestProvisionalReArmDeadlineEstWait(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	payload := json.RawMessage(`{"status":"processing","est_wait_ms":120000}`)
	got := provisionalReArmDeadline(now, payload, 30_000)
	want := now.Add(120 * time.Second)
	if !got.Equal(want) {
		t.Fatalf("est_wait deadline=%v want %v", got, want)
	}
}

// TestProvisionalReArmDeadlineNoEstWait: a provisional with no est_wait_ms
// falls back to now + max_pending_ms (§6.2 second bullet) — the provisional
// itself is treated as "I am still alive".
func TestProvisionalReArmDeadlineNoEstWait(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	payload := json.RawMessage(`{"status":"processing"}`)
	got := provisionalReArmDeadline(now, payload, 30_000)
	want := now.Add(30 * time.Second)
	if !got.Equal(want) {
		t.Fatalf("no-est_wait deadline=%v want %v", got, want)
	}
	// est_wait_ms <= 0 also falls back to max_pending_ms.
	zero := provisionalReArmDeadline(now, json.RawMessage(`{"est_wait_ms":0}`), 30_000)
	if !zero.Equal(want) {
		t.Fatalf("zero est_wait deadline=%v want %v", zero, want)
	}
}

// TestHeartbeatExtendsF3DeadlineWithEstWait: a provisional with est_wait pushes
// the F3 timer past the original short deadline — the request is NOT force-
// failed while the receiver is alive.
func TestHeartbeatExtendsF3DeadlineWithEstWait(t *testing.T) {
	p, fires := newHeartbeatPolicy(t)
	ctx := context.Background()
	id := adapter.CorrelationKey("req-hb-est")

	// Original deadline: 40ms out.
	if err := p.RegisterTimer(ctx, id, time.Now().Add(40*time.Millisecond)); err != nil {
		t.Fatalf("RegisterTimer: %v", err)
	}
	// Heartbeat re-arms to now + 400ms BEFORE the original fires.
	if err := p.ReArm(ctx, id, time.Now().Add(400*time.Millisecond)); err != nil {
		t.Fatalf("ReArm: %v", err)
	}

	// Past the original 40ms deadline, the timer must NOT have fired.
	time.Sleep(150 * time.Millisecond)
	if got := fires.Load(); got != 0 {
		t.Fatalf("F3 fired %d times before extended deadline — heartbeat did not extend", got)
	}
	if !p.armedFor(id) {
		t.Fatal("timer no longer armed after heartbeat extension")
	}
}

// TestHeartbeatExtendsF3DeadlineNoEstWait: same, using the max_pending_ms
// fallback budget via the full provisional write path.
func TestHeartbeatExtendsF3DeadlineNoEstWait(t *testing.T) {
	p, fires := newHeartbeatPolicy(t)
	ctx := context.Background()
	id := adapter.CorrelationKey("req-hb-noest")
	if err := p.RegisterTimer(ctx, id, time.Now().Add(40*time.Millisecond)); err != nil {
		t.Fatalf("RegisterTimer: %v", err)
	}
	// No est_wait → re-arm with a fresh max_pending_ms (=400ms) budget.
	deadline := provisionalReArmDeadline(time.Now(), json.RawMessage(`{"status":"processing"}`), 400)
	if err := p.ReArm(ctx, id, deadline); err != nil {
		t.Fatalf("ReArm: %v", err)
	}
	time.Sleep(150 * time.Millisecond)
	if got := fires.Load(); got != 0 {
		t.Fatalf("F3 fired %d times before extended deadline", got)
	}
}

// TestHeartbeatRepeatedlyExtends: consecutive provisionals keep pushing the
// deadline out — a steady heartbeat keeps the request alive.
func TestHeartbeatRepeatedlyExtends(t *testing.T) {
	p, fires := newHeartbeatPolicy(t)
	ctx := context.Background()
	id := adapter.CorrelationKey("req-hb-loop")
	if err := p.RegisterTimer(ctx, id, time.Now().Add(60*time.Millisecond)); err != nil {
		t.Fatalf("RegisterTimer: %v", err)
	}
	// 5 heartbeats spaced 30ms apart, each granting 80ms — original 60ms
	// deadline would have fired long ago without the extensions.
	for i := 0; i < 5; i++ {
		if err := p.ReArm(ctx, id, time.Now().Add(80*time.Millisecond)); err != nil {
			t.Fatalf("ReArm[%d]: %v", i, err)
		}
		time.Sleep(30 * time.Millisecond)
		if got := fires.Load(); got != 0 {
			t.Fatalf("F3 fired during heartbeat loop at i=%d", i)
		}
	}
	if !p.armedFor(id) {
		t.Fatal("timer dropped during heartbeat loop")
	}
}

// TestHeartbeatHardCeiling: once cumulative heartbeats reach the
// ScheduleToCloseCeiling the deadline is clamped to the ceiling and the
// request F3-fails on schedule; no further heartbeat extends it (§6.3).
func TestHeartbeatHardCeiling(t *testing.T) {
	defer setCeiling(120 * time.Millisecond)()
	p, fires := newHeartbeatPolicy(t)
	ctx := context.Background()
	id := adapter.CorrelationKey("req-hb-ceiling")

	if err := p.RegisterTimer(ctx, id, time.Now().Add(30*time.Millisecond)); err != nil {
		t.Fatalf("RegisterTimer: %v", err)
	}
	// Heartbeat wants a huge extension (10s) but the ceiling is 120ms from
	// creation → deadline must be clamped to the ceiling.
	if err := p.ReArm(ctx, id, time.Now().Add(10*time.Second)); err != nil {
		t.Fatalf("ReArm: %v", err)
	}
	// A second heartbeat also asks for the moon — still clamped, never beyond.
	if err := p.ReArm(ctx, id, time.Now().Add(10*time.Second)); err != nil {
		t.Fatalf("ReArm2: %v", err)
	}

	// Before the ceiling: still alive.
	time.Sleep(60 * time.Millisecond)
	if got := fires.Load(); got != 0 {
		t.Fatalf("F3 fired before ceiling (clamp wrong) fires=%d", got)
	}
	// After the ceiling: F3 fires exactly once, heartbeats no longer save it.
	deadline := time.Now().Add(300 * time.Millisecond)
	for time.Now().Before(deadline) {
		if fires.Load() >= 1 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if got := fires.Load(); got != 1 {
		t.Fatalf("F3 fires=%d want 1 at the hard ceiling", got)
	}
}

// setCeiling overrides ScheduleToCloseCeiling and returns a restore func.
func setCeiling(d time.Duration) func() {
	prev := ScheduleToCloseCeiling
	ScheduleToCloseCeiling = d
	return func() { ScheduleToCloseCeiling = prev }
}

// TestReArmNoOpWhenNotArmed: a heartbeat for a request with no live timer
// (already closed / never armed) must not resurrect a timer.
func TestReArmNoOpWhenNotArmed(t *testing.T) {
	p, fires := newHeartbeatPolicy(t)
	ctx := context.Background()
	id := adapter.CorrelationKey("req-hb-none")
	if err := p.ReArm(ctx, id, time.Now().Add(50*time.Millisecond)); err != nil {
		t.Fatalf("ReArm: %v", err)
	}
	if p.armedFor(id) {
		t.Fatal("ReArm resurrected a timer for a never-armed request")
	}
	time.Sleep(120 * time.Millisecond)
	if got := fires.Load(); got != 0 {
		t.Fatalf("fire ran for an unarmed request: %d", got)
	}
}

// TestProvisionalReArmsButDoesNotResolveClosure exercises the full provisional
// write path through Dispatch: after provisionals, the request stays PENDING
// (closure not resolved) and only the final Respond closes it. It also asserts
// the request envelope's expires_at is NEVER rewritten by the heartbeat
// (append-only log, INVARIANT-12). The final Respond is gated until the test
// has inspected the mid-flight pending state.
func TestProvisionalReArmsButDoesNotResolveClosure(t *testing.T) {
	var (
		release   = make(chan struct{})
		gateOnce  sync.Once
		handleErr = make(chan error, 1)
	)
	mod := &stubModule{
		decl: adapter.Declaration{
			Name:         "xhs",
			ActorID:      "tool:xhs",
			Types:        []string{"xhs.publish"},
			Binding:      actor.BindingRuntimeOutbound,
			MaxPendingMs: 300_000,
		},
	}
	mod.handle = func(ctx context.Context, env *message.Envelope, mctx *adapter.ModuleContext) error {
		go func() {
			if _, err := mctx.Provisional(ctx, adapter.CorrelationKey(env.ID), "processing",
				json.RawMessage(`{"est_wait_ms":600000}`), adapter.ProvisionalOptions{}); err != nil {
				handleErr <- err
				return
			}
			<-release // wait until the test inspected mid-flight pending state
			_, err := mctx.Respond(ctx, adapter.CorrelationKey(env.ID),
				json.RawMessage(`{"note_id":"n-1"}`), adapter.RespondOptions{Status: "completed"})
			handleErr <- err
		}()
		return adapter.Deferred()
	}

	mgr, chain, lookup, _, _ := newTestManager(t, mod)
	defer func() { _ = mgr.Shutdown(context.Background()) }()
	req := newTestRequest("channel:test", "agent:author", "xhs.publish", "req-hb-closure")
	req.Audience = message.Audience{"tool:xhs"}
	lookup.Put(req)
	if err := mgr.Dispatch(context.Background(), req); err != nil {
		t.Fatalf("Dispatch: %v", err)
	}

	bm := mgr.byName["xhs"]
	// Wait for the provisional to be written + re-arm to run.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if len(chain.Written()) >= 1 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if len(chain.Written()) == 0 {
		t.Fatal("provisional never written")
	}

	// Mid-flight: provisional did NOT resolve closure — still pending + armed.
	entry, ok, err := bm.correlation.Get(context.Background(), "req-hb-closure")
	if err != nil || !ok {
		t.Fatalf("correlation get ok=%v err=%v", ok, err)
	}
	if entry.State != adapter.CorrelationPending {
		t.Fatalf("after provisional state=%s want pending (closure must NOT be resolved)", entry.State)
	}
	if !bm.policy.armedFor("req-hb-closure") {
		t.Fatal("F3 timer dropped by provisional — heartbeat must re-arm, not cancel")
	}

	// INVARIANT-12: the request envelope's expires_at is untouched by the
	// heartbeat (re-arm is in-memory only, never rewrites the log).
	stored, ok, err := lookup.FindByID(context.Background(), req.ID)
	if err != nil || !ok {
		t.Fatalf("lookup request: ok=%v err=%v", ok, err)
	}
	if req.ExpiresAt == nil || stored.ExpiresAt == nil {
		t.Fatalf("request expires_at nil: req=%v stored=%v", req.ExpiresAt, stored.ExpiresAt)
	}
	if *stored.ExpiresAt != *req.ExpiresAt {
		t.Fatalf("expires_at rewritten by heartbeat: stored=%d original=%d", *stored.ExpiresAt, *req.ExpiresAt)
	}

	// Release the final Respond; only it resolves closure.
	gateOnce.Do(func() { close(release) })
	if err := <-handleErr; err != nil {
		t.Fatalf("handle goroutine: %v", err)
	}
	finalDeadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(finalDeadline) {
		e, _, _ := bm.correlation.Get(context.Background(), "req-hb-closure")
		if e.State == adapter.CorrelationDone {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	final, _, _ := bm.correlation.Get(context.Background(), "req-hb-closure")
	if final.State != adapter.CorrelationDone {
		t.Fatalf("after final Respond state=%s want done", final.State)
	}
	if bm.policy.armedFor("req-hb-closure") {
		t.Fatal("F3 timer still armed after final close")
	}
}

// newPersistentHeartbeatPolicy builds a timerPolicy wired to a correlation
// tracker that mirrors writes to the given StateStore, so a simulated restart
// (rebuild policy+tracker over the same store) recovers the persisted pending
// entries. The policy uses the real wall clock so time.AfterFunc fire timing
// matches the deadlines; the correlation tracker carries the persisted
// EnqueuedAt / ExpiresAt / RearmedExpiresAt that recovery reads.
func newPersistentHeartbeatPolicy(t *testing.T, store StateStore) (*timerPolicy, *memoryCorrelationTracker, *atomic.Int32) {
	t.Helper()
	var fires atomic.Int32
	corr := newCorrelationTracker("xhs", store)
	p := newTimerPolicy("xhs", corr, NoopLogger{}, NewMemoryMetrics(), time.Now, "channel:test", nil)
	p.bindFallback(func(context.Context, adapter.CorrelationKey, json.RawMessage, adapter.RespondOptions) (adapter.RespondResult, error) {
		fires.Add(1)
		return adapter.RespondResult{MessageID: "fallback"}, nil
	})
	t.Cleanup(p.shutdown)
	return p, corr, &fires
}

// recoverDeadlineMs mirrors recoverTimersForBoundModule's deadline choice: the
// heartbeat-extended RearmedExpiresAt when set, else the original ExpiresAt.
func recoverDeadlineMs(e adapter.CorrelationEntry) int64 {
	if e.RearmedExpiresAt > e.ExpiresAt {
		return e.RearmedExpiresAt
	}
	return e.ExpiresAt
}

// TestReArmPersistsExtendedDeadlineWithoutMutatingExpiresAt: a heartbeat re-arm
// mirrors the (ceiling-clamped) extended deadline onto RearmedExpiresAt so it
// survives a restart, while leaving the immutable ExpiresAt (the tamper anchor
// mirroring the request envelope's expires_at) and EnqueuedAt untouched.
func TestReArmPersistsExtendedDeadlineWithoutMutatingExpiresAt(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStateStore()
	p, corr, _ := newPersistentHeartbeatPolicy(t, store)

	id := adapter.CorrelationKey("req-persist")
	now := time.Now()
	origDeadlineMs := now.Add(30 * time.Second).UnixMilli()
	if _, err := corr.Reserve(ctx, adapter.CorrelationEntry{
		RequestID:  id,
		ChannelID:  "channel:test",
		EnqueuedAt: now.UnixMilli(),
		ExpiresAt:  origDeadlineMs,
		State:      adapter.CorrelationPending,
	}); err != nil {
		t.Fatalf("Reserve: %v", err)
	}
	if err := p.RegisterTimer(ctx, id, now.Add(30*time.Second)); err != nil {
		t.Fatalf("RegisterTimer: %v", err)
	}
	// Heartbeat extends to now + 10min (well inside the 30m ceiling).
	extended := now.Add(10 * time.Minute)
	if err := p.ReArm(ctx, id, extended); err != nil {
		t.Fatalf("ReArm: %v", err)
	}

	entry, ok, err := corr.Get(ctx, id)
	if err != nil || !ok {
		t.Fatalf("Get ok=%v err=%v", ok, err)
	}
	if entry.RearmedExpiresAt != extended.UnixMilli() {
		t.Fatalf("RearmedExpiresAt=%d want extended %d (re-arm did not persist)",
			entry.RearmedExpiresAt, extended.UnixMilli())
	}
	if entry.ExpiresAt != origDeadlineMs {
		t.Fatalf("ExpiresAt=%d mutated by re-arm; must stay original %d (immutable tamper anchor)",
			entry.ExpiresAt, origDeadlineMs)
	}
	if entry.EnqueuedAt != now.UnixMilli() {
		t.Fatalf("EnqueuedAt mutated by re-arm: got %d want %d (anchor must be immutable)",
			entry.EnqueuedAt, now.UnixMilli())
	}
}

// TestRecoverDoesNotForceFailLiveHeartbeatReceiver is the production-side
// assertion the temporal R1 bug was missing: a long-running tool kept alive by
// provisional heartbeats must NOT be force-failed immediately when the daemon
// restarts and recovers its timer.
//
// Scenario: a request whose ORIGINAL 30s max_pending deadline elapsed long ago
// (it was created 20min back) but which has been heart-beating, so its persisted
// RearmedExpiresAt is in the future — exactly how a 20-min-running xhs publish
// looks after a restart. The buggy code re-armed against the stale past
// ExpiresAt → delay<=0 → fired at 1µs → force-failed the live receiver. The fix
// recovers against RearmedExpiresAt, so the F3 timer does NOT fire.
func TestRecoverDoesNotForceFailLiveHeartbeatReceiver(t *testing.T) {
	defer setCeiling(30 * time.Minute)()
	ctx := context.Background()
	store := NewMemoryStateStore()

	// --- pre-restart: reserve (orig deadline long past) + several heartbeats ---
	p1, corr1, _ := newPersistentHeartbeatPolicy(t, store)
	id := adapter.CorrelationKey("req-live")
	created := time.Now().Add(-20 * time.Minute) // created 20min ago, still alive
	if _, err := corr1.Reserve(ctx, adapter.CorrelationEntry{
		RequestID:  id,
		ChannelID:  "channel:test",
		EnqueuedAt: created.UnixMilli(),
		ExpiresAt:  created.Add(30 * time.Second).UnixMilli(), // long-past original deadline
		State:      adapter.CorrelationPending,
	}); err != nil {
		t.Fatalf("Reserve: %v", err)
	}
	// Arm at the (already past) original deadline, then heartbeat-extend three
	// times — the last grants a generous future window, just like a live tool.
	if err := p1.RegisterTimer(ctx, id, created.Add(30*time.Second)); err != nil {
		t.Fatalf("RegisterTimer: %v", err)
	}
	for i := 0; i < 3; i++ {
		if err := p1.ReArm(ctx, id, time.Now().Add(10*time.Minute)); err != nil {
			t.Fatalf("ReArm[%d]: %v", i, err)
		}
	}
	p1.shutdown() // daemon stops before recovery; in-memory timers gone.

	// Persisted state: ExpiresAt stale-past, RearmedExpiresAt ~10min future.
	persisted, _, _ := corr1.Get(ctx, id)
	if persisted.RearmedExpiresAt <= time.Now().UnixMilli() {
		t.Fatalf("precondition: RearmedExpiresAt=%d must be in the future", persisted.RearmedExpiresAt)
	}

	// --- post-restart: rebuild over the same store, recover ---
	p2, corr2, fires := newPersistentHeartbeatPolicy(t, store)
	if err := corr2.recoverFromStore(ctx); err != nil {
		t.Fatalf("recoverFromStore: %v", err)
	}
	pending, err := corr2.ListPending(ctx)
	if err != nil {
		t.Fatalf("ListPending: %v", err)
	}
	if len(pending) != 1 {
		t.Fatalf("ListPending len=%d want 1", len(pending))
	}
	e := pending[0]
	// Mirror manager.recoverTimersForBoundModule: live deadline, EnqueuedAt anchor.
	if err := p2.RecoverTimer(ctx, e.RequestID, time.UnixMilli(recoverDeadlineMs(e)), e.EnqueuedAt); err != nil {
		t.Fatalf("RecoverTimer: %v", err)
	}
	if !p2.armedFor(id) {
		t.Fatal("recovered timer not armed")
	}
	// The recovered deadline is ~10min out, so the timer must NOT fire. Wait past
	// where the buggy 1µs fire would have landed.
	time.Sleep(150 * time.Millisecond)
	if got := fires.Load(); got != 0 {
		t.Fatalf("F3 force-failed a LIVE heartbeat receiver after restart: fires=%d (temporal R1 regression)", got)
	}
	if !p2.armedFor(id) {
		t.Fatal("recovered timer dropped — live request lost its F3 guard")
	}
}

// TestRecoverForceFailsExpiredNonHeartbeatRequest: the counterpart guard — a
// request that NEVER heart-beat (RearmedExpiresAt==0) and whose original
// ExpiresAt elapsed during downtime must still F3-fail promptly on recovery.
// The fix must not turn every recovered request into a survivor.
func TestRecoverForceFailsExpiredNonHeartbeatRequest(t *testing.T) {
	defer setCeiling(30 * time.Minute)()
	ctx := context.Background()
	store := NewMemoryStateStore()

	p1, corr1, _ := newPersistentHeartbeatPolicy(t, store)
	id := adapter.CorrelationKey("req-dead")
	created := time.Now().Add(-2 * time.Minute) // created 2min ago
	if _, err := corr1.Reserve(ctx, adapter.CorrelationEntry{
		RequestID:  id,
		ChannelID:  "channel:test",
		EnqueuedAt: created.UnixMilli(),
		ExpiresAt:  created.Add(30 * time.Second).UnixMilli(), // elapsed ~90s ago, never re-armed
		State:      adapter.CorrelationPending,
	}); err != nil {
		t.Fatalf("Reserve: %v", err)
	}
	p1.shutdown()

	p2, corr2, fires := newPersistentHeartbeatPolicy(t, store)
	if err := corr2.recoverFromStore(ctx); err != nil {
		t.Fatalf("recoverFromStore: %v", err)
	}
	pending, _ := corr2.ListPending(ctx)
	if len(pending) != 1 {
		t.Fatalf("ListPending len=%d want 1", len(pending))
	}
	e := pending[0]
	if e.RearmedExpiresAt != 0 {
		t.Fatalf("precondition: RearmedExpiresAt=%d must be 0 (never re-armed)", e.RearmedExpiresAt)
	}
	if err := p2.RecoverTimer(ctx, e.RequestID, time.UnixMilli(recoverDeadlineMs(e)), e.EnqueuedAt); err != nil {
		t.Fatalf("RecoverTimer: %v", err)
	}
	// Original deadline is in the past and there was no heartbeat → F3 fires.
	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		if fires.Load() >= 1 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if got := fires.Load(); got != 1 {
		t.Fatalf("F3 fires=%d want 1 — an expired non-heartbeat request must still force-fail on recovery", got)
	}
}

// TestRecoverAnchorsCeilingAtOriginalCreation: recovery must anchor the
// ScheduleToClose ceiling at the original creation time (EnqueuedAt), not the
// restart instant — otherwise every restart hands a fresh ceiling window and a
// stuck request that keeps heart-beating could outlive creation + ceiling
// indefinitely (zombie escape).
//
// Here a heart-beating request's persisted RearmedExpiresAt is far in the future
// (it would survive on its own), but its original ceiling has already elapsed by
// recovery time. Recovery must therefore clamp to the ceiling anchored at the
// ORIGINAL creation and fire (near-)immediately, and seed createdAt accordingly.
func TestRecoverAnchorsCeilingAtOriginalCreation(t *testing.T) {
	defer setCeiling(1 * time.Minute)()
	ctx := context.Background()
	p, _, fires := newPersistentHeartbeatPolicy(t, NewMemoryStateStore())

	id := adapter.CorrelationKey("req-ceiling-restart")
	// Created 5 minutes ago; ceiling=1min → hard deadline elapsed 4min ago.
	created := time.Now().Add(-5 * time.Minute)
	// Recovered (heartbeat-extended) deadline far in the future — would survive
	// if the ceiling were misanchored at restart time.
	futureDeadline := time.Now().Add(10 * time.Minute)
	if err := p.RecoverTimer(ctx, id, futureDeadline, created.UnixMilli()); err != nil {
		t.Fatalf("RecoverTimer: %v", err)
	}

	p.mu.Lock()
	seeded, ok := p.createdAt[id]
	p.mu.Unlock()
	if !ok {
		t.Fatal("createdAt not seeded on recovery")
	}
	if seeded.UnixMilli() != created.UnixMilli() {
		t.Fatalf("ceiling anchor=%v want original creation %v (must seed from EnqueuedAt, not restart time)", seeded, created)
	}

	// Ceiling (created+1min) already past → clamp wins over the future deadline
	// and the timer fires immediately.
	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		if fires.Load() >= 1 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if got := fires.Load(); got != 1 {
		t.Fatalf("F3 fires=%d want 1 — recovery must clamp the recovered deadline to the original ceiling", got)
	}
}
