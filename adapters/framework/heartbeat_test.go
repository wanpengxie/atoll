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
