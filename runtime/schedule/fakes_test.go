package schedule

import (
	"context"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/runtime/actorrt"
	"github.com/wanpengxie/atoll/runtime/timerspec"
)

// ---------------------------------------------------------------------
// fakeStore: an in-memory timerspec.TimerStore stub. Every method may be
// scripted to fail via the *Err fields (set BEFORE the engine goroutine
// starts touching it, or under the caller's own synchronization — the store
// itself is safe for concurrent use, mirroring a real sqlite-backed store).
// ---------------------------------------------------------------------

type fakeStore struct {
	mu   sync.Mutex
	rows map[timerspec.TimerID]timerspec.TimerRow

	insertErr error
	deleteErr error
	dueErr    error
	nextErr   error
	cancelErr error
}

func newFakeStore() *fakeStore {
	return &fakeStore{rows: make(map[timerspec.TimerID]timerspec.TimerRow)}
}

func (s *fakeStore) Insert(ctx context.Context, row timerspec.TimerRow) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.insertErr != nil {
		return s.insertErr
	}
	s.rows[row.ID] = row
	return nil
}

func (s *fakeStore) Delete(ctx context.Context, id timerspec.TimerID) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.deleteErr != nil {
		return false, s.deleteErr
	}
	_, existed := s.rows[id]
	delete(s.rows, id)
	return existed, nil
}

func (s *fakeStore) MoveToDead(ctx context.Context, id timerspec.TimerID, class timerspec.DeathClass, reason, detail string, diedAt int64) (bool, int, error) {
	moved, err := s.Delete(ctx, id)
	return moved, 0, err
}

func (s *fakeStore) Due(ctx context.Context, now int64, limit int) ([]timerspec.TimerRow, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.dueErr != nil {
		return nil, s.dueErr
	}
	var out []timerspec.TimerRow
	for _, r := range s.rows {
		if r.FireAt <= now {
			out = append(out, r)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].FireAt < out[j].FireAt })
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

func (s *fakeStore) NextFireAt(ctx context.Context) (int64, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.nextErr != nil {
		return 0, false, s.nextErr
	}
	var next int64
	ok := false
	for _, r := range s.rows {
		if !ok || r.FireAt < next {
			next, ok = r.FireAt, true
		}
	}
	return next, ok, nil
}

func (s *fakeStore) CancelOwned(ctx context.Context, id timerspec.TimerID, author actor.ActorID) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.cancelErr != nil {
		return false, s.cancelErr
	}
	r, ok := s.rows[id]
	if !ok || r.AuthorID != author {
		return false, nil
	}
	delete(s.rows, id)
	return true, nil
}

func (s *fakeStore) rowCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.rows)
}

func (s *fakeStore) hasRow(id timerspec.TimerID) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.rows[id]
	return ok
}

var _ timerspec.TimerStore = (*fakeStore)(nil)

// ---------------------------------------------------------------------
// fakeFireSink: records every Append call and answers via a scriptable
// respond func (nil → always succeed) — the seam the tri-state matrix drives.
// ---------------------------------------------------------------------

type fireCall struct {
	author actor.ActorID
	env    *message.Envelope
}

type fakeFireSink struct {
	mu      sync.Mutex
	calls   []fireCall
	respond func(author actor.ActorID, env *message.Envelope) error
}

func (f *fakeFireSink) Append(ctx context.Context, author actor.ActorID, env *message.Envelope) error {
	f.mu.Lock()
	f.calls = append(f.calls, fireCall{author: author, env: env})
	respond := f.respond
	f.mu.Unlock()
	if respond == nil {
		return nil
	}
	return respond(author, env)
}

func (f *fakeFireSink) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.calls)
}

func (f *fakeFireSink) lastCall() fireCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls[len(f.calls)-1]
}

var _ FireSink = (*fakeFireSink)(nil)

// ---------------------------------------------------------------------
// fakeReviver: records EnsureLive calls, answers with a scriptable error.
// ---------------------------------------------------------------------

type fakeReviver struct {
	mu    sync.Mutex
	calls []actor.ActorID
	err   error
}

func (r *fakeReviver) EnsureLive(ctx context.Context, id actor.ActorID) error {
	r.mu.Lock()
	r.calls = append(r.calls, id)
	err := r.err
	r.mu.Unlock()
	return err
}

func (r *fakeReviver) callCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.calls)
}

var _ Reviver = (*fakeReviver)(nil)

// stubActor is a minimal actorrt.Actor — never actually receives anything in
// these tests (the engine never talks to the mailbox), it exists only
// so a real *actorrt.Runtime has something live to weld an Incarnation to.
type stubActor struct{}

func (stubActor) Receive(ctx context.Context, env *message.Envelope) error { return nil }

// newTestRuntime spins up a real *actorrt.Runtime as the engine's
// LivenessProbe. actorrt.Incarnation's fields are unexported (by design —
// pidfd-analogue, never externally constructible), so a real Runtime is the
// only legitimate way to produce one outside actorrt itself; *actorrt.Runtime
// already satisfies LivenessProbe (CurrentIncarnation + IsLive) directly.
func newTestRuntime(t *testing.T) *actorrt.Runtime {
	t.Helper()
	rt, _ := actorrt.New(actorrt.Config{Parent: context.Background()})
	t.Cleanup(rt.StopAll)
	return rt
}

// ---------------------------------------------------------------------
// fakeClock: a deterministic Clock — Now() reads a manually-advanced instant,
// NewAlarm registers a due-tracked entry (keyed by an ABSOLUTE deadline, per
// clock.go's doc — no relative-duration conversion for Advance() to race)
// fired only by Advance. armedCount / lastArmedDuration observe the run
// loop's own alarm-arming behaviour (busy-loop / backoff-pacing assertions).
// ---------------------------------------------------------------------

type fakeClock struct {
	mu       sync.Mutex
	now      time.Time
	pending  []*fakeTimer
	armCount int
	lastArm  time.Duration // deadline - now AT ARM TIME, for observability only
}

func newFakeClock(start time.Time) *fakeClock {
	return &fakeClock{now: start}
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) NewAlarm(deadline time.Time) Timer {
	c.mu.Lock()
	entry := &fakeTimer{deadline: deadline, ch: make(chan time.Time, 1)}
	c.armCount++
	c.lastArm = deadline.Sub(c.now)
	if !deadline.After(c.now) {
		// Mirrors real time.NewTimer(d<=0): fires AT ONCE rather than
		// waiting for some future Advance() call that may never come once
		// the clock has already passed this deadline (e.g. a stale wake
		// races a re-arm computed from an already-past `next` snapshot —
		// by the time the fresh alarm is armed, Advance() already ran and
		// moved `now` past it; a passively-pending entry would then wait
		// forever for an Advance() call nobody makes again).
		entry.fired = true
		entry.ch <- c.now
	} else {
		c.pending = append(c.pending, entry)
	}
	c.mu.Unlock()
	return entry
}

// Advance moves the clock forward by d and fires (non-blocking send into the
// entry's own buffered channel) every still-armed entry whose deadline is
// now due, in deadline order — mirroring real timers all firing once their
// wall-clock deadline passes.
func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(d)
	now := c.now
	var due, remaining []*fakeTimer
	for _, e := range c.pending {
		if e.markDueIfPending(now) {
			due = append(due, e)
		} else if !e.settled() {
			remaining = append(remaining, e)
		}
		// settled-but-not-due (already stopped, or already fired by a prior
		// Advance) entries are dropped from pending here — lazy cleanup.
	}
	c.pending = remaining
	c.mu.Unlock()
	for _, e := range due {
		e.ch <- now
	}
}

func (c *fakeClock) armedCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.armCount
}

func (c *fakeClock) lastArmedDuration() time.Duration {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.lastArm
}

type fakeTimer struct {
	mu       sync.Mutex
	deadline time.Time
	ch       chan time.Time
	fired    bool
	stopped  bool
}

func (t *fakeTimer) C() <-chan time.Time { return t.ch }

func (t *fakeTimer) Stop() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.fired || t.stopped {
		return false
	}
	t.stopped = true
	return true
}

// markDueIfPending reports whether this entry is due AS OF now, flipping
// fired=true (and thus claiming the send) exactly once.
func (t *fakeTimer) markDueIfPending(now time.Time) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.stopped || t.fired || t.deadline.After(now) {
		return false
	}
	t.fired = true
	return true
}

func (t *fakeTimer) settled() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.stopped || t.fired
}

var _ Clock = (*fakeClock)(nil)

// ---------------------------------------------------------------------
// waitFor: bounded polling for the run-loop goroutine's asynchronous effects
// to become observable. This is test-synchronization glue only — the actual
// fire-timing decisions under test are 100% governed by the injected
// fakeClock (zero wall-clock dependence for correctness), never by how long
// this polling takes to notice (same goroutine-sync pattern as cell_test.go).
// ---------------------------------------------------------------------

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

func waitForArmedAtLeast(t *testing.T, clock *fakeClock, n int) {
	t.Helper()
	waitFor(t, 2*time.Second, func() bool { return clock.armedCount() >= n })
}

// advanceUntil repeatedly nudges the fake clock forward by step and checks
// cond, bounded by an overall timeout. A single big Advance() can race a
// STALE alarm left over from an already-superseded schedule (e.g. one
// Schedule immediately Cancel-and-rescheduled to the same instant): whichever
// pending entry Advance happens to catch is consumed, but the fresh entry the
// engine re-arms afterwards would then need a SECOND Advance to reach. Nudging
// repeatedly converges regardless of exactly which entry the engine currently
// has armed, since the target instant is a fixed point every nudge moves
// strictly closer to (or past).
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

// waitStable polls val until it stops changing for a quiet window, returning
// the settled value — used where a coalesced wake token can legitimately
// cause one extra bounded transition before a loop's steady state holds
// (dropping a wake is harmless since the next round recomputes; an extra
// stale wake is equally harmless, just an extra recompute).
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
