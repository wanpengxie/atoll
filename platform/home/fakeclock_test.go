package home

import (
	"sync"
	"time"

	"github.com/wanpengxie/atoll/runtime/schedule"
)

// fakeClock/fakeAlarm are the platform-package twin of schedule/fakes_test.go's
// own fakeClock (and runtime/timer_integration_test.go's copy) — package-private
// to each test binary, so this package needs its own copy of the same semantics
// to satisfy schedule.Clock deterministically. Needed so the S6 attached-host
// revive test can drive the engine's poll/backoff loop (schedule.backoffDuration
// pace) without a real wall-clock wait.

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
// whose deadline is now due (non-blocking send into each alarm's own buffered
// channel), mirroring real timers all firing once their deadline passes.
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
