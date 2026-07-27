package home

import (
	"sync"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/runtime/schedule"
)

// Shared scaffolding for the restart-dimension acceptance surface: a schedule
// clock whose observed "now" the test can jump forward AFTER a durable row is
// already armed, plus two wait primitives. Every wait here polls a real
// condition or blocks on a channel — a bare sleep is never used as a
// synchroniser.
const (
	// restartWaitBudget is deliberately generous: these tests boot whole Homes
	// under concurrent build load.
	restartWaitBudget = 60 * time.Second
	restartPollEvery  = 5 * time.Millisecond
	// restartAlarmCap bounds every alarm this clock hands the schedule engine.
	// The engine recomputes its full due set on every loop turn, so capping the
	// sleep is behaviour-neutral — it only means a clock jump applied while the
	// engine is parked on an hour-away deadline is observed on the next turn
	// instead of an hour later.
	restartAlarmCap = 20 * time.Millisecond
)

// restartShiftClock is a real-time clock displaced by a test-controlled offset.
// It is how these tests get a REAL durable timer (armed through the ordinary
// production verb, with a FireAt computed from the wall clock) to come due at a
// moment the test chooses, without waiting out the delay and without faking the
// store.
type restartShiftClock struct {
	mu    sync.Mutex
	shift time.Duration
}

func newRestartShiftClock() *restartShiftClock { return &restartShiftClock{} }

func (c *restartShiftClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return time.Now().Add(c.shift)
}

// jump moves the observed now forward by d.
func (c *restartShiftClock) jump(d time.Duration) {
	c.mu.Lock()
	c.shift += d
	c.mu.Unlock()
}

// NewAlarm honours the Clock contract's ABSOLUTE deadline, then caps the wait
// (see restartAlarmCap). A deadline already past still arms an immediately
// firing alarm, exactly like the system clock.
func (c *restartShiftClock) NewAlarm(deadline time.Time) schedule.Timer {
	wait := deadline.Sub(c.Now())
	if wait > restartAlarmCap {
		wait = restartAlarmCap
	}
	return restartAlarm{t: time.NewTimer(wait)}
}

type restartAlarm struct{ t *time.Timer }

func (a restartAlarm) C() <-chan time.Time { return a.t.C }
func (a restartAlarm) Stop() bool          { return a.t.Stop() }

var _ schedule.Clock = (*restartShiftClock)(nil)

// restartEventually polls cond until it holds or the budget runs out.
func restartEventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(restartWaitBudget)
	for {
		if cond() {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(restartPollEvery)
	}
}

// restartRecv takes one value off ch, failing the test if it never arrives.
// Bodies report through channels rather than touching *testing.T, which they
// may not do from their own goroutine.
func restartRecv[T any](t *testing.T, what string, ch <-chan T) T {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(restartWaitBudget):
		var zero T
		t.Fatalf("timed out waiting for %s", what)
		return zero
	}
}
