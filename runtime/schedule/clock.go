package schedule

import "time"

// Clock is the engine's injected time source: Now() drives every scheduling
// decision (Schedule's past-FireAt-is-legal check, the run loop's due
// comparison), and NewAlarm arms the sleep-until-alarm primitive that keeps
// the poll/wake loop from spinning (block until the nearest deadline, never
// a for{sleep} poll — the same discipline as Linux timerfd/epoll). A
// production Clock wraps time.Now/time.NewTimer directly; a fake Clock
// (schedule_test.go) lets the vertical-slice tests advance time
// deterministically — zero sleep, zero wall-clock flake, the time-axis
// analogue of actorrt.Config.Clock (deterministic testability).
//
// NewAlarm takes an ABSOLUTE deadline, deliberately not a relative duration
// (unlike time.NewTimer's `d`): the run loop reads Now() and computes `next`
// (also absolute) in one step, then calls NewAlarm — if it instead converted
// to a relative duration first, any gap between that computation and the
// alarm actually being armed (a scheduler preemption in production; a test
// harness's Advance() landing in exactly that window) would silently arm for
// the WRONG instant. An absolute deadline has no such conversion to go stale.
type Clock interface {
	Now() time.Time
	// NewAlarm arms a one-shot alarm that fires once Now() reaches deadline
	// (fires immediately if deadline is already <= Now()). The caller MUST
	// Stop() it before arming a replacement (standard Go time.Timer
	// discipline — an un-stopped/undrained stale alarm racing a freshly
	// armed one is exactly the compute-then-sleep window that would drop a
	// wake).
	NewAlarm(deadline time.Time) Timer
}

// Timer is the minimal time.Timer-shaped alarm handle. C receives once when
// the alarm fires. Stop reports whether the alarm was still pending (mirrors
// time.Timer.Stop's bool: false means it already fired or was already
// stopped, telling the caller whether a drain is still owed).
type Timer interface {
	C() <-chan time.Time
	Stop() bool
}

// systemClock is the production Clock — a thin wrapper over the standard
// library. The Platform composition root fills this in
// when the downstream-supplied AssemblyDeps.Clock is nil; the engine's own
// New stays fail-fast on a nil Clock — each layer owns its own default.
type systemClock struct{}

// NewSystemClock returns the real-time Clock implementation.
func NewSystemClock() Clock { return systemClock{} }

func (systemClock) Now() time.Time { return time.Now() }

func (systemClock) NewAlarm(deadline time.Time) Timer {
	return systemTimer{t: time.NewTimer(time.Until(deadline))}
}

type systemTimer struct{ t *time.Timer }

func (s systemTimer) C() <-chan time.Time { return s.t.C }
func (s systemTimer) Stop() bool          { return s.t.Stop() }
