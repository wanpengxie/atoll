package runtime

import "github.com/wanpengxie/atoll/runtime/schedule"

// OpenScheduler assembles the time-axis engine over an already-open channel
// store. It is a SECOND assembly step, deliberately split from OpenChannel:
// the engine's one non-ambient collaborator (FireSink — mint-a-pen-per-fire)
// only exists once the harness chain is wired, which happens one layer above
// OpenChannel (platform's home assembly root; runtime's own integration tests
// hold the same authority). So the sequence is always
// OpenChannel(stores) → OpenScheduler(stores, deps).
//
// The durable TimerStore is drawn from cs's unexported timers field — never
// from the caller: a raw TimerStore reachable downstream would be a delayed
// forged-author write path around the pen, same posture as Access's R/byte
// collaborators never crossing the ChannelStores boundary. That is exactly
// why AssemblyDeps has no Store field: Deps.Store is filled here, invisible
// to every caller of this function.
//
// deps.Clock nil defaults to the real wall clock (schedule.Engine.New stays
// fail-fast on a nil Clock — only THIS seam may default it). Every other
// required dep (Fire/Host/Revive) is forwarded unchanged into schedule.Deps
// and rejected by New's existing fail-fast checks — no duplicate validation
// here.
//
// The engine's run-loop goroutine is NOT started here: Start/Close is the
// caller's responsibility, synchronised with the channel's own open/close
// lifecycle. A fresh process rebuilds NextFireAt from the durable timers
// table (and starts with an empty incarnation in-memory due-set) on Start —
// that reconstruction IS the engine's entire "recovery" story, no separate
// resume step: incarnation-bound timers vanish with the process by
// construction and are never something to recover.
func OpenScheduler(cs *ChannelStores, deps schedule.AssemblyDeps) (schedule.Minter, *schedule.Engine, error) {
	clock := deps.Clock
	if clock == nil {
		clock = schedule.NewSystemClock()
	}
	return schedule.New(schedule.Deps{
		Store:  cs.timers,
		Fire:   deps.Fire,
		Clock:  clock,
		Logger: deps.Logger,
	})
}
