package adapterhost

import (
	"context"

	"github.com/wanpengxie/ActOS/lib/behavior"
)

// RunHeartbeat polls the module's Heartbeater (if any) and folds the result
// into the actor's own serviceable-state (advisory domain self-state, surfaced
// via actor.status — no registry projection). The cell self-schedules a ticker
// that delivers a heartbeat job to this method (timer goroutine never touches
// state). Collapse of Manager heartbeat goroutine manager.go:523.
func (a *adapterActor) RunHeartbeat(ctx context.Context) error {
	hb, ok := a.module.(behavior.Heartbeater)
	if !ok {
		return nil
	}
	rep, err := hb.Heartbeat(ctx)
	if err != nil {
		return err
	}
	a.setReadiness(rep.Available, rep.Reason)
	return nil
}

// reapExpired drops in-flight requests whose deadline has passed (actor-local
// bounded reaper). The cell self-schedules this on a slow tick — it runs on the
// cell goroutine so it touches a.inflight with no lock. The terminal itself is
// the caller's caller-scoped responsibility (substrate author #2); this only
// bounds the cell's memory for requests it never answered. A request with no
// deadline (env.ExpiresAt nil) stays until its terminal is written.
func (a *adapterActor) reapExpired(nowMs int64) {
	for k, env := range a.inflight {
		if env.ExpiresAt != nil && *env.ExpiresAt > 0 && nowMs >= *env.ExpiresAt {
			delete(a.inflight, k)
		}
	}
}
