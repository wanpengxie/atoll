package adapterhost

import (
	"context"

	"github.com/wanpengxie/ActOS/kernel/actor"
	"github.com/wanpengxie/ActOS/lib/behavior"
)

// HandleExternalCallback applies an inbound external callback ON THE CELL
// GOROUTINE. The host invokes it via actorrt.Runtime.Ask so the
// permanent/retryable/ok verdict is returned SYNCHRONOUSLY to the device
// transit (bad/duplicate/expired frames rejected — dismantle §2.5-A a).
// Collapse of Manager.OnExternalCallback manager.go:1218.
func (a *adapterActor) HandleExternalCallback(ctx context.Context, payload []byte) error {
	return a.module.OnExternalCallback(ctx, payload)
}

// HandleExternalCallbackFrame applies the framework-wrapped relay callback on
// the cell goroutine (host invokes via Ask). Modules that consume the wrapper
// implement behavior.ExternalCallbackFrameAware; others fall back to the raw
// payload path. Collapse of Manager.OnExternalCallbackFrame manager.go:1255.
func (a *adapterActor) HandleExternalCallbackFrame(ctx context.Context, frame behavior.ExternalCallbackFrame) error {
	if aw, ok := a.module.(behavior.ExternalCallbackFrameAware); ok {
		return aw.OnExternalCallbackFrame(ctx, frame)
	}
	return a.module.OnExternalCallback(ctx, frame.Payload)
}

// HandleRuntimeEvent applies a device lifecycle event ON THE CELL GOROUTINE
// (host invokes via actorrt.Runtime.Post — async, fire-and-forget). The module
// folds it into its own live state with no lock. Collapse of
// Manager.OnRuntimeEvent manager.go:1461.
func (a *adapterActor) HandleRuntimeEvent(ctx context.Context, evt behavior.RuntimeEvent) error {
	if aw, ok := a.module.(behavior.RuntimeEventAware); ok {
		return aw.OnRuntimeEvent(ctx, evt)
	}
	return nil
}

// RunHeartbeat polls the module's Heartbeater (if any) and folds the result
// into the actor's own readiness. The cell self-schedules a ticker that
// delivers a heartbeat job to this method (timer goroutine never touches
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
	state := actor.ReadinessNotReady
	if rep.Available {
		state = actor.ReadinessReady
	}
	_, err = a.doUpdateReadiness(ctx, actor.ReadinessUpdate{
		State:     state,
		Reason:    rep.Reason,
		CheckedAt: a.clock().UnixMilli(),
	})
	return err
}

// reapExpired drops correlation entries whose deadline has passed (actor-local
// bounded reaper; collapse of Manager.RunGC manager.go:1564). The cell
// self-schedules this on a slow tick — it runs on the cell goroutine so it
// touches a.correlation with no lock. The terminal itself is the caller's
// caller-scoped responsibility (lib/behavior); this only bounds memory.
func (a *adapterActor) reapExpired(nowMs int64) {
	for k, e := range a.correlation {
		if e.State != behavior.CorrelationPending {
			delete(a.correlation, k)
			continue
		}
		if e.ExpiresAt > 0 && nowMs >= e.ExpiresAt {
			delete(a.correlation, k)
		}
	}
}
