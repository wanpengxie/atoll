package actorbase

import (
	"context"
	"fmt"
)

// Handler is Serve's per-type entry — a flat function, not a struct with a
// method table (spec §2: "复杂业务直观/易排查/易测试三判据进程形全胜"). ctx
// arrives pre-bound to msg.Ctx() so a Serve-shaped author carries zero ctx-
// provenance burden (the Sys godoc's rule is for raw Proc loops only).
type Handler func(ctx context.Context, msg Msg) (any, error)

// Serve is the ONE blessed sugar over the raw Proc contract (spec §2: "Serve
// 糖之于裸 process" — gen_server 之于 gen_fsm's cautionary tale, no second
// framework layer gets built beside it). It folds a routes table into a Proc
// that loops sys.Recv(), dispatches on msg.Type, and maps the handler's return
// value onto the two write tiers (spec B-P2): a nil error replies with the
// value, a non-nil error fails with the GENERIC internal_error code (the
// precise-code tier is sys.Fail itself — reachable only by dropping to a raw
// Proc, since a Handler has no Sys to call it on). An unrouted Type fails
// type_unsupported without ever reaching a handler.
//
// Serve never stops serving because ONE write came back rejected — the loop's
// job is dispatching deliveries, not policing the substrate's own write
// verdicts; a write failure is the concern of whatever obs/return-value
// surface the engine (S2) attaches to Reply/Fail, not this sugar.
func Serve(routes map[string]Handler) Proc {
	return func(sys Sys) error {
		for {
			msg, err := sys.Recv()
			if err != nil {
				return err
			}
			dispatch(sys, msg, routes)
		}
	}
}

// dispatch runs one delivery through routes against sys — split out of Serve's
// loop so it is a pure, directly testable unit against a fake Sys (spec S1's
// deliverable: "Serve 的路由分发逻辑可测").
func dispatch(sys Sys, msg Msg, routes map[string]Handler) {
	h, ok := routes[msg.Type]
	if !ok {
		_, _ = sys.Fail(msg, "type_unsupported", fmt.Sprintf("no route for type %q", msg.Type))
		return
	}
	v, err := h(msg.Ctx(), msg)
	if err != nil {
		_, _ = sys.Fail(msg, "internal_error", err.Error())
		return
	}
	_ = sys.AckTimer(msg)
	_, _ = sys.Reply(msg, v)
}
