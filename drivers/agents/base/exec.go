package base

import (
	"context"
	"errors"
	"time"

	"github.com/wanpengxie/atoll/lib/actorbase"
	"github.com/wanpengxie/atoll/lib/behavior"
	"github.com/wanpengxie/atoll/lib/metatool"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/message"
)

// ExecFace builds the meta-tool execution face from this incarnation's Sys
// (期10 S5). The seven meta-tools drive TWO substrate faces:
//
//   - the out-station JobTable (call_actor/await_result/cancel/list_pending):
//     the engine IS the JobTable, so the base extracts it via
//     sys.(actorbase.JobTable) — the engine concrete type implements both Sys
//     and JobTable (spec §5"形施工定"授权面内).
//   - a synchronous sys.Call face (describe×2/list_actors): a transient
//     request+await-final round-trip that never litters the durable job table.
//
// fastPathWindow is the volatile inline-wait cap (the sync EXPERIENCE);
// resolver supplies the per-(target,type) closure deadline when a call omits
// its own (nil = DefaultTimeout). The provider builds this once per incarnation
// (inside NewEngine) and threads it into its engine's tool handlers.
func ExecFace(sys actorbase.Sys, fastPathWindow time.Duration, resolver func(target actor.ActorID, reqType string) (time.Duration, bool)) *metatool.Exec {
	jobs, _ := sys.(actorbase.JobTable)
	return &metatool.Exec{
		Jobs:            jobs,
		Call:            callFace(sys),
		Clock:           time.Now,
		FastPathWindow:  fastPathWindow,
		TimeoutResolver: resolver,
	}
}

// callFace wraps sys.Call + Pending.Wait into the metatool.CallFunc the
// introspection queries drive: submit a transient request, block up to window
// for its final. ok=false = no final within the window (the Pending returns a
// zero Msg).
func callFace(sys actorbase.Sys) metatool.CallFunc {
	return func(ctx context.Context, spec behavior.RequestSpec, window time.Duration) (*message.Envelope, bool, error) {
		if len(spec.Audience) == 0 {
			return nil, false, errors.New("agent/base: call audience required")
		}
		p, err := sys.Call(spec.Audience[0], spec.Type, spec.Payload)
		if err != nil {
			return nil, false, err
		}
		msg, err := p.Wait(ctx, window)
		if err != nil {
			return nil, false, err
		}
		if msg.ID == "" {
			// No final within the window (Pending.Wait returns a zero Msg).
			return nil, false, nil
		}
		env := envelopeFromMsg(msg)
		return &env, true, nil
	}
}
