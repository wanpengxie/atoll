package base

import (
	"context"
	"time"

	"github.com/wanpengxie/atoll/lib/actorbase"
	"github.com/wanpengxie/atoll/lib/behavior"
	"github.com/wanpengxie/atoll/lib/metatool"
	"github.com/wanpengxie/atoll/protocol/message"
)

// ExecFace binds all metatool calls to the incarnation's one JobTable. Even
// transient introspection submits the full RequestSpec, awaits it, and cancels
// the row on timeout; no reduced sys.Call path can discard correlation fields.
func ExecFace(sys actorbase.Sys, fastPathWindow time.Duration) *metatool.Exec {
	jobs, _ := sys.(actorbase.JobTable)
	return &metatool.Exec{
		Jobs:           jobs,
		Call:           transientCallBridge(jobs),
		Clock:          time.Now,
		FastPathWindow: fastPathWindow,
	}
}

// transientCallBridge is the bounded introspection face. Unlike call_actor, a
// timed-out transient query is explicitly cancelled instead of retained.
func transientCallBridge(jobs actorbase.JobTable) metatool.CallFunc {
	return func(ctx context.Context, spec behavior.RequestSpec, window time.Duration) (*message.Envelope, bool, error) {
		id, err := jobs.Submit(spec)
		if err != nil {
			return nil, false, err
		}
		env, ok, err := jobs.Await(ctx, id, window)
		if err != nil {
			_ = jobs.Cancel(id)
			return nil, false, err
		}
		if !ok {
			_ = jobs.Cancel(id)
			return nil, false, nil
		}
		return env, true, nil
	}
}
