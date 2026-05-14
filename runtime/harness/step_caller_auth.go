package harness

import (
	"context"

	khar "github.com/wanpengxie/ActOS/kernel/harness"
	"github.com/wanpengxie/ActOS/kernel/message"
)

// stepCallerAuth implements L1 §10.2 step 1 — caller token / channel
// membership. The harness consumes the CallerContext attached via
// CtxWithCaller (set by the binding edge: workerhost, control handler,
// adapter framework). When no caller is attached we reject auth_failed
// (defensive — every legitimate edge wires it).
//
// The step also enforces envelope.channel_id == caller.ChannelID:
// envelope.channel_id NOT matching the bound channel is a routing /
// permission error. The L1 §10.3.1 closed set has no `channel_mismatch`
// reason; we collapse the case to auth_failed (the spec-defined value
// covering "caller has no permission on this channel").
type stepCallerAuth struct {
	deps Deps
}

func newStepCallerAuth(d Deps) khar.Step { return &stepCallerAuth{deps: d} }

func (s *stepCallerAuth) ID() khar.StepID { return khar.StepCallerAuth }

func (s *stepCallerAuth) Run(ctx context.Context, env *message.Envelope) (khar.Outcome, error) {
	caller := CallerFromCtx(ctx)
	if caller.ActorID == "" {
		return khar.Outcome{
			RejectReason: message.HarnessAuthFailed,
			Detail:       "harness: missing caller context",
		}, nil
	}
	if caller.ChannelID != "" && caller.ChannelID != s.deps.ChannelID {
		return khar.Outcome{
			RejectReason: message.HarnessAuthFailed,
			Detail:       "harness: caller bound to a different channel",
		}, nil
	}
	if env.ChannelID != "" && env.ChannelID != string(s.deps.ChannelID) {
		return khar.Outcome{
			RejectReason: message.HarnessAuthFailed,
			Detail:       "harness: envelope.channel_id does not match bound channel",
		}, nil
	}
	// engine ts_received normalize — caller may leave channel_id blank
	// in tests; populate from the bound channel here.
	if env.ChannelID == "" {
		env.ChannelID = string(s.deps.ChannelID)
	}
	return khar.Outcome{}, nil
}
