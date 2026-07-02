package harness

import (
	"context"

	"github.com/wanpengxie/atoll/protocol/message"
)

// stepCallerAuth validates the caller principal. The harness consumes the
// caller attached via ctxWithCaller (set by the boundPen from the welded
// principal before driving the chain). When no caller is attached we reject
// harness_engine_acl_denied (defensive — the boundPen always wires it).
//
// Channel mismatch detection is split:
//
//   - caller.ChannelID vs the harness-bound channel is a *caller* identity
//     mismatch (caller bound to the wrong channel) → harness_engine_acl_denied.
//   - envelope.channel_id vs the harness-bound channel is an *envelope*
//     shape error → harness_channel_mismatch, emitted by step_envelope_shape
//     (Step 2). step 0+1 leaves the envelope.channel_id alone so Step 2 can
//     surface the dedicated reason.
type stepCallerAuth struct {
	deps Deps
}

func newStepCallerAuth(d Deps) step { return &stepCallerAuth{deps: d} }

func (s *stepCallerAuth) ID() stepID { return StepCallerAuth }

func (s *stepCallerAuth) Run(ctx context.Context, env *message.Envelope) (outcome, error) {
	c := callerFromCtx(ctx)
	if c.actorID == "" {
		return outcome{
			RejectReason: HarnessEngineACLDenied,
			Detail:       "harness: missing caller context",
		}, nil
	}
	if c.chID != "" && c.chID != s.deps.ChannelID {
		return outcome{
			RejectReason: HarnessEngineACLDenied,
			Detail:       "harness: caller bound to a different channel",
		}, nil
	}
	return outcome{}, nil
}
