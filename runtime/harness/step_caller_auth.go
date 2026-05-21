package harness

import (
	"context"

	khar "github.com/wanpengxie/ActOS/kernel/harness"
	"github.com/wanpengxie/ActOS/kernel/message"
)

// stepCallerAuth implements proto-layer1 §2.0 step 0+1 — caller principal
// validation and fence check. The harness consumes the CallerContext
// attached via CtxWithCaller (set by the binding edge: workerhost,
// control handler, adapter framework). When no caller is attached we
// reject harness_engine_acl_denied (defensive — every legitimate edge wires it).
//
// Channel mismatch detection is split:
//
//   - caller.ChannelID vs the harness-bound channel is a *caller* identity
//     mismatch (caller bound to the wrong channel) → harness_engine_acl_denied.
//   - envelope.channel_id vs the harness-bound channel is an *envelope*
//     shape error per proto-layer1 §2.2 #2 → harness_channel_mismatch,
//     emitted by step_envelope_shape (Step 2). step 0+1 leaves the
//     envelope.channel_id alone so Step 2 can surface the dedicated
//     reason.
type stepCallerAuth struct {
	deps Deps
}

func newStepCallerAuth(d Deps) khar.Step { return &stepCallerAuth{deps: d} }

func (s *stepCallerAuth) ID() khar.StepID { return khar.StepCallerAuth }

func (s *stepCallerAuth) Run(ctx context.Context, env *message.Envelope) (khar.Outcome, error) {
	caller := CallerFromCtx(ctx)
	if caller.ActorID == "" {
		return khar.Outcome{
			RejectReason: message.HarnessEngineACLDenied,
			Detail:       "harness: missing caller context",
		}, nil
	}
	if caller.ChannelID != "" && caller.ChannelID != s.deps.ChannelID {
		return khar.Outcome{
			RejectReason: message.HarnessEngineACLDenied,
			Detail:       "harness: caller bound to a different channel",
		}, nil
	}
	return khar.Outcome{}, nil
}
