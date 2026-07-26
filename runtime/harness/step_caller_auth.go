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
// There is no channel comparison here: a caller cannot be bound to another
// channel, because nobody hands a channel id to the mint any more. The pen
// stamps the harness's own binding constant (deps.ChannelID) on every write,
// so "caller's channel" and "this harness's channel" are one value by
// construction, not two accounts to reconcile.
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
	return outcome{}, nil
}
