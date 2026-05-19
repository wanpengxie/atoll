package harness

import (
	"context"

	khar "github.com/wanpengxie/ActOS/kernel/harness"
	"github.com/wanpengxie/ActOS/kernel/message"
)

// stepTypeRegistered implements L1 §10.2 step 4 — `type ∈ (core ∪
// type_registry)`. core types pass through; business types require a
// TypeRegistry lookup hit.
type stepTypeRegistered struct {
	deps Deps
}

func newStepTypeRegistered(d Deps) khar.Step { return &stepTypeRegistered{deps: d} }

func (s *stepTypeRegistered) ID() khar.StepID { return khar.StepTypeRegistered }

func (s *stepTypeRegistered) Run(ctx context.Context, env *message.Envelope) (khar.Outcome, error) {
	if _, isCore := message.CoreTypeTable[env.Type]; isCore {
		return khar.Outcome{}, nil
	}
	if s.deps.TypeRegistry == nil {
		return khar.Outcome{
			RejectReason: message.HarnessUnknownType,
			Detail:       "type registry not wired; only core types allowed",
		}, nil
	}
	if _, ok, err := s.deps.TypeRegistry.Lookup(ctx, env.Type); err != nil {
		return khar.Outcome{}, err
	} else if !ok {
		return khar.Outcome{
			RejectReason: message.HarnessUnknownType,
			Detail:       "type not registered: " + env.Type,
		}, nil
	}
	return khar.Outcome{}, nil
}
