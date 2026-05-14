package harness

import (
	"context"

	khar "github.com/wanpengxie/ActOS/kernel/harness"
	"github.com/wanpengxie/ActOS/kernel/message"
)

// PayloadValidator is the per-write seam the harness uses to evaluate
// envelope.payload against a registered JSON Schema fragment. The
// default implementation (adapters/framework.ValidatePayload) handles
// a JSON-Schema subset; production deployments may swap in a full
// JSON Schema Draft 2020-12 implementation by injecting Deps.Validator.
type PayloadValidator func(schema, payload []byte) error

// stepPayloadSchema implements L1 §10.2 step 6 — payload schema check.
//
// core types pass through (M1.5 baseline ships no core payload schema;
// L0 §2.2 covers the minimal "payload is JSON object" constraint, and
// stepNormalize already substituted `{}` when payload was empty). For
// business types the chain consults TypeRegistry.Lookup(type) and runs
// the schema registered under envelope.kind.
type stepPayloadSchema struct {
	deps Deps
}

func newStepPayloadSchema(d Deps) khar.Step {
	return &stepPayloadSchema{deps: d}
}

func (s *stepPayloadSchema) ID() khar.StepID { return khar.StepPayloadSchema }

func (s *stepPayloadSchema) Run(ctx context.Context, env *message.Envelope) (khar.Outcome, error) {
	if _, isCore := CoreTypeTable[env.Type]; isCore {
		return khar.Outcome{}, nil
	}
	if s.deps.TypeRegistry == nil {
		return khar.Outcome{}, nil
	}
	view, ok, err := s.deps.TypeRegistry.Lookup(ctx, env.Type)
	if err != nil {
		return khar.Outcome{}, err
	}
	if !ok {
		return khar.Outcome{}, nil // unknown_type would have rejected at step 4.
	}
	schema, ok := view.SchemasByKind[env.Kind]
	if !ok || len(schema) == 0 {
		return khar.Outcome{}, nil
	}
	// Resolve the validator at run time so tests can rebind
	// DefaultPayloadValidator independently of Chain construction.
	if err := currentPayloadValidator()(schema, env.Payload); err != nil {
		return khar.Outcome{
			RejectReason: message.HarnessPayloadSchemaViolation,
			Detail:       err.Error(),
		}, nil
	}
	return khar.Outcome{}, nil
}

// DefaultPayloadValidator is the package-level PayloadValidator. It is
// overridable at runtime (tests inject a stricter validator; production
// daemon wiring rebinds it via SetPayloadValidator). Resolve via
// currentPayloadValidator to pick up reassignment after construction.
var DefaultPayloadValidator PayloadValidator = noopValidator

func currentPayloadValidator() PayloadValidator {
	if DefaultPayloadValidator == nil {
		return noopValidator
	}
	return DefaultPayloadValidator
}

func noopValidator(_, _ []byte) error { return nil }

// SetPayloadValidator overrides the validator used by every step 6
// instance going forward. Calling with nil reverts to the no-op.
func SetPayloadValidator(v PayloadValidator) {
	if v == nil {
		DefaultPayloadValidator = noopValidator
		return
	}
	DefaultPayloadValidator = v
}
