package harness

import (
	"context"
	"errors"

	khar "github.com/wanpengxie/ActOS/kernel/harness"
	"github.com/wanpengxie/ActOS/kernel/message"
)

// PayloadValidator is the per-write seam the harness uses to evaluate
// envelope.payload against a registered JSON Schema fragment. The
// default implementation (adapters/framework.ValidatePayload) handles
// a JSON-Schema subset; production deployments may swap in a full
// JSON Schema Draft 2020-12 implementation via SetPayloadValidator.
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
	if _, isCore := message.CoreTypeTable[env.Type]; isCore {
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
	// DefaultPayloadValidator independently of Chain construction. Schema
	// presence + missing validator is a protocol violation: fail closed
	// with the distinct harness_schema_missing reason (proto-layer1
	// §2.11.1) so callers can tell "schema absent" apart from "payload
	// didn't match schema".
	validator, ok := currentPayloadValidator()
	if !ok {
		return khar.Outcome{
			RejectReason: message.HarnessSchemaMissing,
			Detail:       ErrPayloadValidatorMissing.Error(),
		}, nil
	}
	if err := validator(schema, env.Payload); err != nil {
		return khar.Outcome{
			RejectReason: message.HarnessPayloadSchemaInvalid,
			Detail:       err.Error(),
		}, nil
	}
	return khar.Outcome{}, nil
}

// ErrPayloadValidatorMissing is reported as the step-6 reject detail when a
// type has a schema but the runtime did not wire a validator.
var ErrPayloadValidatorMissing = errors.New("payload schema validator not configured")

// DefaultPayloadValidator is the package-level PayloadValidator. It is
// overridable at runtime (tests inject a stricter validator; production
// daemon wiring rebinds it via SetPayloadValidator). The zero value is
// intentionally nil: schemas without a validator fail closed.
var DefaultPayloadValidator PayloadValidator

func currentPayloadValidator() (PayloadValidator, bool) {
	if DefaultPayloadValidator == nil {
		return nil, false
	}
	return DefaultPayloadValidator, true
}

// PayloadValidatorConfigured reports whether step 6 can evaluate registered
// schemas. Composition roots use it as a boot-time fail-fast check.
func PayloadValidatorConfigured() bool { return DefaultPayloadValidator != nil }

// SetPayloadValidator overrides the validator used by every step 6
// instance going forward. Calling with nil clears the validator, restoring
// the fail-closed default.
func SetPayloadValidator(v PayloadValidator) { DefaultPayloadValidator = v }
