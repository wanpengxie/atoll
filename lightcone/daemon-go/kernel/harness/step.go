package harness

import (
	"context"

	"github.com/coagent-ai/daemon-go/kernel/message"
)

// RejectError is the synchronous reject result of a Harness Step.
//
// A nil RejectError means the step passed; a non-nil RejectError stops
// the chain and surfaces the (Reason, Message) pair to the caller via
// the binding-specific transport (HTTP status table for daemon_rpc;
// Result.Err for in_worker_bus).
//
// TODO(T1): mirror exact pkg/v4types.RejectError shape and align HTTP
// status mapping.
type RejectError struct {
	Reason  HarnessRejectReason
	Message string
}

// Error implements the error interface.
func (e *RejectError) Error() string {
	if e == nil {
		return ""
	}
	if e.Message == "" {
		return string(e.Reason)
	}
	return string(e.Reason) + ": " + e.Message
}

// Step is one validation unit in the Message-Write Harness chain
// (L1 §10).
//
// A Step inspects the in-flight envelope (and the caller-context the
// chain runner passes in) and either:
//
//   - returns (nil, nil) — pass, proceed to next step
//   - returns (nil, &RejectError{...}) — synchronous reject, stop chain
//   - returns (mutated, nil) — pass + propagate normalized envelope
//   - returns (_, err) — runtime error (treated as 500-class)
//
// kernel/harness only defines the contract; the 9 concrete steps live
// in runtime per T3.
type Step interface {
	Name() string
	Run(ctx context.Context, env *message.Envelope) (*message.Envelope, *RejectError, error)
}
