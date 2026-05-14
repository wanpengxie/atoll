package adapter

import (
	"context"

	"github.com/coagent-ai/daemon-go/kernel/message"
)

// Ctx is the v4 adapter framework F5 contract — the per-call context
// the daemon hands to a Module.Handle invocation.
//
// Ctx wraps the incoming envelope + a Respond callback that submits
// the terminal response envelope back through the daemon's harness
// (same write path as any other actor).
//
// kernel/adapter only defines the contract; concrete implementation
// lives in runtime/scheduler per T3.
type Ctx interface {
	context.Context

	// Request returns the in-flight envelope being handled.
	Request() *message.Envelope

	// Respond submits the terminal response envelope back through the
	// daemon's harness. Implementations MUST set parent_id /
	// correlation_id / sender from the in-flight envelope.
	Respond(env *message.Envelope) error
}
