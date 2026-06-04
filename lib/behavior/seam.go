package behavior

import (
	"context"

	"github.com/wanpengxie/ActOS/kernel/actor"
	"github.com/wanpengxie/ActOS/kernel/message"
)

// seam.go is the INJECTION face of the behaviour base. behaviour must commit
// terminal/event into channel truth, recover an in-flight request, and drain a
// dead actor's open requests — yet it stays pure-kernel (importing it must not
// drag runtime into the importer, arbitration-2). Every such capability is
// therefore expressed as a CONSUMER-side interface over kernel types only; the
// real implementation (the harness write path, the channel store) is injected
// by the composition root.

// WriteOutcome is the pure result of committing an envelope to truth.
//
// Duplicate=true means this write lost the one-terminal-per-request race — a
// benign outcome (the request was already closed by another author / the caller
// timeout). RejectReason is an OPAQUE diagnostic string the behaviour base does
// NOT interpret: the reject vocabulary belongs to runtime (the write engine's
// errno), and the composition-injected writer is what maps a runtime
// harness_terminal_duplicate into Duplicate=true. behaviour reads only the bool.
type WriteOutcome struct {
	MessageID    message.ID
	Duplicate    bool
	RejectReason string
	RejectDetail string
}

// ResponseWriter is the seam for committing an envelope into channel truth.
// All three closure authors write through it.
//
// CONCURRENCY CONTRACT (= the harness): Write may be called from ANY goroutine
// — author#2's caller-scoped timer fires Write OFF the cell goroutine. The
// injected implementation MUST be safe for concurrent calls.
type ResponseWriter interface {
	Write(ctx context.Context, env *message.Envelope) (WriteOutcome, error)
}

// RequestLookup is the consumer-side seam for recovering an original request
// envelope by id (the SERVE side — used by an actor that holds local truth).
// Defined here over kernel types only so behaviour stays pure-kernel; a concrete
// store satisfies it structurally and the composition root injects it.
//
// runtime/storespec declares a same-shaped RequestLookup. That is NOT a
// coincidence to reconcile: both are the same semantic ("fetch a request
// envelope by id") projected into two layers, and this one is the pure-kernel
// projection behaviour is allowed to depend on. The storespec interface
// structurally satisfies this seam — composition injects it directly.
type RequestLookup interface {
	FindByID(ctx context.Context, id message.ID) (*message.Envelope, bool, error)
}

// OpenRequests is author#3's closure-drain seam — all of a (dead) actor's
// in-flight requests as raw envelopes.
//
// It MUST return ALL of them (unbounded): a limit would silently leave some
// caller unclosed (a black hole). No paging — closure correctness requires the
// full set.
type OpenRequests interface {
	OpenRequestsForActor(ctx context.Context, actorID actor.ActorID) ([]*message.Envelope, error)
}
