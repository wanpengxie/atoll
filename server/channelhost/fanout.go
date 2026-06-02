package channelhost

import (
	"context"

	"github.com/wanpengxie/ActOS/kernel/actor"
	khrn "github.com/wanpengxie/ActOS/kernel/harness"
	"github.com/wanpengxie/ActOS/kernel/message"
	"github.com/wanpengxie/ActOS/runtime/actorrt"
)

// fanoutChain wraps the harness write chain so EVERY envelope committed to truth
// is fanned out to its audience AFTER it lands — the single post-commit delivery
// path. Local audience cells get it in their mailbox; a request addressed to a
// compute-hosted actor goes down the wire. This is the root fix for "writes don't
// reach anyone": a cell-originated write (an adapter's response/event, a closure
// terminal, a compute emit) now fans out exactly like an ingress request, instead
// of each call-site hand-rolling delivery (or — the bug — silently skipping it,
// so readiness.changed never reached the sysactor cell and responses never
// reached a waiting local caller).
//
// It implements kernel/harness.Chain, so it is injected wherever a chain is
// needed (sysactor / adapters / channelkit death-closure / fleet emits / ingress).
type fanoutChain struct {
	inner  khrn.Chain
	cells  *actorrt.Runtime
	remote func(actor.ActorID, *message.Envelope) bool
	// onUndeliverable closes out a REQUEST whose delivery to its target cell
	// failed (mailbox full / cell stopped) — the receiver structurally cannot
	// accept it, so the home materialises receiver_unavailable rather than let the
	// caller hang to the timeout (risk §7.2). nil → drop silently.
	onUndeliverable func(context.Context, *message.Envelope)
}

// Write runs the real 9-step harness write, then — only on a committed envelope
// (no error, no reject) — delivers it to each audience member: a locally hosted
// cell receives it in its mailbox; otherwise, for a request, the compute hosting
// the target is reached down the wire. Delivery is enqueue-only (never blocks the
// writer), so a cell may Write from its own goroutine without deadlock.
func (f *fanoutChain) Write(ctx context.Context, env *message.Envelope) (khrn.WriteResult, error) {
	res, err := f.inner.Write(ctx, env)
	if err != nil || res.RejectReason != "" {
		return res, err
	}
	for _, aid := range env.Audience {
		switch {
		case f.cells != nil && f.cells.Has(aid):
			if derr := f.cells.Deliver(ctx, []actor.ActorID{aid}, env); derr != nil && env.Kind == message.KindRequest && f.onUndeliverable != nil {
				// Mailbox full / cell stopped — the receiver can't take this
				// request; close it out instead of hanging the caller.
				f.onUndeliverable(ctx, env)
			}
		case env.Kind == message.KindRequest && f.remote != nil:
			f.remote(aid, env)
		}
	}
	return res, nil
}
