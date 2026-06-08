package channelhost

import (
	"context"

	"github.com/wanpengxie/ActOS/kernel/actor"
	"github.com/wanpengxie/ActOS/kernel/message"
	"github.com/wanpengxie/ActOS/runtime/actorrt"
	"github.com/wanpengxie/ActOS/runtime/harness"
)

// fanoutWriter wraps harness.Writer (the harness *Chain) so EVERY envelope
// committed to truth is fanned out to its audience AFTER it lands -- the single
// post-commit delivery path. Local audience cells get it in their mailbox via
// Deliverer; a request addressed to a compute-hosted actor goes down the wire
// via remoteDispatch; and client WS tails are woken via pushHub.
//
// It satisfies harness.Writer so it is injected wherever a writer is needed
// (sysactor / channelkit death-closure / fleet emits / ingress).
type fanoutWriter struct {
	inner    harness.Writer
	deliverer actorrt.Deliverer
	// remoteDispatch routes an envelope DOWN to the compute hosting target.
	// Returns true if dispatched. Nil = no remote computes.
	remoteDispatch func(actor.ActorID, *message.Envelope) bool
	// hub wakes external client streams after a committed envelope.
	hub *pushHub
}

// Write runs the real 9-step harness write, then -- only on a committed envelope
// (no error, no reject) -- delivers it to each audience member: a locally hosted
// cell receives it in its mailbox; otherwise, for a request, the compute hosting
// the target is reached down the wire. Delivery is enqueue-only (never blocks the
// writer), so a cell may Write from its own goroutine without deadlock.
func (f *fanoutWriter) Write(ctx context.Context, env *message.Envelope) (harness.WriteResult, error) {
	res, err := f.inner.Write(ctx, env)
	if err != nil || res.RejectReason != "" {
		return res, err
	}

	// Wake external client streams (they read forward by seq).
	if f.hub != nil {
		f.hub.notify()
	}

	// Deliver to audience: local cells via Deliverer, remote via wire dispatch.
	if f.deliverer != nil {
		result, _ := f.deliverer.Deliver(env.Audience, env)
		// For each audience member not hosted locally, try remote dispatch.
		if f.remoteDispatch != nil {
			for aid, outcome := range result.Per {
				if outcome == actorrt.NotHosted {
					f.remoteDispatch(aid, env)
				}
			}
		}
	}

	return res, nil
}
