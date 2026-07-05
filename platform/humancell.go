package platform

import (
	"context"

	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/runtime/actorrt"
	"github.com/wanpengxie/atoll/runtime/harness"
)

// humanCellFactory is the platform's built-in home-side human embodiment (CORE1
// minimal). user域 supply is platform internal政 — a per-channel human member's
// authority lives only in the channel's own registry (the app cannot enumerate
// it), so the reconcile ring keeps a live human cell up whenever the member is
// admitted, without any app-injected factory.
//
// Minimal three-选 (三层律 §3): a call to a human whose device is absent is
// answered by leaving the request OPEN — the DEFERRED honest option. The cell
// keeps the actor callable (agent→human delivery lands in its mailbox) but never
// fabricates a Delivered/completed it did not earn (the old humanFront.Receive
// no-op reported Delivered = the dishonest fourth state). The full three-选
// (immediate human.message / deferred human.approve) + the Resolve/Cancel/After
// door land in CORE2 (subjectgate).
//
// Legacy shape (the pen is unused): a human request is answered by the person via
// the door, not by the cell writing truth. The pen is welded by the caps seam like
// any cell's; this occupant simply never reaches for it this period.
func humanCellFactory() ActorFactory {
	return ActorFactory{Legacy: func(harness.Pen) actorrt.Actor { return humanCell{} }}
}

type humanCell struct{}

// Receive leaves every request OPEN (deferred): returning nil records nothing and
// synthesises no terminal — closure is the sender's caller-scoped timer, and the
// person's own Resolve (CORE2) is the real answer.
func (humanCell) Receive(context.Context, *message.Envelope) error { return nil }
