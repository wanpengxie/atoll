package platform

import (
	"github.com/wanpengxie/atoll/lib/actorbase"
)

// humanCellFactory is the platform's built-in home-side human embodiment (CORE1
// minimal). user域 supply is platform internal政 — a per-channel human member's
// authority lives only in the channel's own registry (the app cannot enumerate
// it), so the reconcile ring keeps a live human cell up whenever the member is
// admitted, without any app-injected factory.
//
// Proc shape (through the actorbase engine, NOT a raw actorrt.Actor implementer —
// archtest wall): the full三选 (immediate human.message / deferred human.approve)
// + the Resolve/Cancel/After door land in CORE2 (subjectgate). This minimal form
// answers a call to an absent-device human by leaving the request OPEN — the
// DEFERRED honest option (三层律 §3), never the old humanFront.Receive no-op that
// reported Delivered (the dishonest fourth state).
func humanCellFactory() ActorFactory {
	return ActorFactory{Proc: actorbase.Def{
		Doc: "home-side human embodiment (CORE1 minimal): callable; leaves every request OPEN (deferred三选) — the person answers via the door (CORE2 subjectgate)",
		New: func() (actorbase.Proc, error) { return humanProc, nil },
	}}
}

// humanProc drains its mailbox but never responds: a call to a human whose device
// is absent is answered by leaving the request OPEN (deferred). It never
// fabricates a Reply/Fail it did not earn — closure is the sender's caller-scoped
// timer, and the person's own Resolve (CORE2) is the real answer. Returning on a
// Recv error is the cooperative termination contract (spec §1.6).
func humanProc(sys actorbase.Sys) error {
	for {
		if _, err := sys.Recv(); err != nil {
			return nil
		}
		// deferred: leave the request open (no Reply/Fail).
	}
}
