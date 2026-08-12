package home

import (
	"context"

	"github.com/wanpengxie/atoll/platform"
	"github.com/wanpengxie/atoll/platform/subjectgate"
	"github.com/wanpengxie/atoll/protocol/actor"
)

// The functions in this file are the package-to-package assembly bridge used
// by ChannelHost. They deliberately do not turn Home back into a public organ
// bag: callers receive only the exact operation result, while Bundle exposes
// the stable Gateway/Daemon/View capabilities above this bridge.

func Shutdown(h *Home) error { return h.closeInternal("normal") }

// ShutdownWithin closes the Home under the caller's budget instead of the
// Home's own per-barrier defaults. This is the process-shutdown entry: one
// shared deadline bounds every join across every Home, and whatever refuses
// to leave in time is abandoned with its account in the returned error.
// Lifecycle verbs that carry an explicit caller budget use this path too.
func ShutdownWithin(h *Home, ctx context.Context) error {
	return h.closeInternalUnder("normal", ctx)
}

func GatewaySlot(h *Home, id actor.ActorID) (*subjectgate.Slot, bool) {
	return h.subjectSlotFor(id)
}

func GatewaySubscribe(h *Home) (<-chan struct{}, func()) { return h.subscribe() }

// Poke posts a lossy wake to the level reconciler. Correctness never depends
// on delivery: the periodic sweep is the backstop.
func Poke(h *Home) { h.pokeReconcile() }

func DaemonMembrane(h *Home) platform.DaemonMembrane { return h.daemonMembrane }
