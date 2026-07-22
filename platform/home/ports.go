package home

import (
	"context"
	"net/http"

	"github.com/wanpengxie/atoll/platform/subjectgate"
	"github.com/wanpengxie/atoll/protocol/actor"
)

// The functions in this file are the package-to-package assembly bridge used
// by ChannelHost. They deliberately do not turn Home back into a public organ
// bag: callers receive only the exact operation result, while Bundle exposes
// the stable Gateway/Daemon/View capabilities above this bridge.

func BootstrapOwner(ctx context.Context, h *Home, principal string) (actor.ActorID, error) {
	return h.admitChannelOwner(ctx, principal)
}

func BootstrapDeclaration(ctx context.Context, h *Home, in DeclareRequest) (DeclareResult, error) {
	return h.declare(ctx, in)
}

func Shutdown(h *Home) error { return h.closeInternal("normal") }

func GatewaySlot(h *Home, id actor.ActorID) (*subjectgate.Slot, bool) {
	return h.subjectSlotFor(id)
}

func GatewaySubscribe(h *Home) (<-chan struct{}, func()) { return h.subscribe() }

// Poke posts a lossy wake to the level reconciler. Correctness never depends
// on delivery: the periodic sweep is the backstop.
func Poke(h *Home) { h.pokeReconcile() }

func LinkServe(h *Home, w http.ResponseWriter, r *http.Request, daemonID string) {
	h.serveAttach(w, r, daemonID)
}
