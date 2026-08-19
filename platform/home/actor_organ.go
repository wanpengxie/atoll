package home

import (
	"github.com/wanpengxie/atoll/runtime/actorctl"
	"github.com/wanpengxie/atoll/runtime/actorstore"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

// actorOrgan is the assembled actor-record organ of one channel: the record
// store, wired straight to the durable registry face, plus the Controller that
// owns the value ledger over it.
//
// The store never escapes this assembly. Home keeps the Controller (commands +
// narrow projections) and one closed classification seam it merely threads on
// to the state organ; it never calls into the store itself.
type actorOrgan struct {
	controller *actorctl.Controller
}

// newActorOrgan builds the actor record store and Controller of one channel. It
// takes the durable registry face alone — the one thing it needs. Taking the
// whole assembly bundle would hand this organ a nominal claim on the raw log
// and the leaf ports, which is exactly what its doc comment above disclaims.
func newActorOrgan(registry storespec.ActorRegistryStore, nowMs func() int64) (actorOrgan, error) {
	store, err := actorstore.New(registry, nowMs)
	if err != nil {
		return actorOrgan{}, err
	}
	controller, err := actorctl.New(store, nowMs)
	if err != nil {
		return actorOrgan{}, err
	}
	return actorOrgan{controller: controller}, nil
}
