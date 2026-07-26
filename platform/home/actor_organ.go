package home

import (
	"github.com/wanpengxie/atoll/runtime"
	"github.com/wanpengxie/atoll/runtime/accessdoor"
	"github.com/wanpengxie/atoll/runtime/actorctl"
	"github.com/wanpengxie/atoll/runtime/actorstore"
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

	// entries is the state organ's backing-route seam (IsEntry). It is handed
	// to accessdoor at assembly and consumed nowhere else.
	entries accessdoor.EntryReader
}

// newActorOrgan builds the actor record store and Controller of one channel.
func newActorOrgan(cs *runtime.ChannelStores, nowMs func() int64) (actorOrgan, error) {
	store, err := actorstore.New(cs.Actors, nowMs)
	if err != nil {
		return actorOrgan{}, err
	}
	controller, err := actorctl.New(store, nowMs)
	if err != nil {
		return actorOrgan{}, err
	}
	return actorOrgan{controller: controller, entries: store}, nil
}
