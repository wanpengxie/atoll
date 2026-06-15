package presence

import (
	"context"
	"sync"

	"github.com/wanpengxie/ActOS/lib/introspect"
	"github.com/wanpengxie/ActOS/protocol/actor"
	"github.com/wanpengxie/ActOS/runtime/actorrt"
)

// Fold holds the latest opaque presence snapshot per actor. Absence of an entry =
// unknown (never reported, or decayed on link death). It satisfies both
// actorrt.ObsWatcher (fold edges) and actorrt.PresenceWatcher (decay on death).
type Fold struct {
	mu     sync.Mutex
	latest map[actor.ActorID][]byte
}

// New builds an empty fold.
func New() *Fold {
	return &Fold{latest: map[actor.ActorID][]byte{}}
}

// OnObs implements actorrt.ObsWatcher: fold the latest ObsPresence snapshot
// (edge → level). Non-presence obs kinds are ignored — this fold is presence-only.
func (f *Fold) OnObs(_ context.Context, id actor.ActorID, kind actorrt.ObsKind, val actorrt.ObsValue) {
	if string(kind) != introspect.ObsPresence {
		return
	}
	f.mu.Lock()
	f.latest[id] = append([]byte(nil), val...)
	f.mu.Unlock()
}

// OnDown implements actorrt.PresenceWatcher: the actor died — for a remote actor
// that means its link dropped, so its device presence is no longer observable →
// decay to unknown (the L1-death-cascades-L3 backstop,搭 lease 便车,无独立 reaper).
func (f *Fold) OnDown(_ context.Context, id actor.ActorID, _ error) {
	f.mu.Lock()
	delete(f.latest, id)
	f.mu.Unlock()
}

// Device returns the latest opaque presence snapshot for id. known=false means
// UNKNOWN (never reported / decayed) — NOT offline. The caller interprets the
// bytes via introspect.ParsePresence.
func (f *Fold) Device(id actor.ActorID) (val []byte, known bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	v, ok := f.latest[id]
	if !ok {
		return nil, false
	}
	return append([]byte(nil), v...), true
}
