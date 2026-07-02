package devicepresence

import (
	"context"
	"log/slog"
	"sync"

	"github.com/wanpengxie/atoll/lib/introspect"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/runtime/actorrt"
)

// Fold holds the latest opaque device-presence snapshot per actor. Absence of an entry =
// unknown (never reported, or decayed on link death). It satisfies both
// actorrt.ObsWatcher (fold edges) and actorrt.DownWatcher (decay on death).
type Fold struct {
	mu     sync.Mutex
	latest map[actor.ActorID][]byte
	logger *slog.Logger
}

// New builds an empty fold. logger surfaces device-presence flow at DEBUG (folded /
// decayed) so propagation is diagnosable without Info-noise; nil → discard.
func New(logger *slog.Logger) *Fold {
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	return &Fold{latest: map[actor.ActorID][]byte{}, logger: logger}
}

// OnObs implements actorrt.ObsWatcher: fold the latest ObsDevicePresence snapshot
// (edge → level). Non-device-presence obs kinds are ignored — this fold is device-presence-only.
func (f *Fold) OnObs(_ context.Context, id actor.ActorID, kind actorrt.ObsKind, val actorrt.ObsValue) {
	if string(kind) != introspect.ObsDevicePresence {
		return
	}
	f.mu.Lock()
	f.latest[id] = append([]byte(nil), val...)
	f.mu.Unlock()
	f.logger.Debug("devicepresence.folded", "actor", string(id), "snapshot", string(val))
}

// OnDown implements actorrt.DownWatcher: the actor died — for a remote actor
// that means its link dropped, so its device presence is no longer observable →
// decay to unknown (the down-edge decay backstop: abnormal death only — a clean
// deactivation publishes no edge; decay rides the same lease/link-down signal,
// so there is no separate reaper).
func (f *Fold) OnDown(_ context.Context, id actor.ActorID, _ error) {
	f.mu.Lock()
	_, had := f.latest[id]
	delete(f.latest, id)
	f.mu.Unlock()
	// Only the actors that actually carried a folded device presence "decay" — OnDown
	// fires for every dying actor (global watcher), so guard to avoid logging a
	// decay for actors that never had L3.
	if had {
		f.logger.Debug("devicepresence.decayed", "actor", string(id), "cause", "link/cell down")
	}
}

// Device returns the latest opaque device-presence snapshot for id. known=false means
// UNKNOWN (never reported / decayed) — NOT offline. The caller interprets the
// bytes via introspect.ParseDevicePresence.
func (f *Fold) Device(id actor.ActorID) (val []byte, known bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	v, ok := f.latest[id]
	if !ok {
		return nil, false
	}
	return append([]byte(nil), v...), true
}
