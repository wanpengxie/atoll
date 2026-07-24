package presence

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/runtime/actorhost"
	"github.com/wanpengxie/atoll/runtime/actorrt"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

type entry struct {
	val        []byte
	receivedAt int64
	local      actorrt.Incarnation
	remote     actorhost.AttemptKey
}

// Testimony is the latest level testimony for one kind. It is advisory: an
// absent row means unknown, never offline.
type Testimony struct {
	Val                []byte
	ReceivedAt         int64
	StaleFromPriorLife bool
}

// Snapshot is the total four-cell existence view assembled at read time.
// When Member and L1Present are both false, L3 is always empty: existence is
// decided before testimony is consulted.
type Snapshot struct {
	Member      bool
	L1Present   bool
	L1StartedAt time.Time
	L3          map[actorrt.ObsKind]Testimony
}

// Fold owns the bounded latest-value cache. Each row is keyed by the smallest
// lifecycle unit, (actor id, observation kind). Every row is broker-sourced (an
// actor's own incarnation testimony) — the来源轴 (Source/PutDoor) is归一 removed
// (design v0.6.1): device presence now flows as normal broker obs off the human
// cell, re-established per incarnation from its binding slot, not a special
// door-source row that outlived down edges.
type Fold struct {
	mu              sync.Mutex
	latest          map[actor.ActorID]map[actorrt.ObsKind]entry
	levelKinds      map[actorrt.ObsKind]struct{}
	nonLevelLogOnce sync.Once
	clock           func() time.Time
	grace           time.Duration
	logger          *slog.Logger
}

// New builds a fold for the injected level-kind vocabulary. Non-level obs are
// deliberately not accumulated into a second truth-like ledger; the first one
// observed is logged locally at debug level, then the fold stays silent.
func New(logger *slog.Logger, clock func() time.Time, levelKinds []actorrt.ObsKind, sweepGrace time.Duration) *Fold {
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	if clock == nil {
		clock = time.Now
	}
	if sweepGrace <= 0 {
		sweepGrace = 30 * time.Second
	}
	levels := make(map[actorrt.ObsKind]struct{}, len(levelKinds))
	for _, kind := range levelKinds {
		levels[kind] = struct{}{}
	}
	return &Fold{latest: map[actor.ActorID]map[actorrt.ObsKind]entry{}, levelKinds: levels, clock: clock, grace: sweepGrace, logger: logger}
}

func (f *Fold) OnObs(_ context.Context, id actor.ActorID, gen actorrt.Incarnation, kind actorrt.ObsKind, val actorrt.ObsValue) {
	if !f.isLevel(kind) {
		f.nonLevelLogOnce.Do(func() {
			f.logger.Debug("presence.non_level_ignored", "kind", string(kind))
		})
		return
	}
	f.put(id, kind, val, gen, "")
}

// OnRemoteObs records testimony from one exact daemon Body attempt.
func (f *Fold) OnRemoteObs(
	id actor.ActorID,
	key actorhost.AttemptKey,
	kind actorrt.ObsKind,
	val actorrt.ObsValue,
) {
	if !f.isLevel(kind) {
		return
	}
	f.put(id, kind, val, actorrt.Incarnation{}, key)
}

func (f *Fold) put(
	id actor.ActorID,
	kind actorrt.ObsKind,
	val actorrt.ObsValue,
	local actorrt.Incarnation,
	remote actorhost.AttemptKey,
) {
	f.mu.Lock()
	byKind := f.latest[id]
	if byKind == nil {
		byKind = map[actorrt.ObsKind]entry{}
		f.latest[id] = byKind
	}
	_, existed := byKind[kind]
	byKind[kind] = entry{
		val: append([]byte(nil), val...), receivedAt: f.clock().UnixMilli(),
		local: local, remote: remote,
	}
	f.mu.Unlock()
	if !existed {
		// edge-only: this (actor,kind) row went absent→present (online). Not a
		// re-log of the testimony value (P5) — just the oplog fact that a
		// presence edge occurred.
		f.logger.Info("platform.presence.edge", "actor", string(id), "kind", string(kind), "edge", "online")
	}
}

func (f *Fold) isLevel(kind actorrt.ObsKind) bool {
	_, ok := f.levelKinds[kind]
	return ok
}

func (f *Fold) Forget(id actor.ActorID) {
	f.mu.Lock()
	_, existed := f.latest[id]
	delete(f.latest, id)
	f.mu.Unlock()
	if existed {
		f.logger.Debug("platform.presence.edge", "actor", string(id), "kind", "*", "edge", "forget")
	}
}

func (f *Fold) OnDown(_ context.Context, id actor.ActorID, gen actorrt.Incarnation, _ error) {
	var offlineKinds []actorrt.ObsKind
	f.mu.Lock()
	for kind, row := range f.latest[id] {
		if row.remote == "" && row.local == gen {
			delete(f.latest[id], kind)
			offlineKinds = append(offlineKinds, kind)
		}
	}
	if len(f.latest[id]) == 0 {
		delete(f.latest, id)
	}
	f.mu.Unlock()
	// edge-only: each removed row is a present→absent (offline) edge, not the
	// per-tick down path itself (that already logs elsewhere) — this is the
	// oplog fact that presence testimony went offline for this actor/kind.
	for _, kind := range offlineKinds {
		f.logger.Info("platform.presence.edge", "actor", string(id), "kind", string(kind), "edge", "offline")
	}
}

// OnRemoteDown removes only testimony published by the exact route attempt
// that went down; a stale G1 close cannot erase G2 testimony.
func (f *Fold) OnRemoteDown(id actor.ActorID, key actorhost.AttemptKey) {
	var offlineKinds []actorrt.ObsKind
	f.mu.Lock()
	for kind, row := range f.latest[id] {
		if row.remote == key {
			delete(f.latest[id], kind)
			offlineKinds = append(offlineKinds, kind)
		}
	}
	if len(f.latest[id]) == 0 {
		delete(f.latest, id)
	}
	f.mu.Unlock()
	for _, kind := range offlineKinds {
		f.logger.Info("platform.presence.edge", "actor", string(id), "kind", string(kind), "edge", "offline")
	}
}

// Sweep enforces fold rows ⊆ (live incarnations ∪ active membership). Fresh
// rows survive one reconciliation cadence to close the spawn/read race.
func (f *Fold) Sweep(keep func(actor.ActorID) bool) int {
	cutoff := f.clock().Add(-f.grace).UnixMilli()
	removed := 0
	f.mu.Lock()
	for id, rows := range f.latest {
		if keep(id) {
			continue
		}
		for kind, row := range rows {
			if row.receivedAt < cutoff {
				delete(rows, kind)
				removed++
			}
		}
		if len(rows) == 0 {
			delete(f.latest, id)
		}
	}
	f.mu.Unlock()
	return removed
}

func (f *Fold) copy(id actor.ActorID) map[actorrt.ObsKind]entry {
	f.mu.Lock()
	defer f.mu.Unlock()
	rows := f.latest[id]
	out := make(map[actorrt.ObsKind]entry, len(rows))
	for kind, row := range rows {
		row.val = append([]byte(nil), row.val...)
		out[kind] = row
	}
	return out
}

type View struct {
	fold      *Fold
	runtime   ExecutionView
	authority storespec.ActorAuthority
}

// ExecutionView is the narrow local-observation surface presence needs. It is
// intentionally implemented by actorctl.ChannelActors, not by an execution
// owner or registry.
type ExecutionView interface {
	Stat(actor.ActorID) (actorrt.UnitStat, bool)
	Incarnation(actor.ActorID) (actorrt.Incarnation, bool)
	Attempt(actor.ActorID) (actorhost.AttemptKey, bool)
}

func NewView(fold *Fold, runtime ExecutionView, authority storespec.ActorAuthority) View {
	return View{fold: fold, runtime: runtime, authority: authority}
}

// Snapshot physically copies fold state before reading runtime and registry.
// Those reads are intentionally non-atomic: presence is advisory and reports a
// best-effort current view, never a dispatch guarantee.
func (v View) Snapshot(ctx context.Context, id actor.ActorID) (Snapshot, error) {
	rows := v.fold.copy(id)
	stat, present := v.runtime.Stat(id)
	gen, hasGen := v.runtime.Incarnation(id)
	attempt, hasAttempt := v.runtime.Attempt(id)
	_, member, err := v.authority.LookupActive(ctx, id)
	if err != nil {
		return Snapshot{}, err
	}
	out := Snapshot{Member: member, L1Present: present, L3: map[actorrt.ObsKind]Testimony{}}
	if present {
		out.L1StartedAt = stat.StartedAt
	}
	if !member && !present {
		return out, nil
	}
	for kind, row := range rows {
		stale := (row.remote == "" && hasGen && row.local != gen) ||
			(row.remote != "" && hasAttempt && row.remote != attempt)
		out.L3[kind] = Testimony{Val: row.val, ReceivedAt: row.receivedAt, StaleFromPriorLife: stale}
	}
	return out, nil
}
