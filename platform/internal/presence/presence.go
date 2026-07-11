package presence

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/runtime/actorrt"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

type Source string

const (
	SourceBroker Source = "broker"
	SourceDoor   Source = "door"
)

const unknownDropBucket actorrt.ObsKind = "unknown"

var eventKinds = map[actorrt.ObsKind]struct{}{
	"checkpoint_drop":      {},
	"closure_fault":        {},
	"queue_overflow":       {},
	"reject_lane_overflow": {},
	"stale_delivery":       {},
}

type entry struct {
	val        []byte
	receivedAt int64
	source     Source
	gen        actorrt.Incarnation
}

// Testimony is the latest level testimony for one kind. It is advisory: an
// absent row means unknown, never offline.
type Testimony struct {
	Val                []byte
	ReceivedAt         int64
	Source             Source
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
// lifecycle unit, (actor id, observation kind), and records its source owner.
type Fold struct {
	mu         sync.Mutex
	latest     map[actor.ActorID]map[actorrt.ObsKind]entry
	dropped    map[actorrt.ObsKind]uint64
	levelKinds map[actorrt.ObsKind]struct{}
	clock      func() time.Time
	grace      time.Duration
	logger     *slog.Logger
}

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
	dropped := make(map[actorrt.ObsKind]uint64, len(levelKinds)+len(eventKinds)+1)
	for _, kind := range levelKinds {
		levels[kind] = struct{}{}
		dropped[kind] = 0
	}
	for kind := range eventKinds {
		dropped[kind] = 0
	}
	dropped[unknownDropBucket] = 0
	return &Fold{latest: map[actor.ActorID]map[actorrt.ObsKind]entry{}, dropped: dropped, levelKinds: levels, clock: clock, grace: sweepGrace, logger: logger}
}

func (f *Fold) OnObs(_ context.Context, id actor.ActorID, gen actorrt.Incarnation, kind actorrt.ObsKind, val actorrt.ObsValue) {
	if !f.isLevel(kind) {
		f.countDrop(kind)
		return
	}
	f.put(id, kind, val, SourceBroker, gen)
}

// PutDoor records testimony owned by a subject-gate session, not an actor
// incarnation. Down edges therefore never delete it.
func (f *Fold) PutDoor(id actor.ActorID, kind actorrt.ObsKind, val actorrt.ObsValue) {
	if !f.isLevel(kind) {
		f.countDrop(kind)
		return
	}
	f.put(id, kind, val, SourceDoor, actorrt.Incarnation{})
}

func (f *Fold) put(id actor.ActorID, kind actorrt.ObsKind, val actorrt.ObsValue, source Source, gen actorrt.Incarnation) {
	f.mu.Lock()
	byKind := f.latest[id]
	if byKind == nil {
		byKind = map[actorrt.ObsKind]entry{}
		f.latest[id] = byKind
	}
	byKind[kind] = entry{val: append([]byte(nil), val...), receivedAt: f.clock().UnixMilli(), source: source, gen: gen}
	f.mu.Unlock()
}

func (f *Fold) isLevel(kind actorrt.ObsKind) bool {
	_, ok := f.levelKinds[kind]
	return ok
}

func (f *Fold) countDrop(kind actorrt.ObsKind) {
	f.mu.Lock()
	if _, known := eventKinds[kind]; known {
		f.dropped[kind]++
	} else {
		f.dropped[unknownDropBucket]++
	}
	f.mu.Unlock()
}

func (f *Fold) DroppedCounts() map[actorrt.ObsKind]uint64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make(map[actorrt.ObsKind]uint64, len(f.dropped))
	for kind, count := range f.dropped {
		out[kind] = count
	}
	return out
}

func (f *Fold) Forget(id actor.ActorID) {
	f.mu.Lock()
	delete(f.latest, id)
	f.mu.Unlock()
}

func (f *Fold) OnDown(_ context.Context, id actor.ActorID, gen actorrt.Incarnation, _ error) {
	f.mu.Lock()
	for kind, row := range f.latest[id] {
		if row.source == SourceBroker && row.gen == gen {
			delete(f.latest[id], kind)
		}
	}
	if len(f.latest[id]) == 0 {
		delete(f.latest, id)
	}
	f.mu.Unlock()
}

// Sweep enforces fold rows ⊆ (live embodiments ∪ active membership). Fresh
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
	fold     *Fold
	runtime  *actorrt.Runtime
	registry storespec.Registry
}

func NewView(fold *Fold, runtime *actorrt.Runtime, registry storespec.Registry) View {
	return View{fold: fold, runtime: runtime, registry: registry}
}

// Snapshot physically copies fold state before reading runtime and registry.
// Those reads are intentionally non-atomic: presence is advisory and reports a
// best-effort current view, never a dispatch guarantee.
func (v View) Snapshot(ctx context.Context, id actor.ActorID) (Snapshot, error) {
	rows := v.fold.copy(id)
	stat, present := v.runtime.Stat(id)
	gen, hasGen := v.runtime.CurrentIncarnation(id)
	rec, member, err := v.registry.Lookup(ctx, id)
	if err != nil {
		return Snapshot{}, err
	}
	member = member && rec.IsActive()
	out := Snapshot{Member: member, L1Present: present, L3: map[actorrt.ObsKind]Testimony{}}
	if present {
		out.L1StartedAt = stat.StartedAt
	}
	if !member && !present {
		return out, nil
	}
	for kind, row := range rows {
		stale := row.source == SourceBroker && hasGen && row.gen != gen
		out.L3[kind] = Testimony{Val: row.val, ReceivedAt: row.receivedAt, Source: row.source, StaleFromPriorLife: stale}
	}
	return out, nil
}
