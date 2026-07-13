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

const unknownDropBucket actorrt.ObsKind = "unknown"

type entry struct {
	val        []byte
	receivedAt int64
	gen        actorrt.Incarnation
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
	mu         sync.Mutex
	latest     map[actor.ActorID]map[actorrt.ObsKind]entry
	dropped    map[actorrt.ObsKind]uint64
	loggedDrop map[actorrt.ObsKind]uint64
	levelKinds map[actorrt.ObsKind]struct{}
	eventKinds map[actorrt.ObsKind]struct{}
	clock      func() time.Time
	grace      time.Duration
	logger     *slog.Logger
}

// New builds a fold. Both vocabularies are injected: levelKinds (obs folded into
// the latest-value cache) and eventKinds (non-level diagnostic kinds counted per
// named drop bucket). The fold names no concrete word itself — the producer owns
// the word, the assembly root hands both sets in (substrate 守结构不守词汇).
func New(logger *slog.Logger, clock func() time.Time, levelKinds, eventKinds []actorrt.ObsKind, sweepGrace time.Duration) *Fold {
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
	events := make(map[actorrt.ObsKind]struct{}, len(eventKinds))
	dropped := make(map[actorrt.ObsKind]uint64, len(levelKinds)+len(eventKinds)+1)
	for _, kind := range levelKinds {
		levels[kind] = struct{}{}
		dropped[kind] = 0
	}
	for _, kind := range eventKinds {
		events[kind] = struct{}{}
		dropped[kind] = 0
	}
	dropped[unknownDropBucket] = 0
	return &Fold{latest: map[actor.ActorID]map[actorrt.ObsKind]entry{}, dropped: dropped, loggedDrop: map[actorrt.ObsKind]uint64{}, levelKinds: levels, eventKinds: events, clock: clock, grace: sweepGrace, logger: logger}
}

func (f *Fold) OnObs(_ context.Context, id actor.ActorID, gen actorrt.Incarnation, kind actorrt.ObsKind, val actorrt.ObsValue) {
	if !f.isLevel(kind) {
		f.countDrop(kind)
		return
	}
	f.put(id, kind, val, gen)
}

func (f *Fold) put(id actor.ActorID, kind actorrt.ObsKind, val actorrt.ObsValue, gen actorrt.Incarnation) {
	f.mu.Lock()
	byKind := f.latest[id]
	if byKind == nil {
		byKind = map[actorrt.ObsKind]entry{}
		f.latest[id] = byKind
	}
	byKind[kind] = entry{val: append([]byte(nil), val...), receivedAt: f.clock().UnixMilli(), gen: gen}
	f.mu.Unlock()
}

func (f *Fold) isLevel(kind actorrt.ObsKind) bool {
	_, ok := f.levelKinds[kind]
	return ok
}

func (f *Fold) countDrop(kind actorrt.ObsKind) {
	f.mu.Lock()
	if _, known := f.eventKinds[kind]; known {
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
		if row.gen == gen {
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
	changedDrops := map[actorrt.ObsKind]uint64{}
	for kind, count := range f.dropped {
		if count != f.loggedDrop[kind] {
			changedDrops[kind] = count
			f.loggedDrop[kind] = count
		}
	}
	f.mu.Unlock()
	if len(changedDrops) > 0 {
		// Sweep cadence is the rate limiter: the hot OnObs path only increments
		// bounded counters and never logs.
		f.logger.Debug("presence.non_level_dropped", "counts", changedDrops)
	}
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
		stale := hasGen && row.gen != gen
		out.L3[kind] = Testimony{Val: row.val, ReceivedAt: row.receivedAt, StaleFromPriorLife: stale}
	}
	return out, nil
}
