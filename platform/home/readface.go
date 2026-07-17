package home

import (
	"context"
	"sync"

	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

type controlEntry struct {
	Row   storespec.ActorControlRow
	World storespec.ActorWorld
}

type actorControlIndex struct {
	mu   sync.RWMutex
	rows map[actor.ActorID]controlEntry
}

func newActorControlIndex() *actorControlIndex {
	return &actorControlIndex{rows: make(map[actor.ActorID]controlEntry)}
}

func cloneControlRow(in storespec.ActorControlRow) storespec.ActorControlRow {
	out := in
	if in.Config != nil {
		out.Config = append([]byte(nil), in.Config...)
	}
	return out
}

func cloneControlEntry(in controlEntry) controlEntry {
	in.Row = cloneControlRow(in.Row)
	return in
}

func validControlEntry(e controlEntry) bool {
	return e.Row.ID != "" && e.Row.CurrentDeclVersion > 0 &&
		(e.World == storespec.WorldDurable || e.World == storespec.WorldRun) &&
		e.Row.Placement.Validate() == nil
}

// ReplaceAll validates and clones the complete boot image before swapping it,
// so readers never observe a partly loaded authority.
func (i *actorControlIndex) ReplaceAll(entries []controlEntry) bool {
	next := make(map[actor.ActorID]controlEntry, len(entries))
	for _, entry := range entries {
		if !validControlEntry(entry) {
			return false
		}
		if _, exists := next[entry.Row.ID]; exists {
			return false
		}
		next[entry.Row.ID] = cloneControlEntry(entry)
	}
	i.mu.Lock()
	i.rows = next
	i.mu.Unlock()
	return true
}

// UpsertBatch publishes row and world in one critical section.
func (i *actorControlIndex) UpsertBatch(entries []controlEntry) bool {
	cloned := make([]controlEntry, len(entries))
	for n, entry := range entries {
		if !validControlEntry(entry) {
			return false
		}
		cloned[n] = cloneControlEntry(entry)
	}
	i.mu.Lock()
	for _, entry := range cloned {
		i.rows[entry.Row.ID] = entry
	}
	i.mu.Unlock()
	return true
}

func (i *actorControlIndex) DeleteBatch(ids []actor.ActorID) {
	i.mu.Lock()
	for _, id := range ids {
		delete(i.rows, id)
	}
	i.mu.Unlock()
}

func (i *actorControlIndex) LookupActive(_ context.Context, id actor.ActorID) (storespec.ActorControlRow, bool, error) {
	i.mu.RLock()
	entry, ok := i.rows[id]
	i.mu.RUnlock()
	if !ok {
		return storespec.ActorControlRow{}, false, nil
	}
	return cloneControlRow(entry.Row), true, nil
}

func (i *actorControlIndex) ListActive(context.Context) ([]storespec.ActorControlRow, error) {
	i.mu.RLock()
	out := make([]storespec.ActorControlRow, 0, len(i.rows))
	for _, entry := range i.rows {
		out = append(out, cloneControlRow(entry.Row))
	}
	i.mu.RUnlock()
	return out, nil
}

func (i *actorControlIndex) WorldOf(_ context.Context, id actor.ActorID) (storespec.ActorWorld, bool, error) {
	i.mu.RLock()
	entry, ok := i.rows[id]
	i.mu.RUnlock()
	if !ok {
		return 0, false, nil
	}
	return entry.World, true, nil
}

func (i *actorControlIndex) CheckAuthor(_ context.Context, stamp storespec.AuthorStamp) (storespec.AuthorVerdict, error) {
	i.mu.RLock()
	entry, ok := i.rows[stamp.ID]
	i.mu.RUnlock()
	if !ok {
		return storespec.AuthorNotMember, nil
	}
	if entry.Row.CurrentDeclVersion != stamp.BirthVersion {
		return storespec.AuthorVersionStale, nil
	}
	return storespec.AuthorOK, nil
}

var _ storespec.ActorAuthority = (*actorControlIndex)(nil)
