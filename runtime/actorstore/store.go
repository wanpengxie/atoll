package actorstore

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

var (
	ErrInvalidRecord = errors.New("actorstore: invalid record")
	ErrNotFound      = errors.New("actorstore: record not found")
)

// Store keeps every actor record of one channel. It speaks only record
// language: there is no business operation name here (Admit / Introduce /
// Fork / Restart / End all belong to the Controller command layer, which
// translates them into the record verbs below).
//
// It wires straight to the durable registry face, exactly like harness /
// schedule / accessdoor take their own sub-stores — there is no backend
// interface and no injection seam.
type Store struct {
	registry storespec.ActorRegistryStore
	nowMs    func() int64

	mu sync.RWMutex
	// entries is the process entry table. Its membership IS the whole
	// classification fact: a record is either here or in the durable registry.
	entries map[actor.ActorID]storespec.ActorRecord
}

func New(registry storespec.ActorRegistryStore, nowMs func() int64) (*Store, error) {
	if registry == nil {
		return nil, ErrInvalidRecord
	}
	if nowMs == nil {
		nowMs = func() int64 { return time.Now().UnixMilli() }
	}
	return &Store{
		registry: registry,
		nowMs:    nowMs,
		entries:  make(map[actor.ActorID]storespec.ActorRecord),
	}, nil
}

// RestoreActive is the one boot read. It restores the durable side only; the
// entry table is born empty with the process and is never reconstructed from
// receipts, messages, schedules or stale Host actuals.
func (s *Store) RestoreActive(ctx context.Context) ([]storespec.ActorRecord, error) {
	records, err := s.registry.ListActive(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]storespec.ActorRecord, len(records))
	for i, record := range records {
		out[i] = record.Clone()
	}
	return out, nil
}

// Lookup reads the current value of one record. It is an operation-internal
// read, not a general query face — runtime queries go to the Controller.
func (s *Store) Lookup(
	ctx context.Context,
	id actor.ActorID,
) (storespec.ActorRecord, bool, error) {
	s.mu.RLock()
	record, ok := s.entries[id]
	s.mu.RUnlock()
	if ok {
		return record.Clone(), true, nil
	}
	record, ok, err := s.registry.LookupActive(ctx, id)
	if err != nil || !ok {
		return storespec.ActorRecord{}, ok, err
	}
	return record.Clone(), true, nil
}

// IsEntry answers "does this record live in the entry table". It is the ONLY
// classification read in the whole system, and it exists for exactly one
// consumer: the state organ's backing route (§7.4). It must never be used for
// anything else — no capability, no projection, no protocol field, and never
// across the wire.
func (s *Store) IsEntry(
	ctx context.Context,
	id actor.ActorID,
) (entry bool, found bool, err error) {
	s.mu.RLock()
	_, ok := s.entries[id]
	s.mu.RUnlock()
	if ok {
		return true, true, nil
	}
	_, found, err = s.registry.LookupActive(ctx, id)
	if err != nil || !found {
		return false, false, err
	}
	return false, true, nil
}

// Insert is the declaration-class birth: replay check, id mint and row insert
// on one transaction bed. The draft carries no id when the transaction should
// mint one; the returned record is the authoritative committed value, so the
// caller never reads back.
func (s *Store) Insert(
	ctx context.Context,
	draft storespec.ActorDraft,
) (storespec.ActorRecord, error) {
	record, err := s.registry.Insert(ctx, draft.Clone())
	if err != nil {
		return storespec.ActorRecord{}, err
	}
	return record.Clone(), nil
}

// UpdateDefinition overwrites the current definition of a durable record and
// returns the committed value. A record with no durable declaration (an entry
// record) is refused with a typed error — an operation verdict, not a species
// branch.
func (s *Store) UpdateDefinition(
	ctx context.Context,
	id actor.ActorID,
	def storespec.ActorDefinition,
) (storespec.ActorRecord, error) {
	s.mu.RLock()
	_, isEntry := s.entries[id]
	s.mu.RUnlock()
	if isEntry {
		return storespec.ActorRecord{}, fmt.Errorf(
			"%w: %q has no declaration to change", ErrInvalidRecord, id)
	}
	record, err := s.registry.UpdateDefinition(ctx, id, def.Clone())
	if err != nil {
		return storespec.ActorRecord{}, err
	}
	return record.Clone(), nil
}

// Deregister is the whole terminal record verb, applied to the whole set:
// first the durable transaction (rows that do not exist are naturally no-ops),
// then — only after it commits — the entry deletions. A durable failure leaves
// the entry table untouched. It touches the registry table alone.
func (s *Store) Deregister(ctx context.Context, ids []actor.ActorID) error {
	if len(ids) == 0 {
		return nil
	}
	if err := s.registry.Deregister(ctx, ids, s.nowMs()); err != nil {
		return err
	}
	s.mu.Lock()
	for _, id := range ids {
		delete(s.entries, id)
	}
	s.mu.Unlock()
	return nil
}

// InstallEntry is the fork birth: it installs one record into the entry table.
// Birth semantics — every fallible check happened before the command settled,
// so this step cannot fail. A colliding id is an invariant violation, not a
// last-write-wins update, and fails the process loudly.
func (s *Store) InstallEntry(record storespec.ActorRecord) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.entries[record.ID]; exists {
		panic(fmt.Sprintf("actorstore: entry %q already installed", record.ID))
	}
	s.entries[record.ID] = record.Clone()
}
