package actorstore

import (
	"context"
	"errors"
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
// Restart / End all belong to the Controller command layer, which
// translates them into the record verbs below).
//
// It wires straight to the durable registry face, exactly like harness /
// schedule / accessdoor take their own sub-stores — there is no backend
// interface and no injection seam.
type Store struct {
	registry storespec.ActorRegistryStore
	nowMs    func() int64
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
	}, nil
}

// RestoreActive is the one boot read. It restores the durable side only; the
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
	record, ok, err := s.registry.LookupActive(ctx, id)
	if err != nil || !ok {
		return storespec.ActorRecord{}, ok, err
	}
	return record.Clone(), true, nil
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
// returns the committed value.
func (s *Store) UpdateDefinition(
	ctx context.Context,
	id actor.ActorID,
	def storespec.ActorDefinition,
) (storespec.ActorRecord, error) {
	record, err := s.registry.UpdateDefinition(ctx, id, def.Clone())
	if err != nil {
		return storespec.ActorRecord{}, err
	}
	return record.Clone(), nil
}

// Deregister is the whole terminal record verb, applied to the whole set:
// first the durable transaction (rows that do not exist are naturally no-ops),
// It touches the registry table alone.
func (s *Store) Deregister(ctx context.Context, ids []actor.ActorID) error {
	if len(ids) == 0 {
		return nil
	}
	if err := s.registry.Deregister(ctx, ids, s.nowMs()); err != nil {
		return err
	}
	return nil
}
