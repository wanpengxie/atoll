package identitystore

import (
	"context"
	"errors"
	"sync"

	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

var (
	ErrInvalidIdentity = errors.New("identitystore: invalid identity")
	ErrIdentityMissing = errors.New("identitystore: identity missing")
)

// Home is physical storage routing, not an actor kind or authority attribute.
// It must not cross into Controller values, capabilities, messages or views.
type Home uint8

const (
	HomeDurable Home = iota + 1
	HomeMemory
)

// HomeReader is the narrow physical seam consumed by State backing
// resolution. Generic actor authority deliberately does not implement it.
type HomeReader interface {
	HomeOf(context.Context, actor.ActorID) (Home, bool, error)
}

// Store presents durable and process-local identities as one active namespace.
// The durable backend remains the sole restart source; memory rows never
// participate in RestoreActive.
type Store struct {
	durable storespec.DeclaredControlReader

	mu     sync.RWMutex
	memory map[actor.ActorID]storespec.ActorControlRow
}

func New(durable storespec.DeclaredControlReader) (*Store, error) {
	if durable == nil {
		return nil, ErrInvalidIdentity
	}
	return &Store{
		durable: durable,
		memory:  make(map[actor.ActorID]storespec.ActorControlRow),
	}, nil
}

func cloneRow(row storespec.ActorControlRow) storespec.ActorControlRow {
	row.Config = append([]byte(nil), row.Config...)
	return row
}

// RestoreActive returns only identities whose physical home survives process
// restart. It never reconstructs memory-home rows from durable operation
// receipts.
func (s *Store) RestoreActive(ctx context.Context) ([]storespec.ActorControlRow, error) {
	rows, err := s.durable.ListDeclaredActive(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]storespec.ActorControlRow, len(rows))
	for i, row := range rows {
		out[i] = cloneRow(row)
	}
	return out, nil
}

// LookupActive hides storage home and returns one uniform identity row.
func (s *Store) LookupActive(
	ctx context.Context,
	id actor.ActorID,
) (storespec.ActorControlRow, bool, error) {
	s.mu.RLock()
	row, ok := s.memory[id]
	s.mu.RUnlock()
	if ok {
		return cloneRow(row), true, nil
	}
	row, ok, err := s.durable.LookupDeclaredActive(ctx, id)
	return cloneRow(row), ok, err
}

// HomeOf exposes only the physical fact needed by storage-backed organs. It is
// intentionally separate from ActorAuthority.
func (s *Store) HomeOf(
	ctx context.Context,
	id actor.ActorID,
) (Home, bool, error) {
	s.mu.RLock()
	_, ok := s.memory[id]
	s.mu.RUnlock()
	if ok {
		return HomeMemory, true, nil
	}
	_, ok, err := s.durable.LookupDeclaredActive(ctx, id)
	if err != nil || !ok {
		return 0, ok, err
	}
	return HomeDurable, true, nil
}

// PreparedMemory is a locally validated process-memory identity publication.
// Controller is the only child-ID minter; its UUID-shaped IDs cannot collide
// with declaration-backed IDs, so publication needs no post-commit I/O.
type PreparedMemory struct {
	row storespec.ActorControlRow
}

// PrepareMemory performs every fallible check before the typed birth operation
// commits. The returned value contains no callback or storage-home authority.
func PrepareMemory(row storespec.ActorControlRow) (PreparedMemory, error) {
	if row.ID == "" || row.ID == actor.SystemActorID ||
		row.CurrentDeclVersion <= 0 || row.Placement.Validate() != nil {
		return PreparedMemory{}, ErrInvalidIdentity
	}
	return PreparedMemory{row: cloneRow(row)}, nil
}

// PublishMemory is the infallible post-commit publication step. Re-publication
// of the same Controller-minted ID is idempotent.
func (s *Store) PublishMemory(prepared PreparedMemory) {
	s.mu.Lock()
	if _, exists := s.memory[prepared.row.ID]; !exists {
		s.memory[prepared.row.ID] = cloneRow(prepared.row)
	}
	s.mu.Unlock()
}

// Partition returns the physical terminal plan for exact active IDs. This
// method is used only by the identity Store adapter; Controller never sees it.
func (s *Store) Partition(
	ctx context.Context,
	ids []actor.ActorID,
) (durable []actor.ActorID, memory []actor.ActorID, err error) {
	for _, id := range ids {
		home, found, lookupErr := s.HomeOf(ctx, id)
		if lookupErr != nil {
			return nil, nil, lookupErr
		}
		if !found {
			return nil, nil, ErrIdentityMissing
		}
		switch home {
		case HomeDurable:
			durable = append(durable, id)
		case HomeMemory:
			memory = append(memory, id)
		default:
			return nil, nil, ErrIdentityMissing
		}
	}
	return durable, memory, nil
}

// DeleteMemory applies the memory half of an already committed terminal plan.
// Durable deletion remains owned by the durable backend transaction.
func (s *Store) DeleteMemory(ids []actor.ActorID) {
	s.mu.Lock()
	for _, id := range ids {
		delete(s.memory, id)
	}
	s.mu.Unlock()
}

var _ HomeReader = (*Store)(nil)
