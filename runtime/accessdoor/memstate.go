package accessdoor

import (
	"context"
	"errors"
	"sync"

	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/resource"
	"github.com/wanpengxie/atoll/runtime/resourcespec"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

func newMemoryStateHandle(owner storespec.AuthorStamp, authority storespec.ActorAuthority) AccessHandle {
	return boundStateHandle{door: &door{deps: Deps{State: newMemStateStore(), Authority: authority}}, owner: owner}
}

var ErrStateHandleUnavailable = errors.New("accessdoor: state handle unavailable")

// StateHandleResolver is the sole world-sensitive State resolution seam.
// Callers never receive a raw in-memory backend or implement their own world
// switch. Resolve takes the caller's full authenticated AuthorStamp — never a
// bare id — so the incarnation's welded birth version rides through to the
// minted handle: a stale-generation caller (its declaration has advanced past
// the version it was born with) fails closed here instead of being silently
// re-certified at the current version.
type StateHandleResolver interface {
	AdmitRun(actor.ActorID) error
	Resolve(context.Context, storespec.AuthorStamp) (AccessHandle, error)
	EndBatch([]actor.ActorID)
}

type actorStateHandles struct {
	mu        sync.RWMutex
	authority storespec.ActorAuthority
	durable   AccessMinter
	run       map[actor.ActorID]AccessHandle
}

func NewStateHandleResolver(authority storespec.ActorAuthority, durable AccessMinter) (StateHandleResolver, error) {
	if authority == nil || durable == nil {
		return nil, errors.New("accessdoor: state handle resolver dependencies incomplete")
	}
	return &actorStateHandles{authority: authority, durable: durable, run: make(map[actor.ActorID]AccessHandle)}, nil
}

func (h *actorStateHandles) AdmitRun(id actor.ActorID) error {
	if id == "" {
		return ErrStateHandleUnavailable
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if _, exists := h.run[id]; exists {
		return nil
	}
	h.run[id] = newMemoryStateHandle(storespec.AuthorStamp{ID: id, BirthVersion: 1}, h.authority)
	return nil
}

func (h *actorStateHandles) Resolve(ctx context.Context, stamp storespec.AuthorStamp) (AccessHandle, error) {
	world, ok, err := h.authority.WorldOf(ctx, stamp.ID)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, ErrStateHandleUnavailable
	}
	switch world {
	case storespec.WorldDurable:
		// Version gate through the ONE authority verdict口 (never an inline
		// version comparison — the archtest wall): the handle is minted at
		// the caller's welded birth version, and only while that version is
		// still the current one. A port born at v1 asking after apply v2
		// lands here with a stale stamp and must NOT be re-certified at v2 —
		// that would let a zombie incarnation write durable State
		// concurrently with its successor.
		verdict, err := h.authority.CheckAuthor(ctx, stamp)
		if err != nil {
			return nil, err
		}
		if verdict != storespec.AuthorOK {
			return nil, ErrStateHandleUnavailable
		}
		return h.durable.MintState(stamp), nil
	case storespec.WorldRun:
		// Run-world identities never version-advance (forked rows are pinned
		// at birth version 1), so the welded handle itself is the gate.
		h.mu.RLock()
		handle := h.run[stamp.ID]
		h.mu.RUnlock()
		if handle == nil {
			return nil, ErrStateHandleUnavailable
		}
		return handle, nil
	default:
		return nil, ErrStateHandleUnavailable
	}
}

func (h *actorStateHandles) EndBatch(ids []actor.ActorID) {
	h.mu.Lock()
	for _, id := range ids {
		delete(h.run, id)
	}
	h.mu.Unlock()
}

var _ StateHandleResolver = (*actorStateHandles)(nil)

// memStateStore is the in-memory realizer of resourcespec.StateStore — the same
// contract the durable actor_state store realizes, so it drives the identical
// collapsed decision tree (invokeActorScoped). One instance is dedicated to one
// owner (NewMemoryStateHandle welds it), but the key includes owner to honour the
// (owner, id) contract verbatim.
type memStateStore struct {
	mu   sync.Mutex
	rows map[memStateKey][]byte
}

type memStateKey struct {
	owner actor.ActorID
	id    resource.ResourceID
}

func newMemStateStore() *memStateStore {
	return &memStateStore{rows: map[memStateKey][]byte{}}
}

// Create is the atomic birth: a colliding (owner, id) → ErrAlreadyExists (the
// shared collision sentinel, same verdict mapping as the durable store).
func (s *memStateStore) Create(_ context.Context, owner actor.ActorID, id resource.ResourceID, initial []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	k := memStateKey{owner, id}
	if _, exists := s.rows[k]; exists {
		return resourcespec.ErrAlreadyExists
	}
	s.rows[k] = cloneStateBytes(initial)
	return nil
}

// Read returns the current bytes and whether the ROW exists. exists tracks
// EXISTENCE, not byte-nullness: a stored nil value is resolved-but-empty and reads
// back exists=true / value=nil (the door maps that to Found=false, uniform with
// the durable store); an empty non-nil blob is a value.
func (s *memStateStore) Read(_ context.Context, owner actor.ActorID, id resource.ResourceID) (value []byte, exists bool, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	raw, ok := s.rows[memStateKey{owner, id}]
	if !ok {
		return nil, false, nil
	}
	return cloneStateBytes(raw), true, nil
}

// Write overwrites an EXISTING row (PUT semantics, idempotent); exists=false when
// no row was hit (door → resource_not_found — birth is Create, not Write).
func (s *memStateStore) Write(_ context.Context, owner actor.ActorID, id resource.ResourceID, value []byte) (exists bool, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	k := memStateKey{owner, id}
	if _, ok := s.rows[k]; !ok {
		return false, nil
	}
	s.rows[k] = cloneStateBytes(value)
	return true, nil
}

// Delete removes the row; exists=false when no row was hit (door →
// resource_not_found; repeated delete is honestly not-found).
func (s *memStateStore) Delete(_ context.Context, owner actor.ActorID, id resource.ResourceID) (exists bool, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	k := memStateKey{owner, id}
	if _, ok := s.rows[k]; !ok {
		return false, nil
	}
	delete(s.rows, k)
	return true, nil
}

// cloneStateBytes copies bytes in/out of the store so a caller cannot mutate a
// stored value through a retained slice (the durable store round-trips through the
// DB for free; the memory store must copy to match that isolation).
func cloneStateBytes(b []byte) []byte {
	if b == nil {
		return nil
	}
	out := make([]byte, len(b))
	copy(out, b)
	return out
}

var _ resourcespec.StateStore = (*memStateStore)(nil)
