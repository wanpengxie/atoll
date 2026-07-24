package accessdoor

import (
	"context"
	"errors"
	"sync"

	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/resource"
	"github.com/wanpengxie/atoll/runtime/capauth"
	"github.com/wanpengxie/atoll/runtime/identitystore"
	"github.com/wanpengxie/atoll/runtime/resourcespec"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

func newMemoryStateHandle(owner storespec.AuthorStamp, authority storespec.ActorAuthority) AccessHandle {
	return boundStateHandle{door: &door{deps: Deps{State: newMemStateStore(), Authority: authority}}, owner: owner}
}

var ErrStateHandleUnavailable = errors.New("accessdoor: state handle unavailable")

// StateHandleResolver is the sole physical State-backing resolution seam.
// Identity storage home never enters actor authority or the returned handle.
type StateHandleResolver interface {
	AdmittedStateHandleResolver
	ResolveAuthority(context.Context, capauth.Authority) (AccessHandle, error)
	EndBatch([]actor.ActorID)
}

type AdmittedStateHandleResolver interface {
	ResolvePhysical(context.Context, actor.ActorID) (AdmittedStateBinding, error)
}

// AdmittedStateBinding is a stable physical handle selected before the one
// identity admission. End after admission cannot redirect it to another home.
type AdmittedStateBinding interface {
	MintAdmitted(storespec.IdentityAdmission) AccessHandle
}

type actorStateHandles struct {
	mu        sync.RWMutex
	authority storespec.ActorAuthority
	homes     identitystore.HomeReader
	durable   AccessMinter
	memory    map[actor.ActorID]boundStateHandle
}

func NewStateHandleResolver(
	authority storespec.ActorAuthority,
	homes identitystore.HomeReader,
	durable AccessMinter,
) (StateHandleResolver, error) {
	if authority == nil || homes == nil || durable == nil {
		return nil, errors.New("accessdoor: state handle resolver dependencies incomplete")
	}
	return &actorStateHandles{
		authority: authority,
		homes:     homes,
		durable:   durable,
		memory:    make(map[actor.ActorID]boundStateHandle),
	}, nil
}

type resolvedStateBinding struct {
	id      actor.ActorID
	durable AdmittedMinter
	memory  *boundStateHandle
}

func (b resolvedStateBinding) MintAdmitted(
	admission storespec.IdentityAdmission,
) AccessHandle {
	if !admission.Valid() || admission.Row.ID != b.id {
		return rejectedStateHandle{err: ErrAuthorInactive}
	}
	if b.memory != nil {
		state := *b.memory
		state.owner = storespec.AuthorStamp{ID: b.id}
		state.authority = nil
		state.admitted = true
		return state
	}
	if b.durable != nil {
		return b.durable.MintStateAdmitted(admission)
	}
	return rejectedStateHandle{err: ErrStateHandleUnavailable}
}

func (h *actorStateHandles) ResolvePhysical(
	ctx context.Context,
	id actor.ActorID,
) (AdmittedStateBinding, error) {
	if id == "" {
		return nil, ErrStateHandleUnavailable
	}
	home, found, err := h.homes.HomeOf(ctx, id)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, ErrStateHandleUnavailable
	}
	switch home {
	case identitystore.HomeDurable:
		minter, ok := h.durable.(AdmittedMinter)
		if !ok {
			return nil, ErrStateHandleUnavailable
		}
		return resolvedStateBinding{id: id, durable: minter}, nil
	case identitystore.HomeMemory:
		h.mu.Lock()
		state, ok := h.memory[id]
		if !ok {
			handle, valid := newMemoryStateHandle(
				storespec.AuthorStamp{ID: id}, h.authority,
			).(boundStateHandle)
			if !valid {
				h.mu.Unlock()
				return nil, ErrStateHandleUnavailable
			}
			state = handle
			h.memory[id] = state
		}
		h.mu.Unlock()
		return resolvedStateBinding{id: id, memory: &state}, nil
	default:
		return nil, ErrStateHandleUnavailable
	}
}

func (h *actorStateHandles) ResolveAuthority(
	ctx context.Context,
	authority capauth.Authority,
) (AccessHandle, error) {
	if authority == nil || authority.ActorID() == "" {
		return nil, ErrStateHandleUnavailable
	}
	binding, err := h.ResolvePhysical(ctx, authority.ActorID())
	if err != nil {
		return nil, err
	}
	resolved, ok := binding.(resolvedStateBinding)
	if !ok {
		return nil, ErrStateHandleUnavailable
	}
	if resolved.memory != nil {
		state := *resolved.memory
		state.authority = authority
		return state, nil
	}
	minter, ok := h.durable.(interface {
		MintStateAuthority(capauth.Authority) AccessHandle
	})
	if !ok {
		return nil, ErrStateHandleUnavailable
	}
	return minter.MintStateAuthority(authority), nil
}

func (h *actorStateHandles) EndBatch(ids []actor.ActorID) {
	h.mu.Lock()
	for _, id := range ids {
		delete(h.memory, id)
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
