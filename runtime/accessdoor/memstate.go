package accessdoor

import (
	"context"
	"errors"
	"sync"

	"github.com/wanpengxie/atoll/protocol/access"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/resource"
	"github.com/wanpengxie/atoll/runtime/capauth"
	"github.com/wanpengxie/atoll/runtime/resourcespec"
)

func newMemoryStateHandle(owner actor.ActorID) AccessHandle {
	return boundStateHandle{door: &door{deps: Deps{State: newMemStateStore()}}, owner: owner}
}

var ErrStateHandleUnavailable = errors.New("accessdoor: state handle unavailable")

// StateOp is one actor-scoped state call's operand — the state face's only
// verb (Invoke), carried as a value so the per-call ingress below can take the
// whole operation in one parameter instead of handing a handle outward.
type StateOp struct {
	Operation access.Operation
	Resource  resource.ResourceID
	Args      []byte
}

// StateHandleResolver is the state organ's own face. All members are durable;
// the retired process-entry birth path no longer creates a second backing.
//
//   - ResolveAuthority — the local body's birth mint: route once, weld the
//     chosen backing into a handle the body keeps for its whole term;
//   - StateIngress — the remote body's per-call entry: route on every call
//     (a daemon body holds no backing and the classification never travels),
//     then admit, then execute on the backing already chosen.
type StateHandleResolver interface {
	ResolveAuthority(context.Context, capauth.Authority) (AccessHandle, error)

	// StateIngress is the ONE per-call state entry point. The three steps are
	// pinned inside the organ, in this order: select backing → ActorID
	// admission → execute on the selected backing. Selection never rises to
	// the ingress or the link, and the order never inverts: an End landing
	// after admission cannot redirect an accepted call to another backing
	// (the sliding-window semantics every arm already has).
	StateIngress(context.Context, capauth.Authority, StateOp) (Outcome, error)

	// ForgetActors is the narrow process-memory release port (§5.5). It drops
	// this store's own in-memory rows for dead ids and NOTHING else: durable
	// state rows belonging to the dead are inert data whose correctness is
	// carried by the admission gate, never by deleting them.
	ForgetActors([]actor.ActorID)
}

type actorStateHandles struct {
	durable AccessMinter
}

func NewStateHandleResolver(durable AccessMinter) (StateHandleResolver, error) {
	if durable == nil {
		return nil, errors.New("accessdoor: state handle resolver dependencies incomplete")
	}
	return &actorStateHandles{durable: durable}, nil
}

// route is the ONE backing-selection function of the whole system. Both loci
// call it — the local mint once at birth, the remote ingress once per call —
// so a routing difference between them is structurally impossible.
//
// The returned handle carries the authority, never a snapshot: the verdict is
// the door's own first step on every use.
func (h *actorStateHandles) route(
	ctx context.Context,
	authority capauth.Authority,
) (AccessHandle, error) {
	if authority == nil || authority.ActorID() == "" {
		return nil, ErrStateHandleUnavailable
	}
	return h.durable.MintStateAuthority(authority), nil
}

// ResolveAuthority is the local body's birth mint: one route, one welded
// backing for the whole term.
func (h *actorStateHandles) ResolveAuthority(
	ctx context.Context,
	authority capauth.Authority,
) (AccessHandle, error) {
	if authority == nil || authority.Admit() != nil {
		return nil, ErrStateHandleUnavailable
	}
	return h.route(ctx, authority)
}

// StateIngress is the remote body's per-call entry: route → admit → execute,
// in that order, inside the organ.
func (h *actorStateHandles) StateIngress(
	ctx context.Context,
	authority capauth.Authority,
	op StateOp,
) (Outcome, error) {
	handle, err := h.route(ctx, authority)
	if err != nil {
		return Outcome{}, err
	}
	return handle.Invoke(ctx, op.Operation, op.Resource, op.Args)
}

// ForgetActors is retained as the closed cleanup face; durable state has no
// process-local rows to release.
func (h *actorStateHandles) ForgetActors([]actor.ActorID) {}

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
