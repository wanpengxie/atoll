package runtime

import (
	"context"
	"errors"
	"sync"

	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

var (
	ErrActorAuthorityUnbound      = errors.New("runtime: actor authority is not bound")
	ErrActorAuthorityAlreadyBound = errors.New("runtime: actor authority is already bound")
)

// actorAuthoritySlot breaks the OpenChannel/Home construction cycle without
// introducing a durable fallback. Before Bind every query fails closed; Bind
// is monotonic and succeeds exactly once.
type actorAuthoritySlot struct {
	mu    sync.RWMutex
	bound storespec.ActorAuthority
}

func newActorAuthoritySlot() *actorAuthoritySlot { return &actorAuthoritySlot{} }

func (s *actorAuthoritySlot) Bind(a storespec.ActorAuthority) error {
	if a == nil {
		return errors.New("runtime: nil actor authority")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.bound != nil {
		return ErrActorAuthorityAlreadyBound
	}
	s.bound = a
	return nil
}

func (s *actorAuthoritySlot) get() (storespec.ActorAuthority, error) {
	s.mu.RLock()
	a := s.bound
	s.mu.RUnlock()
	if a == nil {
		return nil, ErrActorAuthorityUnbound
	}
	return a, nil
}

func (s *actorAuthoritySlot) LookupActive(ctx context.Context, id actor.ActorID) (storespec.ActorControlRow, bool, error) {
	a, err := s.get()
	if err != nil {
		return storespec.ActorControlRow{}, false, err
	}
	return a.LookupActive(ctx, id)
}

func (s *actorAuthoritySlot) ListActive(ctx context.Context) ([]storespec.ActorControlRow, error) {
	a, err := s.get()
	if err != nil {
		return nil, err
	}
	return a.ListActive(ctx)
}

func (s *actorAuthoritySlot) WorldOf(ctx context.Context, id actor.ActorID) (storespec.ActorWorld, bool, error) {
	a, err := s.get()
	if err != nil {
		return 0, false, err
	}
	return a.WorldOf(ctx, id)
}

func (s *actorAuthoritySlot) CheckAuthor(ctx context.Context, stamp storespec.AuthorStamp) (storespec.AuthorVerdict, error) {
	a, err := s.get()
	if err != nil {
		return 0, err
	}
	return a.CheckAuthor(ctx, stamp)
}

var _ storespec.ActorAuthority = (*actorAuthoritySlot)(nil)
