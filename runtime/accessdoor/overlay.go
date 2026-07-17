package accessdoor

import (
	"context"
	"errors"
	"sync"

	"github.com/wanpengxie/atoll/protocol/access"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/resource"
	"github.com/wanpengxie/atoll/runtime/resourcespec"
)

type GrantOverlay interface {
	ActorAllows(context.Context, actor.ActorID, resource.ResourceID, access.Operation) (bool, error)
	SetGrant(context.Context, resource.ResourceID, access.Grant) error
	EndBatch([]actor.ActorID)
}

var (
	ErrGrantOverlayUnbound = errors.New("accessdoor: grant overlay is not bound")
	ErrGrantOverlayBound   = errors.New("accessdoor: grant overlay is already bound")
)

type GrantOverlaySlot struct {
	mu    sync.RWMutex
	bound GrantOverlay
}

func NewGrantOverlaySlot() *GrantOverlaySlot { return &GrantOverlaySlot{} }

func (s *GrantOverlaySlot) Bind(overlay GrantOverlay) error {
	if overlay == nil {
		return errors.New("accessdoor: nil grant overlay")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.bound != nil {
		return ErrGrantOverlayBound
	}
	s.bound = overlay
	return nil
}

func (s *GrantOverlaySlot) get() (GrantOverlay, error) {
	s.mu.RLock()
	o := s.bound
	s.mu.RUnlock()
	if o == nil {
		return nil, ErrGrantOverlayUnbound
	}
	return o, nil
}

func (s *GrantOverlaySlot) ActorAllows(ctx context.Context, id actor.ActorID, rid resource.ResourceID, op access.Operation) (bool, error) {
	o, err := s.get()
	if err != nil {
		return false, err
	}
	return o.ActorAllows(ctx, id, rid, op)
}

func (s *GrantOverlaySlot) SetGrant(ctx context.Context, rid resource.ResourceID, grant access.Grant) error {
	o, err := s.get()
	if err != nil {
		return err
	}
	return o.SetGrant(ctx, rid, grant)
}

func (s *GrantOverlaySlot) EndBatch(ids []actor.ActorID) {
	o, err := s.get()
	if err == nil {
		o.EndBatch(ids)
	}
}

type ResourceCompletion interface {
	CommitReservation(context.Context, string) (bool, error)
}

type resourceCompletion struct{ registry resourcespec.Registry }

func NewResourceCompletion(registry resourcespec.Registry) (ResourceCompletion, error) {
	if registry == nil {
		return nil, errors.New("accessdoor: nil resource registry")
	}
	return resourceCompletion{registry: registry}, nil
}

func (c resourceCompletion) CommitReservation(ctx context.Context, id string) (bool, error) {
	return c.registry.CommitReservation(ctx, id)
}
