package accessdoor

import (
	"context"
	"errors"
	"sync"

	"github.com/wanpengxie/atoll/protocol/access"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/resource"
	"github.com/wanpengxie/atoll/runtime/resourcespec"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

type GrantOverlay interface {
	ActorAllows(context.Context, actor.ActorID, resource.ResourceID, access.Operation) (bool, error)
	SetGrant(context.Context, resource.ResourceID, access.Grant) error
	EndBatch([]actor.ActorID)
	DeleteResource(resource.ResourceID)
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

func (s *GrantOverlaySlot) DeleteResource(id resource.ResourceID) {
	o, err := s.get()
	if err == nil {
		o.DeleteResource(id)
	}
}

type ResourceCompletion interface {
	CommitReservation(context.Context, string) (resourcespec.LandedResource, bool, error)
}

type resourceCompletion struct{ door *door }

func (c resourceCompletion) CommitReservation(ctx context.Context, id string) (resourcespec.LandedResource, bool, error) {
	c.door.resourceGate.Lock()
	defer c.door.resourceGate.Unlock()
	return c.door.commitReservationLocked(ctx, id)
}

func (d *door) commitReservationLocked(ctx context.Context, id string) (resourcespec.LandedResource, bool, error) {
	landed, found, err := d.deps.Registry.CommitReservation(ctx, id)
	if err != nil || !found {
		return landed, found, err
	}
	if err := d.installCreatorOverlay(ctx, landed); err != nil {
		return landed, true, err
	}
	return landed, true, nil
}

func (d *door) installCreatorOverlay(ctx context.Context, landed resourcespec.LandedResource) error {
	if landed.Birth.Authority != resourcespec.BirthChannelOwned {
		return nil
	}
	world, active, err := d.deps.Authority.WorldOf(ctx, landed.CreatedBy)
	if err != nil {
		return err
	}
	if !active || world != storespec.WorldRun {
		return nil
	}
	return d.deps.Overlay.SetGrant(ctx, landed.ID, access.Grant{
		GranteeKind: access.GranteeActor,
		Grantee:     landed.CreatedBy,
		Ops:         []access.Operation{access.OpRead, access.OpWrite, access.OpSet, access.OpDelete},
	})
}
