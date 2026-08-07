package accessdoor

import (
	"context"

	"github.com/wanpengxie/atoll/runtime/resourcespec"
)

type ResourceCompletion interface {
	CommitReservation(context.Context, string) (resourcespec.LandedResource, bool, error)
}

type resourceCompletion struct{ door *door }

func (c resourceCompletion) CommitReservation(
	ctx context.Context,
	id string,
) (resourcespec.LandedResource, bool, error) {
	c.door.resourceGate.Lock()
	defer c.door.resourceGate.Unlock()
	return c.door.deps.Registry.CommitReservation(ctx, id)
}
