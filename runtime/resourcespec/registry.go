package resourcespec

import (
	"context"
	"errors"

	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/resource"
)

var ErrAlreadyExists = errors.New("resourcespec: resource already exists")
var ErrOwnerInactive = errors.New("resourcespec: actor-scoped resource owner inactive")
var ErrMalformedCursor = errors.New("resourcespec: malformed list cursor")

// ResourceMeta describes only a kv row. Files have no registry row.
type ResourceMeta struct {
	Kind      ResourceKind
	CreatedAt int64
	CreatedBy actor.ActorID
}

type ResourceRow struct {
	ID   resource.ResourceID
	Meta ResourceMeta
}

// Registry is the channel kv registry. It has no file lifecycle surface.
type Registry interface {
	Resolve(context.Context, resource.ResourceID) (ResourceMeta, bool, error)
	Create(context.Context, resource.ResourceID, ResourceKind, actor.ActorID, []byte) error
	Delete(context.Context, resource.ResourceID) error
	List(context.Context, string, int, string) ([]ResourceRow, string, error)
}
