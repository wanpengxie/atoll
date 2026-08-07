package storespec

import (
	"context"
	"errors"

	"github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/protocol/resource"
)

var ErrResourceCapabilityUnavailable = errors.New("storespec: resource byte capability unavailable")

type ResourceReadStore interface {
	ListReadable(context.Context, channel.ResourceListQuery) (channel.ResourcePage, error)
	StatReadable(context.Context, resource.ResourceID) (channel.ResourceMeta, bool, error)
	FetchReadable(context.Context, resource.ResourceID) (channel.ResourceMeta, []byte, bool, error)
}
