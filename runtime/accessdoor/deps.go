package accessdoor

import (
	"context"

	"github.com/wanpengxie/atoll/protocol/access"
	"github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/protocol/resource"
	"github.com/wanpengxie/atoll/runtime/resourcespec"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

type DriverTable map[resourcespec.ResourceKind]resourcespec.Driver

type StorageMount struct {
	DaemonID string
	Name     string
	Online   bool
}

type StorageMounts interface {
	ResolveStorageDaemon(context.Context, channel.ID, string) (StorageMount, bool, error)
}

type FileInfo struct {
	Path string
	Size int64
}

// FileControl performs metadata operations on the host's channel directory.
// It is not an existence registry: every answer comes from the filesystem.
type FileControl interface {
	Create(context.Context, string, string) error
	Delete(context.Context, string, string) error
	Stat(context.Context, string, string) (FileInfo, bool, error)
	List(context.Context, string, string) ([]FileInfo, error)
}

type TransferControl interface {
	IssueTransfer(context.Context, resource.ResourceID, string, string, access.Operation) (string, error)
}

type Deps struct {
	Registry  resourcespec.Registry
	Drivers   DriverTable
	Authority storespec.ResourceActorAuthority
	State     resourcespec.StateStore

	ChannelID       channel.ID
	ChannelName     string
	StorageMounts   StorageMounts
	Files           FileControl
	TransferControl TransferControl
}
