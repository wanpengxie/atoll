package accessdoor

import (
	"context"

	"github.com/wanpengxie/atoll/protocol/access"
	"github.com/wanpengxie/atoll/protocol/actor"
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

// TransferSpec is one authorized-but-unfinished byte transfer. Caller travels
// with it because the bytes move later, on a connection this door never sees,
// and the actor is half of what makes that later connection the same operation
// as this decision (the channel is the other half, and the issuer knows it).
type TransferSpec struct {
	Address  resource.ResourceID
	HostID   string
	HostName string
	Mode     access.Operation
	Caller   actor.ActorID
}

type TransferControl interface {
	IssueTransfer(context.Context, TransferSpec) (string, error)
}

// TransferRedeem finishes a transfer this door authorized, for a caller running
// in this same process. It is the exact counterpart of TransferControl — one
// issues, one redeems — and its absence is why a server-resident actor could
// hold a file route it had no way to turn into bytes.
type TransferRedeem interface {
	RedeemTransfer(ctx context.Context, caller actor.ActorID, route FileRoute) (FileAccess, error)
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
	TransferRedeem  TransferRedeem
}
