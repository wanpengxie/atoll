package accessdoor

import (
	"context"
	"errors"

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
	// Root is this device's channel directory on its own filesystem, as the
	// device reported it. Empty when the device has not answered (offline, or
	// a mount resolved without asking). It exists so the door can normalize a
	// device-local absolute path into the channel-relative one it addresses by
	// — see normalizeFileName.
	Root string
}

type StorageMounts interface {
	ResolveStorageDaemon(context.Context, channel.ID, string) (StorageMount, bool, error)
	// ListStorageMounts enumerates every device bound to the channel. A
	// device-local absolute path names its device by lying under that device's
	// channel directory, so answering "which file is this" means asking the
	// mounts, not the caller — the same path is the same file whoever names it,
	// including a browser that runs on no device at all.
	ListStorageMounts(context.Context, channel.ID) ([]StorageMount, error)
}

type FileInfo struct {
	Path     string
	NodeType FileNodeType
	Size     int64
	// ModifiedAt is Unix milliseconds, zero when the device reported none.
	ModifiedAt int64
}

// FileNodeType is re-exported at the door face so callers never import the
// kernel-only resourcespec package directly.
type FileNodeType = resourcespec.FileNodeType

const (
	FileNodeRegular   = resourcespec.FileNodeRegular
	FileNodeDirectory = resourcespec.FileNodeDirectory
	FileNodeOther     = resourcespec.FileNodeOther
)

// FileControl performs metadata operations on the host's channel directory.
// It is not an existence registry: every answer comes from the filesystem.
type FileControl interface {
	Create(context.Context, string, string, FileNodeType) error
	Delete(context.Context, string, string) error
	Stat(context.Context, string, string) (FileInfo, bool, error)
	List(context.Context, string, string, int, string) ([]FileInfo, string, error)
}

var ErrMalformedFileCursor = errors.New("accessdoor: malformed file cursor")

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
