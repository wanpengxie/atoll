package home

import (
	"context"
	"errors"
	"fmt"

	"github.com/wanpengxie/atoll/platform"
	"github.com/wanpengxie/atoll/platform/dataplane"
	"github.com/wanpengxie/atoll/platform/internal/link"
	"github.com/wanpengxie/atoll/protocol/access"
	"github.com/wanpengxie/atoll/protocol/actor"
	channelpkg "github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/runtime/accessdoor"
	"github.com/wanpengxie/atoll/runtime/resourcespec"
)

type daemonStorageMounts struct {
	routes    platform.DaemonRoutes
	bindings  BindingReader
	directory DeviceDirectory
	chID      channelpkg.ID
}

func (m daemonStorageMounts) ResolveStorageDaemon(ctx context.Context, ch channelpkg.ID, id string) (accessdoor.StorageMount, bool, error) {
	if m.routes == nil || m.bindings == nil || m.directory == nil {
		return accessdoor.StorageMount{}, false, nil
	}
	name, present, found, err := m.directory.ResolveDeviceID(ctx, id)
	if err != nil {
		return accessdoor.StorageMount{}, false, err
	}
	if found {
		if !present {
			return accessdoor.StorageMount{}, false, nil
		}
		return m.mount(ctx, ch, id, name)
	}
	// Compatibility read only: older ledger rows addressed the first URI
	// segment by mutable device name. New addresses are always DeviceID-based,
	// but old attachments must remain readable while they age out naturally.
	legacyID, present, legacyFound, err := m.directory.ResolveDeviceName(ctx, id)
	if err != nil || !legacyFound || !present {
		return accessdoor.StorageMount{}, false, err
	}
	return m.mount(ctx, ch, legacyID, id)
}

// ListStorageMounts enumerates the channel's devices so the door can ask which
// one holds a given absolute path. A device that resolves to no name is skipped
// rather than reported blank: a mount with no name cannot appear in an address,
// so it can hold nothing the door could go on to open.
func (m daemonStorageMounts) ListStorageMounts(ctx context.Context, ch channelpkg.ID) ([]accessdoor.StorageMount, error) {
	if m.routes == nil || m.bindings == nil || m.directory == nil {
		return nil, nil
	}
	ids, err := m.bindings.ListBoundDeviceIDs(ctx, ch)
	if err != nil {
		return nil, err
	}
	out := make([]accessdoor.StorageMount, 0, len(ids))
	for _, id := range ids {
		name, present, found, err := m.directory.ResolveDeviceID(ctx, id)
		if err != nil {
			return nil, err
		}
		if !found || !present || name == "" {
			continue
		}
		mount, ok, err := m.mount(ctx, ch, id, name)
		if err != nil {
			return nil, err
		}
		if ok {
			out = append(out, mount)
		}
	}
	return out, nil
}

// mount completes a resolved (id, name) pair. The root is asked for only when
// the lane is up: it is the device's own answer about its own filesystem, and
// there is nobody to ask when the lane is gone. An offline mount therefore
// carries an empty root, and a caller needing one says so in its own words
// rather than inheriting "offline" for a question it did not ask.
func (m daemonStorageMounts) mount(ctx context.Context, ch channelpkg.ID, id, name string) (accessdoor.StorageMount, bool, error) {
	bound, err := m.bindings.IsBound(ctx, ch, id)
	if err != nil || !bound {
		return accessdoor.StorageMount{}, false, err
	}
	online := m.routes.LaneAttached(id, string(ch))
	mount := accessdoor.StorageMount{DaemonID: id, Name: name, Online: online}
	if online {
		root, ok, err := m.routes.LaneWorkspace(ctx, id, string(ch))
		if err != nil {
			return accessdoor.StorageMount{}, false, err
		}
		if ok {
			mount.Root = root
		}
	}
	return mount, true, nil
}

type daemonFiles struct {
	routes platform.DaemonRoutes
	chID   channelpkg.ID
}

func (f daemonFiles) Create(ctx context.Context, daemonID, path string, nodeType accessdoor.FileNodeType) error {
	return f.routes.FileCreate(ctx, daemonID, string(f.chID), path, nodeType)
}
func (f daemonFiles) Delete(ctx context.Context, daemonID, path string) error {
	return f.routes.FileDelete(ctx, daemonID, string(f.chID), path)
}
func (f daemonFiles) Stat(ctx context.Context, daemonID, path string) (accessdoor.FileInfo, bool, error) {
	info, found, err := f.routes.FileStat(ctx, daemonID, string(f.chID), path)
	return accessdoor.FileInfo{Path: info.Path, NodeType: info.NodeType, Size: info.Size, ModifiedAt: info.ModifiedAt}, found, err
}
func (f daemonFiles) List(ctx context.Context, daemonID, prefix string, limit int, cursor string) ([]accessdoor.FileInfo, string, error) {
	rows, next, err := f.routes.FileList(ctx, daemonID, string(f.chID), prefix, limit, cursor)
	out := make([]accessdoor.FileInfo, 0, len(rows))
	for _, row := range rows {
		out = append(out, accessdoor.FileInfo{Path: row.Path, NodeType: row.NodeType, Size: row.Size, ModifiedAt: row.ModifiedAt})
	}
	return out, next, err
}

type daemonTransferControl struct {
	issuer dataplane.Issuer
	chID   channelpkg.ID
}

func (c daemonTransferControl) IssueTransfer(ctx context.Context, spec accessdoor.TransferSpec) (string, error) {
	if c.issuer == nil {
		return "", errors.New("platform: dataplane issuer unavailable")
	}
	address := spec.Address
	// The ticket carries the path the holder may touch, relative to the
	// channel's own directory on that machine. It is resolved once, here, where
	// the address is already understood — the redeeming side reads the ticket
	// rather than parsing the address a second time.
	parsed, err := resourcespec.ParseFileAddress(string(address))
	if err != nil {
		return "", err
	}
	grant, err := c.issuer.Issue(ctx, dataplane.IssueSpec{
		Address: address, Path: parsed.Path, ChannelID: c.chID, Mode: spec.Mode,
		HostID: spec.HostID, HostName: spec.HostName,
		Caller: spec.Caller,
	})
	if err != nil {
		var offline *dataplane.HostOfflineError
		if errors.As(err, &offline) {
			return "", accessdoor.NewHostOfflineError(offline.Host)
		}
		return "", err
	}
	return grant.Ticket, nil
}

// daemonTransferRedeem finishes a transfer for an actor living in this process.
// The stream it opens is the same one a browser's transfer rides — the server
// has always been the side that opens these, so redeeming here is not a new
// path to a machine, it is the existing path kept instead of pumped away.
type daemonTransferRedeem struct {
	redeemer dataplane.Redeemer
	chID     channelpkg.ID
}

func (c daemonTransferRedeem) RedeemTransfer(ctx context.Context, caller actor.ActorID, route accessdoor.FileRoute) (accessdoor.FileAccess, error) {
	if c.redeemer == nil {
		return accessdoor.FileAccess{}, errors.New("platform: dataplane redeemer unavailable")
	}
	// A local route says the caller's own machine holds the bytes. Nothing in
	// this process is a machine that holds channel files, so a local route
	// reaching here is a routing fault, not a case to handle.
	if route.Redeem != accessdoor.FileRedeemRemote {
		return accessdoor.FileAccess{}, fmt.Errorf("platform: server-side actor cannot redeem a %q file route", route.Redeem)
	}
	conn, err := c.redeemer.OpenTransfer(ctx, c.chID, caller, route.Token, route.Mode)
	if err != nil {
		var offline *dataplane.HostOfflineError
		if errors.As(err, &offline) {
			return accessdoor.FileAccess{}, accessdoor.NewHostOfflineError(offline.Host)
		}
		return accessdoor.FileAccess{}, err
	}
	switch route.Mode {
	case access.OpRead:
		return accessdoor.FileAccess{Remote: &accessdoor.RemoteFile{Read: link.NewExchangeReader(conn)}}, nil
	case access.OpWrite:
		return accessdoor.FileAccess{Remote: &accessdoor.RemoteFile{Write: link.NewExchangeWriteHandle(conn)}}, nil
	default:
		_ = conn.Close()
		return accessdoor.FileAccess{}, fmt.Errorf("platform: unknown file route mode %q", route.Mode)
	}
}

var _ accessdoor.StorageMounts = daemonStorageMounts{}
var _ accessdoor.FileControl = daemonFiles{}
var _ accessdoor.TransferControl = daemonTransferControl{}
var _ accessdoor.TransferRedeem = daemonTransferRedeem{}
