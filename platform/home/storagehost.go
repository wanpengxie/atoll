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

func (m daemonStorageMounts) ResolveStorageDaemon(ctx context.Context, ch channelpkg.ID, name string) (accessdoor.StorageMount, bool, error) {
	if m.routes == nil || m.bindings == nil || m.directory == nil {
		return accessdoor.StorageMount{}, false, nil
	}
	id, present, found, err := m.directory.ResolveDeviceName(ctx, name)
	if err != nil || !found || !present {
		return accessdoor.StorageMount{}, false, err
	}
	bound, err := m.bindings.IsBound(ctx, ch, id)
	if err != nil || !bound {
		return accessdoor.StorageMount{}, false, err
	}
	return accessdoor.StorageMount{DaemonID: id, Name: name, Online: m.routes.LaneAttached(id, string(ch))}, true, nil
}

type daemonFiles struct {
	routes platform.DaemonRoutes
	chID   channelpkg.ID
}

func (f daemonFiles) Create(ctx context.Context, daemonID, path string) error {
	return f.routes.FileCreate(ctx, daemonID, string(f.chID), path)
}
func (f daemonFiles) Delete(ctx context.Context, daemonID, path string) error {
	return f.routes.FileDelete(ctx, daemonID, string(f.chID), path)
}
func (f daemonFiles) Stat(ctx context.Context, daemonID, path string) (accessdoor.FileInfo, bool, error) {
	info, found, err := f.routes.FileStat(ctx, daemonID, string(f.chID), path)
	return accessdoor.FileInfo{Path: info.Path, Size: info.Size}, found, err
}
func (f daemonFiles) List(ctx context.Context, daemonID, prefix string) ([]accessdoor.FileInfo, error) {
	rows, err := f.routes.FileList(ctx, daemonID, string(f.chID), prefix)
	out := make([]accessdoor.FileInfo, 0, len(rows))
	for _, row := range rows {
		out = append(out, accessdoor.FileInfo{Path: row.Path, Size: row.Size})
	}
	return out, err
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
