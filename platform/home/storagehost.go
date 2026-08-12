package home

import (
	"context"
	"errors"

	"github.com/wanpengxie/atoll/platform"
	"github.com/wanpengxie/atoll/platform/dataplane"
	"github.com/wanpengxie/atoll/protocol/access"
	channelpkg "github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/protocol/resource"
	"github.com/wanpengxie/atoll/runtime/accessdoor"
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

func (c daemonTransferControl) IssueTransfer(ctx context.Context, address resource.ResourceID, targetID, targetName string, mode access.Operation) (string, error) {
	if c.issuer == nil {
		return "", errors.New("platform: dataplane issuer unavailable")
	}
	grant, err := c.issuer.Issue(ctx, dataplane.IssueSpec{Address: address, ChannelID: c.chID, Mode: mode, HostID: targetID, HostName: targetName})
	if err != nil {
		var offline *dataplane.HostOfflineError
		if errors.As(err, &offline) {
			return "", accessdoor.NewHostOfflineError(offline.Host)
		}
		return "", err
	}
	return grant.Ticket, nil
}

var _ accessdoor.StorageMounts = daemonStorageMounts{}
var _ accessdoor.FileControl = daemonFiles{}
var _ accessdoor.TransferControl = daemonTransferControl{}
