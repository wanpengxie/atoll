package devicehost

import (
	"errors"
	"io"

	"github.com/wanpengxie/atoll/drivers/devicehost/internal/storagehost"
	"github.com/wanpengxie/atoll/platform/compute"
	"github.com/wanpengxie/atoll/runtime/accessdoor"
)

type storageHostAdapter struct{ host *storagehost.Host }

func (a storageHostAdapter) OpenRead(path string) (io.ReadSeekCloser, error) {
	return a.host.OpenRead(path)
}

func (a storageHostAdapter) OpenWrite(path string) (accessdoor.WriteHandle, error) {
	return a.host.OpenWrite(path)
}

func (a storageHostAdapter) Create(path string, nodeType accessdoor.FileNodeType) error {
	return a.host.Create(path, storagehost.NodeType(nodeType))
}
func (a storageHostAdapter) Delete(path string) error { return a.host.Delete(path) }
func (a storageHostAdapter) Stat(path string) (compute.FileInfo, bool, error) {
	info, found, err := a.host.Stat(path)
	return compute.FileInfo{Path: info.Path, NodeType: accessdoor.FileNodeType(info.NodeType), Size: info.Size, ModifiedAt: info.ModifiedAt}, found, err
}
func (a storageHostAdapter) List(prefix string, limit int, cursor string) ([]compute.FileInfo, string, error) {
	rows, next, err := a.host.List(prefix, limit, cursor)
	if errors.Is(err, storagehost.ErrMalformedCursor) {
		return nil, "", accessdoor.ErrMalformedFileCursor
	}
	out := make([]compute.FileInfo, 0, len(rows))
	for _, row := range rows {
		out = append(out, compute.FileInfo{Path: row.Path, NodeType: accessdoor.FileNodeType(row.NodeType), Size: row.Size, ModifiedAt: row.ModifiedAt})
	}
	return out, next, err
}

var _ compute.LocalFileOpener = storageHostAdapter{}
