package devicehost

import (
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

func (a storageHostAdapter) Create(path string) error { return a.host.Create(path) }
func (a storageHostAdapter) Delete(path string) error { return a.host.Delete(path) }
func (a storageHostAdapter) Stat(path string) (compute.FileInfo, bool, error) {
	info, found, err := a.host.Stat(path)
	return compute.FileInfo{Path: info.Path, Size: info.Size}, found, err
}
func (a storageHostAdapter) List(prefix string) ([]compute.FileInfo, error) {
	rows, err := a.host.List(prefix)
	out := make([]compute.FileInfo, 0, len(rows))
	for _, row := range rows {
		out = append(out, compute.FileInfo{Path: row.Path, Size: row.Size})
	}
	return out, err
}

var _ compute.LocalFileOpener = storageHostAdapter{}
