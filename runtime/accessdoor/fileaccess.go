package accessdoor

import (
	"context"
	"errors"
	"io"

	"github.com/wanpengxie/atoll/protocol/access"
	"github.com/wanpengxie/atoll/protocol/resource"
)

type FileRoute struct {
	Token  string
	Path   string
	Mode   access.Operation
	Redeem FileRedeem
}

type FileRedeem string

const (
	FileRedeemLocal  FileRedeem = "local"
	FileRedeemRemote FileRedeem = "remote"
)

type WriteHandle interface {
	io.Writer
	Commit() error
	Abort() error
}

type LocalFile struct {
	Read  io.ReadSeekCloser
	Write WriteHandle
}

type RemoteFile struct {
	Read  io.ReadCloser
	Write WriteHandle
}

type FileAccess struct {
	Local  *LocalFile
	Remote *RemoteFile
}

func (f FileAccess) Reader() (io.ReadCloser, bool) {
	if f.Local != nil && f.Local.Read != nil {
		return f.Local.Read, true
	}
	if f.Remote != nil && f.Remote.Read != nil {
		return f.Remote.Read, true
	}
	return nil, false
}

func (f FileAccess) Writer() (WriteHandle, bool) {
	if f.Local != nil && f.Local.Write != nil {
		return f.Local.Write, true
	}
	if f.Remote != nil && f.Remote.Write != nil {
		return f.Remote.Write, true
	}
	return nil, false
}

type FileOpener interface {
	Open(context.Context, resource.ResourceID, access.Operation) (FileAccess, Outcome, error)
	Redeem(context.Context, FileRoute) (FileAccess, error)
}

var ErrFileCapabilityUnavailable = errors.New("accessdoor: capability_unavailable")

type HostOfflineError struct{ Host string }

// Error states the one thing a caller must not conclude on its own. "Offline"
// and "absent" call for opposite moves — wait, or create — and a caller told
// only that something failed picks the wrong one; a model picks it confidently
// and then rebuilds a file that was never gone.
func (e *HostOfflineError) Error() string {
	return "device " + e.Host + " is attached to this channel but not reachable right now. " +
		"The file is most likely still there — do NOT treat it as missing and do NOT recreate it. " +
		"Check the device with system.device.list; retry the same request once it is back."
}
func NewHostOfflineError(host string) error { return &HostOfflineError{Host: host} }
