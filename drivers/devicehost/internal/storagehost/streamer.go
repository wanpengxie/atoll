package storagehost

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
)

type Streamer struct{}

type ReadHandle struct{ f *os.File }

func (h *ReadHandle) Read(p []byte) (int, error)                   { return h.f.Read(p) }
func (h *ReadHandle) ReadAt(p []byte, off int64) (int, error)      { return h.f.ReadAt(p, off) }
func (h *ReadHandle) Seek(offset int64, whence int) (int64, error) { return h.f.Seek(offset, whence) }
func (h *ReadHandle) Close() error                                 { return h.f.Close() }

var _ io.ReadSeekCloser = (*ReadHandle)(nil)

// WriteHandle writes the final path directly. Commit is only the caller's
// close point; Abort also closes and deliberately does not roll bytes back.
type WriteHandle struct {
	f    *os.File
	done bool
}

func (h *WriteHandle) Write(p []byte) (int, error) { return h.f.Write(p) }
func (h *WriteHandle) Commit() error {
	if h.done {
		return fmt.Errorf("storagehost: write handle already closed")
	}
	h.done = true
	return h.f.Close()
}
func (h *WriteHandle) Abort() error {
	if h.done {
		return nil
	}
	h.done = true
	return h.f.Close()
}

func (Streamer) OpenRead(cr *channelRoot, path string) (*ReadHandle, error) {
	f, err := cr.root.Open(path)
	if err != nil {
		return nil, fmt.Errorf("storagehost: open read %q: %w", path, err)
	}
	return &ReadHandle{f: f}, nil
}

func (Streamer) OpenWrite(cr *channelRoot, path string) (*WriteHandle, error) {
	parent := filepath.Dir(path)
	if parent != "." {
		if err := cr.root.MkdirAll(parent, 0o700); err != nil {
			return nil, fmt.Errorf("storagehost: create parent %q: %w", parent, err)
		}
	}
	f, err := cr.root.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		return nil, fmt.Errorf("storagehost: open write %q: %w", path, err)
	}
	return &WriteHandle{f: f}, nil
}
