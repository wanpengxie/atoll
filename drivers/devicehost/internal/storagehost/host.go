package storagehost

import (
	"errors"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
)

// Host exposes files from the channel directory itself.
type Host struct {
	cr       *channelRoot
	streamer Streamer
}

func Open(channelDir string) (*Host, error) {
	cr, err := openChannelRoot(channelDir)
	if err != nil {
		return nil, err
	}
	return &Host{cr: cr}, nil
}

func (h *Host) OpenRead(path string) (io.ReadSeekCloser, error) {
	return h.streamer.OpenRead(h.cr, path)
}

func (h *Host) OpenWrite(path string) (*WriteHandle, error) {
	return h.streamer.OpenWrite(h.cr, path)
}

func (h *Host) Create(path string) error {
	w, err := h.OpenWrite(path)
	if err != nil {
		return err
	}
	return w.Commit()
}

func (h *Host) Delete(path string) error { return h.cr.root.Remove(path) }

type FileInfo struct {
	Path string
	Size int64
	// ModifiedAt is Unix milliseconds, and zero when the filesystem gave none.
	// It travels with Size because the same syscall answers both: a listing
	// that dropped it made every reader that wanted a modified date go back and
	// stat each row for something already in hand.
	ModifiedAt int64
}

// modifiedAt reads a stat result's mtime as Unix milliseconds, matching how
// every other timestamp on this wire is spelled. A zero time stays zero rather
// than becoming the epoch, so "not reported" and "1970" cannot be confused.
func modifiedAt(info fs.FileInfo) int64 {
	t := info.ModTime()
	if t.IsZero() {
		return 0
	}
	return t.UnixMilli()
}

func (h *Host) Stat(path string) (FileInfo, bool, error) {
	info, err := h.cr.root.Stat(path)
	if errors.Is(err, fs.ErrNotExist) {
		return FileInfo{}, false, nil
	}
	if err != nil {
		return FileInfo{}, false, err
	}
	return FileInfo{Path: path, Size: info.Size(), ModifiedAt: modifiedAt(info)}, true, nil
}

func (h *Host) List(prefix string) ([]FileInfo, error) {
	if prefix == "" {
		prefix = "./"
	}
	dir, base := filepath.Dir(prefix), filepath.Base(prefix)
	if strings.HasSuffix(prefix, "/") {
		dir, base = strings.TrimSuffix(prefix, "/"), ""
	}
	if dir == "." {
		dir = "."
	}
	f, err := h.cr.root.Open(dir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}
	defer f.Close()
	entries, err := f.ReadDir(-1)
	if err != nil {
		return nil, err
	}
	out := make([]FileInfo, 0, len(entries))
	for _, entry := range entries {
		if !strings.HasPrefix(entry.Name(), base) {
			continue
		}
		info, err := entry.Info()
		if err != nil {
			return nil, err
		}
		path := entry.Name()
		if dir != "." {
			path = filepath.Join(dir, path)
		}
		out = append(out, FileInfo{Path: path, Size: info.Size(), ModifiedAt: modifiedAt(info)})
	}
	return out, nil
}

func (h *Host) Close() error { return h.cr.Close() }
