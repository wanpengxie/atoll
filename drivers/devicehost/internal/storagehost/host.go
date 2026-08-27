package storagehost

import (
	"encoding/base64"
	"errors"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
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

func (h *Host) Create(path string, nodeType NodeType) error {
	if nodeType == "" {
		nodeType = NodeRegular
	}
	if nodeType == NodeDirectory {
		return h.cr.root.Mkdir(path, 0o700)
	}
	if nodeType != NodeRegular {
		return errors.New("storagehost: invalid node type")
	}
	w, err := h.OpenWrite(path)
	if err != nil {
		return err
	}
	return w.Commit()
}

func (h *Host) Delete(path string) error { return h.cr.root.Remove(path) }

type FileInfo struct {
	Path     string
	NodeType NodeType
	Size     int64
	// ModifiedAt is Unix milliseconds, and zero when the filesystem gave none.
	// It travels with Size because the same syscall answers both: a listing
	// that dropped it made every reader that wanted a modified date go back and
	// stat each row for something already in hand.
	ModifiedAt int64
}

type NodeType string

const (
	NodeRegular   NodeType = "regular"
	NodeDirectory NodeType = "directory"
	NodeOther     NodeType = "other"
)

func nodeType(info fs.FileInfo) NodeType {
	if info.IsDir() {
		return NodeDirectory
	}
	if info.Mode().IsRegular() {
		return NodeRegular
	}
	return NodeOther
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
	return FileInfo{Path: path, NodeType: nodeType(info), Size: info.Size(), ModifiedAt: modifiedAt(info)}, true, nil
}

var ErrMalformedCursor = errors.New("storagehost: malformed file cursor")

func fileSortKey(info FileInfo) string {
	rank := "2"
	switch info.NodeType {
	case NodeDirectory:
		rank = "0"
	case NodeRegular:
		rank = "1"
	}
	return rank + "\x00" + info.Path
}

func encodeCursor(prefix string, info FileInfo) string {
	return base64.RawURLEncoding.EncodeToString([]byte(prefix + "\x00" + fileSortKey(info)))
}

func decodeCursor(cursor, prefix string) (string, error) {
	raw, err := base64.RawURLEncoding.DecodeString(cursor)
	if err != nil {
		return "", ErrMalformedCursor
	}
	separator := strings.IndexByte(string(raw), 0)
	if separator < 0 || string(raw[:separator]) != prefix {
		return "", ErrMalformedCursor
	}
	key := raw[separator+1:]
	if len(key) < 3 || key[1] != 0 || key[0] < '0' || key[0] > '2' {
		return "", ErrMalformedCursor
	}
	return string(key), nil
}

func (h *Host) List(prefix string, limit int, cursor string) ([]FileInfo, string, error) {
	if limit <= 0 {
		return nil, "", errors.New("storagehost: list limit must be positive")
	}
	if prefix == "" {
		prefix = "./"
	}
	var after string
	if cursor != "" {
		var err error
		after, err = decodeCursor(cursor, prefix)
		if err != nil {
			return nil, "", err
		}
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
			return nil, "", nil
		}
		return nil, "", err
	}
	defer f.Close()
	entries, err := f.ReadDir(-1)
	if err != nil {
		return nil, "", err
	}
	out := make([]FileInfo, 0, len(entries))
	for _, entry := range entries {
		if !strings.HasPrefix(entry.Name(), base) {
			continue
		}
		info, err := entry.Info()
		if err != nil {
			return nil, "", err
		}
		path := entry.Name()
		if dir != "." {
			path = filepath.Join(dir, path)
		}
		out = append(out, FileInfo{Path: path, NodeType: nodeType(info), Size: info.Size(), ModifiedAt: modifiedAt(info)})
	}
	sort.Slice(out, func(i, j int) bool { return fileSortKey(out[i]) < fileSortKey(out[j]) })
	start := sort.Search(len(out), func(i int) bool { return fileSortKey(out[i]) > after })
	out = out[start:]
	if len(out) <= limit {
		return out, "", nil
	}
	page := out[:limit]
	return page, encodeCursor(prefix, page[len(page)-1]), nil
}

func (h *Host) Close() error { return h.cr.Close() }
