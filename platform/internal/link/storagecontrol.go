package link

import (
	"errors"
	"fmt"

	"github.com/wanpengxie/atoll/runtime/accessdoor"
)

const (
	FileCreate = "create"
	FileDelete = "delete"
	FileStat   = "stat"
	FileList   = "list"
	// FileRoot answers where this lane's channel directory actually is on the
	// device's own filesystem. The device is the only side that knows it —
	// the server assigns the daemon id and sends the channel name, but
	// $ATOLL_HOME is the device's. The answer is the value the device's
	// storage host was opened on, never a re-derivation of the layout rule,
	// so the rule stays single-sourced in compute.
	//
	// It is constant for the life of the lane and dies with it, so a caller
	// may cache it against the lane. That is not the stale-copy pattern
	// LaneAttached refuses: compartment readiness varies while a lane lives,
	// this does not.
	FileRoot = "root"
	// FileErrorBadCursor preserves the access-plane query verdict across the
	// device lane without publishing filesystem error text as protocol.
	FileErrorBadCursor = "bad_cursor"
)

type FileRequest struct {
	RequestID string                  `json:"request_id"`
	Op        string                  `json:"op"`
	Path      string                  `json:"path"`
	NodeType  accessdoor.FileNodeType `json:"node_type,omitempty"`
	Limit     int                     `json:"limit,omitempty"`
	Cursor    string                  `json:"cursor,omitempty"`
}

type FileEntry struct {
	Path     string                  `json:"path"`
	NodeType accessdoor.FileNodeType `json:"node_type"`
	Size     int64                   `json:"size"`
	// ModifiedAt is Unix milliseconds. It rides with Size because the device's
	// one stat answers both, and omitempty because a device that predates the
	// field simply says nothing rather than claiming the epoch.
	ModifiedAt int64 `json:"modified_at,omitempty"`
}

type FileReply struct {
	RequestID string      `json:"request_id"`
	OK        bool        `json:"ok"`
	Found     bool        `json:"found,omitempty"`
	Entries   []FileEntry `json:"entries,omitempty"`
	Reason    string      `json:"reason,omitempty"`
	Code      string      `json:"code,omitempty"`
	Next      string      `json:"next,omitempty"`
	// Root carries the FileRoot answer only.
	Root string `json:"root,omitempty"`
}

func (m FileRequest) validate() error {
	if err := requiredControlField("file_request.request_id", m.RequestID); err != nil {
		return err
	}
	if m.Op != FileCreate && m.Op != FileDelete && m.Op != FileStat && m.Op != FileList && m.Op != FileRoot {
		return fmt.Errorf("link: invalid file operation %q", m.Op)
	}
	if m.Op == FileRoot {
		if m.Path != "" {
			return errors.New("link: file_request.path must be empty for root")
		}
		return nil
	}
	if m.Op != FileCreate && m.NodeType != "" {
		return errors.New("link: file_request.node_type belongs to create")
	}
	if m.Op != FileList && (m.Limit != 0 || m.Cursor != "") {
		return errors.New("link: file_request limit/cursor belong to list")
	}
	if m.Op == FileList && m.Limit <= 0 {
		return errors.New("link: file_request.list requires a positive limit")
	}
	if m.Op == FileCreate && m.NodeType != "" && m.NodeType != accessdoor.FileNodeRegular && m.NodeType != accessdoor.FileNodeDirectory {
		return fmt.Errorf("link: invalid file node type %q", m.NodeType)
	}
	if m.Op == FileList && m.Path == "" {
		return nil
	}
	return requiredControlField("file_request.path", m.Path)
}

func (m FileReply) validate() error {
	if err := requiredControlField("file_reply.request_id", m.RequestID); err != nil {
		return err
	}
	if !m.OK && m.Reason == "" {
		return errors.New("link: failed file reply requires reason")
	}
	return nil
}
