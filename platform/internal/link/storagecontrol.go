package link

import (
	"errors"
	"fmt"
)

const (
	FileCreate = "create"
	FileDelete = "delete"
	FileStat   = "stat"
	FileList   = "list"
)

type FileRequest struct {
	RequestID string `json:"request_id"`
	Op        string `json:"op"`
	Path      string `json:"path"`
}

type FileEntry struct {
	Path string `json:"path"`
	Size int64  `json:"size"`
}

type FileReply struct {
	RequestID string      `json:"request_id"`
	OK        bool        `json:"ok"`
	Found     bool        `json:"found,omitempty"`
	Entries   []FileEntry `json:"entries,omitempty"`
	Reason    string      `json:"reason,omitempty"`
}

func (m FileRequest) validate() error {
	if err := requiredControlField("file_request.request_id", m.RequestID); err != nil {
		return err
	}
	if m.Op != FileCreate && m.Op != FileDelete && m.Op != FileStat && m.Op != FileList {
		return fmt.Errorf("link: invalid file operation %q", m.Op)
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
