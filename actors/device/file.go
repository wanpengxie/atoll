package device

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/wanpengxie/atoll/lib/actorbase"
)

// handleFileRead implements device.file.read. Offset/Limit slice by line;
// without them the whole file is returned subject to MaxReadBytes.
func (a *Actor) handleFileRead(msg actorbase.Msg) {
	var p FileReadPayload
	if err := json.Unmarshal(msg.Payload, &p); err != nil {
		a.fail(msg, "payload_invalid", fmt.Sprintf("decode payload: %v", err))
		return
	}
	ws, err := a.channelWorkspace(msg.ChannelID)
	if err != nil {
		a.fail(msg, "workspace_unavailable", err.Error())
		return
	}
	full, err := resolvePath(ws, p.Path)
	if err != nil {
		a.fail(msg, "path_invalid", err.Error())
		return
	}

	info, err := os.Stat(full)
	if err != nil {
		a.fail(msg, "file_not_found", err.Error())
		return
	}
	if info.IsDir() {
		a.fail(msg, "path_invalid", "path is a directory; list it via device.exec (ls)")
		return
	}
	if p.Offset == 0 && p.Limit == 0 && info.Size() > MaxReadBytes {
		a.fail(msg, "file_too_large",
			fmt.Sprintf("file is %d bytes (cap %d); read it in slices with offset/limit", info.Size(), MaxReadBytes))
		return
	}

	data, err := os.ReadFile(full)
	if err != nil {
		a.fail(msg, "read_failed", err.Error())
		return
	}

	content := string(data)
	truncated := false
	if p.Offset > 0 || p.Limit > 0 {
		lines := strings.Split(content, "\n")
		if p.Offset >= len(lines) {
			lines = nil
		} else {
			lines = lines[p.Offset:]
		}
		if p.Limit > 0 && len(lines) > p.Limit {
			lines = lines[:p.Limit]
			truncated = true
		}
		content = strings.Join(lines, "\n")
	}

	a.respond(msg, FileReadResult{
		Content:   content,
		Size:      info.Size(),
		Truncated: truncated,
	})
}

// handleFileWrite implements device.file.write: whole-file write, parent
// directories created as needed.
func (a *Actor) handleFileWrite(msg actorbase.Msg) {
	var p FileWritePayload
	if err := json.Unmarshal(msg.Payload, &p); err != nil {
		a.fail(msg, "payload_invalid", fmt.Sprintf("decode payload: %v", err))
		return
	}
	ws, err := a.channelWorkspace(msg.ChannelID)
	if err != nil {
		a.fail(msg, "workspace_unavailable", err.Error())
		return
	}
	full, err := resolvePath(ws, p.Path)
	if err != nil {
		a.fail(msg, "path_invalid", err.Error())
		return
	}
	if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
		a.fail(msg, "write_failed", fmt.Sprintf("create parent dir: %v", err))
		return
	}
	if err := os.WriteFile(full, []byte(p.Content), 0o644); err != nil {
		a.fail(msg, "write_failed", err.Error())
		return
	}
	a.respond(msg, FileWriteResult{OK: true, Bytes: len(p.Content)})
}

// handleFileEdit implements device.file.edit: exact string replacement.
// Without replace_all, old_string must occur exactly once.
func (a *Actor) handleFileEdit(msg actorbase.Msg) {
	var p FileEditPayload
	if err := json.Unmarshal(msg.Payload, &p); err != nil {
		a.fail(msg, "payload_invalid", fmt.Sprintf("decode payload: %v", err))
		return
	}
	if p.OldString == "" {
		a.fail(msg, "payload_invalid", "device.file.edit: old_string required")
		return
	}
	if p.OldString == p.NewString {
		a.fail(msg, "payload_invalid", "device.file.edit: old_string and new_string are identical")
		return
	}
	ws, err := a.channelWorkspace(msg.ChannelID)
	if err != nil {
		a.fail(msg, "workspace_unavailable", err.Error())
		return
	}
	full, err := resolvePath(ws, p.Path)
	if err != nil {
		a.fail(msg, "path_invalid", err.Error())
		return
	}

	data, err := os.ReadFile(full)
	if err != nil {
		a.fail(msg, "file_not_found", err.Error())
		return
	}
	content := string(data)

	count := strings.Count(content, p.OldString)
	if count == 0 {
		a.fail(msg, "old_string_not_found",
			"old_string not found in file; re-read the file and retry with exact text")
		return
	}
	if count > 1 && !p.ReplaceAll {
		a.fail(msg, "old_string_not_unique",
			fmt.Sprintf("old_string occurs %d times; add surrounding context to make it unique, or set replace_all", count))
		return
	}

	replacements := 1
	if p.ReplaceAll {
		content = strings.ReplaceAll(content, p.OldString, p.NewString)
		replacements = count
	} else {
		content = strings.Replace(content, p.OldString, p.NewString, 1)
	}

	if err := os.WriteFile(full, []byte(content), 0o644); err != nil {
		a.fail(msg, "write_failed", err.Error())
		return
	}
	a.respond(msg, FileEditResult{OK: true, Replacements: replacements})
}
