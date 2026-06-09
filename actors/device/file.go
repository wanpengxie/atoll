package device

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/wanpengxie/ActOS/protocol/message"
)

// handleFileRead implements device.file.read. Offset/Limit slice by line;
// without them the whole file is returned subject to MaxReadBytes.
func (a *Actor) handleFileRead(ctx context.Context, env *message.Envelope) error {
	var p FileReadPayload
	if err := json.Unmarshal(env.Payload, &p); err != nil {
		return a.fail(ctx, env, "payload_invalid", fmt.Sprintf("decode payload: %v", err))
	}
	ws, err := a.channelWorkspace(env.ChannelID)
	if err != nil {
		return a.fail(ctx, env, "workspace_unavailable", err.Error())
	}
	full, err := resolvePath(ws, p.Path)
	if err != nil {
		return a.fail(ctx, env, "path_invalid", err.Error())
	}

	info, err := os.Stat(full)
	if err != nil {
		return a.fail(ctx, env, "file_not_found", err.Error())
	}
	if info.IsDir() {
		return a.fail(ctx, env, "path_invalid", "path is a directory; list it via device.exec (ls)")
	}
	if p.Offset == 0 && p.Limit == 0 && info.Size() > MaxReadBytes {
		return a.fail(ctx, env, "file_too_large",
			fmt.Sprintf("file is %d bytes (cap %d); read it in slices with offset/limit", info.Size(), MaxReadBytes))
	}

	data, err := os.ReadFile(full)
	if err != nil {
		return a.fail(ctx, env, "read_failed", err.Error())
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

	return a.respond(ctx, env, FileReadResult{
		Content:   content,
		Size:      info.Size(),
		Truncated: truncated,
	})
}

// handleFileWrite implements device.file.write: whole-file write, parent
// directories created as needed.
func (a *Actor) handleFileWrite(ctx context.Context, env *message.Envelope) error {
	var p FileWritePayload
	if err := json.Unmarshal(env.Payload, &p); err != nil {
		return a.fail(ctx, env, "payload_invalid", fmt.Sprintf("decode payload: %v", err))
	}
	ws, err := a.channelWorkspace(env.ChannelID)
	if err != nil {
		return a.fail(ctx, env, "workspace_unavailable", err.Error())
	}
	full, err := resolvePath(ws, p.Path)
	if err != nil {
		return a.fail(ctx, env, "path_invalid", err.Error())
	}
	if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
		return a.fail(ctx, env, "write_failed", fmt.Sprintf("create parent dir: %v", err))
	}
	if err := os.WriteFile(full, []byte(p.Content), 0o644); err != nil {
		return a.fail(ctx, env, "write_failed", err.Error())
	}
	return a.respond(ctx, env, FileWriteResult{OK: true, Bytes: len(p.Content)})
}

// handleFileEdit implements device.file.edit: exact string replacement.
// Without replace_all, old_string must occur exactly once.
func (a *Actor) handleFileEdit(ctx context.Context, env *message.Envelope) error {
	var p FileEditPayload
	if err := json.Unmarshal(env.Payload, &p); err != nil {
		return a.fail(ctx, env, "payload_invalid", fmt.Sprintf("decode payload: %v", err))
	}
	if p.OldString == "" {
		return a.fail(ctx, env, "payload_invalid", "device.file.edit: old_string required")
	}
	if p.OldString == p.NewString {
		return a.fail(ctx, env, "payload_invalid", "device.file.edit: old_string and new_string are identical")
	}
	ws, err := a.channelWorkspace(env.ChannelID)
	if err != nil {
		return a.fail(ctx, env, "workspace_unavailable", err.Error())
	}
	full, err := resolvePath(ws, p.Path)
	if err != nil {
		return a.fail(ctx, env, "path_invalid", err.Error())
	}

	data, err := os.ReadFile(full)
	if err != nil {
		return a.fail(ctx, env, "file_not_found", err.Error())
	}
	content := string(data)

	count := strings.Count(content, p.OldString)
	if count == 0 {
		return a.fail(ctx, env, "old_string_not_found",
			"old_string not found in file; re-read the file and retry with exact text")
	}
	if count > 1 && !p.ReplaceAll {
		return a.fail(ctx, env, "old_string_not_unique",
			fmt.Sprintf("old_string occurs %d times; add surrounding context to make it unique, or set replace_all", count))
	}

	replacements := 1
	if p.ReplaceAll {
		content = strings.ReplaceAll(content, p.OldString, p.NewString)
		replacements = count
	} else {
		content = strings.Replace(content, p.OldString, p.NewString, 1)
	}

	if err := os.WriteFile(full, []byte(content), 0o644); err != nil {
		return a.fail(ctx, env, "write_failed", err.Error())
	}
	return a.respond(ctx, env, FileEditResult{OK: true, Replacements: replacements})
}
