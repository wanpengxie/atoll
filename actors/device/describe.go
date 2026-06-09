package device

import (
	"context"
	"fmt"

	"github.com/wanpengxie/ActOS/lib/introspect"
	"github.com/wanpengxie/ActOS/protocol/message"
)

// actorDescription is the one-line actor positioning returned by
// actor.describe.
const actorDescription = "Generic device tool: run bash commands and read/write/edit files inside this channel's workspace on the daemon's machine. Everything beyond write/edit (grep, ls, find, git, builds) goes through device.exec."

// actorSkillDoc is the markdown usage guide returned by actor.describe.
const actorSkillDoc = "" +
	"# device\n" +
	"\n" +
	"The physical hands of this daemon's machine. All operations are confined " +
	"to the channel's workspace directory — paths are workspace-relative; " +
	"absolute paths and `..` escapes are rejected.\n" +
	"\n" +
	"## Tool surface\n" +
	"\n" +
	"- `device.exec` — run a bash command line. Non-zero exit codes come back " +
	"as completed results with stdout/stderr; read them and react. Use it for " +
	"everything read-only or long-tail: grep, ls, find, head, git, builds.\n" +
	"- `device.file.read` — read a file; slice big files with offset/limit (lines).\n" +
	"- `device.file.write` — create or fully replace a file.\n" +
	"- `device.file.edit` — exact string replacement. Without replace_all the " +
	"old_string must occur exactly once; on old_string_not_unique add more " +
	"surrounding context lines and retry.\n"

func requestMeta(meta introspect.TypeMeta, maxPendingMs int64) introspect.TypeMeta {
	meta.AllowedKinds = []string{string(message.KindRequest)}
	meta.MaxPendingMs = maxPendingMs
	return meta
}

// typeMetas documents the four request types for actor.describe.
var typeMetas = map[string]introspect.TypeMeta{
	TypeExec: requestMeta(introspect.TypeMeta{
		Description: "Run a bash command inside the channel workspace.",
		PayloadFields: []introspect.FieldDoc{
			{Name: "command", Required: true, Description: "Bash command line.", Example: "grep -rn TODO ."},
			{Name: "cwd", Description: "Working directory relative to the workspace; empty = workspace root."},
			{Name: "timeout_ms", Description: "Execution bound; default 120000, cap 600000."},
		},
		ErrorCodes: []introspect.ErrorDoc{
			{Code: "exec_timeout", Description: "Command exceeded timeout_ms.", Recovery: "Raise timeout_ms or split the work."},
			{Code: "exec_spawn_failed", Description: "Command could not be started."},
			{Code: "path_invalid", Description: "cwd is absolute or escapes the workspace."},
		},
		Notes: "Non-zero exit code is a completed result, not an error.",
	}, MaxExecTimeoutMs),
	TypeFileRead: requestMeta(introspect.TypeMeta{
		Description: "Read a workspace file, optionally a line slice.",
		PayloadFields: []introspect.FieldDoc{
			{Name: "path", Required: true, Description: "Workspace-relative file path."},
			{Name: "offset", Description: "0-based start line."},
			{Name: "limit", Description: "Max lines returned."},
		},
		ErrorCodes: []introspect.ErrorDoc{
			{Code: "file_not_found", Description: "No such file."},
			{Code: "file_too_large", Description: "File exceeds the whole-read cap.", Recovery: "Read in slices with offset/limit."},
			{Code: "path_invalid", Description: "Path is absolute, escapes the workspace, or is a directory."},
		},
	}, DefaultExecTimeoutMs),
	TypeFileWrite: requestMeta(introspect.TypeMeta{
		Description: "Create or fully replace a workspace file (parent dirs auto-created).",
		PayloadFields: []introspect.FieldDoc{
			{Name: "path", Required: true, Description: "Workspace-relative file path."},
			{Name: "content", Required: true, Description: "Full file content."},
		},
		ErrorCodes: []introspect.ErrorDoc{
			{Code: "path_invalid", Description: "Path is absolute or escapes the workspace."},
			{Code: "write_failed", Description: "Filesystem write error."},
		},
	}, DefaultExecTimeoutMs),
	TypeFileEdit: requestMeta(introspect.TypeMeta{
		Description: "Exact string replacement in a workspace file.",
		PayloadFields: []introspect.FieldDoc{
			{Name: "path", Required: true, Description: "Workspace-relative file path."},
			{Name: "old_string", Required: true, Description: "Exact text to replace; must be unique unless replace_all."},
			{Name: "new_string", Required: true, Description: "Replacement text."},
			{Name: "replace_all", Description: "Replace every occurrence."},
		},
		ErrorCodes: []introspect.ErrorDoc{
			{Code: "old_string_not_found", Description: "old_string not in file.", Recovery: "Re-read the file and retry with exact text."},
			{Code: "old_string_not_unique", Description: "old_string occurs more than once.", Recovery: "Add surrounding context lines or set replace_all."},
		},
	}, DefaultExecTimeoutMs),
}

// handleDescribe serves the actor.describe self-answer through the standard
// introspect dispatch: empty payload = full answer, {"type": ...} = single
// type, unknown type = failed terminal.
func (a *Actor) handleDescribe(ctx context.Context, env *message.Envelope) error {
	req, err := introspect.ParseDescribeRequest(env.Payload)
	if err != nil {
		return a.fail(ctx, env, "payload_invalid", fmt.Sprintf("decode describe payload: %v", err))
	}
	answer, ok := introspect.AnswerDescribe(introspect.Describe{
		ActorID:     string(a.actorID),
		Description: actorDescription,
		SkillDoc:    actorSkillDoc,
		Types:       typeMetas,
	}, req)
	if !ok {
		return a.fail(ctx, env, "type_unsupported", fmt.Sprintf("device actor does not handle %s", req.Type))
	}
	return a.respond(ctx, env, answer)
}
