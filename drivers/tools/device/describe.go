package device

import "github.com/wanpengxie/atoll/lib/introspect"

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

// typeMetas documents the four request types for actor.describe.
var typeMetas = map[string]introspect.WordSpec{
	TypeExec: {
		Description: "Run a bash command inside the channel workspace.",
		PayloadFields: []introspect.FieldDoc{
			{Name: "command", Required: true, Description: "Bash command line.", Example: "grep -rn TODO ."},
			{Name: "cwd", Description: "Working directory relative to the workspace; empty = workspace root."},
			{Name: "timeout_ms", Description: "Execution bound; default 120000, cap 600000."},
		},
		ErrorCodes: []string{"exec_timeout", "exec_spawn_failed", "path_invalid"},
		Notes:      "Non-zero exit code is a completed result, not an error.",
	},
	TypeFileRead: {
		Description: "Read a workspace file, optionally a line slice.",
		PayloadFields: []introspect.FieldDoc{
			{Name: "path", Required: true, Description: "Workspace-relative file path."},
			{Name: "offset", Description: "0-based start line."},
			{Name: "limit", Description: "Max lines returned."},
		},
		ErrorCodes: []string{"file_not_found", "file_too_large", "path_invalid"},
	},
	TypeFileWrite: {
		Description: "Create or fully replace a workspace file (parent dirs auto-created).",
		PayloadFields: []introspect.FieldDoc{
			{Name: "path", Required: true, Description: "Workspace-relative file path."},
			{Name: "content", Required: true, Description: "Full file content."},
		},
		ErrorCodes: []string{"path_invalid", "write_failed"},
	},
	TypeFileEdit: {
		Description: "Exact string replacement in a workspace file.",
		PayloadFields: []introspect.FieldDoc{
			{Name: "path", Required: true, Description: "Workspace-relative file path."},
			{Name: "old_string", Required: true, Description: "Exact text to replace; must be unique unless replace_all."},
			{Name: "new_string", Required: true, Description: "Replacement text."},
			{Name: "replace_all", Description: "Replace every occurrence."},
		},
		ErrorCodes: []string{"old_string_not_found", "old_string_not_unique"},
	},
}

func manifest() introspect.Manifest {
	return introspect.Manifest{
		Class: "device", Interfaces: []string{"actor"},
		Words: introspect.CloneWords(typeMetas),
	}
}
