package device

import (
	"encoding/json"

	"github.com/wanpengxie/atoll/lib/introspect"
)

// wordSpecs documents the four request words for actor.describe.
var wordSpecs = map[string]introspect.WordSpec{
	TypeExec: {
		Description:  "Run a bash command inside the channel workspace.",
		InputSchema:  json.RawMessage(`{"type":"object","required":["command"],"properties":{"command":{"type":"string","description":"Bash command line."},"cwd":{"type":"string","description":"Workspace-relative directory; empty selects the root."},"timeout_ms":{"type":"integer","description":"Execution bound; default 120000, cap 600000."}}}`),
		OutputSchema: json.RawMessage(`{"type":"object","required":["exit_code","stdout","stderr","duration_ms"],"properties":{"exit_code":{"type":"integer"},"stdout":{"type":"string"},"stderr":{"type":"string"},"duration_ms":{"type":"integer"},"truncated":{"type":"boolean"}}}`),
		ErrorCodes:   []string{"exec_timeout", "exec_spawn_failed", "path_invalid"},
		Examples:     []json.RawMessage{json.RawMessage(`{"command":"grep -rn TODO ."}`)},
	},
	TypeFileRead: {
		Description:  "Read a workspace file, optionally a line slice.",
		InputSchema:  json.RawMessage(`{"type":"object","required":["path"],"properties":{"path":{"type":"string"},"offset":{"type":"integer","minimum":0},"limit":{"type":"integer","minimum":0}}}`),
		OutputSchema: json.RawMessage(`{"type":"object","required":["content","size"],"properties":{"content":{"type":"string"},"size":{"type":"integer"},"truncated":{"type":"boolean"}}}`),
		ErrorCodes:   []string{"file_not_found", "file_too_large", "path_invalid", "invalid_args"},
	},
	TypeFileWrite: {
		Description:  "Create or fully replace a workspace file (parent dirs auto-created).",
		InputSchema:  json.RawMessage(`{"type":"object","required":["path","content"],"properties":{"path":{"type":"string"},"content":{"type":"string"}}}`),
		OutputSchema: json.RawMessage(`{"type":"object","required":["ok","bytes"],"properties":{"ok":{"type":"boolean"},"bytes":{"type":"integer"}}}`),
		ErrorCodes:   []string{"path_invalid", "write_failed"},
	},
	TypeFileEdit: {
		Description:  "Exact string replacement in a workspace file.",
		InputSchema:  json.RawMessage(`{"type":"object","required":["path","old_string","new_string"],"properties":{"path":{"type":"string"},"old_string":{"type":"string"},"new_string":{"type":"string"},"replace_all":{"type":"boolean"}}}`),
		OutputSchema: json.RawMessage(`{"type":"object","required":["ok","replacements"],"properties":{"ok":{"type":"boolean"},"replacements":{"type":"integer"}}}`),
		ErrorCodes:   []string{"old_string_not_found", "old_string_not_unique"},
	},
}

func manifest() introspect.Manifest {
	return introspect.Manifest{
		Class: "device", Interfaces: []string{"actor"},
		Words: introspect.CloneWords(wordSpecs),
	}
}
