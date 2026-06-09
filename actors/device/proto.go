package device

// Closed set of envelope.type values the device actor handles.
const (
	TypeExec      = "device.exec"
	TypeFileRead  = "device.file.read"
	TypeFileWrite = "device.file.write"
	TypeFileEdit  = "device.file.edit"
)

// AllTypes is the canonical types slice the device actor exposes.
var AllTypes = []string{TypeExec, TypeFileRead, TypeFileWrite, TypeFileEdit}

// Execution limits. Defaults are product decisions for the v0 device tool;
// long-running work rides the caller-side call_actor async machinery, so the
// hard cap only bounds a single exec, not a workflow.
const (
	// DefaultExecTimeoutMs applies when the caller omits timeout_ms.
	DefaultExecTimeoutMs int64 = 120_000
	// MaxExecTimeoutMs is the hard cap on a single exec.
	MaxExecTimeoutMs int64 = 600_000
	// MaxStreamBytes caps each captured stream (stdout / stderr); beyond it
	// the head is kept and the result is flagged truncated.
	MaxStreamBytes = 64_000
	// MaxReadBytes caps a single file.read without offset/limit; larger files
	// must be read in line slices.
	MaxReadBytes = 256_000
)

// ExecPayload is the request payload for device.exec.
type ExecPayload struct {
	// Command is the bash command line to run.
	Command string `json:"command"`
	// Cwd is the working directory, relative to the channel workspace.
	// Empty = the channel workspace root.
	Cwd string `json:"cwd,omitempty"`
	// TimeoutMs bounds the execution; 0 = DefaultExecTimeoutMs.
	TimeoutMs int64 `json:"timeout_ms,omitempty"`
}

// ExecResult is the response payload for device.exec. A non-zero exit code is
// a COMPLETED result (the caller reads stderr and reacts); only a timeout or
// a spawn failure produces a failed terminal.
type ExecResult struct {
	ExitCode   int    `json:"exit_code"`
	Stdout     string `json:"stdout"`
	Stderr     string `json:"stderr"`
	DurationMs int64  `json:"duration_ms"`
	Truncated  bool   `json:"truncated,omitempty"`
}

// FileReadPayload is the request payload for device.file.read.
type FileReadPayload struct {
	// Path is relative to the channel workspace.
	Path string `json:"path"`
	// Offset is the 0-based line to start from (with Limit: slice reads).
	Offset int `json:"offset,omitempty"`
	// Limit is the max number of lines returned; 0 = whole file (subject to
	// MaxReadBytes).
	Limit int `json:"limit,omitempty"`
}

// FileReadResult is the response payload for device.file.read.
type FileReadResult struct {
	Content   string `json:"content"`
	Size      int64  `json:"size"`
	Truncated bool   `json:"truncated,omitempty"`
}

// FileWritePayload is the request payload for device.file.write.
type FileWritePayload struct {
	Path    string `json:"path"`
	Content string `json:"content"`
}

// FileWriteResult is the response payload for device.file.write.
type FileWriteResult struct {
	OK    bool `json:"ok"`
	Bytes int  `json:"bytes"`
}

// FileEditPayload is the request payload for device.file.edit — exact string
// replacement. Without ReplaceAll, OldString must occur exactly once in the
// file; zero or multiple occurrences are errors. The uniqueness constraint is
// the whole source of this tool's reliability — no fuzzy matching.
type FileEditPayload struct {
	Path       string `json:"path"`
	OldString  string `json:"old_string"`
	NewString  string `json:"new_string"`
	ReplaceAll bool   `json:"replace_all,omitempty"`
}

// FileEditResult is the response payload for device.file.edit.
type FileEditResult struct {
	OK           bool `json:"ok"`
	Replacements int  `json:"replacements"`
}
