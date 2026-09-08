package api

type ReadRequest struct {
	Path   string `json:"path"`
	Offset int    `json:"offset,omitempty"`
	Limit  int    `json:"limit,omitempty"`
}

type WriteRequest struct {
	Path    string `json:"path"`
	Content string `json:"content"`
}

type Edit struct {
	OldText string `json:"oldText"`
	NewText string `json:"newText"`
}

type EditRequest struct {
	Path  string `json:"path"`
	Edits []Edit `json:"edits"`
}

type BashRequest struct {
	Command string  `json:"command"`
	Timeout float64 `json:"timeout,omitempty"`
}

type GrepRequest struct {
	Pattern    string `json:"pattern"`
	Path       string `json:"path,omitempty"`
	Glob       string `json:"glob,omitempty"`
	IgnoreCase bool   `json:"ignoreCase,omitempty"`
	Literal    bool   `json:"literal,omitempty"`
	Context    int    `json:"context,omitempty"`
	Limit      int    `json:"limit,omitempty"`
}

type FindRequest struct {
	Pattern string `json:"pattern"`
	Path    string `json:"path,omitempty"`
	Limit   int    `json:"limit,omitempty"`
}

type LSRequest struct {
	Path  string `json:"path,omitempty"`
	Limit int    `json:"limit,omitempty"`
}

const (
	TypeRead       = "workspace.read"
	TypeWrite      = "workspace.write"
	TypeEdit       = "workspace.edit"
	TypeBash       = "workspace.bash"
	TypeGrep       = "workspace.grep"
	TypeFind       = "workspace.find"
	TypeLS         = "workspace.ls"
	TypePowerShell = "workspace.powershell"
)

const (
	ReadInputSchema       = `{"type":"object","required":["path"],"properties":{"path":{"type":"string"},"offset":{"type":"integer","minimum":1},"limit":{"type":"integer","minimum":1}},"additionalProperties":false}`
	WriteInputSchema      = `{"type":"object","required":["path","content"],"properties":{"path":{"type":"string"},"content":{"type":"string"}},"additionalProperties":false}`
	EditInputSchema       = `{"type":"object","required":["path","edits"],"properties":{"path":{"type":"string"},"edits":{"type":"array","minItems":1,"items":{"type":"object","required":["oldText","newText"],"properties":{"oldText":{"type":"string"},"newText":{"type":"string"}},"additionalProperties":false}}},"additionalProperties":false}`
	BashInputSchema       = `{"type":"object","required":["command"],"properties":{"command":{"type":"string"},"timeout":{"type":"number","exclusiveMinimum":0}},"additionalProperties":false}`
	GrepInputSchema       = `{"type":"object","required":["pattern"],"properties":{"pattern":{"type":"string"},"path":{"type":"string"},"glob":{"type":"string"},"ignoreCase":{"type":"boolean"},"literal":{"type":"boolean"},"context":{"type":"integer","minimum":0},"limit":{"type":"integer","minimum":1}},"additionalProperties":false}`
	FindInputSchema       = `{"type":"object","required":["pattern"],"properties":{"pattern":{"type":"string"},"path":{"type":"string"},"limit":{"type":"integer","minimum":1}},"additionalProperties":false}`
	LSInputSchema         = `{"type":"object","properties":{"path":{"type":"string"},"limit":{"type":"integer","minimum":1}},"additionalProperties":false}`
	PowerShellInputSchema = BashInputSchema
)
