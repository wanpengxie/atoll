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

const (
	TypeRead  = "workspace.read"
	TypeWrite = "workspace.write"
	TypeEdit  = "workspace.edit"
	TypeBash  = "workspace.bash"
)

const (
	ReadInputSchema  = `{"type":"object","required":["path"],"properties":{"path":{"type":"string"},"offset":{"type":"integer","minimum":1},"limit":{"type":"integer","minimum":1}},"additionalProperties":false}`
	WriteInputSchema = `{"type":"object","required":["path","content"],"properties":{"path":{"type":"string"},"content":{"type":"string"}},"additionalProperties":false}`
	EditInputSchema  = `{"type":"object","required":["path","edits"],"properties":{"path":{"type":"string"},"edits":{"type":"array","minItems":1,"items":{"type":"object","required":["oldText","newText"],"properties":{"oldText":{"type":"string"},"newText":{"type":"string"}},"additionalProperties":false}}},"additionalProperties":false}`
	BashInputSchema  = `{"type":"object","required":["command"],"properties":{"command":{"type":"string"},"timeout":{"type":"number","exclusiveMinimum":0}},"additionalProperties":false}`
)
