package api

import "encoding/json"

// ExecutionSnapshot is supplied by the branch Holder for one workspace call.
// It is execution input, not pi-workspace state: every request carries a full
// copy so replacing a Bridge cannot change branch semantics.
type ExecutionSnapshot struct {
	CWD string            `json:"cwd"`
	Env map[string]string `json:"env,omitempty"`
}

// ExecuteRequest is the private Looper -> pi-workspace transport envelope.
// Input retains the word-specific model schema while Execution is injected by
// the Holder and is never model-authored.
type ExecuteRequest struct {
	Input     json.RawMessage   `json:"input"`
	Execution ExecutionSnapshot `json:"execution"`
}

const ExecuteInputSchema = `{"type":"object","required":["input","execution"],"properties":{"input":{"type":"object"},"execution":{"type":"object","required":["cwd"],"properties":{"cwd":{"type":"string","minLength":1},"env":{"type":"object","additionalProperties":{"type":"string"}}},"additionalProperties":false}},"additionalProperties":false}`

func TransportInputSchema(input string) json.RawMessage {
	var schema json.RawMessage = json.RawMessage(input)
	type properties struct {
		Input     json.RawMessage `json:"input"`
		Execution any             `json:"execution"`
	}
	type transport struct {
		Type                 string     `json:"type"`
		Required             []string   `json:"required"`
		Properties           properties `json:"properties"`
		AdditionalProperties bool       `json:"additionalProperties"`
	}
	raw, _ := json.Marshal(transport{
		Type: "object", Required: []string{"input", "execution"}, AdditionalProperties: false,
		Properties: properties{Input: schema, Execution: map[string]any{
			"type": "object", "required": []string{"cwd"}, "additionalProperties": false,
			"properties": map[string]any{"cwd": map[string]any{"type": "string", "minLength": 1}, "env": map[string]any{"type": "object", "additionalProperties": map[string]any{"type": "string"}}},
		}},
	})
	return raw
}

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

// ModelToolSpec is one fixed model-facing workspace port. Word is the private
// actor transport target; Schema describes only Input, never the injected
// ExecutionSnapshot.
type ModelToolSpec struct {
	Name, Word, Description, Schema string
}

// ModelToolSpecs is deliberately an ordered slice. The Looper serializes this
// catalog once at incarnation start; a Go map must never determine prompt
// order.
func ModelToolSpecs() []ModelToolSpec {
	return []ModelToolSpec{
		{Name: "read", Word: TypeRead, Description: "Read text or image content from the current branch workspace.", Schema: ReadInputSchema},
		{Name: "write", Word: TypeWrite, Description: "Create or replace a file in the current branch workspace.", Schema: WriteInputSchema},
		{Name: "edit", Word: TypeEdit, Description: "Replace uniquely matching text regions in a workspace file.", Schema: EditInputSchema},
		{Name: "bash", Word: TypeBash, Description: "Run a bash command with the current branch CWD and environment snapshot.", Schema: BashInputSchema},
		{Name: "grep", Word: TypeGrep, Description: "Search workspace file contents with ripgrep.", Schema: GrepInputSchema},
		{Name: "find", Word: TypeFind, Description: "Find workspace paths by glob pattern.", Schema: FindInputSchema},
		{Name: "ls", Word: TypeLS, Description: "List files and directories in the current branch workspace.", Schema: LSInputSchema},
	}
}
