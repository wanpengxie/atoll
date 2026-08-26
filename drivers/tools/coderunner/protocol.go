package coderunner

import (
	"encoding/json"
	"strings"
)

// The host↔runtime link is MCP over stdio (JSON-RPC 2.0, newline-delimited):
// the Go actor is the MCP SERVER, the program's runtime is the MCP CLIENT. The
// declared actors' words are the server's tools, so any MCP client — the
// embedded Node runner, a Python runtime, an inspector — reaches the channel
// the same way. Process ownership is reversed from the usual MCP stdio setup
// (the server spawns the client), but the wire is symmetric so nothing on it
// knows. Three host-owned tools carry what MCP has no word for: the run's
// context, its return value, and its failure. Everything else is the spec.
const (
	mcpProtocolVersion = "2025-06-18"

	toolContext = "atoll_context"
	toolReturn  = "atoll_return"
	toolFail    = "atoll_fail"

	metaTarget   = "atoll/target"
	metaWord     = "atoll/word"
	metaDeadline = "atoll/deadline_ms"
	// argInput wraps a non-object word input: MCP tools/call arguments must be
	// an object, atoll word inputs may be any JSON value.
	argInput = "$input"
)

type rpcMessage struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id,omitempty"`
	Method  string          `json:"method,omitempty"`
	Params  json.RawMessage `json:"params,omitempty"`
	Result  json.RawMessage `json:"result,omitempty"`
	Error   *rpcError       `json:"error,omitempty"`
}

func (m rpcMessage) isRequest() bool      { return m.Method != "" && len(m.ID) > 0 }
func (m rpcMessage) isNotification() bool { return m.Method != "" && len(m.ID) == 0 }
func (m rpcMessage) isResponse() bool     { return m.Method == "" && len(m.ID) > 0 }

type rpcError struct {
	Code    int             `json:"code"`
	Message string          `json:"message"`
	Data    json.RawMessage `json:"data,omitempty"`
}

const (
	rpcParseError     = -32700
	rpcInvalidRequest = -32600
	rpcMethodNotFound = -32601
	rpcInvalidParams  = -32602
	rpcInternalError  = -32603
)

type initializeResult struct {
	ProtocolVersion string          `json:"protocolVersion"`
	Capabilities    map[string]any  `json:"capabilities"`
	ServerInfo      map[string]any  `json:"serverInfo"`
	Instructions    string          `json:"instructions,omitempty"`
	Meta            json.RawMessage `json:"_meta,omitempty"`
}

type toolSpec struct {
	Name         string          `json:"name"`
	Title        string          `json:"title,omitempty"`
	Description  string          `json:"description,omitempty"`
	InputSchema  json.RawMessage `json:"inputSchema"`
	OutputSchema json.RawMessage `json:"outputSchema,omitempty"`
	Meta         map[string]any  `json:"_meta,omitempty"`
}

type toolListResult struct {
	Tools []toolSpec `json:"tools"`
}

type toolCallParams struct {
	Name      string                     `json:"name"`
	Arguments json.RawMessage            `json:"arguments,omitempty"`
	Meta      map[string]json.RawMessage `json:"_meta,omitempty"`
}

type contentBlock struct {
	Type string `json:"type"`
	Text string `json:"text"`
}

type toolCallResult struct {
	Content           []contentBlock  `json:"content"`
	StructuredContent json.RawMessage `json:"structuredContent,omitempty"`
	IsError           bool            `json:"isError,omitempty"`
}

// loggingMessage is notifications/message (MCP logging). level info/debug →
// stdout, warning and above → stderr; logger "atoll" → the "log" stream.
type loggingMessage struct {
	Level  string          `json:"level"`
	Logger string          `json:"logger,omitempty"`
	Data   json.RawMessage `json:"data"`
}

// progressNotification is notifications/progress. progressToken is the run's
// request id (the only long-lived request on this session); message carries
// the atoll provisional status and value the provisional's payload.
type progressNotification struct {
	ProgressToken json.RawMessage `json:"progressToken"`
	Progress      float64         `json:"progress"`
	Message       string          `json:"message,omitempty"`
	Value         json.RawMessage `json:"value,omitempty"`
}

type cancelledNotification struct {
	RequestID json.RawMessage `json:"requestId"`
	Reason    string          `json:"reason,omitempty"`
}

// --- run-level shapes (unchanged by the transport) -------------------------

type logEntry struct {
	Stream string `json:"stream"`
	Text   string `json:"text"`
}

type runResult struct {
	Value json.RawMessage `json:"value"`
	Logs  []logEntry      `json:"logs"`
}

type runtimeFailure struct {
	Kind    string     `json:"kind"`
	Message string     `json:"message"`
	Stack   string     `json:"stack,omitempty"`
	Logs    []logEntry `json:"logs"`
}

type dependencyFailure struct {
	Missing []string `json:"missing"`
}

// toolName is the MCP tool name for one (requirement, word) pair. MCP leaves
// tool names free-form, but model-facing clients restrict them to
// [A-Za-z0-9_-], so the name is sanitized and the exact pair rides in _meta.
func toolName(requirement, word string) string {
	return sanitizeToolName(requirement + "__" + word)
}

func sanitizeToolName(value string) string {
	var b strings.Builder
	b.Grow(len(value))
	for _, r := range value {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '_', r == '-':
			b.WriteRune(r)
		default:
			b.WriteByte('_')
		}
	}
	return b.String()
}
