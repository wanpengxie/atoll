package coderunner

import "encoding/json"

type startFrame struct {
	Op        string            `json:"op"`
	Program   string            `json:"program"`
	Args      json.RawMessage   `json:"args"`
	Actors    map[string]string `json:"actors"`
	Self      string            `json:"self"`
	Channel   string            `json:"channel"`
	RequestID string            `json:"request_id"`
}

type nodeFrame struct {
	Op         string          `json:"op"`
	ID         int64           `json:"id,omitempty"`
	Target     string          `json:"target,omitempty"`
	Type       string          `json:"type,omitempty"`
	Input      json.RawMessage `json:"input,omitempty"`
	DeadlineMS int64           `json:"deadline_ms,omitempty"`
	Stream     string          `json:"stream,omitempty"`
	Text       string          `json:"text,omitempty"`
	Status     string          `json:"status,omitempty"`
	Value      json.RawMessage `json:"value,omitempty"`
	Kind       string          `json:"kind,omitempty"`
	Message    string          `json:"message,omitempty"`
	Stack      string          `json:"stack,omitempty"`
}

type answerFrame struct {
	Op      string          `json:"op"`
	ID      int64           `json:"id"`
	OK      bool            `json:"ok"`
	Payload json.RawMessage `json:"payload,omitempty"`
	Error   *callError      `json:"error,omitempty"`
}

type callError struct {
	Code   string `json:"code"`
	Detail string `json:"detail,omitempty"`
}

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
