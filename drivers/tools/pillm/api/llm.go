package api

import (
	"encoding/json"

	agentbase "github.com/wanpengxie/atoll/drivers/agents/base"
)

const (
	TypeGenerate = "llm.generate"
	TypeModels   = "llm.models"
	TypeCount    = "llm.count"
)

type ModelRef struct {
	Provider string `json:"provider,omitempty"`
	Model    string `json:"model,omitempty"`
}

type GenerateRequest struct {
	ModelRef
	RootSession  string               `json:"session,omitempty"`
	Purpose      string               `json:"purpose,omitempty"`
	SystemPrompt string               `json:"system_prompt,omitempty"`
	Context      agentbase.ContextRef `json:"context"`
	Messages     []json.RawMessage    `json:"-"` // populated only by old in-process test fixtures
	Tools        []json.RawMessage    `json:"tools,omitempty"`
	Options      json.RawMessage      `json:"options,omitempty"`
	APIKey       string               `json:"api_key,omitempty"`
}

type GenerateResponse struct {
	Attempts  int               `json:"attempts,omitempty"`
	Message   json.RawMessage   `json:"message"`
	Events    []json.RawMessage `json:"events,omitempty"`
	Provider  string            `json:"provider"`
	Model     string            `json:"model"`
	Usage     json.RawMessage   `json:"usage,omitempty"`
	ErrorCode string            `json:"error_code,omitempty"`
	Retryable bool              `json:"retryable,omitempty"`
}

type CountRequest struct {
	RootSession string               `json:"session,omitempty"`
	Context     agentbase.ContextRef `json:"context"`
}

type CountResponse struct {
	ContextTokens int `json:"context_tokens"`
}

type ModelsRequest struct {
	Provider string `json:"provider,omitempty"`
}

type ModelsResponse struct {
	Models []json.RawMessage `json:"models"`
}
