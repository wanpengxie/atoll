package api

import "encoding/json"

const (
	TypeGenerate = "llm.generate"
	TypeModels   = "llm.models"
)

type ModelRef struct {
	Provider string `json:"provider,omitempty"`
	Model    string `json:"model,omitempty"`
}

type GenerateRequest struct {
	ModelRef
	SystemPrompt string            `json:"system_prompt,omitempty"`
	Messages     []json.RawMessage `json:"messages"`
	Tools        []json.RawMessage `json:"tools,omitempty"`
	Options      json.RawMessage   `json:"options,omitempty"`
	APIKey       string            `json:"api_key,omitempty"`
}

type GenerateResponse struct {
	Message  json.RawMessage   `json:"message"`
	Events   []json.RawMessage `json:"events,omitempty"`
	Provider string            `json:"provider"`
	Model    string            `json:"model"`
}

type ModelsRequest struct {
	Provider string `json:"provider,omitempty"`
}

type ModelsResponse struct {
	Models []json.RawMessage `json:"models"`
}
