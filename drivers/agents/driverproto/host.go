package driverproto

import (
	"context"
	"encoding/json"
	"log/slog"
)

type EventSink interface {
	Publish(DriverEvent) bool
}

type WorkerHost interface {
	GenerationLife() context.Context
	Events() EventSink
	Logger() *slog.Logger
	Tools() ToolPort
	Resources() ResourcePort
}

type ProviderToolCallID string

type ToolSpec struct {
	Name        string
	Description string
	Schema      json.RawMessage
}

type ToolInvocation struct {
	CallID ProviderToolCallID
	Name   string
	Params json.RawMessage
}

type ToolResult struct {
	Text    string
	IsError bool
}

type ToolPort interface {
	Catalog() []ToolSpec
	Invoke(context.Context, WorkerTurnTarget, ToolInvocation) ToolResult
}

type ResourceInvocation struct {
	CallID     ProviderToolCallID
	Operation  string
	ResourceID string
	Payload    json.RawMessage
}

type ResourceResult struct {
	Payload json.RawMessage
	Error   string
}

type ResourcePort interface {
	Invoke(context.Context, WorkerTurnTarget, ResourceInvocation) ResourceResult
}
