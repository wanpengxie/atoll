package driverproto

import (
	"context"
	"encoding/json"
)

type DriverMessage struct {
	SourceID string
	Type     string
	Sender   string
	Payload  json.RawMessage
	Text     string
}

type ContextMessage struct {
	Seq     int64
	Sender  string
	Kind    string
	Type    string
	Payload json.RawMessage
	Text    string
}

type AttemptToken uint64
type WorkerTurnRef string

type WorkerTurnTarget struct {
	Attempt AttemptToken
	Native  WorkerTurnRef
}

func (t WorkerTurnTarget) Valid() bool { return t.Attempt != 0 && t.Native != "" }

type StartRequest struct {
	Attempt    AttemptToken
	Life       context.Context
	Messages   []DriverMessage
	Background []ContextMessage
}
