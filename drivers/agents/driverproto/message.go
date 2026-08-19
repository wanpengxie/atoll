package driverproto

import (
	"context"
	"encoding/json"

	"github.com/wanpengxie/atoll/runtime/harness"
)

type Attachment struct {
	Address string
}

type DriverMessage struct {
	SourceID    string
	Type        string
	Sender      string
	Caller      harness.Caller
	Payload     json.RawMessage
	Text        string
	Attachments []Attachment
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
type ActionToken uint64
type WorkerTurnRef string

type TurnKind uint8

const (
	TurnChat TurnKind = iota
	TurnCompact
	TurnSelect
)

type TurnOptions struct {
	Model  string `json:"model"`
	Effort string `json:"effort"`
}

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
	Kind       TurnKind
	Options    TurnOptions
}
