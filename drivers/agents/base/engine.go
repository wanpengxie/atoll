package base

import (
	"context"

	"github.com/wanpengxie/atoll/lib/introspect"
	"github.com/wanpengxie/atoll/protocol/message"
)

// OpID identifies one base-to-provider command and all of its asynchronous
// callbacks. It is incarnation-local and deliberately says nothing about a
// provider connection generation.
type OpID string

type TurnStatus string

const (
	TurnStatusOK          TurnStatus = "ok"
	TurnStatusFailed      TurnStatus = "failed"
	TurnStatusInterrupted TurnStatus = "interrupted"
)

type ControlVerdict string

const (
	ControlAccepted     ControlVerdict = "accepted"
	ControlNotSteerable ControlVerdict = "notSteerable"
	ControlNoActiveTurn ControlVerdict = "noActiveTurn"
	ControlMismatch     ControlVerdict = "mismatch"
	ControlEmptyInput   ControlVerdict = "emptyInput"
	ControlRPCError     ControlVerdict = "rpcError"
)

type LostCause string

const (
	LostCrash   LostCause = "crash"
	LostTimeout LostCause = "timeout"
)

// Trigger is one accepted live request. Context cancellation remains attached
// to the original Msg in base; providers receive only its immutable envelope.
type Trigger struct {
	Envelope      message.Envelope
	CorrelationID message.ID
	Index         int
}

// ContextItem is a recent, non-actionable channel-log row rendered before the
// first live batch of an incarnation.
type ContextItem struct {
	Seq      int64
	Sender   string
	Kind     string
	Type     string
	Payload  []byte
	Rendered string
}

// Engine is the frozen asynchronous provider contract. Business and transport
// outcomes are reported through EventPort; returned errors are programmer-state
// errors only (not booted/closed).
//
// Terminate MUST NOT block: it retires the current provider generation and
// returns immediately, with physical reaping as the engine's async internals.
// This is what lets the base arbiter loop execute terminate inline — the
// action lands in the same instant it is decided, so no stale kill can reach
// a generation spawned after the decision.
type Engine interface {
	Boot(ctx context.Context, port BootPort) error
	StartTurn(op OpID, batch []Trigger, background []ContextItem) error
	Steer(op OpID, item Trigger) error
	Interrupt(op OpID) error
	Terminate() error
	EnsureAlive(op OpID) error
	Describe() introspect.Describe
	Close() error
}

type EventPort interface {
	TurnStarted(op OpID, turnID string)
	TurnRejected(op OpID, code, detail string)
	Tool(turnID, callID string, phase, name, status, detail string)
	TurnEnded(turnID string, status TurnStatus, finalText string, errInfo string)
	ControlDone(op OpID, verdict ControlVerdict, turnID, detail string)
	ProviderLost(cause LostCause, detail string)
	Persist(key string, value []byte)
}

type BootPort interface {
	Persist(key string, value []byte)
}
