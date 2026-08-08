package base

import (
	"context"
	"encoding/json"
	"errors"
	"sync"

	"github.com/wanpengxie/atoll/lib/introspect"
)

type OpID string
type TurnID string

type TurnStatus string

const (
	TurnStatusOK          TurnStatus = "ok"
	TurnStatusFailed      TurnStatus = "failed"
	TurnStatusInterrupted TurnStatus = "interrupted"
)

type ControlVerdict string

const (
	ControlAccepted      ControlVerdict = "accepted"
	ControlNotSteerable  ControlVerdict = "notSteerable"
	ControlNoActiveTurn  ControlVerdict = "noActiveTurn"
	ControlMismatch      ControlVerdict = "mismatch"
	ControlEmptyInput    ControlVerdict = "emptyInput"
	ControlInputTooLarge ControlVerdict = "inputTooLarge"
	ControlRPCError      ControlVerdict = "rpcError"
)

type LostCause string

const (
	LostCrash   LostCause = "crash"
	LostTimeout LostCause = "timeout"
)

type ReadyResult struct {
	Ready  bool
	Detail string
}

type RuntimeInput struct {
	SourceID string
	Type     string
	Sender   string
	Payload  json.RawMessage
	Text     string
}

type RuntimeContextItem struct {
	Seq     int64
	Sender  string
	Kind    string
	Type    string
	Payload json.RawMessage
	Text    string
}

type TurnInput struct {
	Messages   []RuntimeInput
	Background []RuntimeContextItem
}

type StartCommand struct {
	Op    OpID
	Input TurnInput
	Scope EffectScope
}

type RuntimeControlKind uint8

const (
	RuntimeSteer RuntimeControlKind = iota
	RuntimeInterrupt
)

type ControlCommand struct {
	Op      OpID
	Kind    RuntimeControlKind
	Target  TurnID
	Content *RuntimeInput
	Scope   EffectScope
}

var (
	ErrRuntimeUnavailable = errors.New("agent runtime unavailable")
	ErrRuntimeClosed      = errors.New("agent runtime closed")
	ErrRuntimeState       = errors.New("invalid agent runtime command state")
)

type Runtime interface {
	Start(StartCommand) error
	Control(ControlCommand) error
	Terminate() error
	EnsureReady(OpID) error
	Close()
}

type ToolEvent struct {
	CallID string
	Phase  string
	Name   string
	Status string
	Detail string
}

type RuntimeEvents interface {
	TurnStarted(OpID, TurnID)
	TurnRejected(OpID, string, string)
	Tool(TurnID, ToolEvent)
	TurnEnded(TurnID, TurnStatus, string, string)
	ControlDone(OpID, TurnID, ControlVerdict, string)
	ReadyDone(OpID, ReadyResult)
	ProviderLost(TurnID, LostCause, string)
	ResumeSeedUpdated([]byte)
}

type RuntimeCapabilities struct {
	Steer     bool
	Interrupt bool
	Resume    bool
}

type RuntimeSpec struct {
	Describe     introspect.Describe
	Capabilities RuntimeCapabilities
}

type RuntimeDeps struct {
	Parent    context.Context
	Tools     ToolBridge
	Resources ResourceBridge
}

type NewRuntime func(RuntimeDeps, []byte, RuntimeEvents) (Runtime, error)

// EffectScope is an opaque, revocable admission capability minted by Base.
// Its contents are intentionally inaccessible outside this package.
type EffectScope struct{ gate *effectGate }

type effectGate struct {
	mu     sync.Mutex
	open   bool
	anchor string
	correl string
}

func NewEffectScope(anchor, correlation string) EffectScope {
	return EffectScope{gate: &effectGate{open: true, anchor: anchor, correl: correlation}}
}

func (s EffectScope) Revoke() {
	if s.gate == nil {
		return
	}
	s.gate.mu.Lock()
	s.gate.open = false
	s.gate.mu.Unlock()
}

type EffectLease struct{ value any }

func NewEffectLease(value any) EffectLease { return EffectLease{value: value} }

type ToolSpec struct {
	Name        string
	Description string
	Schema      json.RawMessage
}
type ToolInvocation struct {
	CallID string
	Name   string
	Params json.RawMessage
}
type ToolResult struct {
	Text    string
	IsError bool
}

type ToolBridge interface {
	Catalog() []ToolSpec
	Acquire(EffectScope) (EffectLease, bool)
	Invoke(context.Context, EffectLease, ToolInvocation) ToolResult
}

type ResourceInvocation struct {
	CallID     string
	Operation  string
	ResourceID string
	Payload    json.RawMessage
}
type ResourceResult struct {
	Payload json.RawMessage
	Error   string
}
type ResourceBridge interface {
	Acquire(EffectScope) (EffectLease, bool)
	Invoke(context.Context, EffectLease, ResourceInvocation) ResourceResult
}
