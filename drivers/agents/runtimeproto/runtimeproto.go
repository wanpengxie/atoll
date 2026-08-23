// Package runtimeproto is the stateless Base-to-Runtime contract.
package runtimeproto

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"time"

	"github.com/wanpengxie/atoll/drivers/agents/driverproto"
	"github.com/wanpengxie/atoll/drivers/agents/effectcap"
	"github.com/wanpengxie/atoll/runtime/harness"
)

// Capability names cross the provider/runtime seam as manifest vocabulary.
// These string values mirror the provider-side spellings without creating the
// reverse driverproto-to-runtimeproto import edge.
const (
	CapabilitySteer     = "steer"
	CapabilityInterrupt = "interrupt"
	CapabilityResume    = "resume"
	CapabilityFork      = "fork"
)

type OpID uint64
type TurnID string
type TurnKind = driverproto.TurnKind

const (
	TurnChat    = driverproto.TurnChat
	TurnCompact = driverproto.TurnCompact
	TurnSelect  = driverproto.TurnSelect
	TurnNew     = driverproto.TurnNew
)

type TurnOptions struct {
	Model  string `json:"model"`
	Effort string `json:"effort"`
}

type Input struct {
	SourceID    string
	Type        string
	Sender      string
	Caller      harness.Caller
	Payload     json.RawMessage
	Text        string
	Attachments []Attachment
}

type Attachment struct {
	Address string `json:"address"`
	Name    string `json:"name,omitempty"`
}

type ContextItem struct {
	Seq     int64
	Sender  string
	Kind    string
	Type    string
	Payload json.RawMessage
	Text    string
}

type StartCommand struct {
	Op         OpID
	Messages   []Input
	Background []ContextItem
	Scope      effectcap.Scope
	Kind       TurnKind
	Options    TurnOptions
}

type ControlKind uint8

const (
	ControlSteer ControlKind = iota
	ControlInterrupt
)

type ControlCommand struct {
	Op      OpID
	Kind    ControlKind
	Target  TurnID
	Content *Input
	Scope   effectcap.Scope
}

var (
	ErrUnavailable = errors.New("agent runtime unavailable")
	ErrClosed      = errors.New("agent runtime closed")
)

type Runtime interface {
	Start(StartCommand) error
	Control(ControlCommand) error
	Terminate() error
	EnsureReady(OpID) error
	Close()
}

type TurnStatus string

const (
	TurnStatusOK          TurnStatus = "ok"
	TurnStatusFailed      TurnStatus = "failed"
	TurnStatusInterrupted TurnStatus = "interrupted"
)

type ControlVerdict string

const (
	ControlAccepted     ControlVerdict = "accepted"
	ControlRejected     ControlVerdict = "rejected"
	ControlNotSteerable ControlVerdict = "not_steerable"
	ControlTargetGone   ControlVerdict = "target_gone"
	ControlTimeout      ControlVerdict = "timeout"
)

type LostCause string

const (
	LostCrash    LostCause = "crash"
	LostTimeout  LostCause = "timeout"
	LostProtocol LostCause = "protocol"
)

type ReadyResult struct {
	Ready  bool
	Code   string
	Detail string
}

// ProgressEvent 是回合内一件已完成中间产物的截断摘要（provider 无关词表，
// 见 driverproto 的 Note* 常量）。展示与阶段推断全在前端，后端只负责发送。
type ProgressEvent struct {
	Kind string
	Text string
}

type ToolEvent struct {
	CallID string
	Phase  string
	Name   string
	Status string
	Detail string
	Input  json.RawMessage
	Output json.RawMessage
}

type TurnUsage struct {
	ContextTokens int64
	ContextWindow int64
	Model         string
	Effort        string
}

type Events interface {
	TurnStarted(OpID, TurnID)
	TurnRejected(OpID, string, string)
	Tool(TurnID, ToolEvent)
	// Progress 是回合内一件已完成中间产物的摘要。与 Tool 同为可丢弃的过程
	// 观测：拥塞时丢，恒不因它拉闸。
	Progress(TurnID, ProgressEvent)
	TurnEnded(TurnID, TurnStatus, string, string, TurnUsage)
	ControlDone(OpID, TurnID, ControlVerdict, string)
	ReadyDone(OpID, ReadyResult)
	ProviderLost(TurnID, LostCause, string)
	ResumeSeedUpdated([]byte)
	RuntimeFault(string, string)
}

// Bounds are immutable construction-time facts used by Base for its coarse
// cross-domain receipt and event-port capacity.
type Bounds struct {
	ReceiptDeadline time.Duration
	EventCapacity   int
}

type Spec struct {
	// Name is the provider's class name (claude/codex/script). Base projects it
	// as the instance manifest's Class — actor.describe must answer the real
	// class, not the generic "agent".
	Name             string
	Documentation    driverproto.Documentation
	Capabilities     map[string]bool
	Bounds           Bounds
	Selections       []TurnOptions
	DefaultSelection int
	// SelectionTitles parallel Selections (same index); display metadata only,
	// never part of options identity or persistence.
	SelectionTitles []SelectionTitle
}

// SelectionTitle is one selection's optional human names (empty = show the raw
// value). Declared here — not borrowed from driverproto — so Base consumes it
// without crossing the base→driverproto layering wall.
type SelectionTitle struct {
	Model  string
	Effort string
}

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

// Bridges are Base-owned capabilities. Runtime may only invoke them with a
// Scope that it already validated against its current turn.
type ToolBridge interface {
	Catalog() []ToolSpec
	Invoke(context.Context, effectcap.Scope, ToolInvocation) ToolResult
}

type ResourceBridge interface {
	Invoke(context.Context, effectcap.Scope, ResourceInvocation) ResourceResult
}

type Deps struct {
	Parent    context.Context
	Tools     ToolBridge
	Resources ResourceBridge
	Logger    *slog.Logger
}

type Factory func(Deps, []byte, TurnOptions, Events) (Runtime, error)

func CloneInput(v Input) Input {
	v.Payload = append(json.RawMessage(nil), v.Payload...)
	v.Attachments = append([]Attachment(nil), v.Attachments...)
	return v
}

func CloneContext(v ContextItem) ContextItem {
	v.Payload = append(json.RawMessage(nil), v.Payload...)
	return v
}
