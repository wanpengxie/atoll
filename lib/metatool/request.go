package metatool

import (
	"encoding/json"
	"time"
)

// DefaultTimeout is the closure deadline used when a RequestSpec leaves
// Timeout unset. The fast-path Await window is derived from whatever deadline
// is in force (min(FastPathWindow, deadline)), never a separate knob.
const (
	DefaultTimeout     = 30 * time.Second
	ToolCallBudget     = 120 * time.Second
	MaxSynchronousWait = ToolCallBudget - 5*time.Second
)

// WaitMode selects the caller-side wait policy for one channel request.
type WaitMode int

const (
	// WaitFastPath is the default: Submit + Await(window~15s).
	WaitFastPath WaitMode = iota
	// WaitUnbounded is call_actor(wait=true): Await to the request deadline.
	WaitUnbounded
	// WaitNone is call_actor(wait=false): window 0, immediate ack.
	WaitNone
)

// RequestSpec is the bag of fields a single call_actor invocation needs
// to emit + wait.
type RequestSpec struct {
	ToolName       string
	EnvelopeType   string
	HandlerActorID string
	Payload        json.RawMessage
	// Timeout is the closure deadline (author#2's ExpiresAt). Zero uses
	// DefaultTimeout. It is not a wait-window knob — the fast-path Await window
	// is always min(FastPathWindow, Timeout).
	Timeout  time.Duration
	WaitMode WaitMode
}
