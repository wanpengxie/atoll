package metatool

import (
	"encoding/json"
	"time"
)

// DefaultTimeout is the closure deadline the short, synchronous introspection
// tools (describe/list/system) declare on their own requests. A generic
// call_actor request that leaves Timeout unset declares NO deadline of its
// own: the engine stamps its sliding default (actorbase.DefaultTimeout, restarted
// by every progress), so a long agent turn that keeps reporting is never
// refused as late. The fast-path Await window is derived from whatever
// deadline is in force (min(FastPathWindow, deadline)), never a separate knob.
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
	// Timeout is the closure deadline (author#2's ExpiresAt), a sliding window
	// restarted by each progress. Zero leaves it to the engine's default.
	// It is not a wait-window knob — the fast-path Await window is always
	// min(FastPathWindow, Timeout).
	Timeout  time.Duration
	WaitMode WaitMode
}
