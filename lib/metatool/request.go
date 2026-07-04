package metatool

import (
	"encoding/json"
	"time"
)

// DefaultTimeout is the closure deadline (RequestSpec.Timeout) used when a
// spec leaves Timeout unset and no ShellConfig.TimeoutResolver overrides it
// (P13). The fast-path Await window is DERIVED from whatever deadline is in
// force (min(FastPathWindow, deadline)), never a separate knob.
const DefaultTimeout = 30 * time.Second

// WaitMode selects the caller-side wait policy for one channel request.
type WaitMode int

const (
	// WaitFastPath is the default: Submit + Await(window~15s).
	WaitFastPath WaitMode = iota
	// WaitUnbounded is call_actor(wait=true): Await to the type timeout.
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
	// Timeout is the closure deadline (author#2's ExpiresAt), per request type.
	// Zero = let the Shell resolve it (ShellConfig.TimeoutResolver, else
	// DefaultTimeout). It is NOT a wait-window knob — the fast-path Await
	// window is always min(FastPathWindow, Timeout), derived, never per-type.
	Timeout  time.Duration
	WaitMode WaitMode
}
