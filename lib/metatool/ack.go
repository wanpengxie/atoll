package metatool

import (
	"time"

	"github.com/wanpengxie/atoll/protocol/message"
)

// AckDescriptor is the immediate-ack shape handed back to the LLM when a
// call outlives the fast-path window (accepted / est wait / how to collect).
type AckDescriptor struct {
	RequestID  message.ID
	Accepted   bool
	Status     string // substrate-level, always "accepted" on the immediate ack
	EstWaitMs  int64  // source: the resolved closure deadline (spec.Timeout, ms)
	Guidance   string // framework template
	ToWait     ToWaitHint
	NotWaiting string
}

// ToWaitHint carries the tool + params for the "to_wait" field.
type ToWaitHint struct {
	Tool   string
	Params map[string]any
}

// newCollectHint builds the shared "collect this request later" pair — the
// to_wait descriptor and the if-not-waiting line — that every ack hands back
// (call_actor's ack in exec.go, await_result's still-pending ack in
// async_tools.go). The wording is parsed on the LLM side, so it is kept
// byte-for-byte identical across both call sites.
func newCollectHint(requestID string) (ToWaitHint, string) {
	toolName := AwaitResultSpec.Name
	return ToWaitHint{
			Tool:   toolName,
			Params: map[string]any{"request_id": requestID},
		},
		"result stays in the caller job table; claim it with " + toolName + "(request_id=" + requestID + ")"
}

// FastPathWindow is the default bounded-wait window for call_actor.
const FastPathWindow = 15 * time.Second

// ResolveFastPathWindow computes the Await window for a call given the
// wait mode and the type-level timeout.
func ResolveFastPathWindow(typeTimeout time.Duration, defaultTimeout time.Duration, waitUnbounded bool) time.Duration {
	if typeTimeout <= 0 {
		typeTimeout = defaultTimeout
	}
	if waitUnbounded {
		return typeTimeout
	}
	if FastPathWindow < typeTimeout {
		return FastPathWindow
	}
	return typeTimeout
}

// AckResult renders an AckDescriptor as a ResultValue.
func AckResult(toolName string, ack AckDescriptor) ResultValue {
	return ResultValue{
		Name: toolName,
		Value: map[string]any{
			"status":         ack.Status,
			"request_id":     ack.RequestID.String(),
			"accepted":       ack.Accepted,
			"est_wait_ms":    ack.EstWaitMs,
			"guidance":       ack.Guidance,
			"to_wait":        map[string]any{"tool": ack.ToWait.Tool, "params": ack.ToWait.Params},
			"if_not_waiting": ack.NotWaiting,
		},
	}
}

// ResultValue is a tiny engine-neutral carrier for caller.go.
type ResultValue struct {
	Name    string
	Value   map[string]any
	IsError bool
}
