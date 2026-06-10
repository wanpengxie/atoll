package metatool

import (
	"time"

	"github.com/wanpengxie/ActOS/protocol/message"
)

// AckDescriptor is the immediate-ack shape handed back to the LLM when a
// call outlives the fast-path window (accepted / est wait / how to collect).
type AckDescriptor struct {
	RequestID message.ID
	Accepted  bool
	Status    string // substrate-level, always "accepted" on the immediate ack
	EstWaitMs int64  // source: type.max_pending_ms (R5)
	Guidance  string // framework template
	ToWait    ToWaitHint
	NotWaitng string
}

// ToWaitHint carries the tool + params for the "to_wait" field.
type ToWaitHint struct {
	Tool   string
	Params map[string]any
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
			"if_not_waiting": ack.NotWaitng,
		},
	}
}

// ResultValue is a tiny carrier so caller.go does not import go-kimi
// types directly.
type ResultValue struct {
	Name    string
	Value   map[string]any
	IsError bool
}
