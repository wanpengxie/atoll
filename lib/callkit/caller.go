package callkit

import (
	"context"
	"time"

	"github.com/wanpengxie/ActOS/protocol/message"
)

// Client is the client-edge call collector backing the LLM tool-loop
// fast-path: subscribe-before-send futures plus a bounded blocking Await.
// It is NOT an actor primitive and never belongs in lib/behavior — anything
// that can block-await is by axiom not an actor (cell serial contract;
// mailbox is the sole ingress). The actor-side call face is
// behavior.BuildRequest + behavior.Caller (author#2). This collector exists
// for the current sync-wrap agent loop and dissolves with the first-class
// async refactor.
type Client struct {
	Futures *RequestCorrelator
}

// NewClient returns a ready-to-use client-edge collector.
func NewClient() *Client {
	return &Client{Futures: NewRequestCorrelator()}
}

// SubmitResult is the submit outcome: the request id plus the immediate ack.
type SubmitResult struct {
	RequestID message.ID
	Ack       AckDescriptor
}

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

// IPCWriter is the minimal interface needed to write envelopes.
type IPCWriter interface {
	WriteEnvelope(ctx context.Context, env message.Envelope) error
}

// Submit registers the future BEFORE writing the envelope
// (subscribe-before-send). Returns the request id + ack once the write
// is accepted.
func (c *Client) Submit(
	ctx context.Context,
	ipc IPCWriter,
	env message.Envelope,
	estWaitMs int64,
	expectsAwait bool,
) (SubmitResult, error) {
	// subscribe-before-send: register first.
	c.Futures.Register(env.ID, expectsAwait)
	if err := ipc.WriteEnvelope(ctx, env); err != nil {
		c.Futures.Cancel(env.ID)
		return SubmitResult{}, err
	}
	return SubmitResult{
		RequestID: env.ID,
		Ack: AckDescriptor{
			RequestID: env.ID,
			Accepted:  true,
			Status:    "accepted",
			EstWaitMs: estWaitMs,
		},
	}, nil
}

// Await blocks until the final for id arrives, the window elapses, or ctx is done.
func (c *Client) Await(ctx context.Context, id message.ID, window time.Duration) (*message.Envelope, bool, error) {
	return c.Futures.Await(ctx, id, window)
}

// Abandon drops the local waiter for id.
func (c *Client) Abandon(id message.ID) {
	c.Futures.Cancel(id)
}

// Pending returns the in-flight request ids.
func (c *Client) Pending() []message.ID {
	return c.Futures.Pending()
}

// Deliver feeds one inbound response envelope into the correlator.
func (c *Client) Deliver(env *message.Envelope) Disposition {
	return c.Futures.Deliver(env)
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
