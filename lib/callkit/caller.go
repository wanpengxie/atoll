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

// IPCWriter is the minimal interface needed to write envelopes.
type IPCWriter interface {
	WriteEnvelope(ctx context.Context, env message.Envelope) error
}

// Submit registers the future BEFORE writing the envelope
// (subscribe-before-send). Ack/result formatting is the binding layer's job
// (lib/metatool) — this collector only owns the mechanics.
func (c *Client) Submit(
	ctx context.Context,
	ipc IPCWriter,
	env message.Envelope,
	expectsAwait bool,
) error {
	// subscribe-before-send: register first.
	c.Futures.Register(env.ID, expectsAwait)
	if err := ipc.WriteEnvelope(ctx, env); err != nil {
		c.Futures.Cancel(env.ID)
		return err
	}
	return nil
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
