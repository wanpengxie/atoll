package worker

import (
	"context"
)

// Bridge is the worker's agent-loop interface. Production implementation
// wraps go-kimi: wire events → message.Envelope → IPCClient.WriteMessage.
//
// Tests inject a fake Bridge to drive specific IPC sequences. The
// production implementation (drives go-kimi Agent + maps the 24 wire
// types to v4 envelopes) lives in cmd/worker/main.go or a future
// adapters/llm/ package — keeping go-kimi out of runtime/worker keeps
// arch-lint happy without listing the vendor.
type Bridge interface {
	// Run takes one turn's worth of work using the IPC client to talk
	// to the daemon. The worker exits once Run returns.
	Run(ctx context.Context, client *IPCClient) error
}

// BridgeFunc adapts a function to the Bridge interface.
type BridgeFunc func(ctx context.Context, client *IPCClient) error

// Run implements Bridge.
func (f BridgeFunc) Run(ctx context.Context, client *IPCClient) error { return f(ctx, client) }
