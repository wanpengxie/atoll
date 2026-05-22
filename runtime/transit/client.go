package transit

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/wanpengxie/ActOS/kernel/daemonbus"
	"github.com/wanpengxie/ActOS/pkg/requestctx"
)

// Transport is the daemonbus transport abstraction (WS in production, or
// an in-process mock in tests).
//
// Send delivers a frame to the server. The implementation MAY return
// immediately after queueing the frame to a write buffer (transit is
// at-least-once via outbox, so transient transport failures are absorbed
// by the outbox replay loop).
//
// Recv blocks until the next frame from the server arrives or the
// supplied context is cancelled. Implementations MUST surface
// connection-level errors (e.g. WS close) by returning a non-nil error
// after which Connect() should be called again with a fresh epoch.
//
// Connect establishes (or re-establishes) the connection. Returns the
// new daemon_connection_epoch the server assigned. Implementations are
// free to choose epoch numbering (per L2 §9.4 — monotonic per daemon
// is sufficient).
type Transport interface {
	Connect(ctx context.Context) (daemonbus.ConnectionEpoch, error)
	Send(ctx context.Context, frame daemonbus.Frame) error
	Recv(ctx context.Context) (daemonbus.Frame, error)
	Close() error
}

// Client wraps a Transport and tracks the current ConnectionEpoch.
// It is the single funnel for daemon → server frame emission.
type Client struct {
	transport Transport
	daemonID  string
	nowFn     func() int64

	mu    sync.Mutex
	epoch atomic.Int64 // current daemon_connection_epoch (post-Connect)
}

// ClientConfig wires a Client.
type ClientConfig struct {
	DaemonID  string
	Transport Transport
	NowFn     func() int64 // unix-ms; default = time.Now().UnixMilli
}

// NewClient builds a Client.
func NewClient(cfg ClientConfig) (*Client, error) {
	if cfg.DaemonID == "" {
		return nil, errors.New("transit: ClientConfig.DaemonID empty")
	}
	if cfg.Transport == nil {
		return nil, errors.New("transit: ClientConfig.Transport nil")
	}
	nowFn := cfg.NowFn
	if nowFn == nil {
		nowFn = func() int64 { return time.Now().UnixMilli() }
	}
	return &Client{
		transport: cfg.Transport,
		daemonID:  cfg.DaemonID,
		nowFn:     nowFn,
	}, nil
}

// Connect (re-)establishes the underlying transport and records the new
// daemon_connection_epoch.
func (c *Client) Connect(ctx context.Context) (daemonbus.ConnectionEpoch, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	epoch, err := c.transport.Connect(ctx)
	if err != nil {
		return 0, err
	}
	c.epoch.Store(int64(epoch))
	return epoch, nil
}

// Epoch returns the current connection epoch (0 before first Connect).
func (c *Client) Epoch() daemonbus.ConnectionEpoch {
	return daemonbus.ConnectionEpoch(c.epoch.Load())
}

// DaemonID returns the daemon id this client identifies as.
func (c *Client) DaemonID() string { return c.daemonID }

// Send wraps the payload in a daemonbus.Frame and pushes it through the
// transport. The caller provides FrameID + FrameType; the client fills
// daemon_id / daemon_connection_epoch / sent_at automatically.
func (c *Client) Send(ctx context.Context, frameID string, frameType daemonbus.FrameType, payload any) error {
	frame, err := Encode(frameID, frameType, c.daemonID, c.Epoch(), c.nowFn(), payload)
	if err != nil {
		return err
	}
	frame.RequestID = requestctx.RequestID(ctx)
	return c.transport.Send(ctx, frame)
}

// Recv blocks for the next incoming frame.
func (c *Client) Recv(ctx context.Context) (daemonbus.Frame, error) {
	return c.transport.Recv(ctx)
}

// Close shuts the underlying transport.
func (c *Client) Close() error { return c.transport.Close() }
