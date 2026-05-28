package daemonbus

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/wanpengxie/ActOS/kernel/daemonbus"
	"github.com/wanpengxie/ActOS/kernel/devicetransit"
	"github.com/wanpengxie/ActOS/kernel/placement"
	"github.com/wanpengxie/ActOS/kernel/viewsync"
	"github.com/wanpengxie/ActOS/pkg/metrics"
	"github.com/wanpengxie/ActOS/pkg/requestctx"
)

const (
	// DefaultSendAndAwaitTimeout bounds server->daemon request/ack waits even
	// when the caller context has no useful deadline.
	DefaultSendAndAwaitTimeout = 30 * time.Second

	// DefaultPendingAwaitLimit caps one daemon connection's outstanding
	// SendAndAwait waiters.
	DefaultPendingAwaitLimit = 1024
)

var (
	ErrSendAndAwaitTimeout       = errors.New("daemonbus: send_and_await timeout")
	ErrPendingAwaitLimitExceeded = errors.New("daemonbus: pending await limit exceeded")
	ErrConnectionClosed          = errors.New("daemonbus: connection closed")
)

// Transport is the wire-level interface a daemonbus.Connection needs.
// gorilla/websocket implementation lives in ws_handler.go; tests
// pass an in-memory pipe.
type Transport interface {
	ReadFrame(ctx context.Context) (daemonbus.Frame, error)
	WriteFrame(ctx context.Context, frame daemonbus.Frame) error
	Close() error
}

// Connection represents one active daemonbus WS session — owns the
// transport, the connection_epoch and the pending-frame registry for
// frame_id ↔ ack pairing.
type Connection struct {
	DaemonID        placement.DaemonID
	ConnectionEpoch daemonbus.ConnectionEpoch
	Generation      uint64

	transport Transport
	log       *slog.Logger

	mu                sync.Mutex
	pending           map[daemonbus.FrameID]chan daemonbus.Frame // frame_id → ack waiter
	awaitTimeout      time.Duration
	pendingAwaitLimit int

	closeOnce sync.Once
	closed    chan struct{}
}

// NewConnection wires a transport.
func NewConnection(daemonID placement.DaemonID, epoch daemonbus.ConnectionEpoch, tx Transport) *Connection {
	return &Connection{
		DaemonID:          daemonID,
		ConnectionEpoch:   epoch,
		transport:         tx,
		pending:           map[daemonbus.FrameID]chan daemonbus.Frame{},
		awaitTimeout:      DefaultSendAndAwaitTimeout,
		pendingAwaitLimit: DefaultPendingAwaitLimit,
		closed:            make(chan struct{}),
	}
}

// IsClosed reports whether the connection has been closed.
func (c *Connection) IsClosed() bool {
	select {
	case <-c.closed:
		return true
	default:
		return false
	}
}

// SetSendAndAwaitOptions overrides timeout/capacity for tests or service
// config. Non-positive values restore production defaults.
func (c *Connection) SetSendAndAwaitOptions(timeout time.Duration, pendingLimit int) {
	if timeout <= 0 {
		timeout = DefaultSendAndAwaitTimeout
	}
	if pendingLimit <= 0 {
		pendingLimit = DefaultPendingAwaitLimit
	}
	c.mu.Lock()
	c.awaitTimeout = timeout
	c.pendingAwaitLimit = pendingLimit
	c.mu.Unlock()
}

// PendingAwaitCount returns the number of outstanding SendAndAwait waiters.
func (c *Connection) PendingAwaitCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.pending)
}

// Close shuts down the transport.
func (c *Connection) Close() error {
	c.closeOnce.Do(func() {
		close(c.closed)
		_ = c.transport.Close()
		c.mu.Lock()
		for _, ch := range c.pending {
			close(ch)
		}
		c.pending = nil
		c.mu.Unlock()
	})
	return nil
}

// SendFrame writes a frame on the transport. Returns the frame_id so
// callers can either fire-and-forget or wait via AwaitAck.
func (c *Connection) SendFrame(ctx context.Context, ft daemonbus.FrameType, payload any) (string, error) {
	frame, err := buildFrame(ctx, ft, c.DaemonID, c.ConnectionEpoch, payload)
	if err != nil {
		return "", err
	}
	if err := c.transport.WriteFrame(ctx, frame); err != nil {
		return "", fmt.Errorf("daemonbus: write %s: %w", ft, err)
	}
	return frame.FrameID.String(), nil
}

// SendAndAwait writes a frame and blocks until an ACK with matching
// frame_id arrives or ctx is cancelled.
func (c *Connection) SendAndAwait(ctx context.Context, ft daemonbus.FrameType, payload any) (daemonbus.Frame, error) {
	waitCtx, cancel := c.awaitContext(ctx)
	defer cancel()

	frame, err := buildFrame(waitCtx, ft, c.DaemonID, c.ConnectionEpoch, payload)
	if err != nil {
		return daemonbus.Frame{}, err
	}
	ch := make(chan daemonbus.Frame, 1)
	c.mu.Lock()
	if c.pending == nil {
		c.mu.Unlock()
		return daemonbus.Frame{}, fmt.Errorf("%w before send", ErrConnectionClosed)
	}
	if c.pendingAwaitLimit > 0 && len(c.pending) >= c.pendingAwaitLimit {
		c.mu.Unlock()
		metrics.Default().IncCounter("daemonbus.send_and_await.rejected",
			"reason", "pending_limit",
			"daemon_id", string(c.DaemonID),
			"frame_kind", string(ft))
		return daemonbus.Frame{}, ErrPendingAwaitLimitExceeded
	}
	c.pending[frame.FrameID] = ch
	c.mu.Unlock()

	defer func() {
		c.mu.Lock()
		if c.pending != nil {
			delete(c.pending, frame.FrameID)
		}
		c.mu.Unlock()
	}()

	if err := c.transport.WriteFrame(waitCtx, frame); err != nil {
		return daemonbus.Frame{}, fmt.Errorf("daemonbus: write %s: %w", ft, err)
	}

	select {
	case ack, ok := <-ch:
		if !ok {
			return daemonbus.Frame{}, fmt.Errorf("%w before ack", ErrConnectionClosed)
		}
		return ack, nil
	case <-waitCtx.Done():
		if ctx.Err() != nil {
			return daemonbus.Frame{}, ctx.Err()
		}
		metrics.Default().IncCounter("daemonbus.send_and_await.timeout",
			"daemon_id", string(c.DaemonID),
			"frame_kind", string(ft))
		return daemonbus.Frame{}, ErrSendAndAwaitTimeout
	case <-c.closed:
		return daemonbus.Frame{}, fmt.Errorf("%w before ack", ErrConnectionClosed)
	}
}

func (c *Connection) awaitContext(ctx context.Context) (context.Context, context.CancelFunc) {
	if ctx == nil {
		ctx = context.Background()
	}
	c.mu.Lock()
	timeout := c.awaitTimeout
	c.mu.Unlock()
	if timeout <= 0 {
		return ctx, func() {}
	}
	deadline := time.Now().Add(timeout)
	if existing, ok := ctx.Deadline(); ok && existing.Before(deadline) {
		return ctx, func() {}
	}
	return context.WithDeadline(ctx, deadline)
}

// matchAck delivers an incoming frame to a SendAndAwait waiter, if
// one is registered for the frame_id.
func (c *Connection) matchAck(frame daemonbus.Frame) bool {
	ackID := frame.FrameID
	if frame.FrameKind == daemonbus.FrameTypeDeviceTransitAck {
		var ack devicetransit.AckFrame
		if err := json.Unmarshal(frame.Payload, &ack); err == nil && ack.CorrelationFrameID != "" {
			ackID = daemonbus.FrameID(ack.CorrelationFrameID)
		}
	}
	c.mu.Lock()
	if c.pending == nil {
		c.mu.Unlock()
		return false
	}
	ch, ok := c.pending[ackID]
	c.mu.Unlock()
	if !ok {
		return false
	}
	select {
	case ch <- frame:
	default:
		// Drop — waiter already cleaned up.
	}
	return true
}

// buildFrame is the shared helper that wraps a payload into the
// daemonbus mux header (L2 §9.2).
func buildFrame(ctx context.Context, ft daemonbus.FrameType, daemonID placement.DaemonID, epoch daemonbus.ConnectionEpoch, payload any) (daemonbus.Frame, error) {
	raw, err := json.Marshal(payload)
	if err != nil {
		return daemonbus.Frame{}, fmt.Errorf("daemonbus: marshal %s: %w", ft, err)
	}
	return daemonbus.Frame{
		FrameID:               daemonbus.FrameID(newFrameID()),
		FrameKind:             ft,
		RequestID:             requestctx.RequestID(ctx),
		DaemonID:              daemonID,
		DaemonConnectionEpoch: epoch,
		Ts:                    nowMs(),
		Payload:               raw,
	}, nil
}

// DecodeViewsyncPush turns a daemonbus.Frame into kernel/viewsync.PushFrame.
func DecodeViewsyncPush(frame daemonbus.Frame) (viewsync.PushFrame, error) {
	var out viewsync.PushFrame
	if frame.FrameKind != daemonbus.FrameTypeViewsyncPush {
		return out, fmt.Errorf("daemonbus: not push frame: %s", frame.FrameKind)
	}
	if err := json.Unmarshal(frame.Payload, &out); err != nil {
		return out, fmt.Errorf("daemonbus: unmarshal push: %w", err)
	}
	return out, nil
}

// DecodeCreateAck turns a daemonbus.Frame into placement.CreateChannelAck.
func DecodeCreateAck(frame daemonbus.Frame) (placement.CreateChannelAck, error) {
	var out placement.CreateChannelAck
	if frame.FrameKind != daemonbus.FrameTypeControlCreateChannelAck {
		return out, fmt.Errorf("daemonbus: not create_channel_ack frame: %s", frame.FrameKind)
	}
	if err := json.Unmarshal(frame.Payload, &out); err != nil {
		return out, fmt.Errorf("daemonbus: unmarshal ack: %w", err)
	}
	if out.FrameID == "" {
		out.FrameID = frame.FrameID.String()
	}
	return out, nil
}

// DecodeRejectChannel turns a daemonbus.Frame into placement.RejectChannel.
func DecodeRejectChannel(frame daemonbus.Frame) (placement.RejectChannel, error) {
	var out placement.RejectChannel
	if frame.FrameKind != daemonbus.FrameTypeControlRejectChannel {
		return out, fmt.Errorf("daemonbus: not reject_channel frame: %s", frame.FrameKind)
	}
	if err := json.Unmarshal(frame.Payload, &out); err != nil {
		return out, fmt.Errorf("daemonbus: unmarshal reject_channel: %w", err)
	}
	if out.FrameID == "" {
		out.FrameID = frame.FrameID.String()
	}
	return out, nil
}

// DecodeHeldChannelsReport parses control.held_channels_report payloads.
func DecodeHeldChannelsReport(frame daemonbus.Frame) (placement.HeldChannelsReport, error) {
	var out placement.HeldChannelsReport
	if frame.FrameKind != daemonbus.FrameTypeControlHeldChannelsReport {
		return out, fmt.Errorf("daemonbus: not held_channels_report frame: %s", frame.FrameKind)
	}
	if err := json.Unmarshal(frame.Payload, &out); err != nil {
		return out, fmt.Errorf("daemonbus: unmarshal held_channels_report: %w", err)
	}
	return out, nil
}
