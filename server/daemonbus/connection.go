package daemonbus

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"

	"github.com/coagent-ai/coagent/kernel/daemonbus"
	"github.com/coagent-ai/coagent/kernel/placement"
	"github.com/coagent-ai/coagent/kernel/viewsync"
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

	transport Transport

	mu      sync.Mutex
	pending map[string]chan daemonbus.Frame // frame_id → ack waiter

	closeOnce sync.Once
	closed    chan struct{}
}

// NewConnection wires a transport.
func NewConnection(daemonID placement.DaemonID, epoch daemonbus.ConnectionEpoch, tx Transport) *Connection {
	return &Connection{
		DaemonID:        daemonID,
		ConnectionEpoch: epoch,
		transport:       tx,
		pending:         map[string]chan daemonbus.Frame{},
		closed:          make(chan struct{}),
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
	frame, err := buildFrame(ft, c.DaemonID, c.ConnectionEpoch, payload)
	if err != nil {
		return "", err
	}
	if err := c.transport.WriteFrame(ctx, frame); err != nil {
		return "", fmt.Errorf("daemonbus: write %s: %w", ft, err)
	}
	return frame.FrameID, nil
}

// SendAndAwait writes a frame and blocks until an ACK with matching
// frame_id arrives or ctx is cancelled.
func (c *Connection) SendAndAwait(ctx context.Context, ft daemonbus.FrameType, payload any) (daemonbus.Frame, error) {
	frame, err := buildFrame(ft, c.DaemonID, c.ConnectionEpoch, payload)
	if err != nil {
		return daemonbus.Frame{}, err
	}
	ch := make(chan daemonbus.Frame, 1)
	c.mu.Lock()
	c.pending[frame.FrameID] = ch
	c.mu.Unlock()

	defer func() {
		c.mu.Lock()
		delete(c.pending, frame.FrameID)
		c.mu.Unlock()
	}()

	if err := c.transport.WriteFrame(ctx, frame); err != nil {
		return daemonbus.Frame{}, fmt.Errorf("daemonbus: write %s: %w", ft, err)
	}

	select {
	case ack, ok := <-ch:
		if !ok {
			return daemonbus.Frame{}, errors.New("daemonbus: connection closed before ack")
		}
		return ack, nil
	case <-ctx.Done():
		return daemonbus.Frame{}, ctx.Err()
	case <-c.closed:
		return daemonbus.Frame{}, errors.New("daemonbus: connection closed before ack")
	}
}

// matchAck delivers an incoming frame to a SendAndAwait waiter, if
// one is registered for the frame_id.
func (c *Connection) matchAck(frame daemonbus.Frame) bool {
	c.mu.Lock()
	ch, ok := c.pending[frame.FrameID]
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
func buildFrame(ft daemonbus.FrameType, daemonID placement.DaemonID, epoch daemonbus.ConnectionEpoch, payload any) (daemonbus.Frame, error) {
	raw, err := json.Marshal(payload)
	if err != nil {
		return daemonbus.Frame{}, fmt.Errorf("daemonbus: marshal %s: %w", ft, err)
	}
	return daemonbus.Frame{
		FrameID:               newFrameID(),
		FrameType:             ft,
		DaemonID:              string(daemonID),
		DaemonConnectionEpoch: epoch,
		SentAt:                nowMs(),
		Payload:               raw,
	}, nil
}

// DecodeViewsyncPush turns a daemonbus.Frame into kernel/viewsync.PushFrame.
func DecodeViewsyncPush(frame daemonbus.Frame) (viewsync.PushFrame, error) {
	var out viewsync.PushFrame
	if frame.FrameType != daemonbus.FrameTypeViewsyncPush {
		return out, fmt.Errorf("daemonbus: not push frame: %s", frame.FrameType)
	}
	if err := json.Unmarshal(frame.Payload, &out); err != nil {
		return out, fmt.Errorf("daemonbus: unmarshal push: %w", err)
	}
	return out, nil
}

// DecodeCreateAck turns a daemonbus.Frame into placement.CreateChannelAck.
func DecodeCreateAck(frame daemonbus.Frame) (placement.CreateChannelAck, error) {
	var out placement.CreateChannelAck
	if frame.FrameType != daemonbus.FrameTypeControlCreateChannelAck {
		return out, fmt.Errorf("daemonbus: not create_channel_ack frame: %s", frame.FrameType)
	}
	if err := json.Unmarshal(frame.Payload, &out); err != nil {
		return out, fmt.Errorf("daemonbus: unmarshal ack: %w", err)
	}
	if out.FrameID == "" {
		out.FrameID = frame.FrameID
	}
	return out, nil
}

// DecodeReclaim parses control.daemon_reclaim payloads.
func DecodeReclaim(frame daemonbus.Frame) (placement.ReclaimRequest, error) {
	var out placement.ReclaimRequest
	if frame.FrameType != daemonbus.FrameTypeControlDaemonReclaim {
		return out, fmt.Errorf("daemonbus: not daemon_reclaim frame: %s", frame.FrameType)
	}
	if err := json.Unmarshal(frame.Payload, &out); err != nil {
		return out, fmt.Errorf("daemonbus: unmarshal reclaim: %w", err)
	}
	return out, nil
}
