package daemonbus

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"

	"github.com/wanpengxie/ActOS/kernel/daemonbus"
	"github.com/wanpengxie/ActOS/kernel/placement"
	"github.com/wanpengxie/ActOS/kernel/viewsync"
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

	mu      sync.Mutex
	pending map[daemonbus.FrameID]chan daemonbus.Frame // frame_id → ack waiter

	closeOnce sync.Once
	closed    chan struct{}
}

// NewConnection wires a transport.
func NewConnection(daemonID placement.DaemonID, epoch daemonbus.ConnectionEpoch, tx Transport) *Connection {
	return &Connection{
		DaemonID:        daemonID,
		ConnectionEpoch: epoch,
		transport:       tx,
		pending:         map[daemonbus.FrameID]chan daemonbus.Frame{},
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
	return frame.FrameID.String(), nil
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
	if c.pending == nil {
		c.mu.Unlock()
		return daemonbus.Frame{}, errors.New("daemonbus: connection closed before send")
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
	if c.pending == nil {
		c.mu.Unlock()
		return false
	}
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
		FrameID:               daemonbus.FrameID(newFrameID()),
		FrameKind:             ft,
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
