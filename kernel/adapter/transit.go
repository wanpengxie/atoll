package adapter

import (
	"context"

	"github.com/wanpengxie/ActOS/kernel/channel"
)

// DeviceSessionID is the server-allocated identifier for one device
// session (L4 §2.6.2 — server.devicebus has single-ownership of the
// id; daemon adapter holds a mirror copy). String form is the uuid the
// server signed.
type DeviceSessionID string

// String returns the wire form.
func (d DeviceSessionID) String() string { return string(d) }

// TransitDirection is the closed set of `direction` field values on
// device_transit frames (L4 §2.6.4).
type TransitDirection string

const (
	DirectionToDevice   TransitDirection = "to_device"
	DirectionFromDevice TransitDirection = "from_device"
)

// SendFrame is the `device_transit.send` payload (L4 §2.6.4 row 1) +
// `device_transit.recv` payload (row 2; same fields, direction flips).
//
// daemon adapter constructs SendFrame in Handle, hands it to
// DeviceTransit.Send; the daemonbus client wraps it inside a
// daemonbus.Frame and ships to server. The reverse direction
// (device_transit.recv) is decoded inside the daemonbus client and
// handed to the framework which routes to Module.OnExternalCallback.
type SendFrame struct {
	ChannelID       channel.ID       `json:"channel_id"`
	DeviceSessionID DeviceSessionID  `json:"device_session_id"`
	Direction       TransitDirection `json:"direction"`
	RequestID       string           `json:"request_id,omitempty"` // envelope.id for to_device; original request id for from_device responses
	ParentID        string           `json:"parent_id,omitempty"`  // populated when this is a response back to a previous request
	CorrelationID   string           `json:"correlation_id,omitempty"`
	Payload         []byte           `json:"payload"`              // device-protocol-layer schema (caller provides; opaque to kernel)
	ExpiresAt       int64            `json:"expires_at,omitempty"` // command / token validity ms epoch (0 = no expiry)
}

// AckFrame is the `device_transit.ack` payload (L4 §2.6.4 row 3). It
// pairs back to a previously sent device_transit frame by FrameID.
type AckFrame struct {
	FrameID string `json:"frame_id"`
	Status  string `json:"status"` // "ok" | "failed"
	Error   string `json:"error,omitempty"`
}

// ErrorFrame is the `device_transit.error` payload (L4 §2.6.4 row 4).
// It can refer to a previously sent frame (FrameID) and / or to the
// request envelope (RequestID).
type ErrorFrame struct {
	FrameID   string `json:"frame_id,omitempty"`
	RequestID string `json:"request_id,omitempty"`
	ErrorCode string `json:"error_code"` // closed: device_offline | device_expired | frame_expired | payload_too_large | channel_unavailable | ...
	Reason    string `json:"reason"`
}

// DeviceTransit is the kernel-level seam the via_server_transit binding
// uses to send device frames into the daemonbus mux (L1 §11.7
// via_server_transit + L4 §2.6.4 frame field set).
//
// The interface is declared here so kernel/adapter Module declarations
// can name it; runtime/transit (T3) composes the implementation that
// actually writes to the daemonbus WS. Tests can hand in a recording
// stub to assert frame shape without spinning up daemonbus.
//
// Covers codex warning #15 — adapter framework holds the DeviceTransit
// interface, not the runtime implementation, so binding the wrong
// transport is a compile-time error (or test-time substitution).
type DeviceTransit interface {
	// Send writes a `device_transit.send` frame inside the daemonbus
	// mux. Returns the frame_id assigned by the transit layer so the
	// adapter can pair the eventual ack / recv.
	Send(ctx context.Context, frame SendFrame) (frameID string, err error)

	// Ack writes a `device_transit.ack` frame — used by adapters that
	// receive an inbound recv frame and need to acknowledge it.
	Ack(ctx context.Context, frame AckFrame) error

	// Error writes a `device_transit.error` frame — used by adapters
	// that detect an invalid inbound recv (payload schema violation,
	// session expired, etc.) before the daemon-side terminal_failure
	// path engages.
	Error(ctx context.Context, frame ErrorFrame) error
}
