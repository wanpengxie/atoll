// Package devicetransit declares the device_transit payload contract shared
// by daemonbus control frames, adapter modules and runtime transit code.
package devicetransit

import (
	"context"
	"encoding/json"

	"github.com/wanpengxie/ActOS/kernel/channel"
	"github.com/wanpengxie/ActOS/kernel/message"
)

// DeviceSessionID is the server-allocated identifier for one device session.
type DeviceSessionID string

// String returns the wire form.
func (d DeviceSessionID) String() string { return string(d) }

// TransitDirection is the closed set of `direction` field values on SendFrame.
type TransitDirection string

const (
	DirectionToDevice   TransitDirection = "to_device"
	DirectionFromDevice TransitDirection = "from_device"
)

// SendFrame is the `device_transit.send`/`device_transit.recv` payload.
type SendFrame struct {
	ChannelID       channel.ID        `json:"channel_id"`
	DeviceSessionID DeviceSessionID   `json:"device_session_id"`
	Direction       TransitDirection  `json:"direction"`
	RequestID       string            `json:"request_id"`
	ParentID        string            `json:"parent_id,omitempty"`
	CorrelationID   string            `json:"correlation_id,omitempty"`
	Payload         json.RawMessage   `json:"payload"`
	EnvelopePartial *message.Envelope `json:"envelope_partial,omitempty"`
	ExpiresAt       int64             `json:"expires_at,omitempty"`
}

// AckFrame is the `device_transit.ack` payload.
type AckFrame struct {
	FrameID         string          `json:"frame_id"`
	DeviceSessionID DeviceSessionID `json:"device_session_id"`
	ChannelID       channel.ID      `json:"channel_id"`
	OK              bool            `json:"ok"`
}

// ErrorFrame is the `device_transit.error` payload.
type ErrorFrame struct {
	RequestID       string          `json:"request_id"`
	DeviceSessionID DeviceSessionID `json:"device_session_id"`
	ChannelID       channel.ID      `json:"channel_id"`
	Code            string          `json:"code"`
	Message         string          `json:"message"`
}

// DeviceTransit is the kernel-level seam the via_server_transit binding uses
// to hand adapter-generated frames to the daemonbus/devicebus bridge.
type DeviceTransit interface {
	Send(ctx context.Context, frame SendFrame) (frameID string, err error)
	Ack(ctx context.Context, frame AckFrame) error
	Error(ctx context.Context, frame ErrorFrame) error
}
