// Package devicetransit declares the device_transit payload contract shared
// by daemonbus control frames, adapter modules and runtime transit code.
package devicetransit

import (
	"context"
	"encoding/json"

	"github.com/wanpengxie/ActOS/kernel/actor"
	"github.com/wanpengxie/ActOS/kernel/channel"
)

// FrameID is the device-transit frame identifier returned by daemonbus.
type FrameID string

// String returns the wire form.
func (f FrameID) String() string { return string(f) }

// SendFrame is the canonical payload carried by both `device_transit.send`
// (impl-layer2 §5.3.1 inbound — device → adapter) and
// `device_transit.recv` (§5.3.2 outbound — adapter → device). Body is
// intentionally opaque to L2; adapter/framework-specific request IDs,
// payloads and deadlines live inside it.
type SendFrame struct {
	AdapterActorID actor.ActorID   `json:"adapter_actor_id"`
	ChannelID      channel.ID      `json:"channel_id"`
	Body           json.RawMessage `json:"body"`
	TransitSeq     int64           `json:"transit_seq,omitempty"`
}

// AckResult is the canonical transport-level device_transit.ack result.
type AckResult string

const (
	AckDelivered AckResult = "delivered"
	AckDropped   AckResult = "dropped"
)

// AckFrame is the `device_transit.ack` payload.
type AckFrame struct {
	CorrelationFrameID FrameID   `json:"correlation_frame_id"`
	Result             AckResult `json:"result"`
	Reason             string    `json:"reason,omitempty"`
	Detail             string    `json:"detail,omitempty"`
}

// DeviceTransit is the kernel-level seam the runtime_inbound_via_relay binding uses
// to hand adapter-generated frames to the daemonbus/devicebus bridge.
type DeviceTransit interface {
	Send(ctx context.Context, frame SendFrame) (frameID FrameID, err error)
	Ack(ctx context.Context, frame AckFrame) error
}
