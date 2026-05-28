// Package devicetransit declares the device_transit payload contract shared
// by daemonbus control frames, adapter modules and runtime transit code.
package devicetransit

import (
	"context"
	"encoding/json"
	"fmt"

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
	AckDelivered         AckResult = "delivered"
	AckDropped           AckResult = "dropped"
	AckAccepted          AckResult = "accepted"
	AckRejectedPermanent AckResult = "rejected_permanent"
	AckRejectedRetryable AckResult = "rejected_retryable"
)

// AckFrame is the `device_transit.ack` payload.
type AckFrame struct {
	CorrelationFrameID FrameID   `json:"correlation_frame_id"`
	Result             AckResult `json:"result"`
	Reason             string    `json:"reason,omitempty"`
	Detail             string    `json:"detail,omitempty"`
}

// AckError lets semantic receivers return a device_transit ack disposition
// without losing retryability/permanence across callback routing layers.
type AckError struct {
	Result AckResult
	Reason string
	Detail string
	Err    error
}

func (e *AckError) Error() string {
	if e == nil {
		return ""
	}
	if e.Detail != "" {
		return fmt.Sprintf("%s: %s", e.Reason, e.Detail)
	}
	if e.Err != nil {
		return e.Err.Error()
	}
	return e.Reason
}

func (e *AckError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

func NewAckError(result AckResult, reason, detail string, err error) *AckError {
	if reason == "" {
		reason = string(result)
	}
	if detail == "" && err != nil {
		detail = err.Error()
	}
	return &AckError{Result: result, Reason: reason, Detail: detail, Err: err}
}

// DeviceTransit is the kernel-level seam the runtime_inbound_via_relay binding uses
// to hand adapter-generated frames to the daemonbus/devicebus bridge.
type DeviceTransit interface {
	Send(ctx context.Context, frame SendFrame) (frameID FrameID, err error)
	Ack(ctx context.Context, frame AckFrame) error
}

// LifecycleEvent enumerates the device runtime lifecycle signals the
// server pushes back to the daemon-side adapter so the adapter can
// project its own device-state without polling transport plumbing.
// Spec ref: impl-layer2 §5 device session routing (post-t167 actor-token
// model — devicebus connection register / unregister maps to these
// events). The semantics are inbound only (server → daemon → adapter);
// adapter never emits this enum back through the transit path.
type LifecycleEvent string

const (
	// LifecycleConnected — devicebus ws upgraded + actor route registered.
	LifecycleConnected LifecycleEvent = "connected"
	// LifecycleDisconnected — devicebus ws read loop ended (clean close or
	// transport error). Server detects, pushes to daemon.
	LifecycleDisconnected LifecycleEvent = "disconnected"
	// LifecycleTokenExpired — server-side ValidateToken refused a frame
	// because the actor token is past expires_at. Adapter MUST treat the
	// device as unreachable until re-bind.
	LifecycleTokenExpired LifecycleEvent = "token_expired"
)

// LifecycleFrame is the payload carried by daemonbus
// `device_transit.lifecycle` frames. Server emits one per ws connection
// register / unregister / token expiry; daemon dispatches to the adapter
// module that owns (channel_id, adapter_actor_id).
type LifecycleFrame struct {
	AdapterActorID actor.ActorID  `json:"adapter_actor_id"`
	ChannelID      channel.ID     `json:"channel_id"`
	Event          LifecycleEvent `json:"event"`
	// DeviceID is the user-facing device identifier the server registered
	// the token against (informative; adapters that distinguish devices
	// internally read it from the actor-token row).
	DeviceID string `json:"device_id,omitempty"`
	// Ts is server emit time (ms epoch). Adapters use it to order events
	// and reject stale lifecycle frames (e.g. an out-of-order "connected"
	// arriving after a later "disconnected").
	Ts int64 `json:"ts,omitempty"`
	// Detail is an optional human-readable reason; not part of the closed
	// set, used for observability only.
	Detail string `json:"detail,omitempty"`
}
