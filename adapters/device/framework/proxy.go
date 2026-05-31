package framework

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/wanpengxie/ActOS/kernel/actor"
	"github.com/wanpengxie/ActOS/kernel/adapter"
	"github.com/wanpengxie/ActOS/kernel/channel"
	"github.com/wanpengxie/ActOS/kernel/message"
)

// ErrDeviceRouteUnavailable is the proxy-level error every adapter
// translates to a closed-set terminal failure when DeviceTransit.Send
// reports the actor route cannot carry the frame right now. Adapters
// may preserve device-specific detail in payload.error_code; the proxy
// itself stays neutral.
var ErrDeviceRouteUnavailable = errors.New("framework.DeviceProxy: device actor route unavailable")

// DeviceProxyDeps bundles the kernel-level seams the proxy needs. Used
// by NewDeviceProxy to keep the constructor signature flat + by tests
// to swap fakes wholesale.
type DeviceProxyDeps struct {
	// Forward sends the already accepted request through the
	// framework-owned external transport path. Required.
	Forward adapter.ExternalRequestFunc

	// LookupPending is optional read-only lifecycle state for callback
	// decoders.
	LookupPending adapter.PendingRequestLookupFunc
}

// DeviceProxy translates one adapter-level request into the
// framework/devicetransit outbound frame body (carried on the wire as
// `device_transit.recv`, impl-layer2 §5.3.2). One instance per
// Module per channel; constructed inside Module.Init after the
// framework hands over a ModuleContext.
//
// The proxy intentionally does NOT call ctx.Respond. The Module owns
// the response phase — when an inbound `device_transit.send` arrives
// (§5.3.1), Module decodes the payload + calls Respond with a
// domain-specific terminal. Request lifecycle reserve/timer ownership
// remains in adapters/framework.Manager.Dispatch.
type DeviceProxy struct {
	// AdapterName is the adapter identifier (e.g. "xhs"). Echoed into
	// log / observability events.
	AdapterName string

	// AdapterActorID is the actor_registry row owning every emitted
	// response envelope. Daemon-side senders always equal this id; the
	// device is NOT an actor (L4 §2.6).
	AdapterActorID actor.ActorID

	// ChannelID identifies the channel the proxy services. Stamped onto
	// every SendFrame so the daemon-bus router can pair frame ↔ channel.
	ChannelID channel.ID

	deps DeviceProxyDeps

	// Clock + frame id injection (test seams).
	now        func() time.Time
	newFrameID func() string
}

// TransitDirection is the adapter-framework body-level direction set.
// It is deliberately not a daemonbus top-level field; L2 treats
// devicetransit.SendFrame.Body as opaque.
type TransitDirection string

const (
	DirectionToDevice   TransitDirection = "to_device"
	DirectionFromDevice TransitDirection = "from_device"
)

// DeviceTransitBody is the xhs/framework-internal body schema carried
// inside devicetransit.SendFrame.Body.
type DeviceTransitBody struct {
	Direction       TransitDirection  `json:"direction"`
	RequestID       message.ID        `json:"request_id"`
	ParentID        message.ID        `json:"parent_id,omitempty"`
	CorrelationID   message.ID        `json:"correlation_id,omitempty"`
	Payload         json.RawMessage   `json:"payload"`
	EnvelopePartial *message.Envelope `json:"envelope_partial,omitempty"`
	ExpiresAt       int64             `json:"expires_at,omitempty"`
}

// NewDeviceProxy validates deps + returns a ready proxy. The defaults
// for Now / NewFrameID are time.Now + uuid.NewString — tests inject
// deterministic versions to make frame_id assertions stable.
func NewDeviceProxy(
	adapterName string,
	adapterActorID actor.ActorID,
	channelID channel.ID,
	deps DeviceProxyDeps,
) (*DeviceProxy, error) {
	if adapterName == "" {
		return nil, errors.New("framework.NewDeviceProxy: adapterName is required")
	}
	if adapterActorID == "" {
		return nil, errors.New("framework.NewDeviceProxy: adapterActorID is required")
	}
	if channelID == "" {
		return nil, errors.New("framework.NewDeviceProxy: channelID is required")
	}
	if deps.Forward == nil {
		return nil, errors.New("framework.NewDeviceProxy: ForwardExternalRequest is required")
	}
	return &DeviceProxy{
		AdapterName:    adapterName,
		AdapterActorID: adapterActorID,
		ChannelID:      channelID,
		deps:           deps,
		now:            time.Now,
		newFrameID:     uuid.NewString,
	}, nil
}

// SetClock replaces the time source. Test-only seam; production code
// keeps the default time.Now.
func (p *DeviceProxy) SetClock(now func() time.Time) {
	if now != nil {
		p.now = now
	}
}

// SetFrameIDFactory replaces the uuid generator. Test-only seam.
func (p *DeviceProxy) SetFrameIDFactory(factory func() string) {
	if factory != nil {
		p.newFrameID = factory
	}
}

// SendRequest is the per-request entry point Modules call from Handle.
// It bundles every runtime_inbound_via_relay transport invariant:
//
//  1. Pre-flight envelope sanity — non-nil + kind=request.
//  2. Pass only the domain payload to ForwardExternalRequest.
//  3. ForwardExternalRequest stamps request identity/deadline and writes
//     the frame onto the daemonbus mux. The
//     concrete transit (T3 runtime/transit) owns persistence + retry;
//     the proxy treats Send as fire-and-forget once it returns nil.
//
// The wirePayload is whatever the adapter wants on the device side —
// the proxy treats it as opaque bytes. The adapter is responsible for
// serializing its protocol (e.g. xhs.Command JSON).
//
// On failure the caller emits a synchronous failed terminal through
// ModuleContext.Fail; pending/timer state is left for that write-first
// semantic path to close.
func (p *DeviceProxy) SendRequest(
	ctx context.Context,
	env *message.Envelope,
	wirePayload []byte,
) (frameID string, err error) {
	if env == nil {
		return "", errors.New("framework.DeviceProxy.SendRequest: envelope is nil")
	}
	if env.Kind != message.KindRequest {
		return "", fmt.Errorf("framework.DeviceProxy.SendRequest: envelope kind must be %q, got %q",
			message.KindRequest, env.Kind)
	}
	if env.ID == "" {
		return "", errors.New("framework.DeviceProxy.SendRequest: envelope id is required")
	}

	res, err := p.deps.Forward(ctx, env, adapter.ExternalRequestPayload(append([]byte(nil), wirePayload...)))
	if err != nil {
		return "", fmt.Errorf("framework.DeviceProxy.SendRequest: forward external request: %w", err)
	}
	return res.FrameID, nil
}

// LookupInFlight returns the correlation entry for a recv frame. Useful
// when the adapter has nothing but a request_id on the wire and needs
// the original envelope.type / audience to build the response payload.
func (p *DeviceProxy) LookupInFlight(ctx context.Context, requestID string) (adapter.CorrelationEntry, bool, error) {
	if p.deps.LookupPending == nil {
		return adapter.CorrelationEntry{}, false, nil
	}
	return p.deps.LookupPending(ctx, adapter.CorrelationKey(message.ID(requestID)))
}

// envelopeDeadline picks the deadline (wall-ms) for the F3 timer.
// Mirrors the M1.3 framePendingDeadline rule:
//
//   - If env.ExpiresAt is set + positive → use that.
//   - Otherwise fall back to env.TS + defaultPending (5 minutes).
//
// Centralised so per-type adapters don't drift on the fallback.
func envelopeDeadline(env *message.Envelope, enqueuedAt int64) int64 {
	if env.ExpiresAt != nil && *env.ExpiresAt > 0 {
		return *env.ExpiresAt
	}
	anchor := env.TS
	if anchor <= 0 {
		anchor = enqueuedAt
	}
	return anchor + defaultPendingMs
}

// defaultPendingMs is the framework-level fallback per-request budget
// when the envelope omits ExpiresAt. Long-running business operations
// must opt in with an explicit type/config override.
const defaultPendingMs int64 = 30 * 1000
