package framework

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/wanpengxie/ActOS/kernel/actor"
	"github.com/wanpengxie/ActOS/kernel/adapter"
	"github.com/wanpengxie/ActOS/kernel/channel"
	"github.com/wanpengxie/ActOS/kernel/devicetransit"
	"github.com/wanpengxie/ActOS/kernel/message"
)

// ErrDeviceSessionUnreachable is the proxy-level error every adapter
// translates to a closed-set terminal failure when DeviceTransit.Send
// reports the session cannot carry the frame right now. Adapters may
// preserve device-specific detail in payload.error_code; the proxy
// itself stays neutral.
var ErrDeviceSessionUnreachable = errors.New("framework.DeviceProxy: device session unreachable")

// DeviceProxyDeps bundles the kernel-level seams the proxy needs. Used
// by NewDeviceProxy to keep the constructor signature flat + by tests
// to swap fakes wholesale.
type DeviceProxyDeps struct {
	// Transit is the DeviceTransit kernel seam (kernel/devicetransit.DeviceTransit).
	// Required.
	Transit devicetransit.DeviceTransit

	// Correlation is the F2 tracker scoped to this adapter. Required.
	Correlation adapter.CorrelationTracker

	// Policy is the F3 timeout / retry emitter. Required — the proxy
	// arms a timer on every Send.
	Policy adapter.ErrorPolicy
}

// DeviceProxy translates one adapter-level request into the
// kernel/devicetransit outbound frame (carried on the wire as
// `device_transit.recv`, impl-layer2 §5.3.2) + the per-request
// bookkeeping (correlation reserve, F3 timer arm). One instance per
// Module per channel; constructed inside Module.Init after the
// framework hands over a ModuleContext.
//
// The proxy intentionally does NOT call ctx.Respond. The Module owns
// the response phase — when an inbound `device_transit.send` arrives
// (§5.3.1), Module decodes the payload + calls Respond with a
// domain-specific terminal. The proxy holds the "outbound +
// bookkeeping" half only.
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
	if deps.Transit == nil {
		return nil, errors.New("framework.NewDeviceProxy: DeviceTransit is required (T3 runtime/transit wires it)")
	}
	if deps.Correlation == nil {
		return nil, errors.New("framework.NewDeviceProxy: CorrelationTracker is required")
	}
	if deps.Policy == nil {
		return nil, errors.New("framework.NewDeviceProxy: ErrorPolicy is required")
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
// It bundles every runtime_inbound_via_relay invariant the daemon side owes
// the harness:
//
//  1. Pre-flight envelope sanity — non-nil + kind=request.
//  2. Correlation.Reserve so a callback that races the network can
//     still find the request id.
//  3. ErrorPolicy.RegisterTimer arms the F3 default timeout fallback;
//     fires `unanswered_timeout` if the device never responds.
//  4. DeviceTransit.Send writes the frame onto the daemonbus mux. The
//     concrete transit (T3 runtime/transit) owns persistence + retry;
//     the proxy treats Send as fire-and-forget once it returns nil.
//
// The wirePayload is whatever the adapter wants on the device side —
// the proxy treats it as opaque bytes. The adapter is responsible for
// serializing its protocol (e.g. xhs.Command JSON).
//
// On any failure path the proxy walks back: cancel the F3 timer if
// armed, mark the correlation expired if reserved, so the caller can
// emit a synchronous terminal without leaving the framework with a
// dangling pending entry.
func (p *DeviceProxy) SendRequest(
	ctx context.Context,
	env *message.Envelope,
	sessionID devicetransit.DeviceSessionID,
	wirePayload []byte,
) (frameID string, err error) {
	if env == nil {
		return "", errors.New("framework.DeviceProxy.SendRequest: envelope is nil")
	}
	if env.Kind != message.KindRequest {
		return "", fmt.Errorf("framework.DeviceProxy.SendRequest: envelope kind must be %q, got %q",
			message.KindRequest, env.Kind)
	}
	if sessionID == "" {
		return "", errors.New("framework.DeviceProxy.SendRequest: device session id is required")
	}
	if env.ID == "" {
		return "", errors.New("framework.DeviceProxy.SendRequest: envelope id is required")
	}

	enqueuedAt := p.now().UnixMilli()
	deadlineMs := envelopeDeadline(env, enqueuedAt)
	deadline := time.UnixMilli(deadlineMs)

	entry := adapter.CorrelationEntry{
		RequestID:     adapter.CorrelationKey(env.ID),
		CorrelationID: env.CorrelationID,
		ChannelID:     p.ChannelID,
		AudienceActor: p.AdapterActorID,
		ParentID:      env.ID,
		EnqueuedAt:    enqueuedAt,
		ExpiresAt:     deadlineMs,
		State:         adapter.CorrelationPending,
	}
	if _, err := p.deps.Correlation.Reserve(ctx, entry); err != nil {
		return "", fmt.Errorf("framework.DeviceProxy.SendRequest: reserve correlation: %w", err)
	}

	requestKey := adapter.CorrelationKey(env.ID)
	if err := p.deps.Policy.RegisterTimer(ctx, requestKey, deadline); err != nil {
		// Best-effort cleanup: expire the correlation so the framework
		// observes the abandoned reserve.
		_ = p.deps.Correlation.MarkExpired(ctx, requestKey)
		return "", fmt.Errorf("framework.DeviceProxy.SendRequest: arm F3 timer: %w", err)
	}

	frame := devicetransit.SendFrame{
		ChannelID:       p.ChannelID,
		DeviceSessionID: sessionID,
		Direction:       devicetransit.DirectionToDevice,
		RequestID:       env.ID,
		ParentID:        env.ParentID,
		CorrelationID:   env.CorrelationID,
		Payload:         append([]byte(nil), wirePayload...), // defensive copy — kernel transit may persist async
		ExpiresAt:       deadlineMs,
	}

	sentFrameID, err := p.deps.Transit.Send(ctx, frame)
	if err != nil {
		// Walk back the reserves so caller can emit synchronous terminal.
		_ = p.deps.Policy.CancelTimer(ctx, requestKey)
		_ = p.deps.Correlation.MarkExpired(ctx, requestKey)
		return "", fmt.Errorf("framework.DeviceProxy.SendRequest: transit send: %w", err)
	}
	return sentFrameID.String(), nil
}

// CancelInFlight is the adapter-driven escape hatch. Called when the
// adapter has decided to short-circuit a pending request (e.g. push
// failure → emit synchronous terminal). Cancels the F3 timer + marks
// the correlation expired. Idempotent.
func (p *DeviceProxy) CancelInFlight(ctx context.Context, requestID string) {
	key := adapter.CorrelationKey(message.ID(requestID))
	_ = p.deps.Policy.CancelTimer(ctx, key)
	_ = p.deps.Correlation.MarkExpired(ctx, key)
}

// CompleteInFlight is the success-path counterpart. Called by the
// Module right before emitting ctx.Respond on a recv frame. Cancels the
// F3 timer + marks correlation done. Idempotent.
func (p *DeviceProxy) CompleteInFlight(ctx context.Context, requestID string) error {
	key := adapter.CorrelationKey(message.ID(requestID))
	if err := p.deps.Policy.CancelTimer(ctx, key); err != nil {
		return fmt.Errorf("framework.DeviceProxy.CompleteInFlight: cancel timer: %w", err)
	}
	if err := p.deps.Correlation.MarkDone(ctx, key); err != nil {
		return fmt.Errorf("framework.DeviceProxy.CompleteInFlight: mark done: %w", err)
	}
	return nil
}

// LookupInFlight returns the correlation entry for a recv frame. Useful
// when the adapter has nothing but a request_id on the wire and needs
// the original envelope.type / audience to build the response payload.
func (p *DeviceProxy) LookupInFlight(ctx context.Context, requestID string) (adapter.CorrelationEntry, bool, error) {
	return p.deps.Correlation.Get(ctx, adapter.CorrelationKey(message.ID(requestID)))
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
// when the envelope omits ExpiresAt. 5 minutes mirrors the M1.3
// xhs.defaultMaxPendingMs constant — long enough for a Chrome extension
// reload + short enough that hung requests still surface within human
// attention span.
const defaultPendingMs int64 = 5 * 60 * 1000
