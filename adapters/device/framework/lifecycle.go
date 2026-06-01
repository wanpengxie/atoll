package framework

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/wanpengxie/ActOS/framework/devicetransit"
	"github.com/wanpengxie/ActOS/kernel/actor"
	"github.com/wanpengxie/ActOS/kernel/adapter"
	"github.com/wanpengxie/ActOS/kernel/channel"
	"github.com/wanpengxie/ActOS/kernel/message"
)

// DeviceState is the closed set of device runtime states a
// binding=runtime_inbound_via_relay adapter projects from its
// devicebus connection lifecycle. Spec ref: proto-layer1 §3.6 O6 +
// impl-layer2 §5.7 Invariant 7 (device state owned by adapter, not
// transport).
type DeviceState string

const (
	// DeviceStateUnknown is the initial state before any lifecycle
	// event arrives. Handle treats this as not-online: fail-fast so the
	// LLM gets a clean terminal instead of waiting on F3 default
	// timeout.
	DeviceStateUnknown DeviceState = "unknown"
	// DeviceStateOnline — devicebus ws is registered + reachable.
	DeviceStateOnline DeviceState = "online"
	// DeviceStateOffline — devicebus ws closed cleanly or unexpectedly.
	DeviceStateOffline DeviceState = "offline"
	// DeviceStateTokenExpired — actor token past expires_at; user
	// must re-bind via the UI. Adapter SHOULD surface a distinct
	// error_code so the UI can prompt for re-bind instead of
	// "transient retry".
	DeviceStateTokenExpired DeviceState = "token_expired"
)

// LifecycleTracker is the canonical state machine for adapters whose
// binding == runtime_inbound_via_relay. It owns:
//
//   - atomic device state (concurrent reads from Handle without lock)
//   - state-transition serialisation (single goroutine sees prev→next)
//   - channel-event emission on state change (envelope via HarnessChain)
//
// Adapters embed a *LifecycleTracker as a field on their Module and
// delegate from OnRuntimeEvent. The tracker doesn't touch the
// adapter's business state; the adapter still owns its in-flight
// requests, payload caches, etc.
//
// Example wire-up (xhs Module):
//
//	m.lifecycle = framework.NewLifecycleTracker(framework.LifecycleTrackerConfig{
//	    EventTypes: map[framework.DeviceState]string{
//	        framework.DeviceStateOnline:       TypeDeviceOnline,
//	        framework.DeviceStateOffline:      TypeDeviceOffline,
//	        framework.DeviceStateTokenExpired: TypeDeviceOffline,
//	    },
//	    AdapterActorID: mctx.AdapterActorID,
//	    ChannelID:      mctx.ChannelID,
//	    EmitEvent:      mctx.EmitEvent,
//	    Clock:          m.now,
//	})
//
// Then OnRuntimeEvent decodes the devicetransit lifecycle payload and calls
// m.lifecycle.Apply(ctx, evt)
// and Handle gate → switch m.lifecycle.State() { … }.
type LifecycleTracker struct {
	state atomic.Value // DeviceState
	mu    sync.Mutex
	cfg   LifecycleTrackerConfig
}

// LifecycleTrackerConfig configures a LifecycleTracker. EventTypes is
// the only adapter-specific field — the rest mirror ModuleContext
// values an adapter already has at Init time.
type LifecycleTrackerConfig struct {
	// EventTypes maps a DeviceState the tracker entered to the
	// channel envelope.type that should be emitted on entry. Missing
	// entries are no-ops (tracker only updates internal state, no
	// channel-side projection). Typical use: emit `<adapter>.device.online`
	// on Online + `<adapter>.device.offline` on Offline / TokenExpired.
	//
	// Every event type listed here MUST be installed in the channel's
	// type_registry (with allowed_kinds=[event], handler_actor_id =
	// AdapterActorID). Strict-mode declarations enforce this at install.
	EventTypes map[DeviceState]string

	// AdapterActorID is the channel-local actor the adapter owns;
	// stamped onto every emitted lifecycle event's sender.
	AdapterActorID actor.ActorID

	// ChannelID is the channel the adapter is bound to. Emitted events
	// carry it on envelope.channel_id.
	ChannelID channel.ID

	// EmitEvent is the semantic event capability the adapter received
	// in ModuleContext. Required when EventTypes is non-empty; ignored
	// otherwise.
	EmitEvent adapter.EmitEventFunc

	// Clock returns the current wall time. Default time.Now.
	Clock func() time.Time

	// SystemActorID is the audience id for emitted lifecycle events.
	// Defaults to actor.SystemActorID (the channel system actor —
	// observability events have no trigger fanout target, the channel
	// system audience is the canonical "addressed to no one in
	// particular" placeholder).
	SystemActorID actor.ActorID
}

func (c *LifecycleTrackerConfig) applyDefaults() {
	if c.Clock == nil {
		c.Clock = time.Now
	}
	if c.SystemActorID == "" {
		c.SystemActorID = actor.SystemActorID
	}
}

// NewLifecycleTracker validates config + returns a tracker starting in
// DeviceStateUnknown. Returns an error when EventTypes is non-empty
// but EmitEvent / AdapterActorID / ChannelID is missing — that
// combo would silently swallow events at emit time.
func NewLifecycleTracker(cfg LifecycleTrackerConfig) (*LifecycleTracker, error) {
	cfg.applyDefaults()
	if len(cfg.EventTypes) > 0 {
		if cfg.EmitEvent == nil {
			return nil, errors.New("framework.LifecycleTracker: EventTypes non-empty but EmitEvent nil")
		}
		if cfg.AdapterActorID == "" {
			return nil, errors.New("framework.LifecycleTracker: AdapterActorID empty")
		}
		if cfg.ChannelID == "" {
			return nil, errors.New("framework.LifecycleTracker: ChannelID empty")
		}
	}
	lt := &LifecycleTracker{cfg: cfg}
	lt.state.Store(DeviceStateUnknown)
	return lt, nil
}

// State returns the current device state. Lock-free — safe for
// concurrent reads from Handle.
func (lt *LifecycleTracker) State() DeviceState {
	v, _ := lt.state.Load().(DeviceState)
	if v == "" {
		return DeviceStateUnknown
	}
	return v
}

// Apply consumes one inbound devicetransit.LifecycleFrame: maps the
// wire event to the target state, transitions if different from the
// current, and (when EventTypes has an entry for the new state) emits
// a channel envelope via the configured EmitEvent semantic path.
//
// Same-state events are no-ops — the adapter doesn't see duplicate
// `xhs.device.online` events when devicebus emits multiple "connected"
// signals for the same logical extension session.
//
// Unknown LifecycleEvent values are ignored (forward-compat); the
// tracker neither transitions nor emits.
func (lt *LifecycleTracker) Apply(ctx context.Context, evt devicetransit.LifecycleFrame) error {
	next, ok := mapLifecycleEvent(evt.Event)
	if !ok {
		return nil
	}

	lt.mu.Lock()
	defer lt.mu.Unlock()
	prev := lt.State()
	if prev == next {
		return nil
	}

	eventType, ok := lt.cfg.EventTypes[next]
	if !ok || eventType == "" {
		// Local-only transition: no channel-observable projection exists for
		// this state, so updating the adapter's in-memory gate is sufficient.
		lt.state.Store(next)
		return nil
	}
	if err := lt.emitTransition(ctx, eventType, prev, next, evt); err != nil {
		return err
	}
	lt.state.Store(next)
	return nil
}

func mapLifecycleEvent(e devicetransit.LifecycleEvent) (DeviceState, bool) {
	switch e {
	case devicetransit.LifecycleConnected:
		return DeviceStateOnline, true
	case devicetransit.LifecycleDisconnected:
		return DeviceStateOffline, true
	case devicetransit.LifecycleTokenExpired:
		return DeviceStateTokenExpired, true
	}
	return "", false
}

func (lt *LifecycleTracker) emitTransition(
	ctx context.Context,
	eventType string,
	prev, next DeviceState,
	evt devicetransit.LifecycleFrame,
) error {
	if lt.cfg.EmitEvent == nil {
		// Config validated this at NewLifecycleTracker; defensive
		// guard in case the field was zeroed post-construction.
		return errors.New("framework.LifecycleTracker.Apply: EmitEvent nil")
	}

	payload := map[string]any{
		"device_state":    string(next),
		"previous_state":  string(prev),
		"lifecycle_event": string(evt.Event),
	}
	if evt.DeviceID != "" {
		payload["device_id"] = evt.DeviceID
	}
	if evt.Detail != "" {
		payload["detail"] = evt.Detail
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("framework.LifecycleTracker.Apply: marshal payload: %w", err)
	}

	if _, err := lt.cfg.EmitEvent(ctx, eventType, body, adapter.EmitEventOptions{
		Visibility: message.VisibilitySystem,
		Audience:   message.Audience{lt.cfg.SystemActorID},
	}); err != nil {
		return fmt.Errorf("framework.LifecycleTracker.Apply: emit event %s: %w", eventType, err)
	}
	return nil
}
