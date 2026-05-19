package main

import (
	"context"
	"fmt"

	deviceframework "github.com/wanpengxie/ActOS/adapters/device/framework"
	devicexhs "github.com/wanpengxie/ActOS/adapters/device/xhs"
	"github.com/wanpengxie/ActOS/adapters/framework"
	"github.com/wanpengxie/ActOS/adapters/xhs"
	"github.com/wanpengxie/ActOS/kernel/actor"
	"github.com/wanpengxie/ActOS/kernel/actorreg"
	"github.com/wanpengxie/ActOS/kernel/adapter"
	"github.com/wanpengxie/ActOS/kernel/channel"
	"github.com/wanpengxie/ActOS/kernel/devicetransit"
	khar "github.com/wanpengxie/ActOS/kernel/harness"
	"github.com/wanpengxie/ActOS/kernel/message"
	"github.com/wanpengxie/ActOS/runtime"
	"github.com/wanpengxie/ActOS/runtime/harness"
	"github.com/wanpengxie/ActOS/runtime/scheduler"
	"github.com/wanpengxie/ActOS/runtime/transit"
)

// AdapterModuleFactory builds one adapter.Module per channel. The
// composition root supplies a list of factories to wireAdapterFramework;
// each factory may inspect the channel id and decide to skip (return
// nil, nil) when its adapter is not part of that channel's template.
//
// Returning (nil, nil) means "this factory does not apply to that
// channel". Returning a non-nil error fails the channel boot.
type AdapterModuleFactory func(ctx context.Context, h runtime.ChannelHooks) (adapter.Module, error)

// wireAdapterFramework returns a runtime.DaemonConfig.OnChannelBoot
// callback that constructs an adapters/framework.Manager per channel
// from the supplied factories, installs every produced Module, and
// registers a scheduler.Deliverer HandlerFn that dispatches inbound
// kind=request envelopes through the Manager.
//
// The returned teardown closure runs Manager.Shutdown on channel
// unload / daemon shutdown.
//
// Lives in cmd/daemon so the wiring crosses the runtime ↔ adapters
// architectural boundary at the composition root rather than inside the
// runtime package (go-arch-lint enforces runtime ↛ adapters).
func wireAdapterFramework(factories ...AdapterModuleFactory) func(ctx context.Context, h runtime.ChannelHooks) (func(context.Context) error, error) {
	return func(ctx context.Context, h runtime.ChannelHooks) (func(context.Context) error, error) {
		modules := make([]adapter.Module, 0, len(factories))
		for _, f := range factories {
			mod, err := f(ctx, h)
			if err != nil {
				return nil, fmt.Errorf("adapter factory for %s: %w", h.ChannelID, err)
			}
			if mod == nil {
				continue
			}
			modules = append(modules, mod)
		}
		if len(modules) == 0 {
			// Nothing to install for this channel — return a no-op teardown
			// so callers can rely on a non-nil teardown closure.
			return func(context.Context) error { return nil }, nil
		}

		// Wrap the channel chain so any envelope authored with
		// sender.kind=tool (i.e. an adapter response or terminal_failure
		// emitted by framework Respond / ErrorPolicy) carries a fresh
		// CallerContext stamping that adapter actor as the caller —
		// otherwise harness step 1 rejects the response because the
		// inherited inbound stamp is the request author. Covers both the
		// synchronous Handle→Respond path and the F3 timer-fire path
		// (which uses context.Background() inside policy.fire).
		adapterChain := &adapterCallerChain{
			inner:     h.HarnessChain,
			channelID: h.ChannelID,
		}

		mgr, err := framework.NewManager(framework.ManagerConfig{
			ChannelID:     h.ChannelID,
			ActorRegistry: h.ActorRegistry,
			TypeRegistry:  h.TypeRegistry,
			HarnessChain:  adapterChain,
			RequestLookup: h.RequestLookup,
			// T147 §A — daemon supplies the per-channel DeviceTransit so
			// the framework can satisfy `via_server_transit` modules at
			// Install time (manager.installOne refuses such a module
			// when DeviceTransit is nil). Safe to pass nil here when the
			// channel hooks don't provide one (in_process-only channels);
			// the manager only consults this field for matching modules.
			DeviceTransit: h.DeviceTransit,
		})
		if err != nil {
			return nil, fmt.Errorf("framework.NewManager(%s): %w", h.ChannelID, err)
		}
		if err := mgr.Install(ctx, modules); err != nil {
			return nil, fmt.Errorf("framework.Manager.Install(%s): %w", h.ChannelID, err)
		}
		if err := mgr.BootRecoverTimers(ctx); err != nil {
			return nil, fmt.Errorf("framework.Manager.BootRecoverTimers(%s): %w", h.ChannelID, err)
		}

		// T147 §A — wire the inbound device→daemon callback. Every
		// via_server_transit adapter installed above gets its
		// OnExternalCallback invoked when a device_transit.recv frame
		// arrives for this channel. M1.6 baseline assumption: 1 channel
		// ↔ 1 device adapter, so we hand the first match. Multi-adapter
		// routing (by SessionStore.session_id → adapter map) is M1.7
		// scope; the loop also covers it best-effort by trying each
		// adapter and returning the first non-nil error.
		if h.SetDeviceCallback != nil {
			deviceAdapters := mgr.AdaptersByBinding(actor.BindingViaServerTransit)
			if len(deviceAdapters) > 0 {
				h.SetDeviceCallback(func(ctx context.Context, frame devicetransit.SendFrame) error {
					var firstErr error
					for _, name := range deviceAdapters {
						if err := mgr.OnExternalCallback(ctx, name, frame.Payload); err != nil && firstErr == nil {
							firstErr = err
						}
					}
					return firstErr
				})
			}
		}

		// Register one scheduler.Deliverer handler per installed module so
		// trigger.Gateway.Dispatch routes inbound request envelopes through
		// the framework.Manager. Each handler re-stamps the ctx with the
		// adapter actor as caller so framework.Respond's inner chain.Write
		// (which produces an envelope with sender=adapter actor) passes the
		// step-1/3 caller-vs-sender check — the inbound stamp was the
		// request author (e.g. user:alice), which doesn't match the
		// response sender.
		for _, mod := range modules {
			decl := mod.Declares()
			h.Deliverer.Register(decl.ActorID,
				deliverThroughManager(mgr, decl.ActorID, h.ChannelID))
		}

		return func(shutdownCtx context.Context) error {
			return mgr.Shutdown(shutdownCtx)
		}, nil
	}
}

// deliverThroughManager wraps Manager.Dispatch into the
// scheduler.HandlerFn shape. The framework Manager only handles
// kind=request envelopes — anything else returned by trigger.Gateway is
// ignored (no error, so the gateway's at-least-once contract holds).
//
// The wrapper re-stamps the harness CallerContext with adapterID so the
// framework.Respond chain.Write — which emits a response with
// sender=adapter actor — passes the harness step-1/step-3 caller-vs-
// sender check. AllowProvidedSenderKind=true lets the framework keep its
// `Sender.Kind = actor.KindTool` value (the registry record agrees).
func deliverThroughManager(mgr adapter.Manager, adapterID actor.ActorID, channelID channel.ID) scheduler.HandlerFn {
	return func(ctx context.Context, _ actor.ActorID, env *message.Envelope) error {
		if env == nil || env.Kind != message.KindRequest {
			return nil
		}
		stamped := harness.CtxWithCaller(ctx, harness.CallerContext{
			ActorID:                 adapterID,
			ChannelID:               channelID,
			AllowProvidedSenderKind: true,
		})
		if err := mgr.Dispatch(stamped, env); err != nil {
			// Log + swallow so the gateway's at-least-once contract holds
			// (a single failed Dispatch must NOT abort the harness write
			// path). The framework already emitted any required failed
			// terminal via ErrorPolicy.
			fmt.Printf("runtime: adapter dispatch %s/%s: %v\n", channelID, env.ID, err)
			return nil
		}
		return nil
	}
}

// adapterCallerChain wraps a kernel/harness.Chain and stamps the
// CallerContext to env.Sender.ID when the envelope carries an explicit
// actor sender. Used by the adapter framework so its inner chain.Write calls
// pass the harness step-1/step-3 caller-vs-sender check regardless of
// whether the call was made on the synchronous Handle→Respond path
// (inbound stamp is the request author) or the F3 timer-fire path
// (no inbound stamp at all). Adapter observability events may be emitted
// by system as well as tool actors, so the stamp follows the envelope
// sender rather than only actor.KindTool.
type adapterCallerChain struct {
	inner     khar.Chain
	channelID channel.ID
}

// Write satisfies kernel/harness.Chain.
func (c *adapterCallerChain) Write(ctx context.Context, env *message.Envelope) (khar.WriteResult, error) {
	if env != nil && env.Sender.ID != "" {
		ctx = harness.CtxWithCaller(ctx, harness.CallerContext{
			ActorID:                 env.Sender.ID,
			ChannelID:               c.channelID,
			AllowProvidedSenderKind: true,
		})
	}
	return c.inner.Write(ctx, env)
}

// XHSScaffoldFactory returns an AdapterModuleFactory that installs the
// adapters/xhs in_process scaffold (M1.6-T2). cmd/daemon supplies it
// during DaemonConfig assembly. T3 will replace this with the
// adapters/device/xhs factory once DeviceTransit is wired.
//
// M1.6-T5 phase-2 — the factory is gated by ChannelHooks.ChannelType:
// it installs only for channels created with type=="xhs-creator" (the
// L4 template that declares xhs.* business types). Channels created
// with any other type (e.g. "group" / "") get a no-op factory so the
// xhs adapter does not pollute generic channels.
func XHSScaffoldFactory(cfg xhs.Config) AdapterModuleFactory {
	return func(_ context.Context, h runtime.ChannelHooks) (adapter.Module, error) {
		if h.ChannelType != XHSCreatorChannelType {
			return nil, nil
		}
		return xhs.New(cfg), nil
	}
}

// XHSCreatorChannelType is the catalog.Channel.Type value the L4
// xhs-creator template binds (per v4-layer4-spec). cmd/daemon registers
// a ChannelTemplate under this key and the AdapterModuleFactory closures
// install the xhs adapter only for channels carrying it.
const XHSCreatorChannelType = "xhs-creator"

// DeviceXHSFactory returns an AdapterModuleFactory that installs the
// production xhs device adapter (adapters/device/xhs) configured to run
// over the via_server_transit binding — daemon → server → device WS,
// per M1.6-T3 §A. The supplied sessionStore is reused across every
// channel (per-daemon mirror of server.device_sessions). When nil, a
// fresh framework.InMemorySessionStore is created — sufficient for
// development / e2e harnesses that don't yet wire sqlite mirroring (the
// store lives only in daemon memory and is rebuilt from server-issued
// control.bind_device_session frames after every restart).
//
// Composition root contract: the caller MUST swap XHSScaffoldFactory
// for this one in the OnChannelBoot wiring AND ensure the channel's
// actor_registry seeds the xhs adapter actor with binding=via_server_transit
// (not in_process — the framework Install path otherwise rejects the
// module per L2 §1.4.6 binding consistency).
func DeviceXHSFactory(sessionStore deviceframework.SessionStore, cfg devicexhs.Config) AdapterModuleFactory {
	return func(_ context.Context, h runtime.ChannelHooks) (adapter.Module, error) {
		if h.ChannelType != XHSCreatorChannelType {
			return nil, nil
		}
		effective := cfg
		if effective.SessionStore == nil {
			if sessionStore == nil {
				sessionStore = deviceframework.NewInMemorySessionStore()
			}
			effective.SessionStore = sessionStore
		}
		return devicexhs.New(effective)
	}
}

// DeviceXHSActorSeed returns the actor_registry seed row for the xhs
// device adapter using the via_server_transit binding. Counterpart to
// adapters/xhs.DefaultActorSeed (which seeds the in_process scaffold).
// cmd/daemon plugs the result into ChannelTemplate.AdapterActorSeeds
// when swapping the in-process scaffold for the real device adapter.
func DeviceXHSActorSeed() actorreg.Record {
	return actorreg.Record{
		ID:      devicexhs.DefaultAdapterActorID,
		Kind:    actor.KindTool,
		Binding: actor.BindingViaServerTransit,
	}
}

// DeviceSessionBinder couples a shared framework.SessionStore with the
// daemon-level control.bind_device_session / control.unbind_device_session
// handlers (T147 §A-S2 phase-4b). cmd/daemon constructs one binder per
// process: the store is shared across every channel (per-channel routing
// happens by DeviceSession.ChannelID), and the handlers map bind / unbind
// frames into Upsert / Delete calls.
//
// Wire the binder via:
//
//	binder := NewDeviceSessionBinder(deviceframework.NewInMemorySessionStore())
//	cfg.OnBindDeviceSession   = binder.OnBind
//	cfg.OnUnbindDeviceSession = binder.OnUnbind
//	cfg.OnChannelBoot         = wireAdapterFramework(
//	    DeviceXHSFactory(binder.SessionStore(), devicexhs.Config{...}),
//	)
type DeviceSessionBinder struct {
	store deviceframework.SessionStore
}

// NewDeviceSessionBinder wires a binder over the supplied store. When
// store is nil a fresh InMemorySessionStore is allocated — sufficient
// for development / e2e harnesses without sqlite mirroring.
func NewDeviceSessionBinder(store deviceframework.SessionStore) *DeviceSessionBinder {
	if store == nil {
		store = deviceframework.NewInMemorySessionStore()
	}
	return &DeviceSessionBinder{store: store}
}

// SessionStore exposes the shared store so the composition root can
// pass it into DeviceXHSFactory.
func (b *DeviceSessionBinder) SessionStore() deviceframework.SessionStore {
	return b.store
}

// OnBind handles control.bind_device_session. Maps the wire payload into
// a framework.DeviceSession in state=ready (server-side state machine has
// already INSERTed the row in pending and will MarkBound on ack), then
// Upserts into the per-daemon SessionStore. Idempotent: re-running with
// the same SessionID overwrites the row (T1.10 — replay safe).
func (b *DeviceSessionBinder) OnBind(ctx context.Context, body transit.BindDeviceSessionBody) transit.BindDeviceSessionAckBody {
	ack := transit.BindDeviceSessionAckBody{
		FrameID:   body.FrameID,
		SessionID: body.SessionID,
	}
	sess := deviceframework.DeviceSession{
		SessionID:  body.SessionID,
		ChannelID:  body.ChannelID,
		DeviceID:   body.DeviceID,
		DeviceType: body.DeviceType,
		// Authoritative server row transitions pending → ready on this
		// ack; the daemon mirror jumps straight to ready so adapter
		// modules see the eventual state immediately. T1.10 allows the
		// daemon to skip the pending intermediate because the daemon
		// never observes "pending → ready" as two distinct events.
		State:            deviceframework.StateReady,
		BoundAt:          body.BoundAt,
		TokenFingerprint: body.TokenFingerprint,
		ExpiresAt:        body.ExpiresAt,
	}
	if err := b.store.Upsert(ctx, sess); err != nil {
		ack.Reason = "session_store_upsert"
		ack.Detail = err.Error()
		return ack
	}
	ack.Accepted = true
	return ack
}

// OnUnbind handles control.unbind_device_session. Deletes the mirror
// row idempotently (missing row is not an error). The server's
// authoritative row stays revoked / expired regardless of the daemon
// response, so this is best-effort tear-down only.
func (b *DeviceSessionBinder) OnUnbind(ctx context.Context, body transit.UnbindDeviceSessionBody) transit.UnbindDeviceSessionAckBody {
	ack := transit.UnbindDeviceSessionAckBody{
		FrameID:   body.FrameID,
		SessionID: body.SessionID,
	}
	if err := b.store.Delete(ctx, body.SessionID); err != nil {
		ack.Reason = "session_store_delete"
		ack.Detail = err.Error()
		return ack
	}
	ack.Accepted = true
	return ack
}

// listDeviceSessionsForChannel returns the DaemonConfig.ListDeviceSessionsForChannel
// closure backed by the supplied DeviceSessionBinder's SessionStore (M1.6
// follow-up — agent self-awareness fix). The runtime daemon calls this
// inside the worker spawn PreSpawn hook so the kimi system prompt
// surfaces the channel's active device sessions to the LLM. Errors from
// the store are swallowed and treated as "no sessions" — the worker
// boots with an empty devices section, same as a channel that never
// bound any device.
//
// Composition-root only: the runtime package cannot import
// adapters/device/framework directly (arch-lint forbids
// runtime ↛ adapters), so this adapter lives in cmd/daemon and
// translates framework.DeviceSession → runtime.DeviceSessionInfo on
// the way out.
func listDeviceSessionsForChannel(binder *DeviceSessionBinder) func(ctx context.Context, ch channel.ID) []runtime.DeviceSessionInfo {
	if binder == nil {
		return nil
	}
	store := binder.SessionStore()
	if store == nil {
		return nil
	}
	return func(ctx context.Context, ch channel.ID) []runtime.DeviceSessionInfo {
		rows, err := store.ListByChannel(ctx, ch)
		if err != nil || len(rows) == 0 {
			return nil
		}
		out := make([]runtime.DeviceSessionInfo, 0, len(rows))
		for _, r := range rows {
			out = append(out, runtime.DeviceSessionInfo{
				SessionID:  string(r.SessionID),
				DeviceID:   r.DeviceID,
				DeviceType: r.DeviceType,
				State:      string(r.State),
			})
		}
		return out
	}
}
