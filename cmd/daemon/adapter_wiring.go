package main

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/rs/zerolog"

	deviceframework "github.com/wanpengxie/ActOS/adapters/device/framework"
	devicexhs "github.com/wanpengxie/ActOS/adapters/device/xhs"
	"github.com/wanpengxie/ActOS/adapters/framework"
	proxyfacade "github.com/wanpengxie/ActOS/adapters/framework/proxy_facade"
	"github.com/wanpengxie/ActOS/adapters/kimibridge"
	"github.com/wanpengxie/ActOS/adapters/xhs"
	"github.com/wanpengxie/ActOS/kernel/actor"
	"github.com/wanpengxie/ActOS/kernel/actorreg"
	"github.com/wanpengxie/ActOS/kernel/adapter"
	"github.com/wanpengxie/ActOS/kernel/channel"
	kerneldaemonbus "github.com/wanpengxie/ActOS/kernel/daemonbus"
	"github.com/wanpengxie/ActOS/kernel/devicetransit"
	khar "github.com/wanpengxie/ActOS/kernel/harness"
	"github.com/wanpengxie/ActOS/kernel/message"
	"github.com/wanpengxie/ActOS/pkg/metrics"
	"github.com/wanpengxie/ActOS/runtime"
	"github.com/wanpengxie/ActOS/runtime/harness"
	"github.com/wanpengxie/ActOS/runtime/scheduler"
	runtimestore "github.com/wanpengxie/ActOS/runtime/store"
	"github.com/wanpengxie/ActOS/runtime/typeinstall"
)

// AdapterModuleFactory builds one adapter.Module per channel. The
// composition root supplies a list of factories to wireAdapterFramework;
// each factory may inspect the channel id and decide to skip (return
// nil, nil) when its adapter is not part of that channel's template.
//
// Returning (nil, nil) means "this factory does not apply to that
// channel". Returning a non-nil error fails the channel boot.
type AdapterModuleFactory func(ctx context.Context, h runtime.ChannelHooks) (adapter.Module, error)

const devAdapterCredentialSecret = "dev-adapter-credential-secret-change-me"

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
	hook, err := wireAdapterFrameworkWithCredentialSecret([]byte(devAdapterCredentialSecret), factories...)
	if err != nil {
		panic(err)
	}
	return hook
}

func wireAdapterFrameworkWithCredentialSecret(secret []byte, factories ...AdapterModuleFactory) (func(ctx context.Context, h runtime.ChannelHooks) (func(context.Context) error, error), error) {
	if len(secret) == 0 {
		return nil, errors.New("cmd/daemon: adapter credential secret required")
	}
	key := deriveAdapterCredentialKey(secret)
	box, err := runtimestore.NewAESGCMSecretBox(key[:])
	if err != nil {
		return nil, fmt.Errorf("cmd/daemon: adapter credential SecretBox: %w", err)
	}
	return wireAdapterFrameworkWithCredentialBox(box, factories...), nil
}

func wireAdapterFrameworkWithCredentialBox(box runtimestore.SecretBox, factories ...AdapterModuleFactory) func(ctx context.Context, h runtime.ChannelHooks) (func(context.Context) error, error) {
	return func(ctx context.Context, h runtime.ChannelHooks) (func(context.Context) error, error) {
		if box == nil {
			return nil, errors.New("cmd/daemon: adapter credential SecretBox required")
		}
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

		clock := time.Now
		if h.NowFn != nil {
			clock = func() time.Time { return time.UnixMilli(h.NowFn()) }
		}
		installer, err := typeinstall.New(typeinstall.Config{
			ChannelID:     h.ChannelID,
			ActorRegistry: h.ActorRegistry,
			TypeRegistry:  h.TypeRegistry,
			HarnessChain:  h.HarnessChain,
			NowFn:         h.NowFn,
		})
		if err != nil {
			return nil, fmt.Errorf("typeinstall.New(%s): %w", h.ChannelID, err)
		}
		credentialStore, err := runtimestore.NewAdapterCredentialStore(h.DB, h.NowFn, box)
		if err != nil {
			return nil, fmt.Errorf("adapter credential store for %s: %w", h.ChannelID, err)
		}

		mgr, err := framework.NewManager(framework.ManagerConfig{
			ChannelID:                 h.ChannelID,
			ActorRegistry:             h.ActorRegistry,
			TypeRegistry:              h.TypeRegistry,
			TypeInstaller:             installer,
			HarnessChain:              adapterChain,
			RequestLookup:             h.RequestLookup,
			StateStore:                runtimestore.NewAdapterStateStore(h.DB, h.NowFn),
			CredentialStore:           credentialStore,
			Clock:                     clock,
			Logger:                    frameworkZerologLogger{log: h.Logger, channelID: h.ChannelID},
			Metrics:                   metrics.Default(),
			EmitInitialReadinessEvent: true,
			// T147 §A — daemon supplies the per-channel DeviceTransit so
			// the framework can satisfy `runtime_inbound_via_relay` modules at
			// Install time (manager.installOne refuses such a module
			// when DeviceTransit is nil). Safe to pass nil here when the
			// channel hooks don't provide one (embedded-only channels);
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
		gcCtx, gcCancel := context.WithCancel(context.Background())
		go mgr.RunGC(gcCtx)

		deviceRouteMu := sync.RWMutex{}
		deviceAdapterByActor := map[actor.ActorID]string{}
		for _, mod := range modules {
			decl := mod.Declares()
			if decl.Binding == actor.BindingRuntimeInboundViaRelay {
				deviceAdapterByActor[decl.ActorID] = decl.Name
			}
		}

		proxyMu := sync.Mutex{}
		proxyInstalled := map[actor.ActorID]struct{}{}
		for _, mod := range modules {
			proxyInstalled[mod.Declares().ActorID] = struct{}{}
		}
		if h.SetProxyActorCallback != nil {
			h.SetProxyActorCallback(func(ctx context.Context, body kerneldaemonbus.UpdateMembersBody) error {
				proxyMu.Lock()
				defer proxyMu.Unlock()
				for _, add := range body.Adds {
					if add.Kind != actor.KindTool || add.Binding != actor.BindingRuntimeInboundViaRelay {
						continue
					}
					if _, ok := proxyInstalled[add.MemberActorID]; ok {
						continue
					}
					decl, err := proxyfacade.DeclarationFromCapability(add.MemberActorID, add.CapabilitySet)
					if err != nil {
						return fmt.Errorf("cmd/daemon: proxy facade declaration %s: %w", add.MemberActorID, err)
					}
					mod, err := proxyfacade.New(decl)
					if err != nil {
						return fmt.Errorf("cmd/daemon: proxy facade module %s: %w", add.MemberActorID, err)
					}
					if err := mgr.Install(ctx, []adapter.Module{mod}); err != nil {
						return fmt.Errorf("cmd/daemon: proxy facade install %s: %w", add.MemberActorID, err)
					}
					h.Deliverer.Register(decl.ActorID,
						deliverThroughManager(mgr, decl.ActorID, h.ChannelID, h.Logger))
					deviceRouteMu.Lock()
					deviceAdapterByActor[decl.ActorID] = decl.Name
					deviceRouteMu.Unlock()
					proxyInstalled[decl.ActorID] = struct{}{}
					if h.Logger != nil {
						h.Logger.Info().
							Str("event", "daemon.proxy_facade_installed").
							Str("channel_id", string(h.ChannelID)).
							Str("actor_id", string(decl.ActorID)).
							Int("type_count", len(decl.Types)).
							Msg("proxy facade actor installed")
					}
				}
				return nil
			})
		}

		// T147 §A — wire the inbound device→daemon callback. T1 proxy
		// facade adds actor_id → adapter routing so runtime_inbound_via_relay
		// adapters can coexist as long as device_transit.SendFrame carries
		// AdapterActorID.
		if h.SetDeviceCallback != nil {
			h.SetDeviceCallback(func(ctx context.Context, frame devicetransit.SendFrame) error {
				deviceRouteMu.RLock()
				adapterName := deviceAdapterByActor[frame.AdapterActorID]
				deviceRouteMu.RUnlock()
				if adapterName == "" {
					return nil
				}
				var body deviceframework.DeviceTransitBody
				if err := json.Unmarshal(frame.Body, &body); err != nil {
					return fmt.Errorf("cmd/daemon: decode device transit body: %w", err)
				}
				return mgr.OnExternalCallback(ctx, adapterName, body.Payload)
			})
			if h.SetDeviceLifecycleCallback != nil {
				channelID := h.ChannelID
				h.SetDeviceLifecycleCallback(func(ctx context.Context, evt devicetransit.LifecycleFrame) error {
					deviceRouteMu.RLock()
					adapterName := deviceAdapterByActor[evt.AdapterActorID]
					deviceRouteMu.RUnlock()
					if adapterName == "" {
						return nil
					}
					return mgr.OnRuntimeEvent(ctx, adapter.RuntimeEvent{
						Kind:            adapter.RuntimeEventDeviceLifecycle,
						ChannelID:       channelID,
						AdapterActorID:  evt.AdapterActorID,
						DeviceLifecycle: &evt,
					})
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
				deliverThroughManager(mgr, decl.ActorID, h.ChannelID, h.Logger))
		}

		return func(shutdownCtx context.Context) error {
			gcCancel()
			return mgr.Shutdown(shutdownCtx)
		}, nil
	}
}

func deriveAdapterCredentialKey(secret []byte) [32]byte {
	// Implementation choice, not protocol semantics: derive the local
	// sqlite-at-rest key from the daemon composition root secret with a
	// fixed context string so the credential box gets exactly 32 bytes.
	mac := hmac.New(sha256.New, secret)
	_, _ = mac.Write([]byte("coagent.adapter.credentials.v1"))
	var out [32]byte
	copy(out[:], mac.Sum(nil))
	return out
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
func deliverThroughManager(mgr adapter.Manager, adapterID actor.ActorID, channelID channel.ID, logger *zerolog.Logger) scheduler.HandlerFn {
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
			if logger != nil {
				logger.Warn().Err(err).
					Str("event", "daemon.adapter_dispatch_failed").
					Str("channel_id", string(channelID)).
					Str("message_id", string(env.ID)).
					Str("adapter_id", string(adapterID)).
					Msg("adapter manager dispatch failed")
			}
			return nil
		}
		return nil
	}
}

type frameworkZerologLogger struct {
	log       *zerolog.Logger
	channelID channel.ID
}

func (l frameworkZerologLogger) Debug(msg string, args ...any) {
	if l.log == nil {
		return
	}
	frameworkLogEvent(l.log.Debug(), l.channelID, args...).Msg(msg)
}

func (l frameworkZerologLogger) Info(msg string, args ...any) {
	if l.log == nil {
		return
	}
	frameworkLogEvent(l.log.Info(), l.channelID, args...).Msg(msg)
}

func (l frameworkZerologLogger) Warn(msg string, args ...any) {
	if l.log == nil {
		return
	}
	frameworkLogEvent(l.log.Warn(), l.channelID, args...).Msg(msg)
}

func (l frameworkZerologLogger) Error(msg string, args ...any) {
	if l.log == nil {
		return
	}
	frameworkLogEvent(l.log.Error(), l.channelID, args...).Msg(msg)
}

func frameworkLogEvent(e *zerolog.Event, channelID channel.ID, args ...any) *zerolog.Event {
	if channelID != "" {
		e = e.Str("channel_id", string(channelID))
	}
	for i := 0; i < len(args); i += 2 {
		key, ok := args[i].(string)
		if !ok || key == "" {
			continue
		}
		var value any
		if i+1 < len(args) {
			value = args[i+1]
		}
		switch v := value.(type) {
		case string:
			e = e.Str(key, v)
		case fmt.Stringer:
			e = e.Str(key, v.String())
		case int:
			e = e.Int(key, v)
		case int64:
			e = e.Int64(key, v)
		case uint64:
			e = e.Uint64(key, v)
		case bool:
			e = e.Bool(key, v)
		case error:
			e = e.Err(v)
		case nil:
			e = e.Str(key, "")
		default:
			e = e.Interface(key, v)
		}
	}
	return e
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
// adapters/xhs embedded scaffold (M1.6-T2). cmd/daemon supplies it
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

// XHSCreatorChannelType is the catalog.Channel.Type value the domain-xhs
// xhs-creator template binds. cmd/daemon registers
// a ChannelTemplate under this key and the AdapterModuleFactory closures
// install the xhs adapter only for channels carrying it.
const XHSCreatorChannelType = "xhs-creator"

// DeviceXHSFactory returns an AdapterModuleFactory that installs the
// production xhs device adapter (adapters/device/xhs) configured to run
// over the runtime_inbound_via_relay binding — daemon → server → device WS.
// Routing is actor-based: the module sends through its adapter actor id
// and the server maps channel_id + actor_id to the live device socket.
//
// Composition root contract: the caller MUST swap XHSScaffoldFactory
// for this one in the OnChannelBoot wiring AND ensure the channel's
// actor_registry seeds the xhs adapter actor with binding=runtime_inbound_via_relay
// (not embedded — the framework Install path otherwise rejects the
// module per L2 §1.4.6 binding consistency).
func DeviceXHSFactory(cfg devicexhs.Config) AdapterModuleFactory {
	return func(_ context.Context, h runtime.ChannelHooks) (adapter.Module, error) {
		if h.ChannelType != XHSCreatorChannelType {
			return nil, nil
		}
		return devicexhs.New(cfg)
	}
}

// DeviceXHSActorSeed returns the actor_registry seed row for the xhs
// device adapter using the runtime_inbound_via_relay binding. Counterpart to
// adapters/xhs.DefaultActorSeed (which seeds the embedded scaffold).
// cmd/daemon plugs the result into ChannelTemplate.AdapterActorSeeds
// when swapping the in-process scaffold for the real device adapter.
func DeviceXHSActorSeed() actorreg.Record {
	return actorreg.Record{
		ID:      devicexhs.DefaultAdapterActorID,
		Kind:    actor.KindTool,
		Binding: actor.BindingRuntimeInboundViaRelay,
	}
}

// KimiWebBridgeFactory returns an AdapterModuleFactory that installs
// the kimi-webbridge adapter on every channel of the given channel
// type. Binding=runtime_outbound — the adapter dials the local
// kimi-webbridge daemon (~/.kimi-webbridge/bin/kimi-webbridge,
// http://127.0.0.1:10086) from inside the daemon process.
//
// Pair with KimiWebBridgeActorSeed via ChannelTemplate.AdapterActorSeeds
// so the install validator sees the actor before the type registry
// install runs.
func KimiWebBridgeFactory(cfg kimibridge.Config, channelType string) AdapterModuleFactory {
	return func(_ context.Context, h runtime.ChannelHooks) (adapter.Module, error) {
		if channelType != "" && h.ChannelType != channelType {
			return nil, nil
		}
		return kimibridge.New(cfg, kimibridge.WithDeps(framework.Deps{}))
	}
}

// KimiWebBridgeActorSeed returns the actor_registry seed row for the
// kimi-webbridge adapter. Binding=runtime_outbound; one actor per
// channel (single kimi-webbridge daemon per host in v1).
func KimiWebBridgeActorSeed() actorreg.Record {
	return actorreg.Record{
		ID:      kimibridge.DefaultAdapterActorID,
		Kind:    actor.KindTool,
		Binding: kimibridge.Binding,
	}
}
