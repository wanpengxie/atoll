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
	"github.com/wanpengxie/ActOS/adapters/framework"
	proxyfacade "github.com/wanpengxie/ActOS/adapters/framework/proxy_facade"
	"github.com/wanpengxie/ActOS/framework/devicetransit"
	kerneldaemonbus "github.com/wanpengxie/ActOS/framework/multiuser/daemonbus"
	"github.com/wanpengxie/ActOS/framework/multiuser/runtime"
	"github.com/wanpengxie/ActOS/kernel/actor"
	"github.com/wanpengxie/ActOS/kernel/adapter"
	"github.com/wanpengxie/ActOS/kernel/channel"
	khar "github.com/wanpengxie/ActOS/kernel/harness"
	"github.com/wanpengxie/ActOS/kernel/message"
	"github.com/wanpengxie/ActOS/pkg/metrics"
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
		if len(modules) == 0 && h.SetProxyActorCallback == nil {
			// Nothing to install or dynamically extend for this channel —
			// return a no-op teardown so callers can rely on a non-nil
			// teardown closure.
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
		gcCtx, gcCancel := context.WithCancel(context.Background())
		go mgr.RunGC(gcCtx)

		// channel-lifecycle-reconcile §3 / §6 step3 — the per-channel
		// Reconciler is the SINGLE serial mutation entry for ALL channel
		// wiring (framework facade install + scheduler.Deliverer handler
		// registration + device route map). Both the static compiled-in
		// modules AND every fact-derived proxy facade flow through it: the
		// static modules are its permanent set (installed + registered by the
		// first Reconcile, never collapsed); proxy facades are derived from
		// facts (system.actor.registered) and reconciled in/out. This collapses
		// the previous dual assembly (composition-root mgr.Install +
		// Deliverer.Register loop, plus update_members one-shot Register) into
		// one level-triggered derivation, so clearing all wiring and re-running
		// Reconcile rebuilds every facade/handler from facts + the static set.
		deviceRouteMu := &sync.RWMutex{}
		deviceAdapterByActor := map[actor.ActorID]string{}
		reconciler := newChannelReconciler(
			h.ChannelID,
			mgr,
			h.Deliverer,
			h.ActorRegistry,
			h.Logger,
			func(actorID actor.ActorID) scheduler.HandlerFn {
				return deliverThroughManager(mgr, actorID, h.ChannelID, h.Logger)
			},
			deviceRouteMu,
			deviceAdapterByActor,
			modules,
		)
		if h.SetProxyActorCallback != nil {
			// update_members now only persists the actor_registry mutation +
			// fact (runtime side); the wiring derivation is the Reconciler's
			// job. The callback degenerates to "facts changed → reconcile".
			h.SetProxyActorCallback(func(ctx context.Context, _ kerneldaemonbus.UpdateMembersBody) error {
				return reconciler.Reconcile(ctx)
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
					return devicetransit.NewAckError(
						devicetransit.AckRejectedRetryable,
						"adapter_route_not_found",
						fmt.Sprintf("no adapter route for actor %s", frame.AdapterActorID),
						nil,
					)
				}
				var body deviceframework.DeviceTransitBody
				if err := json.Unmarshal(frame.Body, &body); err != nil {
					return fmt.Errorf("cmd/daemon: decode device transit body: %w", err)
				}
				callbackChannelID := frame.ChannelID
				if callbackChannelID == "" {
					callbackChannelID = h.ChannelID
				}
				return mgr.OnExternalCallbackFrame(ctx, adapterName, adapter.ExternalCallbackFrame{
					ChannelID:      callbackChannelID,
					AdapterActorID: frame.AdapterActorID,
					RequestID:      body.RequestID,
					ParentID:       body.ParentID,
					CorrelationID:  body.CorrelationID,
					ExpiresAt:      body.ExpiresAt,
					Payload:        body.Payload,
				})
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
					payload, err := devicetransit.EncodeLifecycleRuntimeEventPayload(evt)
					if err != nil {
						return err
					}
					return mgr.OnRuntimeEvent(ctx, adapter.RuntimeEvent{
						Kind:           devicetransit.RuntimeEventKindDeviceLifecycle,
						ChannelID:      channelID,
						AdapterActorID: evt.AdapterActorID,
						Payload:        payload,
					})
				})
			}
		}

		// Boot/reclaim durable trigger — this first Reconcile both installs +
		// registers the static compiled-in modules (the composition root no
		// longer touches mgr.Install / Deliverer.Register) AND rebuilds the
		// fact-derived proxy facades from the channel log alone. On a fresh
		// channel the proxy half is a no-op (no registered proxy facts yet);
		// on cold-start / reclaim it restores facades WITHOUT waiting for a
		// proxy reconnect (§9 DoD #1 / #4). The runtime backfills the
		// registered facts before OnChannelBoot runs, so the facts are
		// present here. Each static handler re-stamps the ctx with the adapter
		// actor as caller so framework.Respond's inner chain.Write (sender=
		// adapter actor) passes the harness step-1/3 caller-vs-sender check.
		if err := reconciler.Reconcile(ctx); err != nil {
			gcCancel()
			_ = mgr.Shutdown(context.Background())
			return nil, fmt.Errorf("cmd/daemon: initial reconcile %s: %w", h.ChannelID, err)
		}

		// Cheap per-channel durable fallback (§3 / §5) — low-frequency resync
		// repairs any wiring whose follow-up reconcile was lost (commit ok,
		// callback failed/crashed). Cancelled by teardown.
		resyncCtx, resyncCancel := context.WithCancel(context.Background())
		go reconciler.runResync(resyncCtx, reconcileResyncInterval)

		return func(shutdownCtx context.Context) error {
			resyncCancel()
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
			// Surface the error to trigger.Gateway so the origin row
			// remains retryable. framework.Manager returns nil when it
			// has accepted responsibility by reserving the request or
			// writing a terminal failure.
			if logger != nil {
				logger.Warn().Err(err).
					Str("event", "daemon.adapter_dispatch_failed").
					Str("channel_id", string(channelID)).
					Str("message_id", string(env.ID)).
					Str("adapter_id", string(adapterID)).
					Msg("adapter manager dispatch failed")
			}
			return err
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

// proxyCapabilityValidator is the runtime.DaemonConfig.ProxyCapabilityValidator
// hook: it fail-fast validates a runtime_inbound_via_relay proxy actor's
// capability_set BEFORE update_members commits, by attempting the exact facade
// construction the Reconciler would later perform (DeclarationFromCapability +
// New). An empty/incomplete capability_set is rejected at the source so no
// actor row + system.actor.registered fact is committed for a facade that can
// never be wired. Lives in cmd/daemon (the composition root) because runtime/
// must not import adapters/** (INVARIANT-5).
func proxyCapabilityValidator(actorID actor.ActorID, capability json.RawMessage) error {
	decl, err := proxyfacade.DeclarationFromCapability(actorID, capability)
	if err != nil {
		return err
	}
	if _, err := proxyfacade.New(decl); err != nil {
		return err
	}
	return nil
}
