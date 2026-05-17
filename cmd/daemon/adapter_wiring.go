package main

import (
	"context"
	"fmt"

	"github.com/wanpengxie/ActOS/adapters/framework"
	"github.com/wanpengxie/ActOS/adapters/xhs"
	"github.com/wanpengxie/ActOS/kernel/actor"
	"github.com/wanpengxie/ActOS/kernel/adapter"
	"github.com/wanpengxie/ActOS/kernel/message"
	"github.com/wanpengxie/ActOS/runtime"
	"github.com/wanpengxie/ActOS/runtime/scheduler"
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

		mgr, err := framework.NewManager(framework.ManagerConfig{
			ChannelID:     h.ChannelID,
			ActorRegistry: h.ActorRegistry,
			TypeRegistry:  h.TypeRegistry,
			HarnessChain:  h.HarnessChain,
			RequestLookup: h.RequestLookup,
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

		// Register one scheduler.Deliverer handler per installed module so
		// trigger.Gateway.Dispatch routes inbound request envelopes through
		// the framework.Manager.
		for _, mod := range modules {
			decl := mod.Declares()
			h.Deliverer.Register(decl.ActorID, deliverThroughManager(mgr))
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
func deliverThroughManager(mgr adapter.Manager) scheduler.HandlerFn {
	return func(ctx context.Context, _ actor.ActorID, env *message.Envelope) error {
		if env == nil || env.Kind != message.KindRequest {
			return nil
		}
		return mgr.Dispatch(ctx, env)
	}
}

// XHSScaffoldFactory returns an AdapterModuleFactory that installs the
// adapters/xhs in_process scaffold (M1.6-T2). cmd/daemon supplies it
// during DaemonConfig assembly. T3 will replace this with the
// adapters/device/xhs factory once DeviceTransit is wired.
func XHSScaffoldFactory(cfg xhs.Config) AdapterModuleFactory {
	return func(_ context.Context, _ runtime.ChannelHooks) (adapter.Module, error) {
		return xhs.New(cfg), nil
	}
}
