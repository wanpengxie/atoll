package adapter

import (
	"context"

	"github.com/wanpengxie/ActOS/kernel/message"
)

// Manager is the per-channel adapter framework lifecycle contract (L2
// §8.6 — Install / BootRecover / Dispatch / OnExternalCallback /
// Shutdown).
//
// Concrete implementations live in adapters/framework (T4); kernel
// only declares the interface so Daemon composition root + tests can
// substitute fakes.
type Manager interface {
	// Install runs the L2 §8.6 install path:
	//   - Create framework helper instances (Correlation / ErrorPolicy /
	//     Respond) per Module.
	//   - Validate every Declaration against actor_registry +
	//     type_registry (handler_actor_id existence, binding match,
	//     max_pending_ms required for tool receivers).
	//   - Call Module.Init with the populated ModuleContext.
	//
	// Returns an error wrapping message.InstallReason on validation
	// failure (caller may inspect via errors.As).
	Install(ctx context.Context, modules []Module) error

	// BootRecoverTimers walks the channel's pending tool requests and
	// re-arms F3 timers per L2 §8.6 step 3. Called once after Install
	// returns nil.
	BootRecoverTimers(ctx context.Context) error

	// Dispatch routes one inbound kind=request envelope to the right
	// Module.Handle + auto-registers the F3 timeout timer. Returns the
	// error from Module.Handle (or any framework-internal failure).
	Dispatch(ctx context.Context, env *message.Envelope) error

	// OnExternalCallback dispatches an inbound external callback
	// (webhook body / relay callback frame / WS message) to the named
	// Module.OnExternalCallback. The
	// framework de-dupes duplicate callbacks (terminal already exists)
	// before invoking.
	OnExternalCallback(ctx context.Context, adapterName string, payload []byte) error

	// OnExternalCallbackFrame dispatches an inbound callback with the
	// framework-owned transport wrapper identity preserved. New
	// runtime_inbound_via_relay bridges MUST use this instead of passing
	// payload bytes only.
	OnExternalCallbackFrame(ctx context.Context, adapterName string, frame ExternalCallbackFrame) error

	// OnRuntimeEvent fans a RuntimeEvent out to every installed Module
	// that implements RuntimeEventAware AND matches the event's
	// (ChannelID, AdapterActorID) routing key. Modules without the
	// sub-interface are skipped silently; framework drops events with
	// no matching Module.
	//
	// Errors are aggregated: the first non-nil error from a Module is
	// returned; remaining Modules still receive the event. Implementations
	// MUST NOT panic — a panicking Module is logged and skipped.
	OnRuntimeEvent(ctx context.Context, evt RuntimeEvent) error

	// RunGC starts the periodic GC ticker that scans every adapter's
	// correlation table for expired entries (best-effort cleanup;
	// the F3 timer already covers terminal correctness).
	//
	// MUST be safe to call concurrently with Dispatch / OnExternalCallback.
	// Stops on ctx cancellation.
	RunGC(ctx context.Context)

	// Shutdown stops timers + GC ticker and propagates Module.Shutdown.
	Shutdown(ctx context.Context) error
}
