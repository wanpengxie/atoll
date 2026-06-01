package adapterhost

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/wanpengxie/ActOS/kernel/actor"
	"github.com/wanpengxie/ActOS/kernel/message"
	"github.com/wanpengxie/ActOS/lib/behavior"
)

// adapterActor is one adapter hosted as a real serial actor cell. It is the
// collapse of adapters/framework.{Manager per-adapter slice + boundModule +
// memoryCorrelationTracker + timerPolicy + responseRouter receiver side} into
// ONE object whose single cell goroutine is the sole owner of all logical
// state — so every field below is a PLAIN field with NO mutex/atomic
// (dismantle-spec §1; the mailbox IS the serialization).
//
// It implements runtime/actorrt.Actor (Receive) + Starter/Stopper. The host
// (daemon/host in v2) spawns one cell per adapter via the installer.
type adapterActor struct {
	// Identity + static metadata (was boundModule.{module, declaration}).
	self        actor.ActorID
	module      behavior.Module
	declaration behavior.Declaration

	// Receiver-side correlation — INLINED as a plain map, NO lock (was
	// memoryCorrelationTracker.{mu, entries}; the cell goroutine serialises
	// all access). request_id → entry.
	correlation map[behavior.CorrelationKey]behavior.CorrelationEntry

	// Readiness — plain field (was actor_registry.ready_state + Manager.
	// readinessMu; the actor owns its readiness and emits
	// actor.readiness.changed itself). Projection in the registry is a
	// downstream consumer, never read back as a dispatch gate.
	readiness actor.Readiness

	// Observability (injected; behavior interfaces, impl from obs via cmd).
	logger  behavior.Logger
	metrics behavior.Metrics

	// state is the adapter's persistent KV seam (was boundModule.state).
	state behavior.StateStore

	// mctx is the ModuleContext handed to module.Init (built by the installer;
	// its Respond/Provisional/Resolve seams write terminals through the
	// channel harness). Populated in Start.
	mctx *behavior.ModuleContext
}

// --- correlation (inlined, lock-free; cell goroutine is sole caller) ---
// These replace memoryCorrelationTracker. No mutex: every call arrives on the
// cell goroutine via Receive/Ask, so access is already serial.

func (a *adapterActor) reserve(e behavior.CorrelationEntry) behavior.CorrelationEntry {
	if existing, ok := a.correlation[e.RequestID]; ok {
		return existing // idempotent
	}
	if e.State == "" {
		e.State = behavior.CorrelationPending
	}
	a.correlation[e.RequestID] = e
	return e
}

func (a *adapterActor) correlationGet(id behavior.CorrelationKey) (behavior.CorrelationEntry, bool) {
	e, ok := a.correlation[id]
	return e, ok
}

func (a *adapterActor) markDone(id behavior.CorrelationKey) {
	if e, ok := a.correlation[id]; ok {
		e.State = behavior.CorrelationDone
		a.correlation[id] = e
	}
}

// --- actorrt.Actor ---

// Receive dispatches one envelope SERIALLY on the cell goroutine (collapse of
// Manager.Dispatch manager.go:887). NO sticky-readiness gate: dispatch is dumb
// delivery; a not-ready adapter self-answers receiver_unavailable; reachability
// is the OUTCOME of send→terminal, never a stored gate (P15/P16).
func (a *adapterActor) Receive(ctx context.Context, env *message.Envelope) error {
	if env.Kind != message.KindRequest {
		// Non-request envelopes addressed to an adapter are ignored at this
		// seam (the adapter is a request/reply driver). Out-of-band signals
		// (device callback/lifecycle/heartbeat) arrive via Post/Ask closures,
		// not as protocol envelopes.
		return nil
	}
	switch env.Type {
	case "actor.status":
		return a.respondStatus(ctx, env)
	case "actor.describe":
		return a.respondDescribe(ctx, env)
	default:
		return a.handleRequest(ctx, env)
	}
}

// handleRequest is the declared-type path: reserve the pending correlation then
// hand the envelope to module.Handle. The terminal is produced later by the
// module via mctx.Respond/Resolve (or, on a caller timeout, by the caller's
// caller-scoped closure — lib/behavior). A Handle error that is not the
// deferred sentinel collapses to a receiver_internal_error terminal.
func (a *adapterActor) handleRequest(ctx context.Context, env *message.Envelope) error {
	if !declAllowsRequest(a.declaration, env.Type) {
		return fmt.Errorf("adapterhost: type %s not request-capable for adapter %s",
			env.Type, a.declaration.Name)
	}
	a.reserve(behavior.CorrelationEntry{RequestID: behavior.CorrelationKey(env.ID)})
	if a.metrics != nil {
		a.metrics.IncCounter("adapter.dispatch", "adapter", a.declaration.Name, "type", env.Type)
	}
	return a.module.Handle(ctx, env)
}

// respondStatus self-answers the reserved actor.status request (collapse of
// Manager.respondActorStatus manager.go:1015). Reads the actor's OWN readiness
// (no registry round-trip, no dispatch gate). Minimal projection for now;
// TODO(next): StatusReporter live enrichment (manager.go:1041) +
// last_ready_at/last_state_change_at/detail/checked_at fields.
func (a *adapterActor) respondStatus(ctx context.Context, env *message.Envelope) error {
	a.reserve(behavior.CorrelationEntry{RequestID: behavior.CorrelationKey(env.ID)})
	r := a.readiness.Normalize()
	payload, err := json.Marshal(map[string]any{
		"available": r.IsReady(),
		"reason":    r.Reason,
		"kind":      string(a.declaration.Binding),
	})
	if err != nil {
		return fmt.Errorf("adapterhost: respondStatus marshal: %w", err)
	}
	return a.selfRespond(ctx, env, payload)
}

// respondDescribe self-answers the reserved actor.describe request (collapse of
// Manager.respondActorDescribe manager.go:1095). Minimal Declaration projection
// for now; TODO(next): DeclarationCatalog type_declarations + skill_doc +
// per-type filter.
func (a *adapterActor) respondDescribe(ctx context.Context, env *message.Envelope) error {
	a.reserve(behavior.CorrelationEntry{RequestID: behavior.CorrelationKey(env.ID)})
	payload, err := json.Marshal(map[string]any{
		"name":        a.declaration.Name,
		"description": a.declaration.Description,
		"types":       a.declaration.Types,
	})
	if err != nil {
		return fmt.Errorf("adapterhost: respondDescribe marshal: %w", err)
	}
	return a.selfRespond(ctx, env, payload)
}

// selfRespond writes a terminal response through the ModuleContext Respond seam
// (built by the installer) and marks the correlation done. Shared by the
// reserved self-answer paths.
func (a *adapterActor) selfRespond(ctx context.Context, env *message.Envelope, payload json.RawMessage) error {
	if a.mctx == nil || a.mctx.Respond == nil {
		return fmt.Errorf("adapterhost: selfRespond before Init (mctx not built)")
	}
	key := behavior.CorrelationKey(env.ID)
	if _, err := a.mctx.Respond(ctx, key, payload, behavior.RespondOptions{}); err != nil {
		return err
	}
	a.markDone(key)
	return nil
}

// declAllowsRequest reports whether the declaration accepts type as a request.
func declAllowsRequest(decl behavior.Declaration, typ string) bool {
	for _, t := range decl.Types {
		if t == typ {
			return true
		}
	}
	return false
}
