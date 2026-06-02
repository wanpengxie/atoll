package adapterhost

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/wanpengxie/ActOS/kernel/actor"
	"github.com/wanpengxie/ActOS/kernel/channel"
	"github.com/wanpengxie/ActOS/kernel/harness"
	"github.com/wanpengxie/ActOS/kernel/message"
	"github.com/wanpengxie/ActOS/lib/behavior"
	"github.com/wanpengxie/ActOS/runtime/actorrt"
)

// tickType is the internal self-schedule signal an adapterActor delivers to
// itself (NOT a protocol type — it never leaves the cell). Receive intercepts
// it to run heartbeat + correlation reaper on the cell goroutine.
const tickType = "adapterhost.__tick__"

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

	// channelID is the channel this adapter services.
	channelID channel.ID

	// chain is the harness write path (kernel/harness.Chain INTERFACE — pure
	// contract; the runtime/harness impl is injected by the installer). The
	// adapter writes terminals/events through it.
	chain harness.Chain

	// lookup recovers the original request envelope by id (F5; kernel/message
	// contract, impl in runtime/store).
	lookup message.RequestLookup

	// clock stamps response ts.
	clock func() time.Time

	// forward is the transport-backed external-request seam (relay adapters
	// like proxyfacade), injected by the installer (daemon wires the device
	// transit). nil for adapters that never forward.
	forward behavior.ExternalRequestFunc

	// futures is the channel-level caller-side future hub (Submit/Await/Watch),
	// injected by the installer. nil for pure receiver adapters.
	futures callerFutures

	// inflight caches the dispatched request envelope per pending correlation.
	// A compute cell has NO local truth to look up, so it builds responses from
	// this cached request (BuildResponseFromRequest) instead of lookup. Plain
	// map — cell goroutine sole owner, no lock. Cleared on markDone.
	inflight map[behavior.CorrelationKey]*message.Envelope

	// selfCtx is the substrate handle for self-scheduling (heartbeat + reaper
	// tick delivered via selfCtx.Deliver). Set in Start. The ticker goroutine
	// only ENQUEUES the tick; the actual heartbeat/reaper work runs serially in
	// Receive on the cell goroutine (no lock on logical state).
	selfCtx actorrt.ActorContext
	// stopTick stops the self-schedule loop on Stop.
	stopTick chan struct{}
	// tickEvery overrides the self-schedule cadence (0 → binding default). Set by
	// the installer; tests use a short interval.
	tickEvery time.Duration

	// mctx is the ModuleContext handed to module.Init (built in Start; its
	// Respond/Fail/Provisional/EmitEvent seams close over THIS adapterActor so
	// they touch a.correlation/a.chain on the cell goroutine — no god-object).
	mctx *behavior.ModuleContext
}

// callerFutures is the minimal caller-side surface adapterActor needs from the
// channel-level futureHub (lib/behavior/futurereg). Kept as a local interface
// so the cell depends only on what it uses.
type callerFutures interface {
	Submit(ctx context.Context, env *message.Envelope) (message.ID, error)
	Await(ctx context.Context, id message.ID, timeout time.Duration) (*message.Envelope, error)
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

// remember caches the request envelope so a compute cell can build responses
// without a truth lookup (cleared on markDone).
func (a *adapterActor) remember(env *message.Envelope) {
	if a.inflight == nil {
		a.inflight = map[behavior.CorrelationKey]*message.Envelope{}
	}
	a.inflight[behavior.CorrelationKey(env.ID)] = env
}

func (a *adapterActor) markDone(id behavior.CorrelationKey) {
	if e, ok := a.correlation[id]; ok {
		e.State = behavior.CorrelationDone
		a.correlation[id] = e
	}
	delete(a.inflight, id)
}

// buildResponse builds a response envelope, preferring the cached in-flight
// request (compute self-contained, no truth lookup) and falling back to the
// lookup seam (server-side adapters that have local truth).
func (a *adapterActor) buildResponse(ctx context.Context, requestID behavior.CorrelationKey, sender message.Sender, spec behavior.ResponseSpec) (*message.Envelope, error) {
	if req, ok := a.inflight[requestID]; ok {
		return behavior.BuildResponseFromRequest(req, a.clock, sender, requestID, spec)
	}
	if a.lookup != nil {
		return behavior.BuildResponseEnvelope(ctx, a.lookup, a.clock, sender, requestID, spec)
	}
	return nil, fmt.Errorf("adapterhost: request %s neither cached nor lookupable", requestID)
}

// --- actorrt.Actor ---

// Receive dispatches one envelope SERIALLY on the cell goroutine (collapse of
// Manager.Dispatch manager.go:887). NO sticky-readiness gate: dispatch is dumb
// delivery; a not-ready adapter self-answers receiver_unavailable; reachability
// is the OUTCOME of send→terminal, never a stored gate (P15/P16).
func (a *adapterActor) Receive(ctx context.Context, env *message.Envelope) error {
	if env.Type == tickType {
		// Internal self-tick (heartbeat + reaper), runs on the cell goroutine.
		a.onTick(ctx)
		return nil
	}
	if env.Kind != message.KindRequest {
		// Non-request envelopes addressed to an adapter are ignored at this
		// seam (the adapter is a request/reply driver). Out-of-band signals
		// (device callback/lifecycle/heartbeat) arrive via Post/Ask closures,
		// not as protocol envelopes.
		return nil
	}
	a.remember(env) // cache request so respond works without a truth lookup
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
	key := behavior.CorrelationKey(env.ID)
	if !declAllowsRequest(a.declaration, env.Type) {
		// Type the adapter does not handle → fail-fast terminal (don't leave
		// the caller hanging on a request this actor structurally can't answer).
		return a.collapseInternalError(ctx, key, fmt.Sprintf("type %s not request-capable for adapter %s", env.Type, a.declaration.Name))
	}
	var exp int64
	if env.ExpiresAt != nil {
		exp = *env.ExpiresAt
	}
	a.reserve(behavior.CorrelationEntry{RequestID: key, ExpiresAt: exp})
	if a.metrics != nil {
		a.metrics.IncCounter("adapter.dispatch", "adapter", a.declaration.Name, "type", env.Type)
	}
	err := a.module.Handle(ctx, env)
	if err == nil || errors.Is(err, behavior.ErrHandleDeferred) {
		// Success (module already Responded) or deferred (terminal arrives later
		// via callback/Resolve — keep pending, caller timer / death bounds it).
		return nil
	}
	// Hard Handle error → collapse to a receiver_internal_error terminal
	// (receiver-authored, author #1). The caller MUST NOT hang on a Handle that
	// errored; correlation MUST NOT stay pending.
	if a.logger != nil {
		a.logger.Warn("adapterhost.handle.error", "adapter", a.declaration.Name, "type", env.Type, "request", string(env.ID), "err", err.Error())
	}
	return a.collapseInternalError(ctx, key, err.Error())
}

// collapseInternalError writes a receiver_internal_error final terminal for a
// request the module could not handle, and marks the correlation done.
func (a *adapterActor) collapseInternalError(ctx context.Context, key behavior.CorrelationKey, detail string) error {
	sender := message.Sender{Kind: actor.KindTool, ID: a.self}
	payload, _ := json.Marshal(map[string]any{"detail": detail})
	term, berr := a.buildResponse(ctx, key, sender, behavior.ResponseSpec{
		Status:  "failed",
		Reason:  string(message.TerminalReceiverInternalError),
		Payload: payload,
	})
	if berr != nil {
		return berr
	}
	if _, werr := a.chain.Write(ctx, term); werr != nil {
		return werr
	}
	a.markDone(key)
	return nil
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

// onTick runs the self-scheduled maintenance on the cell goroutine: poll the
// module's Heartbeater (fold readiness) and reap expired correlation (bounded).
func (a *adapterActor) onTick(ctx context.Context) {
	_ = a.RunHeartbeat(ctx)
	a.reapExpired(a.clock().UnixMilli())
}

// tickLoop is the adapter's self-schedule ticker (it owns its own ticker per
// dismantle §2.5-A — the substrate does NOT add a timer primitive). The
// goroutine only ENQUEUES a tick via Deliver; all state work runs serially in
// Receive. Stops on stopTick.
func (a *adapterActor) tickLoop(interval time.Duration, stop chan struct{}) {
	t := time.NewTicker(interval)
	defer t.Stop()
	tick := &message.Envelope{Kind: message.KindEvent, Type: tickType, ChannelID: a.channelID}
	for {
		select {
		case <-stop:
			return
		case <-t.C:
			_ = a.selfCtx.Deliver(tick) // full-mailbox tick is droppable
		}
	}
}

// heartbeatInterval is the binding-specific self-schedule cadence.
func (a *adapterActor) heartbeatInterval() time.Duration {
	switch a.declaration.Binding {
	case actor.BindingRuntimeOutbound:
		return 15 * time.Second
	case actor.BindingRuntimeInboundViaRelay:
		return 30 * time.Second
	default:
		return 60 * time.Second
	}
}
