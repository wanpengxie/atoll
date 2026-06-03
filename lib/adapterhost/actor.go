package adapterhost

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/wanpengxie/ActOS/kernel/actor"
	"github.com/wanpengxie/ActOS/kernel/channel"
	"github.com/wanpengxie/ActOS/kernel/message"
	"github.com/wanpengxie/ActOS/lib/behavior"
	"github.com/wanpengxie/ActOS/lib/introspect"
	"github.com/wanpengxie/ActOS/runtime/actorrt"
	rtharness "github.com/wanpengxie/ActOS/runtime/harness"
)

// tickType is the internal self-schedule signal an adapterActor delivers to
// itself (NOT a protocol type — it never leaves the cell). Receive intercepts
// it to run heartbeat + the in-flight reaper on the cell goroutine.
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

	// Observability (injected from the edge; logger is the std slog facade,
	// metrics is the behavior.Metrics seam).
	logger  *slog.Logger
	metrics behavior.Metrics

	// channelID is the channel this adapter services.
	channelID channel.ID

	// chain is the harness write path (runtime/harness.Writer — the consumer-side
	// write contract; the concrete *harness.Chain (server) or an UplinkChain
	// (compute) is injected by the installer). The adapter writes
	// terminals/events through it.
	chain rtharness.Writer

	// lookup recovers the original request envelope by id (F5; behaviour's
	// consumer-side RequestLookup, satisfied by the runtime store).
	lookup behavior.RequestLookup

	// clock stamps response ts.
	clock func() time.Time

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
	// they touch a.inflight/a.chain on the cell goroutine — no god-object).
	mctx *behavior.ModuleContext
}

// --- in-flight request tracking (lock-free; cell goroutine is sole caller) ---
// The cached request envelope IS the pending tracker — no parallel correlation
// structure. No mutex: every call arrives on the cell goroutine via Receive, so
// access is already serial.

// remember caches the request envelope, registering it as in-flight: the cached
// envelope is the cell's single source of truth for the pending request (id /
// expires_at / sender), used to build responses without a truth lookup and
// iterated by the reaper. markDone removes it on the terminal write.
func (a *adapterActor) remember(env *message.Envelope) {
	if a.inflight == nil {
		a.inflight = map[behavior.CorrelationKey]*message.Envelope{}
	}
	a.inflight[behavior.CorrelationKey(env.ID)] = env
}

// markDone drops the in-flight request once its terminal is written. Absence
// from a.inflight IS the "done" state — no parallel entry to flip.
func (a *adapterActor) markDone(id behavior.CorrelationKey) {
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

// writeCtx stamps the adapter's OWN caller identity onto ctx so the home harness
// ACL (steps 0/1/3) authenticates the write as this adapter actor. Every chain
// write the cell makes (respond/provisional/event/internal-error) goes through
// here — without it the harness rejects the write as harness_engine_acl_denied
// "missing caller context" and the caller hangs forever. For a compute cell
// whose chain is an UplinkChain this is harmless: ctx is not serialised over the
// wire, so the home's fleet re-stamps from EmitFrame.Source on arrival.
func (a *adapterActor) writeCtx(ctx context.Context) context.Context {
	return rtharness.CtxWithCaller(ctx, rtharness.CallerContext{
		ActorID: a.self, ChannelID: a.channelID,
	})
}

// --- actorrt.Actor ---

// Receive dispatches one envelope SERIALLY on the cell goroutine (collapse of
// Manager.Dispatch manager.go:887). NO sticky-readiness gate: dispatch is dumb
// delivery; a not-ready adapter self-answers receiver_unavailable; reachability
// is the OUTCOME of send→terminal, never a stored gate (P15/P16).
func (a *adapterActor) Receive(ctx context.Context, env *message.Envelope) error {
	if env.Type == tickType {
		// Internal self-tick (heartbeat + reaper), runs on the cell goroutine.
		a.onTick()
		return nil
	}
	if env.Kind != message.KindRequest {
		// Non-request envelopes addressed to an adapter are ignored at this
		// seam (the adapter is a request/reply driver). An adapter that drives an
		// external resource folds inbound results back by self-delivering an
		// envelope onto its own cell (ActorContext.Deliver) and routing it here —
		// that is the adapter's own concern, not a framework callback.
		return nil
	}
	a.remember(env) // cache request so respond works without a truth lookup
	switch env.Type {
	case introspect.QueryDescribe:
		return a.respondDescribe(ctx, env)
	default:
		return a.handleRequest(ctx, env)
	}
}

// handleRequest is the declared-type path: hand the envelope to module.Handle
// (the request is already cached in-flight by Receive). The terminal is produced
// later by the module via mctx.Respond (or, on a caller timeout, by the caller's
// caller-scoped closure — lib/behavior). A Handle error that is not the
// deferred sentinel collapses to a receiver_internal_error terminal.
func (a *adapterActor) handleRequest(ctx context.Context, env *message.Envelope) error {
	key := behavior.CorrelationKey(env.ID)
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
	if _, werr := a.chain.Write(a.writeCtx(ctx), term); werr != nil {
		return werr
	}
	a.markDone(key)
	return nil
}

// respondDescribe self-answers the reserved actor.describe request. The
// capability surface is the ACTOR's dynamic answer: if the module implements
// introspect.Describer it is asked live (so it reports its CURRENT APIs); otherwise
// the answer is identity only. No predefined type list / catalog.
func (a *adapterActor) respondDescribe(ctx context.Context, env *message.Envelope) error {
	resp := introspect.Describe{
		Name:    a.declaration.Name,
		Binding: string(a.declaration.Binding),
	}
	if d, ok := a.module.(introspect.Describer); ok {
		apis, err := d.Describe(ctx)
		if err != nil {
			return fmt.Errorf("adapterhost: module.Describe: %w", err)
		}
		resp.APIs = apis
	}
	payload, err := json.Marshal(resp)
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

// onTick runs the self-scheduled maintenance on the cell goroutine: reap
// expired in-flight requests (bound memory). That is all the tick is for now.
func (a *adapterActor) onTick() {
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

// tickInterval is the binding-specific self-schedule cadence for the reaper.
func (a *adapterActor) tickInterval() time.Duration {
	switch a.declaration.Binding {
	case actor.BindingRuntimeOutbound:
		return 15 * time.Second
	case actor.BindingRuntimeInboundViaRelay:
		return 30 * time.Second
	default:
		return 60 * time.Second
	}
}
