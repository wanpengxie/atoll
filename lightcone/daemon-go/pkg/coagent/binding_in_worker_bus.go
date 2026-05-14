package coagent

import (
	"context"
	"errors"

	pkgharness "github.com/coagent-ai/daemon-go/pkg/harness"
	"github.com/coagent-ai/daemon-go/pkg/v4types"
)

// inWorkerBusBinding is the in-process binding (L2 §3.6.2). It
// invokes pkgharness.InWorkerBus directly with the worker-owned Deps
// and CallerCtx — same harness chain, no HTTP hop.
type inWorkerBusBinding struct {
	deps      pkgharness.Deps
	callerCtx pkgharness.CallerCtx
	// lookupReq optionally fetches a prior request envelope for
	// `coagent answer`. Workers wire this from their Store.FindByID
	// (or any other store with read access). nil → unsupported.
	lookupReq func(ctx context.Context, requestID string) (*v4types.Envelope, error)
}

// InWorkerBusOptions wires the in-process binding. Deps + CallerCtx
// come from the worker's harness setup; lookupReq is optional but
// recommended when the worker wants `coagent answer` ergonomics.
type InWorkerBusOptions struct {
	// Deps is the harness dependency bundle (Store / Actors / Types /
	// WorkerLocks / Dispatcher / Clock / ChannelID). Required.
	Deps pkgharness.Deps

	// CallerCtx carries the caller's authenticated identity. The
	// binding forwards it verbatim into pkgharness.Write so the
	// harness Step 1 / Step 3 see a trusted ActorID. SendOptions
	// fields from the CLI overlay onto this ctx per call.
	CallerCtx pkgharness.CallerCtx

	// LookupRequest is the optional read-side adapter for
	// `coagent answer`. Workers typically wire this to the same
	// Store.FindByID they use elsewhere. nil disables answer's
	// auto-fill (caller must pass --type / --audience).
	LookupRequest func(ctx context.Context, requestID string) (*v4types.Envelope, error)
}

// NewInWorkerBusBinding constructs the in-process binding.
func NewInWorkerBusBinding(opts InWorkerBusOptions) Binding {
	return &inWorkerBusBinding{
		deps:      opts.Deps,
		callerCtx: opts.CallerCtx,
		lookupReq: opts.LookupRequest,
	}
}

// Send drives pkgharness.InWorkerBus. The per-call SendOptions overlay
// onto the binding-level CallerCtx so the same binding instance can
// serve many turns without rebuilding (trigger context, fencing token,
// etc. change per-turn).
func (b *inWorkerBusBinding) Send(ctx context.Context, env *v4types.Envelope, opts SendOptions) (*SendResult, error) {
	cc := b.callerCtx
	if opts.DeclaredSenderKind != "" {
		cc.DeclaredSenderKind = opts.DeclaredSenderKind
	}
	if opts.FencingToken != 0 {
		cc.FencingToken = opts.FencingToken
	}
	if opts.TriggerCorrelationID != "" {
		cc.Trigger = &pkgharness.TriggerCtx{CorrelationID: opts.TriggerCorrelationID}
	}
	if opts.ExplicitCorrelationID != "" {
		cc.ExplicitCorrelationID = opts.ExplicitCorrelationID
	}

	res, err := pkgharness.InWorkerBus(ctx, b.deps, env, cc)
	if err != nil {
		// Non-RejectError: real infra failure (sql / ctx). Surface as-is.
		return nil, err
	}
	if !res.OK {
		if res.Error == nil {
			return nil, errors.New("in_worker_bus: empty reject (binding bug)")
		}
		return nil, &RejectError{
			Reason:             res.Error.Reason,
			Detail:             res.Error.Detail,
			MessageIDIfPartial: res.Error.MessageIDIfPartial,
			DedupeResponseID:   res.Error.DedupeResponseID,
			// HTTPStatus stays 0 — in_worker_bus has no HTTP.
		}
	}
	if res.Value == nil {
		return nil, errors.New("in_worker_bus: nil Value on OK result (binding bug)")
	}
	return &SendResult{
		ID:            res.Value.ID,
		CorrelationID: res.Value.CorrelationID,
		Kind:          res.Value.Kind,
		Dedupe:        res.Value.Dedupe,
	}, nil
}

// LookupRequest forwards to the worker-supplied adapter when wired;
// otherwise reports unsupported (CLI then requires explicit flags).
func (b *inWorkerBusBinding) LookupRequest(ctx context.Context, requestID string) (*v4types.Envelope, bool, error) {
	if b.lookupReq == nil {
		// Fall back to the Store interface — every in_worker_bus binding
		// has it wired anyway, and FindByID matches the read shape we
		// need for `coagent answer`.
		if b.deps.Store == nil {
			return nil, false, nil
		}
		env, err := b.deps.Store.FindByID(ctx, requestID)
		if err != nil {
			return nil, false, err
		}
		if env == nil {
			return nil, false, nil
		}
		return env, true, nil
	}
	env, err := b.lookupReq(ctx, requestID)
	if err != nil {
		return nil, false, err
	}
	if env == nil {
		return nil, false, nil
	}
	return env, true, nil
}

// ResolveHandlerActorID consults pkg/harness.TypeLookup. Returns
// ("", false, nil) when the type is unknown (no row in type_registry)
// or handler_actor_id is empty.
func (b *inWorkerBusBinding) ResolveHandlerActorID(_ context.Context, typeName string) (string, bool, error) {
	if b.deps.Types == nil {
		return "", false, nil
	}
	info, ok := b.deps.Types.Get(typeName)
	if !ok || info == nil {
		return "", false, nil
	}
	if info.HandlerActorID == "" {
		return "", false, nil
	}
	return info.HandlerActorID, true, nil
}
