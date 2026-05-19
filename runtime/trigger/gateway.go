package trigger

import (
	"context"
	"errors"
	"time"

	"github.com/wanpengxie/ActOS/kernel/actor"
	"github.com/wanpengxie/ActOS/kernel/actorreg"
	"github.com/wanpengxie/ActOS/kernel/message"
	"github.com/wanpengxie/ActOS/runtime/scheduler"
)

// Deliverer is the minimal Deliver(audience, env) surface the gateway
// invokes. It is satisfied by *runtime/scheduler.Deliverer; we declare a
// local interface so unit tests can substitute a counting fake without
// importing the scheduler timer machinery.
type Deliverer interface {
	Deliver(ctx context.Context, audience []actor.ActorID, env *message.Envelope) error
}

// Compile-time check: the production Deliverer must satisfy our
// interface.
var _ Deliverer = (*scheduler.Deliverer)(nil)

// Gateway is the post-harness fan-out seam wired into the daemon's
// write path. After the harness chain returns Accepted=true, daemon /
// workerhost / control callers invoke Gateway.Dispatch with the same
// envelope; the gateway runs Resolve and hands the result to the
// scheduler Deliverer.
type Gateway struct {
	registry actorreg.Registry
	deliver  Deliverer
	nowFn    func() int64
}

// Config wires a Gateway.
type Config struct {
	Registry  actorreg.Registry
	Deliverer Deliverer
	// NowFn returns unix-ms. Defaults to time.Now when nil. Used by
	// Dispatch to decide whether NotBefore has passed (L1 §5.3
	// future-message bypass).
	NowFn func() int64
}

// New builds a Gateway.
func New(cfg Config) (*Gateway, error) {
	if cfg.Registry == nil {
		return nil, errors.New("trigger: Config.Registry nil")
	}
	if cfg.Deliverer == nil {
		return nil, errors.New("trigger: Config.Deliverer nil")
	}
	if cfg.NowFn == nil {
		cfg.NowFn = func() int64 { return time.Now().UnixMilli() }
	}
	return &Gateway{
		registry: cfg.Registry,
		deliver:  cfg.Deliverer,
		nowFn:    cfg.NowFn,
	}, nil
}

// DispatchResult describes the outcome of one Gateway.Dispatch call.
// Callers (daemon adapter / scheduler scan) use it to decide whether to
// also stamp messages.delivered_at: deferred-future messages must NOT
// be marked delivered yet.
type DispatchResult struct {
	// Audience is the resolved actor set (post §5.1). nil for skipped
	// envelopes (system.heartbeat / visibility=system / deferred future).
	Audience []actor.ActorID

	// Deferred reports whether the envelope was skipped because
	// `NotBefore > now`. The scheduler periodic scan will pick it up
	// later (L1 §5.3).
	Deferred bool
}

// Dispatch is the seam that the daemon's harness adapter / scheduler
// scan invokes once an envelope has been durably appended.
//
// Semantics:
//
//   - env.NotBefore > now  → return {Deferred=true, Audience=nil}; the
//     caller MUST NOT mark messages.delivered_at — scheduler will pick
//     this row up via PendingDue once not_before passes.
//   - else                 → run Resolve; invoke deliverer.Deliver; return
//     the resolved audience. Deliver errors are returned so callers keep
//     messages.delivered_at NULL and preserve retryability per L1 §6.1.
func (g *Gateway) Dispatch(ctx context.Context, env *message.Envelope, opts Options) (DispatchResult, error) {
	if env == nil {
		return DispatchResult{}, errors.New("trigger: dispatch nil envelope")
	}
	if env.NotBefore != nil && *env.NotBefore > g.nowFn() {
		return DispatchResult{Deferred: true}, nil
	}
	audience, err := Resolve(ctx, env, g.registry, opts)
	if err != nil {
		return DispatchResult{}, err
	}
	if len(audience) == 0 {
		return DispatchResult{Audience: nil}, nil
	}
	if err := g.deliver.Deliver(ctx, audience, env); err != nil {
		return DispatchResult{Audience: audience}, err
	}
	return DispatchResult{Audience: audience}, nil
}
