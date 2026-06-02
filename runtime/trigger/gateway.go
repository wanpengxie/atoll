package trigger

import (
	"context"
	"errors"

	"github.com/wanpengxie/ActOS/kernel/actor"
	"github.com/wanpengxie/ActOS/kernel/message"
	"github.com/wanpengxie/ActOS/runtime/storespec"
)

// Deliverer is the minimal Deliver(audience, env) surface the gateway
// invokes. It is satisfied by *runtime/actorrt.Runtime (the per-channel
// cell runtime); we declare a local interface so unit tests can
// substitute a counting fake without importing the runtime machinery.
type Deliverer interface {
	Deliver(ctx context.Context, audience []actor.ActorID, env *message.Envelope) error
}

// Gateway is the post-harness fan-out seam wired into the daemon's
// write path. After the harness chain returns Accepted=true, daemon /
// workerhost / control callers invoke Gateway.Dispatch with the same
// envelope; the gateway runs Resolve and hands the result to the
// Deliverer for immediate fan-out.
type Gateway struct {
	registry storespec.Registry
	deliver  Deliverer
}

// Config wires a Gateway.
type Config struct {
	Registry  storespec.Registry
	Deliverer Deliverer
}

// New builds a Gateway.
func New(cfg Config) (*Gateway, error) {
	if cfg.Registry == nil {
		return nil, errors.New("trigger: Config.Registry nil")
	}
	if cfg.Deliverer == nil {
		return nil, errors.New("trigger: Config.Deliverer nil")
	}
	return &Gateway{
		registry: cfg.Registry,
		deliver:  cfg.Deliverer,
	}, nil
}

// DispatchResult describes the outcome of one Gateway.Dispatch call.
type DispatchResult struct {
	// Audience is the resolved actor set (post §5.1). nil for skipped
	// envelopes (system.heartbeat / visibility=system / empty audience).
	Audience []actor.ActorID
}

// Dispatch is the seam that the daemon's harness adapter invokes once an
// envelope has been durably appended. It runs Resolve and hands the
// resolved audience to the Deliverer for immediate fan-out. Deliver
// errors are returned so callers can preserve retryability per L1 §6.1.
func (g *Gateway) Dispatch(ctx context.Context, env *message.Envelope) (DispatchResult, error) {
	if env == nil {
		return DispatchResult{}, errors.New("trigger: dispatch nil envelope")
	}
	audience, err := Resolve(ctx, env, g.registry)
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
