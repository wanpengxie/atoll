package runtime

import (
	"fmt"
	"time"

	"github.com/wanpengxie/atoll/drivers/agents/driverproto"
	"github.com/wanpengxie/atoll/drivers/agents/runtimeproto"
)

// Build returns the production Runtime factory and its immutable Base-facing
// specification. Runtime never constructs a Base definition.
func Build(provider driverproto.Provider, policy Policy) (runtimeproto.Factory, runtimeproto.Spec, error) {
	if provider == nil {
		return nil, runtimeproto.Spec{}, fmt.Errorf("agent/runtime: provider required")
	}
	ps := provider.Spec()
	if ps.Name == "" {
		return nil, runtimeproto.Spec{}, fmt.Errorf("agent/runtime: provider name required")
	}
	policy = policy.normalized()
	if policy.CommandCapacity < 2 {
		return nil, runtimeproto.Spec{}, fmt.Errorf("agent/runtime: command capacity must be at least 2")
	}
	receipt := 20 * time.Minute
	for _, bound := range []time.Duration{policy.OpenFactDeadline + policy.ReapedDemand, policy.StartFactDeadline + policy.ReapedDemand, policy.ControlFactDeadline + policy.InterruptEnded} {
		if bound >= receipt {
			receipt = bound + time.Second
		}
	}
	spec := runtimeproto.Spec{
		Describe:     ps.Describe,
		Capabilities: runtimeproto.Capabilities{Steer: ps.Capabilities.Steer, Interrupt: ps.Capabilities.Interrupt, Resume: ps.Capabilities.Resume},
		Bounds:       runtimeproto.Bounds{ReceiptDeadline: receipt, EventCapacity: policy.EventCapacity},
	}
	factory := func(deps runtimeproto.Deps, seed []byte, events runtimeproto.Events) (runtimeproto.Runtime, error) {
		return newEngine(provider, ps, policy, deps, seed, events)
	}
	return factory, spec, nil
}

func Default(provider driverproto.Provider) (runtimeproto.Factory, runtimeproto.Spec, error) {
	return Build(provider, DefaultPolicy())
}
