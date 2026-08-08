package runtime

import (
	"fmt"
	"time"

	"github.com/wanpengxie/atoll/drivers/agents/base"
	"github.com/wanpengxie/atoll/drivers/agents/driverproto"
)

// Def produces the only production Base runtime definition for a provider.
func Def(provider driverproto.Provider) (base.Definition, error) {
	return DefWithPolicy(provider, DefaultPolicy())
}

func DefWithPolicy(provider driverproto.Provider, policy Policy) (base.Definition, error) {
	if provider == nil {
		return base.Definition{}, fmt.Errorf("agent/runtime: provider required")
	}
	spec := provider.Spec()
	if spec.Name == "" {
		return base.Definition{}, fmt.Errorf("agent/runtime: provider name required")
	}
	policy = policy.normalized()
	receipt := 20 * time.Minute
	for _, bound := range []time.Duration{policy.OpenCall + policy.Started, policy.ControlCall + policy.InterruptEnded, policy.Reap + policy.OpenCall} {
		if bound >= receipt {
			receipt = bound + time.Second
		}
	}
	cfg := base.Config{
		ReceiptDeadline: receipt,
		Runtime: base.RuntimeSpec{Describe: spec.Describe, Capabilities: base.RuntimeCapabilities{
			Steer: spec.Capabilities.Steer, Interrupt: spec.Capabilities.Interrupt, Resume: spec.Capabilities.Resume,
		}},
		NewRuntime: func(deps base.RuntimeDeps, seed []byte, events base.RuntimeEvents) (base.Runtime, error) {
			adapter, err := provider.NewAdapter()
			if err != nil {
				return nil, err
			}
			return New(adapter, spec, policy, deps, seed, events)
		},
	}
	doc := spec.Describe.SkillDoc
	if doc == "" {
		doc = spec.Describe.Description
	}
	return base.Def(doc, cfg)
}
