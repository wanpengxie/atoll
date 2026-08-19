package script

import (
	"github.com/wanpengxie/atoll/drivers/agents/driverproto"
)

type provider struct {
	toolID   string
	toolType string
}

func NewProvider(toolID string) driverproto.Provider {
	return NewProviderForTool(toolID, defaultToolType)
}

func NewProviderForTool(toolID, toolType string) driverproto.Provider {
	if toolType == "" {
		toolType = defaultToolType
	}
	return provider{toolID: toolID, toolType: toolType}
}
func (provider) Spec() driverproto.ProviderSpec {
	return driverproto.ProviderSpec{
		Name:         Class,
		Capabilities: map[string]bool{driverproto.CapabilityFork: true},
		Documentation: driverproto.Documentation{
			Description: ActorDoc,
			SkillDoc:    "# script\n\nDeterministic regression provider.",
		},
	}
}
func (p provider) NewWorker(h driverproto.WorkerHost) (driverproto.Worker, error) {
	return newWorker(p.toolID, p.toolType, h), nil
}

var _ driverproto.Provider = provider{}
