package script

import (
	"github.com/wanpengxie/atoll/drivers/agents/driverproto"
	"github.com/wanpengxie/atoll/lib/introspect"
	"github.com/wanpengxie/atoll/protocol/message"
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
		Name: Class,
		Describe: introspect.Describe{
			Description: ActorDoc,
			SkillDoc:    "# script\n\nDeterministic regression provider.",
			Types: map[string]introspect.TypeMeta{
				TypeChat:   {Description: "call echo and persist payload", AllowedKinds: []string{string(message.KindRequest)}},
				TypeVerify: {Description: "verify persisted resource", AllowedKinds: []string{string(message.KindRequest)}},
			},
		},
	}
}
func (p provider) NewWorker(h driverproto.WorkerHost) (driverproto.Worker, error) {
	return newWorker(p.toolID, p.toolType, h), nil
}

var _ driverproto.Provider = provider{}
