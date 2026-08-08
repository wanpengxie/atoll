package script

import (
	"github.com/wanpengxie/atoll/drivers/agents/driverproto"
	"github.com/wanpengxie/atoll/lib/introspect"
	"github.com/wanpengxie/atoll/protocol/message"
)

type provider struct{ toolID string }

func NewProvider(toolID string) driverproto.Provider { return provider{toolID: toolID} }
func (provider) Spec() driverproto.ProviderSpec {
	return driverproto.ProviderSpec{
		Name: "script",
		Describe: introspect.Describe{
			Description: actorDoc,
			SkillDoc:    "# script\n\nDeterministic regression provider.",
			Types: map[string]introspect.TypeMeta{
				TypeChat:   {Description: "call echo and persist payload", AllowedKinds: []string{string(message.KindRequest)}},
				TypeVerify: {Description: "verify persisted resource", AllowedKinds: []string{string(message.KindRequest)}},
			},
		},
	}
}
func (p provider) NewAdapter() (driverproto.Adapter, error) { return adapter{toolID: p.toolID}, nil }

type adapter struct{ toolID string }

func (a adapter) NewWorker(h driverproto.WorkerHost) (driverproto.Worker, error) {
	return newWorker(a.toolID, h), nil
}

var _ driverproto.Provider = provider{}
var _ driverproto.Adapter = adapter{}
