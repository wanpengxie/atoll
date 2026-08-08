package codex

import (
	"github.com/wanpengxie/atoll/drivers/agents/driverproto"
	"github.com/wanpengxie/atoll/lib/introspect"
)

type provider struct{ cfg Config }

func NewProvider(cfg Config) driverproto.Provider { return provider{cfg: cfg} }
func (provider) Spec() driverproto.ProviderSpec {
	return driverproto.ProviderSpec{Name: Class, Capabilities: driverproto.Capabilities{Steer: true, Interrupt: true, Resume: true}, Describe: introspect.Describe{Description: "Codex workspace agent backed by a dedicated local app-server.", SkillDoc: agentSkillDoc}}
}
func (p provider) NewAdapter() (driverproto.Adapter, error) { return &adapter{cfg: p.cfg}, nil }

type adapter struct{ cfg Config }

func (a *adapter) NewWorker(host driverproto.WorkerHost) (driverproto.Worker, error) {
	return newWorker(a.cfg, host), nil
}

var _ driverproto.Provider = provider{}
var _ driverproto.Adapter = (*adapter)(nil)
