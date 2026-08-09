package codex

import (
	"github.com/wanpengxie/atoll/drivers/agents/driverproto"
	"github.com/wanpengxie/atoll/lib/introspect"
)

type provider struct{ cfg Config }

const Class = "codex"
const AgentSkillDoc = "# codex agent\n\nWorkspace-backed assistant using the local Codex app-server."

func NewProvider(cfg Config) driverproto.Provider { return provider{cfg: cfg} }
func (provider) Spec() driverproto.ProviderSpec {
	return driverproto.ProviderSpec{Name: Class, Capabilities: driverproto.Capabilities{Steer: true, Interrupt: true, Resume: true}, Describe: introspect.Describe{Description: "Codex workspace agent backed by a dedicated local app-server.", SkillDoc: AgentSkillDoc}}
}
func (p provider) NewWorker(host driverproto.WorkerHost) (driverproto.Worker, error) {
	return newWorker(p.cfg, host), nil
}

var _ driverproto.Provider = provider{}
