package claude

import (
	"github.com/wanpengxie/atoll/drivers/agents/driverproto"
	"github.com/wanpengxie/atoll/lib/introspect"
)

type provider struct{ cfg Config }

const Class = "claude"
const AgentSkillDoc = "# claude agent\n\nWorkspace-backed assistant using a dedicated local Claude Code CLI process."

func NewProvider(cfg Config) driverproto.Provider { return provider{cfg: cfg} }
func (provider) Spec() driverproto.ProviderSpec {
	return driverproto.ProviderSpec{Name: Class, Capabilities: driverproto.Capabilities{Steer: true, Interrupt: true, Resume: true}, Describe: introspect.Describe{Description: "Claude Code workspace agent backed by a dedicated local CLI process.", SkillDoc: AgentSkillDoc}}
}
func (p provider) NewWorker(host driverproto.WorkerHost) (driverproto.Worker, error) {
	return newWorker(p.cfg, host), nil
}

var _ driverproto.Provider = provider{}
