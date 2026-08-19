package claude

import (
	"github.com/wanpengxie/atoll/drivers/agents/driverproto"
)

type provider struct{ cfg Config }

const Class = "claude"
const AgentSkillDoc = "# claude agent\n\nWorkspace-backed assistant using a dedicated local Claude Code CLI process."

func NewProvider(cfg Config) driverproto.Provider { return provider{cfg: cfg} }
func (p provider) Spec() driverproto.ProviderSpec {
	return driverproto.ProviderSpec{Name: Class, Capabilities: map[string]bool{driverproto.CapabilitySteer: true, driverproto.CapabilityInterrupt: true, driverproto.CapabilityResume: true, driverproto.CapabilityFork: true}, Documentation: driverproto.Documentation{Description: "Claude Code workspace agent backed by a dedicated local CLI process.", SkillDoc: AgentSkillDoc}, Selections: append([]driverproto.TurnOptions(nil), p.cfg.Selections...), DefaultSelection: p.cfg.Default}
}
func (p provider) NewWorker(host driverproto.WorkerHost) (driverproto.Worker, error) {
	return newWorker(p.cfg, host), nil
}

var _ driverproto.Provider = provider{}
