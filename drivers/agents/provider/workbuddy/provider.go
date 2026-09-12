package workbuddy

import "github.com/wanpengxie/atoll/drivers/agents/driverproto"

const Class = "workbuddy"
const AgentSkillDoc = "# workbuddy agent\n\nWorkspace-backed assistant using the authenticated WorkBuddy desktop CLI over ACP."

type provider struct{ cfg Config }

func NewProvider(cfg Config) driverproto.Provider { return provider{cfg: cfg} }
func (p provider) Spec() driverproto.ProviderSpec {
	return driverproto.ProviderSpec{
		Name:          Class,
		Capabilities:  map[string]bool{driverproto.CapabilitySteer: true, driverproto.CapabilityInterrupt: true, driverproto.CapabilityResume: true},
		Documentation: driverproto.Documentation{Description: "WorkBuddy workspace agent backed by the local desktop CLI and subscription.", SkillDoc: AgentSkillDoc},
		Selections:    append([]driverproto.TurnOptions(nil), p.cfg.Selections...), DefaultSelection: p.cfg.Default,
		SelectionTitles: append([]driverproto.SelectionTitle(nil), p.cfg.Titles...),
	}
}
func (p provider) NewWorker(host driverproto.WorkerHost) (driverproto.Worker, error) {
	return newWorker(p.cfg, host), nil
}

var _ driverproto.Provider = provider{}
