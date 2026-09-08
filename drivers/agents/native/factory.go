package native

import (
	"encoding/json"
	"fmt"

	"github.com/wanpengxie/atoll/drivers/agents/workapi"
	"github.com/wanpengxie/atoll/lib/actorbase"
	"github.com/wanpengxie/atoll/lib/introspect"
	"github.com/wanpengxie/atoll/platform"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/registry"
)

func Manifest() introspect.Manifest {
	return workapi.Manifest(Class, map[string]bool{
		workapi.CapabilityMultiWork:          true,
		workapi.CapabilityWorkTextSteer:      true,
		workapi.CapabilityTargetedInterrupt:  true,
		workapi.CapabilityAgentWideInterrupt: true,
		workapi.CapabilityBranchFromWork:     true,
	})
}

func ValidateConfig(raw json.RawMessage) error { _, err := ParseConfig(raw); return err }

func New(spec registry.InstanceSpec, deps registry.Deps) (platform.ActorDecl, error) {
	if spec.ID == "" {
		return platform.ActorDecl{}, fmt.Errorf("native-agent: explicit instance id required")
	}
	if deps.ChannelID == "" {
		return platform.ActorDecl{}, fmt.Errorf("native-agent: channel required")
	}
	cfg, err := ParseConfig(spec.Config)
	if err != nil {
		return platform.ActorDecl{}, err
	}
	return platform.ActorDecl{ID: spec.ID, Kind: actor.KindAgent, Factory: platform.ActorFactory{Proc: actorbase.Def{
		Manifest: Manifest(), New: func() (actorbase.Proc, error) { return func(sys actorbase.Sys) error { return run(sys, cfg) }, nil },
	}}}, nil
}
