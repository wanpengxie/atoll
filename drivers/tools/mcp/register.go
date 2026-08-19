package mcp

import (
	"encoding/json"

	"github.com/wanpengxie/atoll/lib/introspect"
	"github.com/wanpengxie/atoll/platform"
	"github.com/wanpengxie/atoll/platform/channelspec"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/registry"
)

func init() {
	registry.Register("mcp", registry.ClassDecl{
		Kind:      actor.KindTool,
		Placement: channelspec.PlacementDaemon,
		Manifest: introspect.Manifest{
			Class: "mcp", Interfaces: []string{"actor"}, Words: map[string]introspect.WordSpec{},
		},
		New: construct,
		ValidateConfig: func(raw json.RawMessage) error {
			_, err := parseConfig(raw)
			return err
		},
	})
}

func construct(spec registry.InstanceSpec, _ registry.Deps) (platform.ActorDecl, error) {
	cfg, err := parseConfig(spec.Config)
	if err != nil {
		return platform.ActorDecl{}, err
	}
	id := spec.ID
	if id == "" {
		id = "mcp"
	}
	return platform.ActorDecl{
		ID: id, Kind: actor.KindTool,
		Factory: platform.ActorFactory{Proc: Def(cfg)},
	}, nil
}
