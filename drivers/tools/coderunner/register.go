package coderunner

import (
	"encoding/json"

	"github.com/wanpengxie/atoll/lib/introspect"
	"github.com/wanpengxie/atoll/platform"
	"github.com/wanpengxie/atoll/platform/channelspec"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/registry"
)

const DefaultActorID actor.ActorID = "coderunner"

func init() {
	registry.Register("coderunner", registry.ClassDecl{
		Kind:           actor.KindTool,
		Placement:      channelspec.PlacementDaemon,
		Manifest:       manifest(),
		New:            construct,
		ValidateConfig: func(raw json.RawMessage) error { _, err := parseConfig(raw); return err },
		ConfigSchema:   json.RawMessage(ConfigSchema),
	})
}

func construct(spec registry.InstanceSpec, deps registry.Deps) (platform.ActorDecl, error) {
	cfg, err := parseConfig(spec.Config)
	if err != nil {
		return platform.ActorDecl{}, err
	}
	id := spec.ID
	if id == "" {
		id = DefaultActorID
	}
	return platform.ActorDecl{
		ID: id, Kind: actor.KindTool,
		Factory: platform.ActorFactory{Proc: Def(cfg, deps)},
	}, nil
}

func manifest() introspect.Manifest {
	return introspect.Manifest{
		Class: "coderunner", Interfaces: []string{"actor", "code"},
		Words: map[string]introspect.WordSpec{
			TypeRun: {
				Description:  "Run one JavaScript ES module and expose declared Atoll actors as calls.",
				InputSchema:  json.RawMessage(runInputSchema),
				OutputSchema: json.RawMessage(runOutputSchema),
				ErrorCodes:   []string{"invalid_input", "dependency_missing", "runtime_failed", "timeout", "cancelled", "output_limit"},
			},
		},
	}
}

const runInputSchema = `{"type":"object","additionalProperties":false,"properties":{"program":{"type":"string"},"requires":{"type":"array","items":{"type":"string"}},"args":{}}}`
const runOutputSchema = `{"type":"object","required":["value","logs"],"properties":{"value":{},"logs":{"type":"array","items":{"type":"object","required":["stream","text"],"properties":{"stream":{"enum":["stdout","stderr","log"]},"text":{"type":"string"}}}}}}`
