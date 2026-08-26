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
			TypeValidate: {
				Description:  "Resolve a requires list against the channel exactly as code.run would, without running anything: which present actor each requirement maps to, its words with input/output schemas, what is missing, and what is ambiguous. A fixed-program member validates its own config and accepts no input.",
				InputSchema:  json.RawMessage(validateInputSchema),
				OutputSchema: json.RawMessage(validateOutputSchema),
				ErrorCodes:   []string{"invalid_input", "dependency_missing"},
			},
		},
	}
}

const runInputSchema = `{"type":"object","additionalProperties":false,"properties":{"program":{"type":"string"},"requires":{"type":"array","items":{"type":"string"}},"args":{}}}`
const validateInputSchema = `{"type":"object","additionalProperties":false,"properties":{"requires":{"type":"array","items":{"type":"string"}}}}`
const validateOutputSchema = `{"type":"object","required":["ok","resolved","missing"],"properties":{"ok":{"type":"boolean"},"resolved":{"type":"object","additionalProperties":{"type":"object","required":["actor"],"properties":{"actor":{"type":"string"},"class":{"type":"string"},"words":{"type":"object"}}}},"missing":{"type":"array","items":{"type":"string"}},"ambiguous":{"type":"object","additionalProperties":{"type":"array","items":{"type":"string"}}},"errors":{"type":"object","additionalProperties":{"type":"string"}}}}`
const runOutputSchema = `{"type":"object","required":["value","logs"],"properties":{"value":{},"logs":{"type":"array","items":{"type":"object","required":["stream","text"],"properties":{"stream":{"enum":["stdout","stderr","log"]},"text":{"type":"string"}}}}}}`
