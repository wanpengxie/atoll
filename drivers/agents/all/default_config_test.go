package all

import (
	"encoding/json"
	"testing"

	"github.com/wanpengxie/atoll/drivers/agents/base"
	"github.com/wanpengxie/atoll/platform/channelspec"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/registry"
)

func TestOrdinaryAgentBuildInheritsProviderDefaultConfig(t *testing.T) {
	tests := []struct {
		class string
	}{
		{class: "codex"},
		{class: "claude"},
	}
	for _, test := range tests {
		t.Run(test.class, func(t *testing.T) {
			decl, err := registry.Build(test.class, registry.InstanceSpec{
				ID: actor.ActorID("agent:" + test.class + ":1"), Config: json.RawMessage(`{}`),
			}, registry.Deps{ChannelID: channelspec.C0ChannelID, WorkspaceDir: t.TempDir()})
			if err != nil {
				t.Fatal(err)
			}
			word := decl.Factory.Proc.Manifest.Words[base.TypeSelect]
			var schema map[string]any
			if err := json.Unmarshal(word.InputSchema, &schema); err != nil {
				t.Fatalf("agent.select schema: %v (%s)", err, word.InputSchema)
			}
			if _, dynamic := schema["oneOf"]; dynamic {
				t.Fatal("agent.select manifest must be stable; dynamic values belong to agent.options")
			}
			required, _ := schema["required"].([]any)
			if len(required) != 1 || required[0] != "model" {
				t.Fatalf("required=%v want [model]", required)
			}
			if _, ok := decl.Factory.Proc.Manifest.Words[base.TypeOptions]; !ok {
				t.Fatal("agent.options missing from manifest")
			}
		})
	}
}
