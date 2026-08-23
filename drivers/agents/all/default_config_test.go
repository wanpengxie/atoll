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
		class    string
		branches int
	}{
		{class: "codex", branches: 15},
		{class: "claude", branches: 20},
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
			var schema struct {
				OneOf []json.RawMessage `json:"oneOf"`
			}
			if err := json.Unmarshal(word.InputSchema, &schema); err != nil {
				t.Fatalf("agent.select schema: %v (%s)", err, word.InputSchema)
			}
			if len(schema.OneOf) != test.branches {
				t.Fatalf("oneOf branches=%d want=%d", len(schema.OneOf), test.branches)
			}
		})
	}
}
