package engineboot

import (
	"encoding/json"
	"os"
	"reflect"
	"testing"

	"github.com/wanpengxie/atoll/drivers/agents/native"
	"github.com/wanpengxie/atoll/platform/channelmember"
)

func TestNativeChannelRecipeUsesExplicitHandleAndCurrentWorkSchemas(t *testing.T) {
	var declaration struct {
		ID, Class string
		Config    json.RawMessage
	}
	raw, err := os.ReadFile("../../../docs/examples/native-agent-handle.json")
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(raw, &declaration); err != nil {
		t.Fatal(err)
	}
	if declaration.Class != channelmember.HandleClass {
		t.Fatal("not an ordinary handle declaration")
	}
	config, err := channelmember.ParseHandleConfig(declaration.Config)
	if err != nil {
		t.Fatal(err)
	}
	manifest := native.Manifest()
	if len(config.Words) != len(manifest.Words) {
		t.Fatal("recipe has stale native word set")
	}
	for name, word := range manifest.Words {
		var want, got any
		if err := json.Unmarshal(word.InputSchema, &want); err != nil {
			t.Fatal(err)
		}
		if err := json.Unmarshal(config.Words[name].Schema, &got); err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(want, got) || config.Words[name].Target != "native-agent" {
			t.Fatalf("stale native schema/target: %s", name)
		}
	}
	var template struct {
		Body struct {
			Declarations []struct {
				DeclID string `json:"decl_id"`
			}
			Profile map[string]any
		}
	}
	raw, err = os.ReadFile("../../../docs/examples/native-agent-channel.json")
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(raw, &template); err != nil {
		t.Fatal(err)
	}
	if _, ok := template.Body.Profile["svc_agent"]; ok {
		t.Fatal("member relation still depends on svc_agent")
	}
	found := false
	for _, d := range template.Body.Declarations {
		found = found || d.DeclID == declaration.ID
	}
	if !found {
		t.Fatal("recipe does not explicitly introduce its handle")
	}
}
