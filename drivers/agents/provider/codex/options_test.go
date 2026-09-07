package codex

import (
	"encoding/json"
	"testing"

	"github.com/wanpengxie/atoll/drivers/agents/driverproto"
)

func TestNativeOptionsUsesAppServerCatalogAndRebasesStaleSelection(t *testing.T) {
	models := []codexModel{
		{Model: "gpt-new", DisplayName: "GPT New", IsDefault: true, DefaultReasoningEffort: "medium", SupportedReasoningEfforts: []struct {
			ReasoningEffort string `json:"reasoningEffort"`
			Description     string `json:"description"`
		}{{ReasoningEffort: "low"}, {ReasoningEffort: "medium"}}},
		{Model: "gpt-hidden", Hidden: true},
	}
	snapshot, ok := nativeOptions(models, driverproto.TurnOptions{Model: "gpt-old", Effort: "high"}, "0.153.4")
	if !ok || snapshot.Source != driverproto.OptionsSourceNative || len(snapshot.Models) != 1 {
		t.Fatalf("snapshot=%+v ok=%v", snapshot, ok)
	}
	want := driverproto.TurnOptions{Model: "gpt-new", Effort: "medium"}
	if snapshot.Default != want || snapshot.Current != want || snapshot.Client.Current != "0.153.4" {
		t.Fatalf("default/current/client=%+v/%+v/%+v", snapshot.Default, snapshot.Current, snapshot.Client)
	}
}

func TestCodexClientVersionAndUpdateSignal(t *testing.T) {
	if got := codexClientVersion("atoll-probe/0.153.4 (Linux; x86_64)"); got != "0.153.4" {
		t.Fatalf("version=%q", got)
	}
	client := driverproto.ClientInfo{Current: "0.153.4"}
	applyUpdateSignal(&client, "0.154.0")
	if client.Latest != "0.154.0" || client.UpdateStatus != driverproto.UpdateAvailable {
		t.Fatalf("available=%+v", client)
	}
	applyUpdateSignal(&client, "0.153.4")
	if client.UpdateStatus != driverproto.UpdateCurrent {
		t.Fatalf("current=%+v", client)
	}
}

func TestDecodeModelListRejectsEmptyOrMalformedResponse(t *testing.T) {
	if _, ok := decodeModelList(json.RawMessage(`{"data":[]}`)); ok {
		t.Fatal("empty model list accepted")
	}
	if _, ok := decodeModelList(json.RawMessage(`{`)); ok {
		t.Fatal("malformed model list accepted")
	}
}

func TestCodexSelectionClearsEffortForModelWithoutEffort(t *testing.T) {
	catalog := driverproto.OptionsSnapshot{Models: []driverproto.ModelOption{
		{Value: "gpt-reasoning", Efforts: []driverproto.EffortOption{{Value: "high"}}},
		{Value: "gpt-plain"},
	}}
	got := codexSelectionForCatalog(
		driverproto.TurnOptions{Model: "gpt-reasoning", Effort: "high"},
		driverproto.TurnOptions{Model: "gpt-plain"},
		catalog,
	)
	if got != (driverproto.TurnOptions{Model: "gpt-plain"}) {
		t.Fatalf("selection=%+v", got)
	}
}
