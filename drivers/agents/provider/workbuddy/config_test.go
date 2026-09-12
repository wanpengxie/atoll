package workbuddy

import (
	"encoding/json"
	"testing"

	"github.com/wanpengxie/atoll/drivers/agents/driverproto"
)

func TestConfigRequiresWorkspaceAndRejectsUnknownFields(t *testing.T) {
	if _, err := ParseConfig(nil, "", nil); err == nil {
		t.Fatal("missing workspace accepted")
	}
	if _, err := ParseConfig(json.RawMessage(`{"unknown":true}`), "/workspace", nil); err == nil {
		t.Fatal("unknown field accepted")
	}
	cfg, err := ParseConfig(json.RawMessage(`{"binary":"cbc","config_dir":"/auth"}`), "/workspace", nil)
	if err != nil || cfg.Binary != "cbc" || cfg.ConfigDir != "/auth" {
		t.Fatalf("cfg=%+v err=%v", cfg, err)
	}
}

func TestDefaultConfigPublishesACPModelAndThoughtLevelPairs(t *testing.T) {
	cfg, err := ParseConfig(DefaultConfig(), "/workspace", nil)
	if err != nil {
		t.Fatal(err)
	}
	spec := NewProvider(cfg).Spec()
	if got, want := len(spec.Selections), len(defaultModels)*len(defaultEfforts); got != want {
		t.Fatalf("selections=%d want=%d", got, want)
	}
	if got := spec.Selections[0]; got != (driverproto.TurnOptions{Model: "fast-model", Effort: "enabled"}) {
		t.Fatalf("default=%+v", got)
	}
	if len(spec.SelectionTitles) != len(spec.Selections) || spec.SelectionTitles[0].Model != "快速" {
		t.Fatalf("titles=%+v", spec.SelectionTitles[:1])
	}
}

func TestConfigRejectsInvalidSelectionCatalog(t *testing.T) {
	if err := ValidateConfig(json.RawMessage(`{"selections":[{"model":"m","effort":"e"},{"model":"m","effort":"e"}]}`)); err == nil {
		t.Fatal("duplicate accepted")
	}
	if err := ValidateConfig(json.RawMessage(`{"selections":[{"model":"m","effort":"e"}],"default":2}`)); err == nil {
		t.Fatal("out-of-range default accepted")
	}
}

func TestSessionCatalogAndResumeSeed(t *testing.T) {
	var s sessionResponse
	raw := []byte(`{"sessionId":"s1","models":{"availableModels":[{"modelId":"fast-model","name":"Fast"}],"currentModelId":"fast-model"},"configOptions":[{"id":"thought_level","currentValue":"enabled","options":[{"value":"enabled"},{"value":"high"}]}]}`)
	if err := json.Unmarshal(raw, &s); err != nil {
		t.Fatal(err)
	}
	models, efforts := catalogs(s)
	if err := validateSelection(driverproto.TurnOptions{Model: "fast-model", Effort: "high"}, models, efforts); err != nil {
		t.Fatal(err)
	}
	if err := validateSelection(driverproto.TurnOptions{Model: "missing", Effort: "high"}, models, efforts); err == nil {
		t.Fatal("missing model accepted")
	}
	seed := encodeResumeSeed("s1", "digest")
	if got, ok := decodeResumeSeed(seed, "digest"); !ok || got != "s1" {
		t.Fatalf("resume=%q ok=%v", got, ok)
	}
	if _, ok := decodeResumeSeed(seed, "changed"); ok {
		t.Fatal("stale tool surface accepted")
	}
}

func TestDesktopProfileDropsCompetingShellCredentials(t *testing.T) {
	got := envWithout([]string{"PATH=/bin", "CODEBUDDY_API_KEY=other", "CODEBUDDY_AUTH_TOKEN=other"}, "CODEBUDDY_API_KEY", "CODEBUDDY_AUTH_TOKEN")
	if len(got) != 1 || got[0] != "PATH=/bin" {
		t.Fatalf("env=%v", got)
	}
}
