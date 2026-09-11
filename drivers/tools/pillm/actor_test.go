package pillm

import (
	"encoding/json"
	"testing"
)

func TestConfigAcceptsProviderActorAPIKey(t *testing.T) {
	cfg, err := parseConfig(json.RawMessage(`{"node":"node","api_key":"test-key","max_concurrency":2}`))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.APIKey != "test-key" {
		t.Fatalf("config=%+v", cfg)
	}
	if _, err := parseConfig(json.RawMessage(`{"default_provider":"deepseek"}`)); err == nil {
		t.Fatal("retired default_provider was accepted")
	}
	if _, err := parseConfig(json.RawMessage(`{"api_key":"   "}`)); err == nil {
		t.Fatal("blank configured credential was accepted")
	}
	if _, err := parseConfig(json.RawMessage(`{"credential":"test-key"}`)); err == nil {
		t.Fatal("unknown credential field was accepted")
	}
}

func TestDecodeToolsJSONRejectsWhitespaceAndPreservesBytes(t *testing.T) {
	if _, err := decodeToolsJSON("   \n\t"); err == nil {
		t.Fatal("whitespace-only tools_json was accepted")
	}
	want := "  [ {\"name\":\"read\"} ]  "
	got, err := decodeToolsJSON(want)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != want {
		t.Fatalf("tools_json bytes changed: got=%q want=%q", got, want)
	}
	for _, invalid := range []string{`[1]`, `["tool"]`, `[null]`} {
		if _, err := decodeToolsJSON(invalid); err == nil {
			t.Fatalf("non-object tool definition accepted: %s", invalid)
		}
	}
}
