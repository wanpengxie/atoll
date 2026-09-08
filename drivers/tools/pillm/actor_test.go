package pillm

import (
	"encoding/json"
	"testing"
)

func TestConfigAcceptsProviderActorAPIKey(t *testing.T) {
	cfg, err := parseConfig(json.RawMessage(`{"node":"node","default_provider":"deepseek","default_model":"deepseek-v4-pro","api_key":"test-key","max_concurrency":2}`))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.APIKey != "test-key" || cfg.DefaultProvider != "deepseek" || cfg.DefaultModel != "deepseek-v4-pro" {
		t.Fatalf("config=%+v", cfg)
	}
	if _, err := parseConfig(json.RawMessage(`{"api_key":"   "}`)); err == nil {
		t.Fatal("blank configured credential was accepted")
	}
	if _, err := parseConfig(json.RawMessage(`{"credential":"test-key"}`)); err == nil {
		t.Fatal("unknown credential field was accepted")
	}
}
