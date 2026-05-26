package daemon

import (
	"strings"
	"testing"

	"github.com/wanpengxie/ActOS/kernel/actor"
)

func TestConfigNormalizeDefaults(t *testing.T) {
	cfg := (Config{APIKey: " dk_test "}).Normalize()
	if cfg.APIKey != "dk_test" {
		t.Fatalf("api key trimmed = %q", cfg.APIKey)
	}
	if cfg.ServerWS != DefaultServerWS {
		t.Fatalf("server ws = %q want %q", cfg.ServerWS, DefaultServerWS)
	}
	if cfg.Port != DefaultPort {
		t.Fatalf("port = %d want %d", cfg.Port, DefaultPort)
	}
	if len(cfg.EnabledActors) != 1 || cfg.EnabledActors[0] != actor.ActorID("tool:kimi") {
		t.Fatalf("enabled actors = %+v", cfg.EnabledActors)
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate: %v", err)
	}
}

func TestBuildDevicebusURL(t *testing.T) {
	got, err := BuildDevicebusURL("https://coagent.example", "dk_secret")
	if err != nil {
		t.Fatalf("BuildDevicebusURL: %v", err)
	}
	if !strings.HasPrefix(got, "wss://coagent.example"+WSPathV2+"?") {
		t.Fatalf("url = %q", got)
	}
	if !strings.Contains(got, QueryParamAPIKey+"=dk_secret") {
		t.Fatalf("url missing api key query: %q", got)
	}
}
