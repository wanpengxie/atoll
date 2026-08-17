package claude

import (
	"encoding/json"
	"reflect"
	"testing"
)

func TestConfigRequiresWorkspaceAndRejectsUnknownKnobs(t *testing.T) {
	if _, err := ParseConfig(nil, "", nil); err == nil {
		t.Fatal("empty workspace accepted")
	}
	if _, err := ParseConfig(json.RawMessage(`{"unsupported":true}`), "/workspace", nil); err == nil {
		t.Fatal("unknown config field accepted")
	}
	cfg, err := ParseConfig(json.RawMessage(`{"model":"sonnet"}`), "/workspace", nil)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Binary != "claude" || cfg.Model != "sonnet" || cfg.processFactory == nil {
		t.Fatalf("cfg=%+v", cfg)
	}
}

func TestSpawnArgsGolden(t *testing.T) {
	tests := []struct {
		name    string
		cfg     Config
		session string
		resume  bool
		want    []string
	}{
		{name: "new session", session: "new", want: []string{"--print", "--input-format", "stream-json", "--output-format", "stream-json", "--verbose", "--permission-mode", "bypassPermissions", "--session-id", "new"}},
		{name: "resume", session: "old", resume: true, want: []string{"--print", "--input-format", "stream-json", "--output-format", "stream-json", "--verbose", "--permission-mode", "bypassPermissions", "--resume", "old"}},
		{name: "model", cfg: Config{Model: "opus"}, session: "new", want: []string{"--print", "--input-format", "stream-json", "--output-format", "stream-json", "--verbose", "--permission-mode", "bypassPermissions", "--session-id", "new", "--model", "opus"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := spawnArgs(test.cfg, test.session, test.resume); !reflect.DeepEqual(got, test.want) {
				t.Fatalf("args=%q want=%q", got, test.want)
			}
		})
	}
}
