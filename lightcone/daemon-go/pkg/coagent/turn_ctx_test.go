package coagent

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadTurnCtx_EnvOnly(t *testing.T) {
	env := EnvFromMap(map[string]string{
		"DAEMON_URL":                     "http://daemon:1",
		"COAGENT_CHANNEL_ID":             "ch-1",
		"COAGENT_SELF_ID":                "alice",
		"COAGENT_TRIGGER_MSG_ID":         "trig-msg",
		"COAGENT_TRIGGER_CORRELATION_ID": "trig-corr",
		"COAGENT_FENCING_TOKEN":          "42",
		"COAGENT_AUTH_TOKEN":             "tok",
		"COAGENT_SENDER_KIND":            "agent",
		"COAGENT_IN_WORKER":              "1",
	})
	tc, err := LoadTurnCtx(env, "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if tc.DaemonURL != "http://daemon:1" ||
		tc.ChannelID != "ch-1" ||
		tc.SelfID != "alice" ||
		tc.TriggerMsgID != "trig-msg" ||
		tc.TriggerCorrelationID != "trig-corr" ||
		tc.FencingToken != 42 ||
		tc.AuthToken != "tok" ||
		tc.SenderKind != "agent" ||
		!tc.InWorker {
		t.Fatalf("turn ctx not populated: %+v", tc)
	}
}

func TestLoadTurnCtx_FileFallback(t *testing.T) {
	dir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dir, ".coagent"), 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.WriteFile(
		filepath.Join(dir, ".coagent", "turn-ctx.json"),
		[]byte(`{"daemon_url":"http://file:9","channel_id":"ch-file","actor_id":"alice-file","trigger_correlation_id":"corr-file","fencing_token":7,"auth_token":"file-tok"}`),
		0o644,
	); err != nil {
		t.Fatalf("write: %v", err)
	}

	// Env is empty — file should supply every field.
	env := EnvFromMap(map[string]string{})
	tc, err := LoadTurnCtx(env, dir)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if tc.DaemonURL != "http://file:9" {
		t.Fatalf("DaemonURL not from file: %q", tc.DaemonURL)
	}
	if tc.ChannelID != "ch-file" {
		t.Fatalf("ChannelID not from file: %q", tc.ChannelID)
	}
	if tc.SelfID != "alice-file" {
		t.Fatalf("SelfID not from file: %q", tc.SelfID)
	}
	if tc.TriggerCorrelationID != "corr-file" {
		t.Fatalf("TriggerCorrelationID not from file: %q", tc.TriggerCorrelationID)
	}
	if tc.FencingToken != 7 {
		t.Fatalf("FencingToken not from file: %d", tc.FencingToken)
	}
	if tc.AuthToken != "file-tok" {
		t.Fatalf("AuthToken not from file: %q", tc.AuthToken)
	}
}

func TestLoadTurnCtx_EnvWinsOverFile(t *testing.T) {
	dir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dir, ".coagent"), 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	_ = os.WriteFile(
		filepath.Join(dir, ".coagent", "turn-ctx.json"),
		[]byte(`{"actor_id":"alice-file","trigger_correlation_id":"corr-file"}`),
		0o644,
	)
	env := EnvFromMap(map[string]string{
		"COAGENT_SELF_ID":                "alice-env",
		"COAGENT_TRIGGER_CORRELATION_ID": "corr-env",
	})
	tc, err := LoadTurnCtx(env, dir)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if tc.SelfID != "alice-env" {
		t.Fatalf("expected env to win: SelfID=%q", tc.SelfID)
	}
	if tc.TriggerCorrelationID != "corr-env" {
		t.Fatalf("expected env to win: corr=%q", tc.TriggerCorrelationID)
	}
}

func TestLoadTurnCtx_MissingFileIsNotAnError(t *testing.T) {
	dir := t.TempDir()
	env := EnvFromMap(map[string]string{"COAGENT_SELF_ID": "alice"})
	tc, err := LoadTurnCtx(env, dir)
	if err != nil {
		t.Fatalf("expected nil error when file missing, got %v", err)
	}
	if tc.SelfID != "alice" {
		t.Fatalf("SelfID lost: %q", tc.SelfID)
	}
}

func TestLoadTurnCtx_MalformedFileSurfacesError(t *testing.T) {
	dir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dir, ".coagent"), 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	_ = os.WriteFile(filepath.Join(dir, ".coagent", "turn-ctx.json"),
		[]byte("not-json"), 0o644)
	env := EnvFromMap(map[string]string{})
	_, err := LoadTurnCtx(env, dir)
	if err == nil {
		t.Fatalf("expected parse error on malformed file")
	}
}

func TestEnvFromMap_NilSafe(t *testing.T) {
	g := EnvFromMap(nil)
	if g("DAEMON_URL") != "" {
		t.Fatalf("expected empty string from nil map")
	}
}
