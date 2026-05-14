package worker

import (
	"errors"
	"flag"
	"os"
	"path/filepath"
	"testing"

	"github.com/coagent-ai/daemon-go/pkg/coagent"
)

// TestTurnCtx_ParseFlags_HappyPath asserts every supervisor-supplied
// argv pair lands on the matching TurnCtx field. ExecSpawner (phase-spawner)
// will assemble exactly this argv shape — keeping the mapping covered
// here means a flag rename anywhere shows up as a failing test.
func TestTurnCtx_ParseFlags_HappyPath(t *testing.T) {
	var tc TurnCtx
	fs := flag.NewFlagSet("worker", flag.ContinueOnError)
	tc.ParseFlags(fs)

	args := []string{
		"--daemon-url=http://daemon:8080",
		"--channel-id=ch-1",
		"--agent-id=alice",
		"--worker-id=w-001",
		"--fencing-token=42",
		"--trigger-msg-id=trig-msg",
		"--trigger-correlation-id=trig-corr",
		"--auth-token=tok-123",
		"--sender-kind=agent",
		"--channel-workdir=/tmp/ch1",
		"--lease-ttl=120",
	}
	if err := fs.Parse(args); err != nil {
		t.Fatalf("fs.Parse: %v", err)
	}

	if tc.DaemonURL != "http://daemon:8080" {
		t.Errorf("DaemonURL: %q", tc.DaemonURL)
	}
	if tc.ChannelID != "ch-1" {
		t.Errorf("ChannelID: %q", tc.ChannelID)
	}
	if tc.AgentID != "alice" {
		t.Errorf("AgentID: %q", tc.AgentID)
	}
	if tc.WorkerID != "w-001" {
		t.Errorf("WorkerID: %q", tc.WorkerID)
	}
	if tc.FencingToken != 42 {
		t.Errorf("FencingToken: %d", tc.FencingToken)
	}
	if tc.TriggerMsgID != "trig-msg" {
		t.Errorf("TriggerMsgID: %q", tc.TriggerMsgID)
	}
	if tc.TriggerCorrelationID != "trig-corr" {
		t.Errorf("TriggerCorrelationID: %q", tc.TriggerCorrelationID)
	}
	if tc.AuthToken != "tok-123" {
		t.Errorf("AuthToken: %q", tc.AuthToken)
	}
	if tc.SenderKind != "agent" {
		t.Errorf("SenderKind: %q", tc.SenderKind)
	}
	if tc.ChannelWorkdir != "/tmp/ch1" {
		t.Errorf("ChannelWorkdir: %q", tc.ChannelWorkdir)
	}
	if tc.LeaseTTL != 120 {
		t.Errorf("LeaseTTL: %d", tc.LeaseTTL)
	}
}

// Validate must enforce the minimum required field set so a bad spawn
// fails at startup instead of silently writing partial state.
func TestTurnCtx_Validate(t *testing.T) {
	base := TurnCtx{
		ChannelID:      "ch",
		AgentID:        "a",
		WorkerID:       "w",
		FencingToken:   1,
		ChannelWorkdir: "/tmp",
		LeaseTTL:       60,
	}
	if err := base.Validate(); err != nil {
		t.Fatalf("happy: %v", err)
	}

	cases := []struct {
		name   string
		mutate func(*TurnCtx)
	}{
		{"empty channel_id", func(c *TurnCtx) { c.ChannelID = "" }},
		{"empty agent_id", func(c *TurnCtx) { c.AgentID = "" }},
		{"empty worker_id", func(c *TurnCtx) { c.WorkerID = "" }},
		{"zero fencing_token", func(c *TurnCtx) { c.FencingToken = 0 }},
		{"negative fencing_token", func(c *TurnCtx) { c.FencingToken = -1 }},
		{"empty channel_workdir", func(c *TurnCtx) { c.ChannelWorkdir = "" }},
		{"zero lease_ttl", func(c *TurnCtx) { c.LeaseTTL = 0 }},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			c := base
			tc.mutate(&c)
			err := c.Validate()
			if err == nil {
				t.Fatalf("expected validate error for %s", tc.name)
			}
			if !errors.Is(err, ErrTurnCtxInvalid) {
				t.Fatalf("error must wrap ErrTurnCtxInvalid, got %v", err)
			}
		})
	}
}

// FromEnv fills empty fields from env without trampling existing
// (flag-supplied) values. The L2 §3.4.2 contract is "explicit args >
// env" — this matrix verifies it.
func TestTurnCtx_FromEnv_FillsEmptyOnly(t *testing.T) {
	env := map[string]string{
		"DAEMON_URL":                     "http://env",
		"COAGENT_CHANNEL_ID":             "env-ch",
		"COAGENT_SELF_ID":                "env-agent",
		"COAGENT_WORKER_ID":              "env-worker",
		"COAGENT_FENCING_TOKEN":          "9",
		"COAGENT_TRIGGER_MSG_ID":         "env-trig",
		"COAGENT_TRIGGER_CORRELATION_ID": "env-corr",
		"COAGENT_AUTH_TOKEN":             "env-tok",
		"COAGENT_SENDER_KIND":            "agent",
		"COAGENT_CHANNEL_WORKDIR":        "/env/workdir",
		"COAGENT_LEASE_TTL":              "180",
	}
	getenv := func(k string) string { return env[k] }

	// Pre-populate ChannelID + LeaseTTL via "flags". FromEnv must NOT
	// overwrite either; everything else must come from env.
	tc := TurnCtx{ChannelID: "flag-ch", LeaseTTL: 30}
	tc.FromEnv(getenv)

	if tc.ChannelID != "flag-ch" {
		t.Errorf("env overrode flag ChannelID: %q", tc.ChannelID)
	}
	if tc.LeaseTTL != 30 {
		t.Errorf("env overrode flag LeaseTTL: %d", tc.LeaseTTL)
	}
	if tc.DaemonURL != "http://env" ||
		tc.AgentID != "env-agent" ||
		tc.WorkerID != "env-worker" ||
		tc.FencingToken != 9 ||
		tc.TriggerMsgID != "env-trig" ||
		tc.TriggerCorrelationID != "env-corr" ||
		tc.AuthToken != "env-tok" ||
		tc.SenderKind != "agent" ||
		tc.ChannelWorkdir != "/env/workdir" {
		t.Fatalf("env fill incomplete: %+v", tc)
	}
}

// WriteTurnCtxFile must produce JSON that pkg/coagent.LoadTurnCtx reads
// back without mismatch — this is the critical cross-package contract.
// If a worker bump renames a key, this test fails immediately.
func TestWriteTurnCtxFile_RoundTripsThroughPkgCoagent(t *testing.T) {
	dir := t.TempDir()

	src := TurnCtx{
		DaemonURL:            "http://daemon:7",
		ChannelID:            "ch-1",
		AgentID:              "alice",
		WorkerID:             "w-1",
		FencingToken:         11,
		TriggerMsgID:         "trig-1",
		TriggerCorrelationID: "corr-1",
		AuthToken:            "auth-tok",
		SenderKind:           "agent",
		ChannelWorkdir:       "/tmp/ch1",
		LeaseTTL:             60,
	}
	path, err := WriteTurnCtxFile(src, dir)
	if err != nil {
		t.Fatalf("WriteTurnCtxFile: %v", err)
	}
	if got, want := path, filepath.Join(dir, ".coagent", "turn-ctx.json"); got != want {
		t.Errorf("path = %q, want %q", got, want)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	if mode := info.Mode().Perm(); mode != 0o600 {
		t.Errorf("file mode = %o, want 0600", mode)
	}

	// Read back through pkg/coagent — env empty so LoadTurnCtx falls
	// back to the file we just wrote.
	tc, err := coagent.LoadTurnCtx(coagent.EnvFromMap(nil), dir)
	if err != nil {
		t.Fatalf("coagent.LoadTurnCtx: %v", err)
	}
	if tc.DaemonURL != src.DaemonURL ||
		tc.ChannelID != src.ChannelID ||
		tc.SelfID != src.AgentID ||
		tc.TriggerMsgID != src.TriggerMsgID ||
		tc.TriggerCorrelationID != src.TriggerCorrelationID ||
		tc.FencingToken != src.FencingToken ||
		tc.AuthToken != src.AuthToken ||
		tc.SenderKind != src.SenderKind {
		t.Fatalf("round-trip mismatch:\n  src=%+v\n  got=%+v", src, tc)
	}
}

// WriteTurnCtxFile rejects empty HOME with a clear error rather than
// writing to /.coagent/. Surfacing the misconfig at write time is
// cheaper than debugging a phantom write in /.
func TestWriteTurnCtxFile_RejectsEmptyHome(t *testing.T) {
	t.Setenv("HOME", "")
	_, err := WriteTurnCtxFile(TurnCtx{}, "")
	if err == nil {
		t.Fatal("expected error when HOME is empty and no homeDir given")
	}
}

// readTurnCtxFile is a thin helper used by future phases — verify it
// returns the same schema we wrote so internal callers can rely on it.
func TestReadTurnCtxFile_SeesWrittenKeys(t *testing.T) {
	dir := t.TempDir()
	src := TurnCtx{ChannelID: "ch", AgentID: "a", AuthToken: "tok"}
	if _, err := WriteTurnCtxFile(src, dir); err != nil {
		t.Fatalf("write: %v", err)
	}
	got, err := readTurnCtxFile(dir)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if got.ChannelID != "ch" || got.SelfID != "a" || got.AuthToken != "tok" {
		t.Fatalf("readTurnCtxFile mismatch: %+v", got)
	}
}
