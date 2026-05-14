package worker

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/coagent-ai/daemon-go/internal/supervisor"
)

// We use the standard "re-exec the test binary" pattern (`go test`
// supports this idiom) to simulate the worker binary without compiling
// a separate target. When the helper env var is set, TestMain
// detects it and runs a tiny scripted process; otherwise the test
// suite runs normally.
//
// Why bother? exec.Cmd is fiddly to test without a real child — and
// we want to verify the spawner's argv + env wiring against a process
// that actually reads them.

const helperEnvVar = "COAGENT_TEST_WORKER_HELPER"

func TestMain(m *testing.M) {
	if cmd := os.Getenv(helperEnvVar); cmd != "" {
		runHelper(cmd)
		return
	}
	os.Exit(m.Run())
}

// runHelper is the test-binary's "worker" persona. Behaviour depends
// on the helper command name:
//
//   - "dump": print "ARGS:<argv>" + "ENV:<filtered env>" then exit 0.
//     Used to assert the spawner wired flags + env correctly.
//   - "sleep": block for 5 seconds (covers Kill / Wait paths).
//   - "fail": exit 1 immediately (covers Wait error reporting).
func runHelper(cmd string) {
	switch cmd {
	case "dump":
		fmt.Printf("ARGS:%s\n", strings.Join(os.Args[1:], "|"))
		// Only emit COAGENT_* + IN_WORKER env keys so the test does
		// not have to filter parent inheritance noise.
		var env []string
		for _, e := range os.Environ() {
			if strings.HasPrefix(e, "COAGENT_") {
				env = append(env, e)
			}
		}
		fmt.Printf("ENV:%s\n", strings.Join(env, "|"))
		os.Exit(0)
	case "sleep":
		time.Sleep(5 * time.Second)
		os.Exit(0)
	case "fail":
		os.Exit(1)
	default:
		fmt.Fprintf(os.Stderr, "helper: unknown command %q\n", cmd)
		os.Exit(2)
	}
}

// helperPath returns the path of the running test binary. exec.Command
// re-runs it with COAGENT_TEST_WORKER_HELPER set so runHelper takes over.
func helperPath(t *testing.T) string {
	t.Helper()
	path, err := os.Executable()
	if err != nil {
		t.Fatalf("os.Executable: %v", err)
	}
	return path
}

// NewExecSpawner enforces required fields.
func TestNewExecSpawner_Validates(t *testing.T) {
	if _, err := NewExecSpawner(ExecSpawnerConfig{ChannelWorkdir: "/tmp"}); err == nil {
		t.Fatal("expected error when BinaryPath missing")
	}
	if _, err := NewExecSpawner(ExecSpawnerConfig{BinaryPath: "/tmp/bin"}); err == nil {
		t.Fatal("expected error when ChannelWorkdir missing")
	}
}

// Spawn handed the helper binary in "dump" mode writes the supervisor's
// SpawnContext fields back via stdout. The test parses that to confirm
// argv + env contract is intact.
func TestExecSpawner_Spawn_PropagatesContext(t *testing.T) {
	sp, err := NewExecSpawner(ExecSpawnerConfig{
		BinaryPath:     helperPath(t),
		ChannelWorkdir: "/tmp/ch-1",
		LeaseTTL:       45,
		ExtraEnv:       []string{helperEnvVar + "=dump"},
		InheritEnv:     true,
	})
	if err != nil {
		t.Fatalf("NewExecSpawner: %v", err)
	}

	sc := supervisor.SpawnContext{
		ChannelID:    "ch-1",
		AgentID:      "alice",
		WorkerID:     "w-1",
		FencingToken: 42,
	}

	// Use exec directly so we capture stdout — execWorker.Wait() only
	// returns the exit code, not output. The spawner's exec.Cmd path
	// is what we're indirectly verifying via the args / env contract.
	args := []string{
		"--channel-id=ch-1",
		"--agent-id=alice",
		"--worker-id=w-1",
		"--fencing-token=42",
		"--channel-workdir=/tmp/ch-1",
		"--lease-ttl=45",
	}
	expectedArgsLine := "ARGS:" + strings.Join(args, "|")

	// Drive the spawner's argv / env construction by calling Spawn,
	// then read combined output through a custom invocation. To avoid
	// duplicating exec wiring, we re-call the same buildArgsEnv via
	// the spawner's Spawn but use cmd.Output() through a one-shot
	// command we set up below.
	gotArgs, gotEnv, err := sp.buildArgsEnv(sc)
	if err != nil {
		t.Fatalf("buildArgsEnv: %v", err)
	}
	c := exec.Command(helperPath(t), gotArgs...)
	c.Env = gotEnv
	out, err := c.CombinedOutput()
	if err != nil {
		t.Fatalf("helper: %v; output=%q", err, out)
	}
	got := string(out)

	if !strings.Contains(got, expectedArgsLine) {
		t.Errorf("argv missing; got=%q want substring %q", got, expectedArgsLine)
	}
	// COAGENT_* env vars must include channel/agent/worker/fencing/workdir/lease.
	for _, kv := range []string{
		"COAGENT_IN_WORKER=1",
		"COAGENT_CHANNEL_ID=ch-1",
		"COAGENT_SELF_ID=alice",
		"COAGENT_WORKER_ID=w-1",
		"COAGENT_FENCING_TOKEN=42",
		"COAGENT_CHANNEL_WORKDIR=/tmp/ch-1",
		"COAGENT_LEASE_TTL=45",
	} {
		if !strings.Contains(got, kv) {
			t.Errorf("env missing %q; got=%q", kv, got)
		}
	}
}

// Spawn rejects an incomplete SpawnContext at the argv assembly step —
// surfaces the misuse rather than starting a child with garbage.
func TestExecSpawner_Spawn_RejectsIncompleteContext(t *testing.T) {
	sp, err := NewExecSpawner(ExecSpawnerConfig{
		BinaryPath:     helperPath(t),
		ChannelWorkdir: "/tmp/ch",
	})
	if err != nil {
		t.Fatalf("NewExecSpawner: %v", err)
	}
	_, err = sp.Spawn(context.Background(), supervisor.SpawnContext{
		ChannelID: "ch", AgentID: "a", // worker_id missing
	})
	if err == nil {
		t.Fatal("expected error for missing worker_id")
	}
}

// Wait + Kill cover the live-process lifecycle. We use the helper
// "sleep" path so the child blocks until Kill arrives.
func TestExecSpawner_KillTerminatesWorker(t *testing.T) {
	sp, err := NewExecSpawner(ExecSpawnerConfig{
		BinaryPath:     helperPath(t),
		ChannelWorkdir: "/tmp/ch",
		ExtraEnv:       []string{helperEnvVar + "=sleep"},
		InheritEnv:     true,
	})
	if err != nil {
		t.Fatalf("NewExecSpawner: %v", err)
	}
	w, err := sp.Spawn(context.Background(), supervisor.SpawnContext{
		ChannelID:    "c",
		AgentID:      "a",
		WorkerID:     "w",
		FencingToken: 1,
	})
	if err != nil {
		t.Fatalf("Spawn: %v", err)
	}
	if w.PID() <= 0 {
		t.Fatalf("pid not set: %d", w.PID())
	}

	done := make(chan error, 1)
	go func() { done <- w.Wait() }()

	// Give the child a moment to actually start before SIGKILL.
	time.Sleep(50 * time.Millisecond)
	if err := w.Kill(); err != nil {
		t.Fatalf("Kill: %v", err)
	}

	select {
	case err := <-done:
		// SIGKILL surfaces as a non-nil error.
		if err == nil {
			t.Error("expected non-nil error after Kill")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Wait did not return after Kill")
	}
}

// "fail" helper exits 1 immediately — Wait should surface the exit
// error rather than blocking.
func TestExecSpawner_WaitReportsExitError(t *testing.T) {
	sp, err := NewExecSpawner(ExecSpawnerConfig{
		BinaryPath:     helperPath(t),
		ChannelWorkdir: "/tmp/ch",
		ExtraEnv:       []string{helperEnvVar + "=fail"},
		InheritEnv:     true,
	})
	if err != nil {
		t.Fatalf("NewExecSpawner: %v", err)
	}
	w, err := sp.Spawn(context.Background(), supervisor.SpawnContext{
		ChannelID: "c", AgentID: "a", WorkerID: "w", FencingToken: 1,
	})
	if err != nil {
		t.Fatalf("Spawn: %v", err)
	}
	werr := w.Wait()
	if werr == nil {
		t.Fatal("expected non-nil exit error from failing helper")
	}
	var exitErr *exec.ExitError
	if !errors.As(werr, &exitErr) {
		t.Errorf("expected ExitError, got %T: %v", werr, werr)
	}
}
