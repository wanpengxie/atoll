package worker

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"strings"
	"sync"
	"syscall"
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
//   - "dump":      print "ARGS:<argv>" + "ENV:<filtered env>" then exit 0.
//     Used to assert the spawner wired flags + env correctly.
//   - "sleep":     block for 5 seconds (covers Kill / Wait paths).
//   - "fail":      exit 1 immediately (covers Wait error reporting).
//   - "graceful":  install a SIGTERM handler that prints
//     "graceful_exit\n" to stdout then exits 0 — covers Stop()'s
//     SIGTERM grace branch.
//   - "ignore":    install a SIG_IGN-style handler so SIGTERM is
//     swallowed; loops until SIGKILL terminates it. Covers Stop()'s
//     SIGKILL fallback branch.
//   - "logline":   print the JSON line `{"event":"worker.start"}` to
//     stdout then exit 0 — covers the spawner's stdout passthrough
//     contract.
func runHelper(cmd string) {
	switch cmd {
	case "dump":
		fmt.Printf("ARGS:%s\n", strings.Join(os.Args[1:], "|"))
		// Only emit COAGENT_* + DAEMON_URL env keys so the test does
		// not have to filter parent inheritance noise.
		var env []string
		for _, e := range os.Environ() {
			if strings.HasPrefix(e, "COAGENT_") || strings.HasPrefix(e, "DAEMON_URL=") {
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
	case "graceful":
		ch := make(chan os.Signal, 1)
		signal.Notify(ch, syscall.SIGTERM)
		<-ch
		fmt.Println("graceful_exit")
		os.Exit(0)
	case "ignore":
		ch := make(chan os.Signal, 1)
		signal.Notify(ch, syscall.SIGTERM)
		// Drain SIGTERMs forever; only SIGKILL stops us.
		go func() {
			for range ch {
			}
		}()
		for {
			time.Sleep(50 * time.Millisecond)
		}
	case "logline":
		fmt.Println(`{"event":"worker.start"}`)
		os.Exit(0)
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

// FIX-4 trigger / daemon-url propagation + R2-FIX-2 auth-token off-argv
// invariant: when the supervisor hands a fully-populated SpawnContext,
// every Trigger field + DaemonURL surfaces as both an argv flag and an
// env var, BUT AuthToken surfaces ONLY as COAGENT_AUTH_TOKEN env and
// MUST NOT appear in argv (Linux /proc/<pid>/cmdline / ps / audit
// pipelines would otherwise leak the daemon master bearer token).
func TestExecSpawner_Spawn_PropagatesTriggerAuth(t *testing.T) {
	sp, err := NewExecSpawner(ExecSpawnerConfig{
		BinaryPath:     helperPath(t),
		ChannelWorkdir: "/tmp/ch-trig",
		LeaseTTL:       45,
		ExtraEnv:       []string{helperEnvVar + "=dump"},
		InheritEnv:     true,
	})
	if err != nil {
		t.Fatalf("NewExecSpawner: %v", err)
	}

	sc := supervisor.SpawnContext{
		ChannelID:    "ch-trig",
		AgentID:      "alice",
		WorkerID:     "w-trig",
		FencingToken: 9,
		AuthToken:    "secret-bearer",
		DaemonURL:    "http://daemon.local:3101",
		Trigger: supervisor.SpawnTrigger{
			MsgID:         "msg-42",
			CorrelationID: "corr-42",
			SenderKind:    "human",
		},
	}

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

	for _, want := range []string{
		"--trigger-msg-id=msg-42",
		"--trigger-correlation-id=corr-42",
		"--sender-kind=human",
		"--daemon-url=http://daemon.local:3101",
		"COAGENT_TRIGGER_MSG_ID=msg-42",
		"COAGENT_TRIGGER_CORRELATION_ID=corr-42",
		"COAGENT_SENDER_KIND=human",
		"COAGENT_AUTH_TOKEN=secret-bearer",
		"DAEMON_URL=http://daemon.local:3101",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("missing %q in helper output\n%s", want, got)
		}
	}

	// R2-FIX-2 negative invariants: auth-token must never appear on
	// argv even when AuthToken is non-empty. Check both the assembled
	// argv directly (so this assertion is independent of helper output
	// formatting) and the dump-helper's ARGS line.
	joinedArgs := strings.Join(gotArgs, " ")
	if strings.Contains(joinedArgs, "--auth-token") {
		t.Errorf("argv leaked --auth-token: %s", joinedArgs)
	}
	if strings.Contains(joinedArgs, "secret-bearer") {
		t.Errorf("argv leaked AuthToken value: %s", joinedArgs)
	}
	if strings.Contains(got, "--auth-token") {
		t.Errorf("helper observed --auth-token on argv\n%s", got)
	}
}

// When SpawnContext leaves Trigger / AuthToken / DaemonURL empty, the
// argv MUST NOT carry stub flags with empty values — otherwise the
// child's flag parser logs "--auth-token=" with empty content + the
// receiving worker stamps "" as the trigger correlation_id.
func TestExecSpawner_Spawn_OmitsEmptyOptionalFlags(t *testing.T) {
	sp, err := NewExecSpawner(ExecSpawnerConfig{
		BinaryPath:     helperPath(t),
		ChannelWorkdir: "/tmp/ch-no-trig",
		LeaseTTL:       30,
	})
	if err != nil {
		t.Fatalf("NewExecSpawner: %v", err)
	}
	gotArgs, gotEnv, err := sp.buildArgsEnv(supervisor.SpawnContext{
		ChannelID:    "ch-no-trig",
		AgentID:      "bob",
		WorkerID:     "w-bob",
		FencingToken: 1,
	})
	if err != nil {
		t.Fatalf("buildArgsEnv: %v", err)
	}
	joinedArgs := strings.Join(gotArgs, " ")
	for _, banned := range []string{
		"--trigger-msg-id",
		"--trigger-correlation-id",
		"--sender-kind",
		"--auth-token",
		"--daemon-url",
	} {
		if strings.Contains(joinedArgs, banned) {
			t.Errorf("argv leaked stub flag %q: %s", banned, joinedArgs)
		}
	}
	// Per-entry exact match: the stub-key bug surfaces as an env entry
	// that is EXACTLY "KEY=" (empty value). A substring scan over the
	// joined env is fragile because the inherited parent env (forced on
	// by NewExecSpawner) may contain unrelated values that happen to
	// embed these prefixes — e.g. a log line or ticket description text
	// that mentions "COAGENT_AUTH_TOKEN=token" as a literal.
	for _, entry := range gotEnv {
		for _, banned := range []string{
			"COAGENT_TRIGGER_MSG_ID=",
			"COAGENT_TRIGGER_CORRELATION_ID=",
			"COAGENT_SENDER_KIND=",
			"COAGENT_AUTH_TOKEN=",
			"DAEMON_URL=",
		} {
			if entry == banned {
				t.Errorf("env leaked stub key %q (empty value)", banned)
			}
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

// FIX-4 §"spawner 增 Stop()": SIGTERM grace path. The "graceful"
// helper installs a SIGTERM handler and exits 0, so the spawner's
// Stop must NOT escalate to SIGKILL within the grace window.
func TestExecSpawner_Stop_GracefulSIGTERM(t *testing.T) {
	sp, err := NewExecSpawner(ExecSpawnerConfig{
		BinaryPath:      helperPath(t),
		ChannelWorkdir:  "/tmp/ch-stop-graceful",
		ExtraEnv:        []string{helperEnvVar + "=graceful"},
		StopGracePeriod: 1 * time.Second, // generous; helper exits immediately
		InheritEnv:      true,
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
	// Give the helper a moment to install its handler.
	time.Sleep(100 * time.Millisecond)

	stopper, ok := w.(interface{ Stop() error })
	if !ok {
		t.Fatalf("execWorker does not satisfy Stop()")
	}

	start := time.Now()
	if serr := stopper.Stop(); serr != nil {
		t.Fatalf("Stop: %v", serr)
	}
	elapsed := time.Since(start)
	if elapsed > 800*time.Millisecond {
		t.Errorf("Stop took %s — graceful path should finish well under grace window", elapsed)
	}

	// Wait surfaces nil because SIGTERM-handled exit returns 0.
	werr := w.Wait()
	if werr != nil {
		t.Errorf("Wait after graceful Stop = %v, want nil", werr)
	}
}

// FIX-4 §"等 gracePeriod 5s 后 SIGKILL": fallback path. The "ignore"
// helper swallows SIGTERM and only dies on SIGKILL — Stop MUST escalate
// once the grace period elapses.
func TestExecSpawner_Stop_FallbackSIGKILL(t *testing.T) {
	sp, err := NewExecSpawner(ExecSpawnerConfig{
		BinaryPath:      helperPath(t),
		ChannelWorkdir:  "/tmp/ch-stop-ignore",
		ExtraEnv:        []string{helperEnvVar + "=ignore"},
		StopGracePeriod: 200 * time.Millisecond, // tiny so the test is fast
		InheritEnv:      true,
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
	time.Sleep(100 * time.Millisecond)

	stopper, ok := w.(interface{ Stop() error })
	if !ok {
		t.Fatalf("execWorker does not satisfy Stop()")
	}
	start := time.Now()
	if serr := stopper.Stop(); serr != nil {
		t.Fatalf("Stop: %v", serr)
	}
	elapsed := time.Since(start)
	if elapsed < 200*time.Millisecond {
		t.Errorf("Stop returned in %s — should have waited at least one grace window before SIGKILL", elapsed)
	}
	if elapsed > 2*time.Second {
		t.Errorf("Stop took %s — SIGKILL should fire promptly after grace expiry", elapsed)
	}
	// Wait surfaces a non-nil error because SIGKILL'd processes exit
	// non-zero.
	werr := w.Wait()
	if werr == nil {
		t.Errorf("Wait after SIGKILL fallback = nil, want non-nil exit error")
	}
}

// FIX-4 §"ExecSpawner.cmd.Stdout = os.Stdout": the spawner MUST route
// the worker's stdout to the configured writer. We capture into a
// *bytes.Buffer guarded by a mutex (cmd.Stdout writes happen on the
// child's exit goroutine).
func TestExecSpawner_Stdout_Passthrough(t *testing.T) {
	var (
		mu  sync.Mutex
		buf bytes.Buffer
	)
	sp, err := NewExecSpawner(ExecSpawnerConfig{
		BinaryPath:     helperPath(t),
		ChannelWorkdir: "/tmp/ch-stdout",
		ExtraEnv:       []string{helperEnvVar + "=logline"},
		Stdout:         lockedWriter{mu: &mu, w: &buf},
		Stderr:         lockedWriter{mu: &mu, w: &buf},
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
	if werr := w.Wait(); werr != nil {
		t.Fatalf("Wait: %v", werr)
	}

	mu.Lock()
	got := buf.String()
	mu.Unlock()
	if !strings.Contains(got, `{"event":"worker.start"}`) {
		t.Errorf("stdout did not capture worker JSON line; got=%q", got)
	}
}

// lockedWriter serialises Write calls — exec.Cmd writes from the
// child's reaper goroutine, so a raw bytes.Buffer would race.
type lockedWriter struct {
	mu *sync.Mutex
	w  *bytes.Buffer
}

func (l lockedWriter) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.w.Write(p)
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
