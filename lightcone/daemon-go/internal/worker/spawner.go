package worker

// spawner.go is the production exec.Cmd-based supervisor.Spawner
// implementation. The supervisor (T6) accepts any Spawner; tests use
// fakes, daemon production wiring calls NewExecSpawner with the path
// of the worker binary built from cmd/worker.
//
// Spawn translates supervisor.SpawnContext into an argv slice + an env
// slice and starts the child process. Wait + Kill are wired through
// os/exec.Cmd directly. The double-channel design (one waitDone closed
// by the goroutine running cmd.Wait, the result stashed under a mutex)
// keeps the Worker interface contract:
//
//   - Wait() blocks until the OS reports the child exited;
//   - Kill() returns nil if the child already exited;
//   - PID() always returns the correct pid for log lines.

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/coagent-ai/daemon-go/internal/supervisor"
)

// DefaultStopGracePeriod is how long Stop() waits for a worker to
// react to SIGTERM before escalating to SIGKILL. The 5-second value
// is the FIX-4 spec baseline ("先对 pgid 发 SIGTERM, 等 gracePeriod
// 5s 后 SIGKILL"); tests inject a sub-second value for speed.
const DefaultStopGracePeriod = 5 * time.Second

// ExecSpawnerConfig tunes the production spawner. BinaryPath is the
// only required field; the rest tune logging + path-to-worker-binary
// override in tests.
type ExecSpawnerConfig struct {
	// BinaryPath is the absolute path of the worker binary (the one
	// built from cmd/worker). Required.
	BinaryPath string

	// ExtraArgs is appended verbatim to argv after the supervisor
	// turn-ctx flags. Useful for "--prompt foo" overrides in
	// integration tests. Optional.
	ExtraArgs []string

	// ExtraEnv is appended to the child's env. Each entry must be in
	// "KEY=VAL" form. Optional.
	ExtraEnv []string

	// ChannelWorkdir is the absolute path of the channel workdir the
	// worker should open `messages.sqlite` under. Required — the
	// supervisor's SpawnContext does NOT carry workdir, so it must be
	// supplied at spawner-construction time.
	ChannelWorkdir string

	// LeaseTTL is the worker_locks lease ttl in seconds. The spawner
	// forwards it to the child so heartbeat cadence is consistent
	// across supervisor + worker. Defaults to supervisor.DefaultLeaseTTL.
	LeaseTTL int64

	// Logger receives spawn / wait events. Defaults to noopLogger.
	Logger Logger

	// InheritEnv, when true, prepends the parent process env to the
	// child's env (defaults to true — production wants $PATH visible
	// to the worker). Tests set false for hermetic isolation.
	InheritEnv bool

	// Stdout / Stderr override the destinations for the spawned
	// worker's stdout / stderr. Defaults to os.Stdout / os.Stderr so
	// the worker's slog JSON lines show up in the daemon log
	// aggregation pipe by default. Tests pipe to a *bytes.Buffer
	// to make assertions; production may pipe through a logger.Forward
	// wrapper.
	Stdout io.Writer
	Stderr io.Writer

	// StopGracePeriod is the wait between SIGTERM and the SIGKILL
	// fallback in Stop(). Defaults to DefaultStopGracePeriod (5s).
	// Tests set 50ms so the assertion finishes inside the standard
	// `go test` timeout.
	StopGracePeriod time.Duration
}

// NewExecSpawner builds the production spawner.
func NewExecSpawner(cfg ExecSpawnerConfig) (*ExecSpawner, error) {
	if strings.TrimSpace(cfg.BinaryPath) == "" {
		return nil, errors.New("exec_spawner: BinaryPath required")
	}
	if strings.TrimSpace(cfg.ChannelWorkdir) == "" {
		return nil, errors.New("exec_spawner: ChannelWorkdir required")
	}
	if cfg.LeaseTTL <= 0 {
		cfg.LeaseTTL = supervisor.DefaultLeaseTTL
	}
	if cfg.Logger == nil {
		cfg.Logger = noopLogger{}
	}
	if cfg.StopGracePeriod <= 0 {
		cfg.StopGracePeriod = DefaultStopGracePeriod
	}
	if cfg.Stdout == nil {
		cfg.Stdout = os.Stdout
	}
	if cfg.Stderr == nil {
		cfg.Stderr = os.Stderr
	}
	// InheritEnv defaults to true (zero value false → caller intent
	// would be ambiguous; we force inherit so $PATH / TMPDIR are
	// visible). Tests can flip via the field directly after
	// construction.
	cfg.InheritEnv = true
	return &ExecSpawner{cfg: cfg}, nil
}

// ExecSpawner satisfies supervisor.Spawner. One instance per
// (channel, agent) pair — the channel workdir + binary path are
// constant per supervisor Loop.
type ExecSpawner struct {
	cfg ExecSpawnerConfig
}

// Spawn implements supervisor.Spawner.
func (s *ExecSpawner) Spawn(ctx context.Context, sc supervisor.SpawnContext) (supervisor.Worker, error) {
	args, env, err := s.buildArgsEnv(sc)
	if err != nil {
		return nil, err
	}

	cmd := exec.CommandContext(ctx, s.cfg.BinaryPath, args...)
	cmd.Env = env
	// Per FIX-4 §"ExecSpawner 不设 cmd.Stdout/Stderr": route the
	// worker's slog JSON to the configured writers (default os.Stdout /
	// os.Stderr) so the daemon log aggregation pipe sees them. Without
	// these the worker's structured logs were silently dropped to
	// /dev/null.
	cmd.Stdout = s.cfg.Stdout
	cmd.Stderr = s.cfg.Stderr

	// Detach from the supervisor's process group so a SIGINT to the
	// daemon doesn't propagate to every child — the supervisor sends
	// explicit Kill() when it wants the worker dead.
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}

	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("exec_spawner: start %s: %w", s.cfg.BinaryPath, err)
	}

	s.cfg.Logger.Info("exec_spawner.spawn",
		"binary", s.cfg.BinaryPath,
		"pid", cmd.Process.Pid,
		"channel_id", sc.ChannelID,
		"agent_id", sc.AgentID,
		"worker_id", sc.WorkerID,
		"fencing_token", sc.FencingToken,
		"trigger_msg_id", sc.Trigger.MsgID,
		"backlog_size", len(sc.Backlog),
	)

	w := &execWorker{
		cmd:      cmd,
		waitDone: make(chan struct{}),
		grace:    s.cfg.StopGracePeriod,
	}
	go w.runWait()
	return w, nil
}

// buildArgsEnv assembles the argv + env that ship the supervisor's
// SpawnContext into the child. CLI flags are the primary channel
// (the worker's TurnCtx.ParseFlags consumes them); env vars carry the
// same info as a fallback so coagent CLI subprocesses can read them
// without re-parsing argv.
//
// Per L2 §3.4.2 + FIX-4 the trigger context (msg_id + correlation_id +
// sender_kind) and the auth/daemon-url pair are first-class fields:
// they ship as both flags and env vars so the worker's TurnCtx and any
// downstream coagent CLI subprocess see them without ambiguity.
func (s *ExecSpawner) buildArgsEnv(sc supervisor.SpawnContext) ([]string, []string, error) {
	if sc.ChannelID == "" || sc.AgentID == "" || sc.WorkerID == "" {
		return nil, nil, fmt.Errorf("exec_spawner: incomplete SpawnContext: %+v", sc)
	}
	args := []string{
		"--channel-id=" + sc.ChannelID,
		"--agent-id=" + sc.AgentID,
		"--worker-id=" + sc.WorkerID,
		"--fencing-token=" + strconv.FormatInt(sc.FencingToken, 10),
		"--channel-workdir=" + s.cfg.ChannelWorkdir,
		"--lease-ttl=" + strconv.FormatInt(s.cfg.LeaseTTL, 10),
	}
	// Trigger / daemon-url are emitted only when populated so the
	// child's flag parser does not see e.g. "--daemon-url=" with an
	// empty value (which would still parse but pollute the log lines).
	//
	// AuthToken is INTENTIONALLY omitted from argv per R2-FIX-2: argv is
	// world-readable on Linux (/proc/<pid>/cmdline, ps, audit pipelines,
	// accidental log dumps), so the daemon master bearer token must not
	// ride there. The worker reads it from COAGENT_AUTH_TOKEN env below,
	// which env's per-process visibility (only owner uid via /proc/.../environ)
	// keeps it within the worker's trust boundary. TurnCtx.ParseFlags still
	// registers --auth-token so manual debugging / tests can pass it, but
	// production spawn does not.
	if sc.Trigger.MsgID != "" {
		args = append(args, "--trigger-msg-id="+sc.Trigger.MsgID)
	}
	if sc.Trigger.CorrelationID != "" {
		args = append(args, "--trigger-correlation-id="+sc.Trigger.CorrelationID)
	}
	if sc.Trigger.SenderKind != "" {
		args = append(args, "--sender-kind="+sc.Trigger.SenderKind)
	}
	if sc.DaemonURL != "" {
		args = append(args, "--daemon-url="+sc.DaemonURL)
	}
	args = append(args, s.cfg.ExtraArgs...)

	env := []string{
		"COAGENT_IN_WORKER=1",
		"COAGENT_CHANNEL_ID=" + sc.ChannelID,
		"COAGENT_SELF_ID=" + sc.AgentID,
		"COAGENT_WORKER_ID=" + sc.WorkerID,
		"COAGENT_FENCING_TOKEN=" + strconv.FormatInt(sc.FencingToken, 10),
		"COAGENT_CHANNEL_WORKDIR=" + s.cfg.ChannelWorkdir,
		"COAGENT_LEASE_TTL=" + strconv.FormatInt(s.cfg.LeaseTTL, 10),
	}
	if sc.Trigger.MsgID != "" {
		env = append(env, "COAGENT_TRIGGER_MSG_ID="+sc.Trigger.MsgID)
	}
	if sc.Trigger.CorrelationID != "" {
		env = append(env, "COAGENT_TRIGGER_CORRELATION_ID="+sc.Trigger.CorrelationID)
	}
	if sc.Trigger.SenderKind != "" {
		env = append(env, "COAGENT_SENDER_KIND="+sc.Trigger.SenderKind)
	}
	if sc.AuthToken != "" {
		env = append(env, "COAGENT_AUTH_TOKEN="+sc.AuthToken)
	}
	if sc.DaemonURL != "" {
		env = append(env, "DAEMON_URL="+sc.DaemonURL)
	}
	if s.cfg.InheritEnv {
		// Prepend parent env so the child sees $HOME / $PATH / etc.
		env = append(parentEnv(), env...)
	}
	env = append(env, s.cfg.ExtraEnv...)
	return args, env, nil
}

// execWorker is the supervisor.Worker view of one live child process.
type execWorker struct {
	cmd *exec.Cmd

	// grace is the SIGTERM → SIGKILL escalation window used by Stop().
	// 0 means "skip the SIGTERM step and go straight to SIGKILL"; the
	// spawner's NewExecSpawner always sets a non-zero default.
	grace time.Duration

	waitDone chan struct{}
	mu       sync.Mutex
	waitErr  error
}

// PID implements supervisor.Worker.
func (w *execWorker) PID() int {
	if w.cmd.Process == nil {
		return 0
	}
	return w.cmd.Process.Pid
}

// Wait blocks until the child exits.
func (w *execWorker) Wait() error {
	<-w.waitDone
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.waitErr
}

// Kill sends SIGKILL to the child's process group (so any forked
// helper coagent CLI processes also exit). Returns nil if the child
// is already gone. Use Stop() instead when graceful shutdown matters
// — Kill bypasses the worker's defer-Release step so the supervisor
// must wait LeaseTTL before reclaiming the lock.
func (w *execWorker) Kill() error {
	if w.cmd.Process == nil {
		return nil
	}
	// Kill the whole process group: the worker may have forked a
	// coagent CLI; killing only the leader could orphan grandchildren.
	pgid, err := syscall.Getpgid(w.cmd.Process.Pid)
	if err == nil && pgid > 0 {
		_ = syscall.Kill(-pgid, syscall.SIGKILL)
		return nil
	}
	// Fallback to single-process kill when getpgid fails (rare —
	// usually means the process already exited).
	return w.cmd.Process.Kill()
}

// Stop is the graceful shutdown variant: SIGTERM the worker's process
// group, wait up to `grace` for the worker's signal handler to release
// its worker_locks row + close the agent, then escalate to SIGKILL on
// the same group when the deadline elapses.
//
// Returns nil after the child has exited (either via SIGTERM-induced
// graceful path or SIGKILL fallback). The worker's slog "worker.exit"
// line still surfaces under both paths because the spawner's
// cmd.Stdout / cmd.Stderr stay attached.
func (w *execWorker) Stop() error {
	if w.cmd.Process == nil {
		return nil
	}
	pgid, err := syscall.Getpgid(w.cmd.Process.Pid)
	if err != nil || pgid <= 0 {
		// Process likely already exited. Fall back to single-pid Kill
		// which will return nil for a dead process.
		return w.cmd.Process.Kill()
	}

	// Phase 1: SIGTERM the whole pgroup. Errors are best-effort — a
	// concurrently-exited child surfaces ESRCH which we ignore.
	_ = syscall.Kill(-pgid, syscall.SIGTERM)

	grace := w.grace
	if grace <= 0 {
		grace = DefaultStopGracePeriod
	}
	timer := time.NewTimer(grace)
	defer timer.Stop()
	select {
	case <-w.waitDone:
		return nil
	case <-timer.C:
		// Phase 2: escalate to SIGKILL — graceful window expired.
		_ = syscall.Kill(-pgid, syscall.SIGKILL)
		<-w.waitDone
		return nil
	}
}

// runWait drains cmd.Wait into waitDone + waitErr. The function is the
// only goroutine that closes waitDone — sync.Once would be redundant
// because supervisor.Loop holds a single execWorker per spawn.
func (w *execWorker) runWait() {
	err := w.cmd.Wait()
	w.mu.Lock()
	w.waitErr = err
	w.mu.Unlock()
	close(w.waitDone)
}
