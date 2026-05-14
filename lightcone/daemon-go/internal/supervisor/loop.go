package supervisor

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
)

// DefaultPeriod is the protocol-baseline supervisor scan period per
// L2 §1.4.10 ("Supervisor 周期：扫 worker_locks 默认每 10 秒一次").
// Production callers may shrink this for tests (1ms is common) or
// stretch it via channel config.
const DefaultPeriod = 10 * time.Second

// Spawner is the abstraction the supervisor calls to bring a fresh
// worker process online. The production implementation (added in
// T10) returns an exec.Cmd wrapper; tests inject a fake that
// records the SpawnContext and signals "alive / killed" via channels.
//
// Spawn MUST return promptly — long blocking work (downloading
// binaries, etc.) belongs upstream of the supervisor loop.
type Spawner interface {
	Spawn(ctx context.Context, sc SpawnContext) (Worker, error)
}

// Worker is the supervisor's handle on a live worker process. The
// three methods cover the supervisor's full needs:
//
//   - PID returns the OS pid for log lines (test fakes return 0).
//   - Wait blocks until the process exits and returns its terminal
//     error (nil on clean exit). The supervisor launches one
//     goroutine per Wait so the OS-level exit hook fires immediately
//     without waiting for the next 10s tick.
//   - Kill sends SIGKILL (or equivalent). Used on supervisor shutdown.
type Worker interface {
	PID() int
	Wait() error
	Kill() error
}

// SpawnTrigger captures the message that woke the supervisor up enough
// to spawn a worker. Per L2 §3.4.2 the trigger context (msg_id +
// correlation_id + sender_kind) MUST flow into the worker so the
// harness Step 3 can stamp parent_id / correlation_id on every emitted
// envelope.
//
// All three fields are optional from the type's perspective — empty
// means "no trigger known yet". The supervisor populates them from the
// first backlog row in `acquireAndSpawn`; future trigger sources
// (long_pending, future_scheduler) can populate them out-of-band.
type SpawnTrigger struct {
	// MsgID is the canonical messages.id of the trigger envelope. The
	// worker uses it as the default parent_id of any reply.
	MsgID string

	// CorrelationID is the trigger envelope's correlation_id (empty when
	// the trigger itself was a root). Per L1 §2.2.1 the harness uses
	// this as the default correlation_id when the worker omits the
	// field on emit.
	CorrelationID string

	// SenderKind is the actor_kind of the trigger sender (typically
	// "agent" or "human"). Informational — used for logging and for the
	// worker's prompt context.
	SenderKind string
}

// SpawnContext is the turn-ctx the supervisor injects into every new
// worker. The fields match L2 §3.4.2 ("Trigger Context 自动注入") +
// §1.4.9 (fencing_token) + §1.4.10 (backlog scan result).
//
// The Spawner implementation chooses HOW to deliver these to the
// worker process — env vars, CLI flags, stdin JSON, etc. The default
// production wiring (T10) uses env vars per the ticket plan
// ("exec.Command + env 注入 turn-ctx").
type SpawnContext struct {
	ChannelID    string
	AgentID      string
	WorkerID     string
	FencingToken int64

	// Trigger is the message that motivated this spawn. Populated by
	// the supervisor from the first backlog row when present; empty
	// when the spawn is a maintenance / no-trigger boot. Per FIX-4 the
	// worker MUST exit cleanly without driving agent.Run when both
	// Trigger.MsgID is empty AND Backlog is empty.
	Trigger SpawnTrigger

	// AuthToken is the bearer token the worker uses when calling back
	// into the daemon over HTTP (daemon_rpc binding fallback). Empty
	// for in_worker_bus actors that never leave the process boundary.
	AuthToken string

	// DaemonURL is the daemon HTTP RPC endpoint. Optional — the worker
	// fallback path (pkg/coagent CLI subprocesses) needs it; in-process
	// in_worker_bus binding ignores it.
	DaemonURL string

	// Backlog is the L2 §1.4.10 backlog scan result — every message the
	// worker MUST replay (in seq ASC order) before the spawn closes.
	// Empty slice means "nothing to replay".
	Backlog []BacklogMessage
}

// LoopConfig tunes the supervisor loop. Zero value gives the
// protocol-baseline behaviour (10s period, 60s lease, real wall-clock,
// UUID worker ids). Tests inject custom values via LoopConfig{...}.
type LoopConfig struct {
	// Period is the ticker interval; defaults to DefaultPeriod (10s).
	Period time.Duration

	// LeaseTTL is the worker_locks lease lifetime in seconds; defaults
	// to DefaultLeaseTTL (60s).
	LeaseTTL int64

	// Now returns Unix seconds. Defaults to time.Now().Unix(). Tests
	// inject a fixed-clock pointer so they can advance time
	// deterministically.
	Now func() int64

	// NewWorkerID generates a fresh worker_id for each spawn attempt.
	// Defaults to uuid.NewString(). Tests inject a counter to make
	// log lines and assertions readable.
	NewWorkerID func() string

	// Logger receives the structured "supervisor.*" events emitted
	// across one Loop iteration. Defaults to slog.Default().
	Logger *slog.Logger

	// AuthToken is forwarded to every spawned worker as
	// SpawnContext.AuthToken — the bearer token the worker attaches to
	// daemon_rpc HTTP calls. Optional (in_worker_bus actors leave it
	// empty).
	AuthToken string

	// DaemonURL is forwarded to every spawned worker as
	// SpawnContext.DaemonURL — the daemon HTTP RPC endpoint used by
	// the coagent CLI fallback path. Optional.
	DaemonURL string
}

// Loop drives one (channel, agent) pair: it watches worker_locks,
// spawns fresh workers when the lease is empty or stolen, kicks
// graceful restarts on detected exits, and feeds backlog into every
// fresh spawn.
//
// One Loop per agent — the daemon owns N loops, one per active
// channel-agent pair. They share the channel sqlite *sql.DB but never
// each other's state.
type Loop struct {
	db        *sql.DB
	channelID string
	agentID   string
	spawner   Spawner
	cfg       LoopConfig

	// State guarded by mu — current worker (nil if no spawn yet),
	// the lock token for that worker, and a one-shot exit channel
	// closed by the wait goroutine when the worker terminates.
	mu         sync.Mutex
	current    Worker
	currentID  string
	currentTok int64
	exitCh     chan struct{}
}

// New constructs a Loop. Returns an error on missing inputs (nil db,
// empty channel/agent, nil spawner) so misuse surfaces at startup
// rather than as a 10s-delayed panic.
func New(
	db *sql.DB,
	channelID, agentID string,
	spawner Spawner,
	cfg LoopConfig,
) (*Loop, error) {
	if db == nil {
		return nil, fmt.Errorf("%w: db is nil", ErrInvalidInput)
	}
	if strings.TrimSpace(channelID) == "" {
		return nil, fmt.Errorf("%w: channel_id required", ErrInvalidInput)
	}
	if strings.TrimSpace(agentID) == "" {
		return nil, fmt.Errorf("%w: agent_id required", ErrInvalidInput)
	}
	if spawner == nil {
		return nil, fmt.Errorf("%w: spawner is nil", ErrInvalidInput)
	}
	if cfg.Period <= 0 {
		cfg.Period = DefaultPeriod
	}
	if cfg.LeaseTTL <= 0 {
		cfg.LeaseTTL = DefaultLeaseTTL
	}
	if cfg.Now == nil {
		cfg.Now = func() int64 { return time.Now().Unix() }
	}
	if cfg.NewWorkerID == nil {
		cfg.NewWorkerID = uuid.NewString
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	return &Loop{
		db:        db,
		channelID: channelID,
		agentID:   agentID,
		spawner:   spawner,
		cfg:       cfg,
	}, nil
}

// Run drives the supervisor loop until ctx is cancelled.
//
// Wake-up sources (per L2 §1.4.10 "OS-level 进程退出 hook 即时触发，
// 不等周期"):
//
//   - ticker every cfg.Period (default 10s) — drives lease-expiry
//     checks for the case where the worker hangs without crashing.
//   - exitCh closed by the Wait goroutine — fires immediately when
//     the OS reports the worker process died.
//   - ctx.Done — graceful supervisor shutdown.
//
// Each wake-up runs exactly one Tick. Tick is idempotent — calling
// it twice in a row when nothing changed is safe and cheap.
func (l *Loop) Run(ctx context.Context) error {
	ticker := time.NewTicker(l.cfg.Period)
	defer ticker.Stop()

	// First tick fires immediately so daemon startup doesn't wait
	// `Period` seconds before noticing pending backlog.
	if err := l.Tick(ctx); err != nil && !errors.Is(err, context.Canceled) {
		l.cfg.Logger.Error("supervisor.tick.error",
			"channel_id", l.channelID, "agent_id", l.agentID, "err", err.Error())
	}

	for {
		// Snapshot the exit channel under the lock so concurrent
		// closes (Tick spawning a new worker) don't race with the
		// select below.
		l.mu.Lock()
		exitCh := l.exitCh
		l.mu.Unlock()

		select {
		case <-ctx.Done():
			l.shutdown()
			return ctx.Err()
		case <-ticker.C:
		case <-orNever(exitCh):
		}

		if err := l.Tick(ctx); err != nil && !errors.Is(err, context.Canceled) {
			l.cfg.Logger.Error("supervisor.tick.error",
				"channel_id", l.channelID, "agent_id", l.agentID, "err", err.Error())
		}
	}
}

// Tick executes one supervisor iteration. The decision tree mirrors
// L2 §1.4.10 pseudocode (with the FIX-4 lease-validation tightening):
//
//	current worker recorded and lease still ours → no-op
//	current worker recorded but lease lost/expired → Stop + steal + spawn
//	lock missing OR expired                       → CAS acquire/steal → spawn → backlog
//	lock present and not expired                  → another supervisor owns it; no-op
//	lock owned by us but worker exited            → release + spawn next tick
//
// Tick is exported primarily for tests — Run calls Tick on every
// wake-up, but a unit test can drive the state machine manually
// without spinning real timers.
//
// Tick reads l.current under mu; the wait goroutine clears l.current
// atomically before closing exitCh, so a `current == nil` snapshot
// always means "no live worker, even if the previous one was alive
// on the previous tick".
func (l *Loop) Tick(ctx context.Context) error {
	l.mu.Lock()
	cur := l.current
	curID := l.currentID
	curTok := l.currentTok
	lastWorkerID := l.currentID
	l.mu.Unlock()

	now := l.cfg.Now()

	if cur != nil {
		// FIX-4 codex t90: even when the supervisor still believes a
		// worker is live, validate the worker_locks row before bailing
		// out. The worker's heartbeat goroutine can die (panic, leaked
		// stop, sqlite EAGAIN starvation) while the OS-level process
		// keeps running — without this guard the supervisor would
		// happily skip every Tick until Run noticed the OS exit, which
		// may never come.
		lock, err := Get(ctx, l.db, l.agentID)
		if err != nil && !errors.Is(err, ErrLockMissing) {
			return fmt.Errorf("supervisor: get lock (current alive): %w", err)
		}
		matches := err == nil && lock.WorkerID == curID && lock.FencingToken == curTok && !lock.Expired(now)
		if matches {
			// Healthy: heartbeat is keeping the lease green. Wait
			// goroutine handles the OS-exit path.
			return nil
		}
		// Lease lost (stolen by another supervisor) or expired
		// (heartbeat goroutine dead). Stop the orphaned worker so it
		// doesn't keep emitting under the now-stale fencing_token,
		// clear our snapshot, then fall through to acquireAndSpawn
		// this tick.
		l.cfg.Logger.Warn("supervisor.tick.lease_invalid",
			"channel_id", l.channelID, "agent_id", l.agentID,
			"worker_id", curID, "fencing_token", curTok,
			"lock_present", err == nil,
		)
		if serr := stopOrKill(cur); serr != nil {
			l.cfg.Logger.Warn("supervisor.tick.stop.error",
				"err", serr.Error(), "worker_id", curID)
		}
		l.mu.Lock()
		// Only clear when the snapshot still matches — Run/wait
		// may have raced and already replaced l.current.
		if l.currentID == curID {
			l.current = nil
			l.currentID = ""
			l.currentTok = 0
		}
		l.mu.Unlock()
		// Continue into the lock-state branches below — we want this
		// tick to immediately spawn a replacement rather than wait
		// another full Period.
	}
	lock, err := Get(ctx, l.db, l.agentID)
	switch {
	case errors.Is(err, ErrLockMissing):
		// No row at all — first-ever spawn or post-release respawn.
		return l.acquireAndSpawn(ctx, now)
	case err != nil:
		return fmt.Errorf("supervisor: get lock: %w", err)
	}

	if lock.Expired(now) {
		// Lease expired (worker crashed or hung). Steal + spawn.
		return l.acquireAndSpawn(ctx, now)
	}

	// Lock still valid. If WE used to own it (lastWorkerID matches
	// the row owner) but the wait goroutine cleared l.current, the
	// process died before the lease expired (e.g. SIGKILL'd before
	// heartbeat tick). Release the orphan row so the next tick spawns
	// a replacement immediately instead of waiting for lease expiry.
	if lastWorkerID != "" && lastWorkerID == lock.WorkerID {
		if err := Release(ctx, l.db, l.agentID, lock.WorkerID); err != nil && !errors.Is(err, ErrLockMissing) {
			return fmt.Errorf("supervisor: release after exit: %w", err)
		}
		l.cfg.Logger.Info("supervisor.lock.released_after_exit",
			"channel_id", l.channelID, "agent_id", l.agentID,
			"worker_id", lock.WorkerID, "fencing_token", lock.FencingToken)
		// Clear the stale lastWorkerID so subsequent Ticks don't
		// re-release on stale memory.
		l.mu.Lock()
		if l.currentID == lastWorkerID {
			l.currentID = ""
			l.currentTok = 0
		}
		l.mu.Unlock()
		// Don't re-spawn this tick — next wake-up runs through the
		// "lock missing" branch and starts fresh.
	}
	return nil
}

// acquireAndSpawn runs the L2 §1.4.10 spawn branch:
//
//  1. CAS acquire/steal → returns Lock with fresh fencing_token
//  2. backlog scan; empty AND no external trigger → release + return
//     (T110 / R2-FIX-4 idle-respawn guard — see below)
//  3. spawn worker; on failure release lock (avoid orphan)
//  4. record (worker, fencing_token, exitCh); start Wait goroutine
//
// Idle-respawn guard (R2-FIX-4): the worker runtime exits cleanly with
// SkippedNoTrigger=true when both Trigger and Backlog are empty
// (runtime.go:321-356). Without a supervisor-side guard the closed
// exitCh wakes Run, Tick sees lock-missing (worker Released on exit),
// re-enters acquireAndSpawn → spawns another noop worker → hot-loop.
//
// "External trigger signal" in M1.3 baseline reduces to
// `len(backlog) > 0`: future_scheduler / long_pending / RPC paths all
// land messages in the channel sqlite that BacklogScan picks up on the
// next tick. There is no separate kick channel into the supervisor
// today; the Period ticker covers idle-channel latency. When an
// explicit kick channel lands, extend the guard to
// `len(backlog) == 0 && !externalKick`.
func (l *Loop) acquireAndSpawn(ctx context.Context, now int64) error {
	workerID := l.cfg.NewWorkerID()
	if strings.TrimSpace(workerID) == "" {
		return fmt.Errorf("supervisor: NewWorkerID returned empty")
	}

	lock, err := Acquire(ctx, l.db, l.agentID, workerID, l.cfg.LeaseTTL, func() int64 { return now })
	if err != nil {
		if errors.Is(err, ErrLockHeld) {
			// Another supervisor / daemon won the race; skip this tick.
			l.cfg.Logger.Info("supervisor.acquire.lost",
				"channel_id", l.channelID, "agent_id", l.agentID)
			return nil
		}
		return fmt.Errorf("supervisor: acquire: %w", err)
	}

	// Backlog scan BEFORE spawn so we can hand it to the worker.
	backlog, err := BacklogScan(ctx, l.db, l.agentID, now)
	if err != nil {
		// Release the lock we just took — otherwise it would expire
		// naturally but block the next supervisor tick for `LeaseTTL`
		// seconds.
		_ = Release(ctx, l.db, l.agentID, workerID)
		return fmt.Errorf("supervisor: backlog scan: %w", err)
	}

	// R2-FIX-4 idle-respawn guard: nothing to do this tick. Release the
	// lock immediately so peer supervisors / the next ticker iteration
	// don't have to wait for the lease to expire. Returning nil leaves
	// l.current == nil.
	//
	// R4-FIX-B: we ALSO reset l.exitCh to nil here. Background: when a
	// previous worker exited cleanly, waitWorker closed the exitCh but
	// left l.exitCh pointing at that (now-closed) channel. orNever only
	// short-circuits on nil — a closed channel is permanently readable,
	// so Run.select would wake every iteration on `<-orNever(exitCh)`,
	// run Tick → idle-guard → return, and immediately loop again
	// (hot-spin). Clearing l.exitCh = nil tells orNever to return a nil
	// receive-only channel, which blocks forever in select; Run then
	// falls back to the ticker until either a fresh spawn writes a new
	// exitCh or a new backlog arrives.
	if len(backlog) == 0 {
		if rerr := Release(ctx, l.db, l.agentID, workerID); rerr != nil && !errors.Is(rerr, ErrLockMissing) {
			// Surface the release failure but do NOT return it: another
			// supervisor that later sees this row will steal-on-expiry,
			// so the lock can't strand indefinitely. Logging at Warn
			// level is the right operator signal — turning this into a
			// hard error would convert an empty-channel tick into a
			// retry storm.
			l.cfg.Logger.Warn("supervisor.acquire.idle.release.error",
				"channel_id", l.channelID, "agent_id", l.agentID,
				"worker_id", workerID, "err", rerr.Error())
		}
		// R4-FIX-B: drop the stale exitCh reference so Run.select can
		// idle on the ticker. Must be under l.mu — shutdown / Run /
		// acquireAndSpawn all touch l.exitCh under the same lock.
		l.mu.Lock()
		l.exitCh = nil
		l.mu.Unlock()
		l.cfg.Logger.Info("supervisor.acquire.idle",
			"channel_id", l.channelID, "agent_id", l.agentID,
			"worker_id", workerID, "fencing_token", lock.FencingToken)
		return nil
	}

	sc := SpawnContext{
		ChannelID:    l.channelID,
		AgentID:      l.agentID,
		WorkerID:     workerID,
		FencingToken: lock.FencingToken,
		AuthToken:    l.cfg.AuthToken,
		DaemonURL:    l.cfg.DaemonURL,
		Backlog:      backlog,
	}
	// Per L2 §3.4.2: populate Trigger from the first backlog row so the
	// worker's harness can stamp parent_id / correlation_id without
	// re-reading the message. Empty backlog → Trigger stays zero, the
	// worker treats this as a "no-trigger" boot and exits cleanly.
	if len(backlog) > 0 {
		first := backlog[0]
		corr := first.CorrelationID
		if corr == "" {
			// Per L1 §2.2.1 fall back to the trigger's own message id
			// when the row's correlation_id column is NULL — the worker
			// then propagates that value as the correlation_id of every
			// reply.
			corr = first.ID
		}
		sc.Trigger = SpawnTrigger{
			MsgID:         first.ID,
			SenderKind:    first.SenderKind,
			CorrelationID: corr,
		}
	}
	w, err := l.spawner.Spawn(ctx, sc)
	if err != nil {
		// Spawn failed — release the lock so we don't orphan it for
		// LeaseTTL seconds (the §1.4.10 pseudocode's "SpawnFailed →
		// release lock" branch).
		if rerr := Release(ctx, l.db, l.agentID, workerID); rerr != nil && !errors.Is(rerr, ErrLockMissing) {
			l.cfg.Logger.Warn("supervisor.release.after_spawn_fail.error",
				"err", rerr.Error())
		}
		return fmt.Errorf("supervisor: spawn: %w", err)
	}

	// Hand the worker off — record its identity and start the Wait
	// goroutine that signals exitCh on process death.
	exitCh := make(chan struct{})
	l.mu.Lock()
	l.current = w
	l.currentID = workerID
	l.currentTok = lock.FencingToken
	l.exitCh = exitCh
	l.mu.Unlock()

	go l.waitWorker(w, workerID, exitCh)

	l.cfg.Logger.Info("supervisor.spawn.ok",
		"channel_id", l.channelID, "agent_id", l.agentID,
		"worker_id", workerID, "fencing_token", lock.FencingToken,
		"pid", w.PID(), "backlog_size", len(backlog))
	return nil
}

// waitWorker blocks on Worker.Wait() and, when the process exits,
// performs the supervisor's "worker died" bookkeeping in a single
// strict order:
//
//  1. clear l.current under mu (Tick will now see no live worker)
//  2. close exitCh (Run wakes up early; the order matters — a Run
//     iteration that wakes on exitCh MUST observe l.current == nil)
//  3. emit the structured "worker.exit" log
//
// sync.Once defends against future callers that might invoke close
// twice; today Wait returns once so the guard is purely defensive.
func (l *Loop) waitWorker(w Worker, workerID string, exitCh chan struct{}) {
	var once sync.Once
	closer := func() { once.Do(func() { close(exitCh) }) }

	err := w.Wait()

	l.mu.Lock()
	if l.currentID == workerID {
		l.current = nil
	}
	l.mu.Unlock()

	closer()

	if err != nil {
		l.cfg.Logger.Warn("supervisor.worker.exit",
			"channel_id", l.channelID, "agent_id", l.agentID,
			"worker_id", workerID, "err", err.Error())
	} else {
		l.cfg.Logger.Info("supervisor.worker.exit",
			"channel_id", l.channelID, "agent_id", l.agentID,
			"worker_id", workerID)
	}
}

// currentWorker returns the live worker reference (nil when no spawn
// has succeeded yet, or after Wait completed). Package-private — kept
// as a test seam so loop_test.go can poll the post-Crash transition
// without racing on the wait goroutine's bookkeeping.
func (l *Loop) currentWorker() Worker {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.current
}

// GracefulStopper is the optional interface a Worker may implement to
// participate in the SIGTERM-then-SIGKILL graceful shutdown path. When
// the supervisor wants the worker dead "soon, but with a chance to
// release its lock first" it prefers Stop over Kill — the production
// ExecSpawner provides Stop, while test fakes typically just satisfy
// Worker.Kill.
type GracefulStopper interface {
	Stop() error
}

// stopOrKill prefers Worker.Stop (graceful) and falls back to Kill
// (forceful) when the implementation doesn't expose Stop. Used by the
// supervisor's shutdown + lease-mismatch reclaim paths so production
// workers get a chance to defer-Release their worker_locks row.
func stopOrKill(w Worker) error {
	if s, ok := w.(GracefulStopper); ok {
		return s.Stop()
	}
	return w.Kill()
}

// shutdown stops the current worker (if any) — graceful first, fallback
// to Kill — and waits for the exit channel so callers can rely on
// "Run returned ⇒ no goroutines outstanding". The lock row stays —
// the next supervisor (this daemon restart, or another instance)
// handles it via lease expiry. When the worker honoured SIGTERM it
// will already have Released the row from its own defer.
func (l *Loop) shutdown() {
	l.mu.Lock()
	w := l.current
	exitCh := l.exitCh
	l.mu.Unlock()

	if w == nil {
		return
	}
	if err := stopOrKill(w); err != nil {
		l.cfg.Logger.Warn("supervisor.shutdown.stop.error", "err", err.Error())
	}
	if exitCh != nil {
		<-exitCh
	}
}

// orNever returns ch unchanged (non-nil) or a nil channel (which
// blocks forever in select) when ch is nil. Lets Run() compose a
// "select with optional exit channel" without branching on nil.
func orNever(ch chan struct{}) <-chan struct{} {
	if ch == nil {
		return nil
	}
	return ch
}
