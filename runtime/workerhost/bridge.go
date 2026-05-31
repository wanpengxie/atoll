package workerhost

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/wanpengxie/ActOS/kernel/actor"
	"github.com/wanpengxie/ActOS/kernel/channel"
	khar "github.com/wanpengxie/ActOS/kernel/harness"
	"github.com/wanpengxie/ActOS/kernel/message"
	"github.com/wanpengxie/ActOS/kernel/placement"
	"github.com/wanpengxie/ActOS/runtime/ipc"
)

// Bridge is the per-channel worker bridge: it lazily spawns a single
// worker subprocess on the first trigger envelope, reuses it across
// subsequent triggers, and re-spawns on crash. Built around the
// existing Spawner / LeaseStore / Host primitives so the only new
// behavioural surface is "when does a worker exist".
//
// Wiring:
//
//	scheduler.Deliverer.Register(ChannelAgentID, bridge.OnTrigger)
//	bootChannel constructs Bridge, stores it on channelRuntime, and
//	defers bridge.Close in the unloader teardown.
//
// Concurrency: OnTrigger is called from the daemon dispatcher (write
// path) and from the long-pending scheduler scan. Both paths can race;
// a single mutex serialises spawn / session replacement, but the worker
// stdin write happens outside the mutex with a timeout so a slow worker
// cannot block unrelated trigger dispatchers behind Bridge.mu.
type Bridge struct {
	cfg BridgeConfig

	mu       sync.Mutex
	cur      *workerSession
	closed   bool
	spawnSeq atomic.Int64
	leaseSeq atomic.Int64
	pushDrop atomic.Int64
}

// BridgeConfig wires a Bridge.
type BridgeConfig struct {
	ChannelID     channel.ID
	AgentID       actor.ActorID // lease + actor target — e.g. ChannelAgentID
	WorkerActorID actor.ActorID // principal the worker speaks as on IPC writes

	Spawner    Spawner
	LeaseStore *LeaseStore
	Chain      khar.Chain
	Ledger     LedgerOps

	NowFn func() int64

	// Fencing snapshot at construction. Bridge assumes the channel
	// lock doesn't change during its lifetime; daemon unloads + re-
	// builds the bridge on placement reclaim, so this is safe.
	FencingToken placement.FencingToken
	DaemonEpoch  placement.DaemonEpoch

	// HandshakeTimeout caps how long OnTrigger waits for the freshly
	// spawned worker's handshake before giving up. Default 5s.
	HandshakeTimeout time.Duration

	// WorkerIDPrefix prepends a stable label to generated worker ids.
	// Defaults to "w" — production wiring sets to the daemon id so
	// log lines correlate.
	WorkerIDPrefix string

	// ServeCtx is the long-lived context the Host.Serve goroutine
	// inherits. Defaults to context.Background() if unset, but
	// production should pass the daemon runCtx so shutdown cascades
	// reach the worker subprocesses.
	ServeCtx context.Context

	// WorkerEnv is the per-channel "KEY=VALUE" list the daemon
	// composition root wires in so every Spawn invocation carries the
	// channel-scoped env (e.g. COAGENT_CHANNEL_TYPE,
	// COAGENT_DOMAIN_PROMPT, COAGENT_CHANNEL_ID). M1.6-T5 phase-3
	// runtime/daemon.ensureChannelAgent populates this from
	// ChannelLock.ChannelType + resolveTemplate(...).DomainPrompt so
	// the worker bridge can hash / grep the L4 §2.4 prompt without
	// re-resolving the template. Empty/nil ⇒ Bridge spawns the
	// worker with only os.Environ + the Spawner's static env list.
	WorkerEnv []string

	// PreSpawn is an optional hook invoked at the top of every
	// Spawner.Spawn call (i.e. once per worker subprocess, NOT once
	// per trigger — the bridge reuses a live worker across triggers
	// and only re-spawns on crash / first trigger after boot). The
	// hook returns an extra "KEY=VALUE" env slice that is appended
	// AFTER WorkerEnv so PreSpawn-supplied values override defaults.
	//
	// Production wiring (M1.6 agent self-awareness fix): cmd/daemon
	// implements PreSpawn to (1) snapshot the channel's actor_registry
	// + type_registry, (2) write a JSON file into the channel workdir,
	// (3) return ["COAGENT_CHANNEL_CONTEXT_FILE=
	// <abs-path>"]. The worker bridge folds the file into its system
	// prompt so the LLM knows what tools / actors / devices live in
	// the channel — without it the agent is blind and falls back to
	// host-filesystem exploration ("Chrome 145 / type_registry空"
	// hallucinated reply class).
	//
	// Hook errors do NOT abort the spawn — the bridge logs (via the
	// returned error path is currently best-effort) and proceeds with
	// only WorkerEnv. Worker boots cleanly without the appendix, same
	// as a legacy channel. Nil hook → behaviour identical to before
	// this field was added.
	PreSpawn func(ctx context.Context) (extraEnv []string, err error)

	// PushTimeout caps how long the daemon waits for the worker's
	// ACCEPT acknowledgement of a pushed trigger (§3 ack 三分: accept is
	// "received, will run", NOT "turn complete"). This is deliberately a
	// short ms-seconds budget — the worker enqueues the turn and acks
	// accepted immediately, then runs the turn asynchronously and reports
	// completion through the channel log (envelope), so this timeout is
	// fully DECOUPLED from turn duration. Default 10s. It is intentionally
	// NOT clamped by the envelope's expires_at: a short request deadline
	// must not abort a slow-but-live turn — that closure is the daemon's
	// long-pending / F3 job, not the trigger push.
	PushTimeout time.Duration

	// OnPushDrop observes trigger pushes that failed or timed out. It is
	// invoked after the bridge increments PushDropCount.
	OnPushDrop func(PushDrop)

	// HeartbeatTTL is the freshness window for a worker heartbeat. When an
	// accept ACK times out (ErrAcceptTimeout) but the worker has heartbeat
	// (or just spawned) within this window, the bridge treats the worker as
	// "live but not yet accepted" and retries delivery under AcceptRetries
	// instead of killing + respawning it. A worker whose heartbeat is stale
	// past this window is treated as dead transport and recycled. Default
	// 90s (three missed 30s worker heartbeats).
	HeartbeatTTL time.Duration

	// AcceptRetries bounds how many extra accept-timeout retries a single
	// OnTrigger performs against a heartbeat-fresh worker before giving up
	// (and recycling it). The bound is what keeps "don't kill a live worker"
	// from becoming "retry forever". Default 2 (so up to 3 push attempts).
	AcceptRetries int
}

// PushDrop describes one failed daemon → worker trigger push.
type PushDrop struct {
	ChannelID  channel.ID
	AgentID    actor.ActorID
	WorkerID   string
	EnvelopeID string
	Err        error
}

// workerSession is the daemon-side state for one live worker subprocess.
type workerSession struct {
	leaseID   string
	lockLease Lease
	workerID  string
	proc      WorkerProc
	host      *Host

	// spawnedAt is the unix-ms the session was created. Until the first
	// heartbeat arrives it doubles as the liveness baseline so a worker that
	// just handshook (and has not yet had a chance to heartbeat) is still
	// considered fresh during the accept-retry window.
	spawnedAt int64
	// lastHeartbeat is the unix-ms of the most recent worker heartbeat, set
	// from HostConfig.OnHeartbeat. Read/written atomically — the serve
	// goroutine writes it while OnTrigger reads it.
	lastHeartbeat atomic.Int64

	// done closes when Host.Serve returns (worker exit or fatal IPC
	// error). Set under Bridge.mu via serve goroutine close-on-exit.
	done chan struct{}
}

// fresh reports whether the worker is heartbeat-fresh as of now: either a
// heartbeat (or the spawn baseline) landed within ttl. A fresh worker that
// merely failed to accept a trigger in time is "live but not accepted", not
// dead transport, so the bridge retries delivery instead of killing it.
func (s *workerSession) fresh(now, ttl int64) bool {
	if s == nil || ttl <= 0 {
		return false
	}
	last := s.lastHeartbeat.Load()
	if last == 0 {
		last = s.spawnedAt
	}
	return now-last <= ttl
}

// dead reports whether the session's serve goroutine has returned.
func (s *workerSession) dead() bool {
	if s == nil {
		return true
	}
	select {
	case <-s.done:
		return true
	default:
		return false
	}
}

// NewBridge builds a Bridge. Defaults to a 5s handshake timeout, a 30s
// trigger ACK timeout, and a background ServeCtx when callers don't pass
// them.
func NewBridge(cfg BridgeConfig) (*Bridge, error) {
	if cfg.ChannelID == "" {
		return nil, errors.New("workerhost: BridgeConfig.ChannelID empty")
	}
	if cfg.AgentID == "" {
		return nil, errors.New("workerhost: BridgeConfig.AgentID empty")
	}
	if cfg.WorkerActorID == "" {
		return nil, errors.New("workerhost: BridgeConfig.WorkerActorID empty")
	}
	if cfg.Spawner == nil {
		return nil, errors.New("workerhost: BridgeConfig.Spawner nil")
	}
	if cfg.LeaseStore == nil {
		return nil, errors.New("workerhost: BridgeConfig.LeaseStore nil")
	}
	if cfg.Chain == nil {
		return nil, errors.New("workerhost: BridgeConfig.Chain nil")
	}
	if cfg.Ledger == nil {
		return nil, errors.New("workerhost: BridgeConfig.Ledger nil")
	}
	if cfg.NowFn == nil {
		return nil, errors.New("workerhost: BridgeConfig.NowFn nil")
	}
	if cfg.HandshakeTimeout <= 0 {
		cfg.HandshakeTimeout = 5 * time.Second
	}
	if cfg.PushTimeout <= 0 {
		// Accept budget, not turn budget — see BridgeConfig.PushTimeout.
		cfg.PushTimeout = 10 * time.Second
	}
	if cfg.HeartbeatTTL <= 0 {
		// Three missed worker heartbeats (worker default cadence 30s).
		cfg.HeartbeatTTL = 90 * time.Second
	}
	if cfg.AcceptRetries < 0 {
		cfg.AcceptRetries = 0
	}
	if cfg.AcceptRetries == 0 {
		cfg.AcceptRetries = 2
	}
	if cfg.WorkerIDPrefix == "" {
		cfg.WorkerIDPrefix = "w"
	}
	if cfg.ServeCtx == nil {
		cfg.ServeCtx = context.Background()
	}
	return &Bridge{cfg: cfg}, nil
}

// OnTrigger is the scheduler.Deliverer.HandlerFn entry point. Per the
// L2 worker-bridge contract: spawn a worker if there isn't one alive,
// then push the envelope as a KindTrigger IPC frame.
//
// Errors here are surfaced back to gateway.Dispatch which logs them
// and continues — fan-out is at-least-once (L1 §6.1) so a single push
// failure does not stall the harness write or reject the originating
// human envelope. The next trigger retries the spawn.
func (m *Bridge) OnTrigger(ctx context.Context, _ actor.ActorID, env *message.Envelope) error {
	if env == nil {
		return errors.New("workerhost: OnTrigger nil envelope")
	}

	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return errors.New("workerhost: bridge closed")
	}

	if m.cur == nil || m.cur.dead() {
		// Best-effort cleanup of any tombstoned previous session so
		// the lease row reflects the new worker.
		if m.cur != nil {
			_ = m.cfg.LeaseStore.Release(ctx, m.cur.lockLease.ID)
			m.cur = nil
		}
		if err := m.spawnLocked(ctx); err != nil {
			m.mu.Unlock()
			return fmt.Errorf("workerhost: spawn: %w", err)
		}
	}

	sess := m.cur
	payload := ipc.TriggerPayload{
		Envelope:      *env,
		CorrelationID: env.CorrelationID,
		Cursor:        env.Seq,
	}
	m.mu.Unlock()

	// The push deadline is the ACCEPT budget only (§3 ack 三分). It is
	// decoupled from the envelope's expires_at / turn duration: the worker
	// acks accepted in ms-seconds and runs the turn asynchronously.
	//
	// Accept-timeout retry policy (codex P1 bridge.go:255): an accept
	// timeout against a HEARTBEAT-FRESH worker means "live but busy / queue
	// backpressure", NOT dead transport — killing it would recreate the 30s
	// kill/respawn loop on the accept path. We retry delivery under a bounded
	// AcceptRetries budget while the worker stays heartbeat-fresh; only a
	// transport error, a rejection, or an accept timeout on a STALE worker
	// (or budget exhaustion) recycles the session.
	ttl := m.cfg.HeartbeatTTL.Milliseconds()
	var err error
	for attempt := 0; ; attempt++ {
		pushCtx, cancel := context.WithTimeout(ctx, m.cfg.PushTimeout)
		err = sess.host.PushTrigger(pushCtx, payload)
		cancel()
		if err == nil {
			return nil
		}
		// Only an accept timeout is retryable, and only while the worker is
		// heartbeat-fresh and we have retry budget left. Everything else
		// (write/transport failure, rejection, stale worker) recycles.
		if errors.Is(err, ErrAcceptTimeout) &&
			attempt < m.cfg.AcceptRetries &&
			sess.fresh(m.cfg.NowFn(), ttl) &&
			ctx.Err() == nil {
			continue
		}
		break
	}
	// Push failure (transport broken, or a heartbeat-stale / budget-exhausted
	// accept timeout) — drop the session so the next trigger re-spawns. Don't
	// release the lease synchronously here; the serve goroutine runs that when
	// Host.Serve returns.
	m.onPushFailure(sess, string(env.ID), err)
	return fmt.Errorf("workerhost: push trigger: %w", err)
}

func (m *Bridge) onPushFailure(sess *workerSession, envelopeID string, err error) {
	m.pushDrop.Add(1)
	if m.cfg.OnPushDrop != nil {
		m.cfg.OnPushDrop(PushDrop{
			ChannelID:  m.cfg.ChannelID,
			AgentID:    m.cfg.AgentID,
			WorkerID:   sess.workerID,
			EnvelopeID: envelopeID,
			Err:        err,
		})
	}

	var stale *workerSession
	m.mu.Lock()
	if m.cur == sess {
		stale = sess
		m.cur = nil
	}
	m.mu.Unlock()

	if stale != nil {
		_ = stale.proc.Stdin.Close()
		_ = stale.proc.Kill()
	}
}

// spawnLocked must be called with m.mu held. Acquires the lease, starts
// the worker subprocess, builds a Host, fires off Host.Serve, and waits
// for the handshake (so the first PushTrigger lands after the worker's
// IPC client is reading).
func (m *Bridge) spawnLocked(ctx context.Context) error {
	leaseID := fmt.Sprintf("lease-%s-%d", m.cfg.ChannelID, m.leaseSeq.Add(1))
	workerID := fmt.Sprintf("%s-%s-%d", m.cfg.WorkerIDPrefix, m.cfg.ChannelID, m.spawnSeq.Add(1))

	lease, ok, err := m.cfg.LeaseStore.Acquire(
		ctx,
		string(m.cfg.AgentID),
		workerID,
		m.cfg.FencingToken,
		m.cfg.DaemonEpoch,
		m.cfg.NowFn(),
	)
	if err != nil {
		return fmt.Errorf("lease acquire: %w", err)
	}
	if !ok {
		return errors.New("workerhost: lease acquire conflict")
	}
	lease.ChannelID = m.cfg.ChannelID

	// PreSpawn: composition root may inject extra env (e.g.
	// COAGENT_CHANNEL_CONTEXT_FILE pointing at a freshly-written
	// channel context snapshot for the LLM system prompt). Errors are
	// non-fatal — the bridge logs nothing today (no logger plumbed)
	// and proceeds with only the static WorkerEnv. Worker boots
	// cleanly without the appendix, matching the legacy behaviour.
	spawnEnv := m.cfg.WorkerEnv
	if m.cfg.PreSpawn != nil {
		extra, _ := m.cfg.PreSpawn(ctx)
		if len(extra) > 0 {
			combined := make([]string, 0, len(spawnEnv)+len(extra))
			combined = append(combined, spawnEnv...)
			combined = append(combined, extra...)
			spawnEnv = combined
		}
	}

	proc, err := m.cfg.Spawner.Spawn(m.cfg.ServeCtx, leaseID, spawnEnv)
	if err != nil {
		_ = m.cfg.LeaseStore.Release(ctx, lease.ID)
		return fmt.Errorf("spawn: %w", err)
	}

	sess := &workerSession{
		leaseID:   leaseID,
		lockLease: lease,
		workerID:  workerID,
		proc:      proc,
		spawnedAt: m.cfg.NowFn(),
		done:      make(chan struct{}),
	}

	host, err := NewHost(proc.Stdout, proc.Stdin, HostConfig{
		ChannelID:     m.cfg.ChannelID,
		WorkerID:      workerID,
		LeaseID:       leaseID,
		FencingToken:  m.cfg.FencingToken,
		DaemonEpoch:   m.cfg.DaemonEpoch,
		Chain:         m.cfg.Chain,
		WorkerActorID: m.cfg.WorkerActorID,
		Ledger:        m.cfg.Ledger,
		NowFn:         m.cfg.NowFn,
		// Track heartbeat freshness so OnTrigger can distinguish a
		// live-but-not-accepted worker (retry) from dead transport (kill).
		OnHeartbeat: func(workerNowMs int64) {
			sess.lastHeartbeat.Store(m.cfg.NowFn())
		},
	})
	if err != nil {
		_ = proc.Kill()
		_ = m.cfg.LeaseStore.Release(ctx, lease.ID)
		return fmt.Errorf("host: %w", err)
	}
	sess.host = host
	m.cur = sess

	// Start the serve goroutine. When Serve returns (worker EOF or
	// fatal IPC error), tombstone the session so the next OnTrigger
	// re-spawns; release the lease so a future spawn can grab a new
	// fencing-token-stamped row.
	go func() {
		_ = host.Serve(m.cfg.ServeCtx)
		// Close stdin so any half-spoken worker write side terminates;
		// then wait for the subprocess so we don't leak zombies.
		_ = proc.Stdin.Close()
		_ = proc.Wait()
		// Release lease; ignore err — best-effort cleanup.
		releaseCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		_ = m.cfg.LeaseStore.Release(releaseCtx, sess.lockLease.ID)
		cancel()
		close(sess.done)
	}()

	// Wait until the worker handshakes (Host writes the ack and
	// signals Ready). PushTrigger only goes onto the wire after this.
	select {
	case <-host.Ready():
		return nil
	case <-sess.done:
		// Spawned process died before completing handshake. Drop the
		// session pointer so the next OnTrigger retries.
		m.cur = nil
		return errors.New("workerhost: worker exited before handshake")
	case <-time.After(m.cfg.HandshakeTimeout):
		_ = proc.Kill()
		<-sess.done
		m.cur = nil
		return errors.New("workerhost: handshake timeout")
	case <-ctx.Done():
		_ = proc.Kill()
		<-sess.done
		m.cur = nil
		return ctx.Err()
	}
}

// Close terminates the active worker session (if any) and marks the
// bridge as no longer accepting triggers. Idempotent.
func (m *Bridge) Close(ctx context.Context) error {
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return nil
	}
	m.closed = true
	cur := m.cur
	m.cur = nil
	m.mu.Unlock()

	if cur == nil {
		return nil
	}
	// Force exit — worker subprocess sees stdin EOF + ctx cancel via
	// ServeCtx upstream cancellation. The serve goroutine releases
	// the lease on its way out.
	_ = cur.proc.Stdin.Close()
	_ = cur.proc.Kill()
	select {
	case <-cur.done:
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(3 * time.Second):
		return errors.New("workerhost: bridge close timeout")
	}
	return nil
}

// CurrentWorkerID exposes the running worker's id for tests; "" when
// no worker is alive OR when the current session's serve goroutine has
// already exited (worker crash / Close-in-progress). The next
// OnTrigger will clear the dead session pointer and re-spawn.
func (m *Bridge) CurrentWorkerID() string {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.cur == nil || m.cur.dead() {
		return ""
	}
	return m.cur.workerID
}

// PushDropCount returns the number of trigger pushes that failed or timed out.
func (m *Bridge) PushDropCount() int64 {
	return m.pushDrop.Load()
}
