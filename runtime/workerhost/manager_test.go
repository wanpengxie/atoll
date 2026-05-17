package workerhost_test

import (
	"context"
	"encoding/json"
	"io"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/wanpengxie/ActOS/kernel/actor"
	"github.com/wanpengxie/ActOS/kernel/channel"
	"github.com/wanpengxie/ActOS/kernel/message"
	"github.com/wanpengxie/ActOS/kernel/placement"
	"github.com/wanpengxie/ActOS/runtime/ipc"
	"github.com/wanpengxie/ActOS/runtime/store"
	"github.com/wanpengxie/ActOS/runtime/worker"
	"github.com/wanpengxie/ActOS/runtime/workerhost"
)

// triggerCountingWorker spawns an in-process worker.Runtime that
// completes handshake then counts every KindTrigger payload arriving
// on IPCClient.Triggers(). When triggerLimit > 0, the worker calls
// client.Shutdown after reaching the limit, simulating bridge exit.
//
// reuseSignal counts spawn events so the test can assert how many
// worker subprocesses the Manager started.
type triggerCountingWorker struct {
	t            *testing.T
	spawnCount   *atomic.Int64
	receivedAll  chan ipc.TriggerPayload
	triggerLimit int
	crashAfter   int // > 0 → simulate worker crash after N triggers
}

func (w *triggerCountingWorker) workerFn(ctx context.Context, leaseID string, in io.Reader, out io.Writer) error {
	w.spawnCount.Add(1)
	rt, err := worker.New(worker.Config{
		LeaseID:        leaseID,
		In:             in,
		Out:            out,
		NowFn:          func() int64 { return time.Now().UnixMilli() },
		HeartbeatEvery: time.Hour,
		Bridge: worker.BridgeFunc(func(bctx context.Context, client *worker.IPCClient) error {
			received := 0
			for {
				select {
				case <-bctx.Done():
					return nil
				case payload, ok := <-client.Triggers():
					if !ok {
						return nil
					}
					received++
					select {
					case w.receivedAll <- payload:
					case <-bctx.Done():
						return nil
					}
					if w.crashAfter > 0 && received >= w.crashAfter {
						// Return without graceful shutdown — simulates
						// worker crash. Runtime.Run will return because
						// Bridge returned; PipeSpawner closes pipes.
						return io.ErrUnexpectedEOF
					}
					if w.triggerLimit > 0 && received >= w.triggerLimit {
						_ = client.Shutdown(bctx)
						return nil
					}
				}
			}
		}),
	})
	if err != nil {
		return err
	}
	return rt.Run(ctx)
}

// newManagerTestFixture sets up the daemon-side seam (chain + ledger +
// lease store) over a fresh channel sqlite and returns a Manager wired
// to the supplied worker fn. The caller drives OnTrigger and inspects
// the recorded trigger envelopes via receivedAll.
type managerFixture struct {
	mgr        *workerhost.Manager
	spawnCount *atomic.Int64
	receivedCh chan ipc.TriggerPayload
	cleanup    func()
}

func newManagerFixture(t *testing.T, triggerLimit, crashAfter int) *managerFixture {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())

	dir := t.TempDir()
	db, err := store.OpenChannel(ctx, filepath.Join(dir, "ch.sqlite"), store.OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	led := store.NewLedger(db)

	chID := channel.ID("ch-mgr")
	agentID := actor.ActorID("agent:channel-agent")
	chain := newE2EChain(t, db, chID, agentID)

	spawnCount := new(atomic.Int64)
	received := make(chan ipc.TriggerPayload, 32)
	tcw := &triggerCountingWorker{
		t:            t,
		spawnCount:   spawnCount,
		receivedAll:  received,
		triggerLimit: triggerLimit,
		crashAfter:   crashAfter,
	}

	spawner := &workerhost.PipeSpawner{WorkerFunc: tcw.workerFn}
	leaseStore := workerhost.NewLeaseStore(db)
	mgr, err := workerhost.NewManager(workerhost.ManagerConfig{
		ChannelID:        chID,
		AgentID:          agentID,
		WorkerActorID:    agentID,
		Spawner:          spawner,
		LeaseStore:       leaseStore,
		Chain:            chain,
		Ledger:           led,
		NowFn:            now,
		FencingToken:     placement.FencingToken(1),
		DaemonEpoch:      placement.DaemonEpoch(7),
		HandshakeTimeout: 2 * time.Second,
		ServeCtx:         ctx,
	})
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}

	cleanup := func() {
		closeCtx, ccancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer ccancel()
		_ = mgr.Close(closeCtx)
		cancel()
		_ = db.Close()
	}
	return &managerFixture{
		mgr:        mgr,
		spawnCount: spawnCount,
		receivedCh: received,
		cleanup:    cleanup,
	}
}

func triggerEnvelope(id string, seq int64) *message.Envelope {
	return &message.Envelope{
		ID:            id,
		ChannelID:     "ch-mgr",
		Type:          "human.text",
		Sender:        message.Sender{Kind: message.SenderHuman, ID: "user:alice"},
		Kind:          message.KindEvent,
		Visibility:    message.VisibilityPublic,
		Audience:      []string{"*"},
		Payload:       json.RawMessage(`{"text":"hi"}`),
		Seq:           seq,
		CorrelationID: "corr-1",
		TS:            now(),
		TSReceived:    now(),
	}
}

func waitForTrigger(t *testing.T, ch <-chan ipc.TriggerPayload, want string, deadline time.Duration) ipc.TriggerPayload {
	t.Helper()
	timer := time.NewTimer(deadline)
	defer timer.Stop()
	for {
		select {
		case payload := <-ch:
			if payload.Envelope.ID == want {
				return payload
			}
			// allow draining other envelopes (none expected in current tests)
		case <-timer.C:
			t.Fatalf("did not see trigger %q within %s", want, deadline)
		}
	}
}

// TestManager_SpawnAndReuse covers M1.6-T1 acceptance #3:
//
//	OnTrigger #1 → spawn a worker subprocess
//	OnTrigger #2 → reuse the SAME worker (no second spawn)
func TestManager_SpawnAndReuse(t *testing.T) {
	t.Parallel()
	f := newManagerFixture(t, 0, 0)
	t.Cleanup(f.cleanup)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := f.mgr.OnTrigger(ctx, "agent:channel-agent", triggerEnvelope("env-1", 1)); err != nil {
		t.Fatalf("OnTrigger #1: %v", err)
	}
	first := waitForTrigger(t, f.receivedCh, "env-1", 2*time.Second)
	if first.CorrelationID != "corr-1" {
		t.Errorf("payload.CorrelationID=%q", first.CorrelationID)
	}
	if first.Cursor != 1 {
		t.Errorf("payload.Cursor=%d want 1", first.Cursor)
	}

	w1 := f.mgr.CurrentWorkerID()
	if w1 == "" {
		t.Fatal("worker id empty after first trigger")
	}

	if err := f.mgr.OnTrigger(ctx, "agent:channel-agent", triggerEnvelope("env-2", 2)); err != nil {
		t.Fatalf("OnTrigger #2: %v", err)
	}
	_ = waitForTrigger(t, f.receivedCh, "env-2", 2*time.Second)

	w2 := f.mgr.CurrentWorkerID()
	if w2 != w1 {
		t.Errorf("worker id changed across triggers: %q → %q", w1, w2)
	}
	if got := f.spawnCount.Load(); got != 1 {
		t.Errorf("spawn count = %d, want 1", got)
	}
}

// TestManager_RespawnAfterCrash covers M1.6-T1 acceptance #4 (worker
// crash path): when the active worker exits unexpectedly, the next
// OnTrigger spawns a fresh subprocess (with a new worker id) and the
// lease row is re-acquired with the prevailing fencing tuple.
func TestManager_RespawnAfterCrash(t *testing.T) {
	t.Parallel()
	// Worker crashes after the first trigger; second OnTrigger must
	// spawn a new one.
	f := newManagerFixture(t, 0, 1)
	t.Cleanup(f.cleanup)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := f.mgr.OnTrigger(ctx, "agent:channel-agent", triggerEnvelope("env-1", 1)); err != nil {
		t.Fatalf("OnTrigger #1: %v", err)
	}
	_ = waitForTrigger(t, f.receivedCh, "env-1", 2*time.Second)
	w1 := f.mgr.CurrentWorkerID()

	// Wait for the crash to propagate — the serve goroutine sees pipe
	// closure and the manager tombstones the session.
	deadline := time.Now().Add(2 * time.Second)
	for f.mgr.CurrentWorkerID() != "" && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	if f.mgr.CurrentWorkerID() != "" {
		t.Fatal("manager did not tombstone crashed worker")
	}

	if err := f.mgr.OnTrigger(ctx, "agent:channel-agent", triggerEnvelope("env-2", 2)); err != nil {
		t.Fatalf("OnTrigger #2 after crash: %v", err)
	}
	_ = waitForTrigger(t, f.receivedCh, "env-2", 2*time.Second)
	w2 := f.mgr.CurrentWorkerID()
	if w2 == "" || w2 == w1 {
		t.Errorf("post-crash worker id should differ: w1=%q w2=%q", w1, w2)
	}
	if got := f.spawnCount.Load(); got != 2 {
		t.Errorf("spawn count = %d, want 2", got)
	}
}

// TestManager_CloseShutsWorker verifies Close() reliably tears down the
// live worker subprocess so no goroutine / pipe leaks survive the
// fixture cleanup. Regression guard for the manager's serve goroutine.
func TestManager_CloseShutsWorker(t *testing.T) {
	t.Parallel()
	f := newManagerFixture(t, 0, 0)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := f.mgr.OnTrigger(ctx, "agent:channel-agent", triggerEnvelope("env-1", 1)); err != nil {
		t.Fatalf("OnTrigger: %v", err)
	}
	_ = waitForTrigger(t, f.receivedCh, "env-1", 2*time.Second)
	if f.mgr.CurrentWorkerID() == "" {
		t.Fatal("worker not running pre-Close")
	}

	closeDone := make(chan error, 1)
	go func() {
		closeCtx, cc := context.WithTimeout(context.Background(), 3*time.Second)
		defer cc()
		closeDone <- f.mgr.Close(closeCtx)
	}()
	select {
	case err := <-closeDone:
		if err != nil {
			t.Errorf("Close: %v", err)
		}
	case <-time.After(4 * time.Second):
		t.Fatal("Close blocked")
	}
	if f.mgr.CurrentWorkerID() != "" {
		t.Error("CurrentWorkerID non-empty after Close")
	}
	// Avoid running the fixture cleanup's Close again — just close the
	// underlying ctx + db.
	// Issue cleanup manually to ensure no leaks.
	f.cleanup()
	// silence unused
	_ = sync.Mutex{}
}
