package workerhost_test

import (
	"bytes"
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
	"github.com/wanpengxie/ActOS/runtime/fence"
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
// worker subprocesses the Bridge started.
type triggerCountingWorker struct {
	t            *testing.T
	spawnCount   *atomic.Int64
	receivedAll  chan ipc.TriggerPayload
	triggerLimit int
	crashAfter   int // > 0 → simulate worker crash after N triggers
	crashGate    <-chan struct{}
	crashBudget  atomic.Int64
}

func (w *triggerCountingWorker) workerFn(ctx context.Context, leaseID string, _ []string, in io.Reader, out io.Writer) error {
	w.spawnCount.Add(1)
	crashAfter := 0
	if w.crashAfter > 0 && w.crashBudget.Add(-1) >= 0 {
		crashAfter = w.crashAfter
	}
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
					// ACK three-split (channel-lifecycle-reconcile-
					// architecture.md §3): ACCEPT immediately on dequeue,
					// decoupled from turn completion. Mirrors the real worker
					// bridges (MockBridge / kimi) — the daemon-side PushTrigger
					// resolves on this accept, not on the simulated turn work
					// below, so a slow/blocked worker turn never reads back as
					// a delivery failure that kills + respawns the worker.
					if err := client.AckTrigger(bctx, payload, true, ""); err != nil {
						return err
					}
					received++
					select {
					case w.receivedAll <- payload:
					case <-bctx.Done():
						return nil
					}
					if crashAfter > 0 && received >= crashAfter {
						if w.crashGate != nil {
							select {
							case <-w.crashGate:
							case <-bctx.Done():
								return nil
							}
						}
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

// newBridgeTestFixture sets up the daemon-side seam (chain + ledger +
// lease store) over a fresh channel sqlite and returns a Bridge wired
// to the supplied worker fn. The caller drives OnTrigger and inspects
// the recorded trigger envelopes via receivedAll.
type bridgeFixture struct {
	mgr        *workerhost.Bridge
	spawnCount *atomic.Int64
	receivedCh chan ipc.TriggerPayload
	crashGate  chan struct{}
	cleanup    func()
}

func newBridgeFixture(t *testing.T, triggerLimit, crashAfter int) *bridgeFixture {
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
	var crashGate chan struct{}
	if crashAfter > 0 {
		crashGate = make(chan struct{})
	}
	tcw := &triggerCountingWorker{
		t:            t,
		spawnCount:   spawnCount,
		receivedAll:  received,
		triggerLimit: triggerLimit,
		crashAfter:   crashAfter,
		crashGate:    crashGate,
	}
	if crashAfter > 0 {
		tcw.crashBudget.Store(1)
	}

	spawner := &workerhost.PipeSpawner{WorkerFunc: tcw.workerFn}
	leaseStore := workerhost.NewLeaseStore(db)
	mgr, err := workerhost.NewBridge(workerhost.BridgeConfig{
		ChannelID:        chID,
		AgentID:          agentID,
		WorkerActorID:    agentID,
		Spawner:          spawner,
		LeaseStore:       leaseStore,
		Chain:            chain,
		Ledger:           led,
		NowFn:            now,
		FencingToken:     fence.FencingToken("tok-1"),
		DaemonEpoch:      fence.DaemonEpoch(7),
		HandshakeTimeout: 2 * time.Second,
		ServeCtx:         ctx,
	})
	if err != nil {
		t.Fatalf("NewBridge: %v", err)
	}

	cleanup := func() {
		closeCtx, ccancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer ccancel()
		_ = mgr.Close(closeCtx)
		cancel()
		_ = db.Close()
	}
	return &bridgeFixture{
		mgr:        mgr,
		spawnCount: spawnCount,
		receivedCh: received,
		crashGate:  crashGate,
		cleanup:    cleanup,
	}
}

type blockingAfterFirstWrite struct {
	allow     int64
	writes    atomic.Int64
	closeOnce sync.Once
	closed    chan struct{}
	onFrame   func(ipc.Frame)
}

func newBlockingAfterFirstWrite(allow int64) *blockingAfterFirstWrite {
	return &blockingAfterFirstWrite{allow: allow, closed: make(chan struct{})}
}

func (w *blockingAfterFirstWrite) Write(p []byte) (int, error) {
	if w.writes.Add(1) <= w.allow {
		if w.onFrame != nil {
			var frame ipc.Frame
			if err := json.Unmarshal(p, &frame); err == nil && frame.Kind != "" {
				w.onFrame(frame)
			}
		}
		return len(p), nil
	}
	<-w.closed
	return 0, io.ErrClosedPipe
}

func (w *blockingAfterFirstWrite) Close() error {
	w.closeOnce.Do(func() { close(w.closed) })
	return nil
}

type blockingPushSpawner struct {
	allowWrites int64
}

func (s *blockingPushSpawner) Spawn(ctx context.Context, leaseID string, _ []string) (workerhost.WorkerProc, error) {
	stdoutR, stdoutW := io.Pipe()
	stdin := newBlockingAfterFirstWrite(s.allowWrites)
	done := make(chan error, 1)
	workerCodec := ipc.NewCodec(bytes.NewReader(nil), stdoutW)
	var acked atomic.Bool
	stdin.onFrame = func(frame ipc.Frame) {
		if frame.Kind != ipc.KindTrigger || acked.Swap(true) {
			return
		}
		var trigger ipc.TriggerPayload
		_ = json.Unmarshal(frame.Payload, &trigger)
		payload, _ := json.Marshal(ipc.TriggerAckPayload{
			Accepted: true,
			Cursor:   trigger.Cursor,
		})
		_ = workerCodec.Write(ipc.Frame{
			ID:           frame.ID,
			Kind:         ipc.KindTriggerAck,
			ChannelID:    frame.ChannelID,
			WorkerID:     frame.WorkerID,
			FencingToken: frame.FencingToken,
			DaemonEpoch:  frame.DaemonEpoch,
			Payload:      payload,
		})
	}

	go func() {
		payload, err := json.Marshal(ipc.HandshakePayload{LeaseID: leaseID})
		if err == nil {
			err = workerCodec.Write(ipc.Frame{
				ID:      "handshake-" + leaseID,
				Kind:    ipc.KindHandshake,
				Payload: payload,
			})
		}
		if err != nil {
			_ = stdoutW.CloseWithError(err)
			done <- err
			return
		}

		select {
		case <-ctx.Done():
			err = ctx.Err()
		case <-stdin.closed:
		}
		_ = stdoutW.Close()
		done <- err
	}()

	return workerhost.WorkerProc{
		LeaseID: leaseID,
		Stdin:   stdin,
		Stdout:  stdoutR,
		Wait: func() error {
			return <-done
		},
		Kill: func() error {
			_ = stdin.Close()
			_ = stdoutR.Close()
			_ = stdoutW.Close()
			return nil
		},
	}, nil
}

func triggerEnvelope(id string, seq int64) *message.Envelope {
	return &message.Envelope{
		ID:            message.ID(id),
		ChannelID:     "ch-mgr",
		Type:          "human.text",
		Sender:        message.Sender{Kind: actor.KindHuman, ID: "user:alice"},
		Kind:          message.KindEvent,
		Visibility:    message.VisibilityPublic,
		Audience:      message.Audience{"agent:channel-agent"},
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
			if payload.Envelope.ID == message.ID(want) {
				return payload
			}
			// allow draining other envelopes (none expected in current tests)
		case <-timer.C:
			t.Fatalf("did not see trigger %q within %s", want, deadline)
		}
	}
}

// TestBridge_SpawnAndReuse covers M1.6-T1 acceptance #3:
//
//	OnTrigger #1 → spawn a worker subprocess
//	OnTrigger #2 → reuse the SAME worker (no second spawn)
func TestBridge_SpawnAndReuse(t *testing.T) {
	t.Parallel()
	f := newBridgeFixture(t, 0, 0)
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

// TestBridge_RespawnAfterCrash covers M1.6-T1 acceptance #4 (worker
// crash path): when the active worker exits unexpectedly, the next
// OnTrigger spawns a fresh subprocess (with a new worker id) and the
// lease row is re-acquired with the prevailing fencing tuple.
func TestBridge_RespawnAfterCrash(t *testing.T) {
	t.Parallel()
	// Worker crashes after the first trigger; second OnTrigger must
	// spawn a new one.
	f := newBridgeFixture(t, 0, 1)
	t.Cleanup(f.cleanup)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := f.mgr.OnTrigger(ctx, "agent:channel-agent", triggerEnvelope("env-1", 1)); err != nil {
		t.Fatalf("OnTrigger #1: %v", err)
	}
	_ = waitForTrigger(t, f.receivedCh, "env-1", 2*time.Second)
	w1 := f.mgr.CurrentWorkerID()
	close(f.crashGate)

	// Wait for the crash to propagate — the serve goroutine sees pipe
	// closure and the bridge tombstones the session.
	deadline := time.Now().Add(2 * time.Second)
	for f.mgr.CurrentWorkerID() != "" && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	if f.mgr.CurrentWorkerID() != "" {
		t.Fatal("bridge did not tombstone crashed worker")
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

func TestBridge_OnTriggerPushTimeoutDoesNotHoldLock(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dir := t.TempDir()
	db, err := store.OpenChannel(ctx, filepath.Join(dir, "ch.sqlite"), store.OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })

	chID := channel.ID("ch-blocking-push")
	agentID := actor.ActorID("agent:channel-agent")
	chain := newE2EChain(t, db, chID, agentID)
	leaseStore := workerhost.NewLeaseStore(db)
	led := store.NewLedger(db)
	var hookDrops atomic.Int64

	mgr, err := workerhost.NewBridge(workerhost.BridgeConfig{
		ChannelID:        chID,
		AgentID:          agentID,
		WorkerActorID:    agentID,
		Spawner:          &blockingPushSpawner{allowWrites: 4},
		LeaseStore:       leaseStore,
		Chain:            chain,
		Ledger:           led,
		NowFn:            now,
		FencingToken:     fence.FencingToken("tok-1"),
		DaemonEpoch:      fence.DaemonEpoch(7),
		HandshakeTimeout: 2 * time.Second,
		PushTimeout:      80 * time.Millisecond,
		ServeCtx:         ctx,
		OnPushDrop: func(drop workerhost.PushDrop) {
			if drop.EnvelopeID == "" || drop.Err == nil {
				t.Errorf("invalid push drop: %+v", drop)
			}
			hookDrops.Add(1)
		},
	})
	if err != nil {
		t.Fatalf("NewBridge: %v", err)
	}
	t.Cleanup(func() {
		closeCtx, ccancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer ccancel()
		_ = mgr.Close(closeCtx)
	})

	if err := mgr.OnTrigger(ctx, agentID, triggerEnvelope("env-warm", 1)); err != nil {
		t.Fatalf("warm OnTrigger: %v", err)
	}

	const n = 6
	start := make(chan struct{})
	errs := make(chan error, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			callCtx, ccancel := context.WithTimeout(ctx, 2*time.Second)
			defer ccancel()
			errs <- mgr.OnTrigger(callCtx, agentID, triggerEnvelope("env-block-"+string(rune('a'+i)), int64(i+2)))
		}()
	}

	t0 := time.Now()
	close(start)
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(300 * time.Millisecond):
		t.Fatal("concurrent OnTrigger calls waited behind earlier blocked pushes")
	}
	elapsed := time.Since(t0)
	if elapsed > 300*time.Millisecond {
		t.Fatalf("concurrent OnTrigger calls took %s; want under 300ms", elapsed)
	}

	for i := 0; i < n; i++ {
		if err := <-errs; err == nil {
			t.Fatal("OnTrigger returned nil for blocking worker stdin")
		}
	}
	if got := mgr.PushDropCount(); got != n {
		t.Fatalf("PushDropCount=%d want %d", got, n)
	}
	if got := hookDrops.Load(); got != n {
		t.Fatalf("OnPushDrop calls=%d want %d", got, n)
	}
}

// TestBridge_WorkerEnvPropagates covers M1.6-T5 phase-3: the
// BridgeConfig.WorkerEnv slice flows verbatim into every Spawner.Spawn
// invocation so per-channel COAGENT_* keys (channel id, channel type,
// domain prompt) reach the worker subprocess without an extra IPC frame.
func TestBridge_WorkerEnvPropagates(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dir := t.TempDir()
	db, err := store.OpenChannel(ctx, filepath.Join(dir, "ch.sqlite"), store.OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	led := store.NewLedger(db)

	chID := channel.ID("ch-env")
	agentID := actor.ActorID("agent:channel-agent")
	chain := newE2EChain(t, db, chID, agentID)

	wantEnv := []string{
		"COAGENT_CHANNEL_TYPE=xhs-creator",
		"COAGENT_DOMAIN_PROMPT=你是 xhs 内容创作 agent",
	}

	gotEnvCh := make(chan []string, 1)
	spawned := new(atomic.Int64)
	tcw := &triggerCountingWorker{
		t:           t,
		spawnCount:  spawned,
		receivedAll: make(chan ipc.TriggerPayload, 4),
	}
	spawner := &workerhost.PipeSpawner{
		WorkerFunc: func(ctx context.Context, leaseID string, extraEnv []string, in io.Reader, out io.Writer) error {
			// Snapshot the env Bridge handed us. A nil-equivalent slice
			// also lands here (empty list) so a missing wire is visible
			// as `len(extraEnv)==0`.
			snap := append([]string(nil), extraEnv...)
			select {
			case gotEnvCh <- snap:
			default:
			}
			return tcw.workerFn(ctx, leaseID, extraEnv, in, out)
		},
	}

	leaseStore := workerhost.NewLeaseStore(db)
	mgr, err := workerhost.NewBridge(workerhost.BridgeConfig{
		ChannelID:        chID,
		AgentID:          agentID,
		WorkerActorID:    agentID,
		Spawner:          spawner,
		LeaseStore:       leaseStore,
		Chain:            chain,
		Ledger:           led,
		NowFn:            now,
		FencingToken:     fence.FencingToken("tok-1"),
		DaemonEpoch:      fence.DaemonEpoch(7),
		HandshakeTimeout: 2 * time.Second,
		ServeCtx:         ctx,
		WorkerEnv:        wantEnv,
	})
	if err != nil {
		t.Fatalf("NewBridge: %v", err)
	}
	t.Cleanup(func() {
		closeCtx, cc := context.WithTimeout(context.Background(), 3*time.Second)
		defer cc()
		_ = mgr.Close(closeCtx)
	})

	if err := mgr.OnTrigger(ctx, agentID, triggerEnvelope("env-env-1", 1)); err != nil {
		t.Fatalf("OnTrigger: %v", err)
	}

	select {
	case got := <-gotEnvCh:
		if len(got) != len(wantEnv) {
			t.Fatalf("env len mismatch: got=%v want=%v", got, wantEnv)
		}
		for i := range wantEnv {
			if got[i] != wantEnv[i] {
				t.Errorf("env[%d]=%q want %q", i, got[i], wantEnv[i])
			}
		}
	case <-time.After(2 * time.Second):
		t.Fatal("spawner never invoked")
	}
}

// TestBridge_CloseShutsWorker verifies Close() reliably tears down the
// live worker subprocess so no goroutine / pipe leaks survive the
// fixture cleanup. Regression guard for the bridge's serve goroutine.
func TestBridge_CloseShutsWorker(t *testing.T) {
	t.Parallel()
	f := newBridgeFixture(t, 0, 0)

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
