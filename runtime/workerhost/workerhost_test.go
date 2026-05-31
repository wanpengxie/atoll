package workerhost_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/wanpengxie/ActOS/kernel/actor"
	"github.com/wanpengxie/ActOS/kernel/actorreg"
	"github.com/wanpengxie/ActOS/kernel/channel"
	"github.com/wanpengxie/ActOS/kernel/fencing"
	"github.com/wanpengxie/ActOS/kernel/ledger"
	"github.com/wanpengxie/ActOS/kernel/message"
	"github.com/wanpengxie/ActOS/runtime/harness"
	"github.com/wanpengxie/ActOS/runtime/ipc"
	"github.com/wanpengxie/ActOS/runtime/store"
	"github.com/wanpengxie/ActOS/runtime/worker"
	"github.com/wanpengxie/ActOS/runtime/workerhost"
)

// newE2EChain wires a harness.Chain on top of a channel sqlite + actor
// registry seeded with the worker's actor. Returns the chain so the
// caller wires it into HostConfig.
func newE2EChain(t *testing.T, db *sql.DB, channelID channel.ID, workerActor actor.ActorID) *harness.Chain {
	t.Helper()
	areg := store.NewActorRegistry(db)
	if err := areg.Insert(context.Background(), actorreg.Record{
		ID:        workerActor,
		Kind:      actor.KindAgent,
		CreatedAt: now(),
	}); err != nil {
		t.Fatalf("seed actor: %v", err)
	}
	chain, err := harness.New(harness.Deps{
		ChannelID:     channelID,
		ActorRegistry: areg,
		Log:           store.NewMessages(db),
		NowMs:         now,
	})
	if err != nil {
		t.Fatalf("harness.New: %v", err)
	}
	return chain
}

func now() int64 { return time.Now().UnixMilli() }

func TestExecSpawnerCheckReady(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	dir := t.TempDir()
	bin := filepath.Join(dir, "worker")
	if err := os.WriteFile(bin, []byte("#!/bin/sh\nexit 0\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := (&workerhost.ExecSpawner{BinaryPath: bin}).CheckReady(ctx); err != nil {
		t.Fatalf("CheckReady executable: %v", err)
	}

	noExec := filepath.Join(dir, "worker-noexec")
	if err := os.WriteFile(noExec, []byte("#!/bin/sh\nexit 0\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := (&workerhost.ExecSpawner{BinaryPath: noExec}).CheckReady(ctx); err == nil || !strings.Contains(err.Error(), "not executable") {
		t.Fatalf("CheckReady noexec err=%v want not executable", err)
	}

	missing := filepath.Join(dir, "missing")
	if err := (&workerhost.ExecSpawner{BinaryPath: missing}).CheckReady(ctx); err == nil {
		t.Fatal("CheckReady missing binary succeeded")
	}
}

// TestLeaseAcquireRelease verifies the worker_locks CAS Acquire + sweep.
func TestLeaseAcquireRelease(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	dir := t.TempDir()
	db, err := store.OpenChannel(ctx, filepath.Join(dir, "ch.sqlite"), store.OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	leases := workerhost.NewLeaseStore(db)
	l, ok, err := leases.Acquire(ctx, "agent:a", "w-1",
		fencing.FencingToken("tok-1"), fencing.DaemonEpoch(1), now())
	if err != nil || !ok {
		t.Fatalf("Acquire: ok=%v err=%v", ok, err)
	}
	if l.WorkerID != "w-1" {
		t.Errorf("WorkerID = %q", l.WorkerID)
	}

	// Re-Acquire by another worker with same fencing token — rejected
	// while existing lease is fresh.
	_, ok2, err := leases.Acquire(ctx, "agent:a", "w-2",
		fencing.FencingToken("tok-1"), fencing.DaemonEpoch(1), now())
	if err != nil || ok2 {
		t.Errorf("conflicting Acquire should fail: ok=%v err=%v", ok2, err)
	}

	// Higher daemon_epoch wins (daemon restart scenario). fencing_token
	// is opaque random per proto-foundation §3.6.1; ordering uses
	// daemon_epoch instead.
	_, ok3, err := leases.Acquire(ctx, "agent:a", "w-3",
		fencing.FencingToken("tok-2"), fencing.DaemonEpoch(2), now())
	if err != nil || !ok3 {
		t.Errorf("stronger Acquire should win: ok=%v err=%v", ok3, err)
	}

	if err := leases.Release(ctx, "agent:a"); err != nil {
		t.Fatal(err)
	}
	_, ok4, _ := leases.Acquire(ctx, "agent:a", "w-4",
		fencing.FencingToken("tok-1"), fencing.DaemonEpoch(1), now())
	if !ok4 {
		t.Errorf("Acquire after Release should succeed")
	}

	// Future-dated sweep removes everything.
	if n, err := leases.SweepExpired(ctx, now()+10*workerhost.LeaseTTL.Milliseconds()); err != nil || n != 1 {
		t.Errorf("SweepExpired n=%d err=%v", n, err)
	}
}

// TestPool_Quota verifies the in-memory pool quota.
func TestPool_Quota(t *testing.T) {
	p := workerhost.NewPool(workerhost.PoolConfig{MaxConcurrent: 2})
	if err := p.Reserve("a"); err != nil {
		t.Fatal(err)
	}
	if err := p.Reserve("b"); err != nil {
		t.Fatal(err)
	}
	if err := p.Reserve("c"); err == nil {
		t.Error("expected ErrPoolFull")
	}
	p.Release("a")
	if err := p.Reserve("c"); err != nil {
		t.Errorf("post-release reserve: %v", err)
	}
}

// TestWorker_LeaseE2E covers acceptance gate #5 (T3):
//
//	spawn worker → handshake → write message via IPC → reserve+commit
//	ledger → graceful shutdown → daemon-side verification.
func TestWorker_LeaseE2E(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	dir := t.TempDir()
	db, _ := store.OpenChannel(ctx, filepath.Join(dir, "ch.sqlite"), store.OpenOptions{})
	defer func() { _ = db.Close() }()
	msgs := store.NewMessages(db)
	led := store.NewLedger(db)

	bridgeDone := make(chan error, 1)
	spawner := &workerhost.PipeSpawner{
		WorkerFunc: func(ctx context.Context, leaseID string, _ []string, in io.Reader, out io.Writer) error {
			rt, err := worker.New(worker.Config{
				LeaseID:        leaseID,
				In:             in,
				Out:            out,
				NowFn:          now,
				HeartbeatEvery: time.Hour,
				Bridge: worker.BridgeFunc(func(ctx context.Context, client *worker.IPCClient) error {
					env := message.Envelope{
						ID: "m-1", TS: now(), TSReceived: now(),
						ChannelID: "ch-1",
						Sender:    message.Sender{Kind: actor.KindAgent, ID: "agent:a"},
						Kind:      message.KindEvent, Type: "agent.text",
						Payload: json.RawMessage(`{"text":"turn.start"}`), Visibility: message.VisibilityPublic,
						Audience: message.Audience{"agent:channel-agent"},
					}
					if _, err := client.WriteMessage(ctx, env); err != nil {
						bridgeDone <- err
						return err
					}
					key, _ := ledger.DeriveKey("turn-1", "act-1")
					if _, err := client.ReserveLedger(ctx, ledger.Entry{
						Key: key, TurnID: "turn-1", ActorID: "agent:a",
						EnvelopeID: "m-1", Status: ledger.StatusReserved, ReservedAt: now(),
					}); err != nil {
						bridgeDone <- err
						return err
					}
					if err := client.CommitLedger(ctx, key, now()); err != nil {
						bridgeDone <- err
						return err
					}
					bridgeDone <- nil
					return nil
				}),
			})
			if err != nil {
				return err
			}
			return rt.Run(ctx)
		},
	}

	proc, err := spawner.Spawn(ctx, "lease-1", nil)
	if err != nil {
		t.Fatal(err)
	}

	chain := newE2EChain(t, db, "ch-1", actor.ActorID("agent:a"))
	host, err := workerhost.NewHost(proc.Stdout, proc.Stdin, workerhost.HostConfig{
		ChannelID:     "ch-1",
		WorkerID:      "w-1",
		LeaseID:       "lease-1",
		FencingToken:  fencing.FencingToken("tok-1"),
		DaemonEpoch:   fencing.DaemonEpoch(7),
		Chain:         chain,
		WorkerActorID: "agent:a",
		Ledger:        led,
		NowFn:         now,
	})
	if err != nil {
		t.Fatal(err)
	}

	if err := host.Serve(ctx); err != nil {
		t.Fatalf("Serve: %v", err)
	}
	if err := proc.Wait(); err != nil {
		t.Errorf("worker Wait: %v", err)
	}
	if err := <-bridgeDone; err != nil {
		t.Errorf("bridge err: %v", err)
	}

	// Daemon-side verification.
	got, ok, _ := msgs.FindByID(ctx, channel.ID("ch-1"), "m-1")
	if !ok {
		t.Fatal("expected m-1 to be persisted via IPC")
	}
	if got.Seq == 0 {
		t.Error("expected non-zero seq")
	}
	key, _ := ledger.DeriveKey("turn-1", "act-1")
	entry, ok, _ := led.Find(ctx, key)
	if !ok || entry.Status != ledger.StatusCommitted {
		t.Errorf("ledger entry not committed: %+v", entry)
	}
}

func TestHost_PushTriggerWaitsForAck(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	dir := t.TempDir()
	db, err := store.OpenChannel(ctx, filepath.Join(dir, "ch.sqlite"), store.OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	hostInR, workerOutW := io.Pipe()
	workerInR, hostOutW := io.Pipe()
	defer func() {
		_ = hostInR.Close()
		_ = workerOutW.Close()
		_ = workerInR.Close()
		_ = hostOutW.Close()
	}()

	host, err := workerhost.NewHost(hostInR, hostOutW, workerhost.HostConfig{
		ChannelID:     "ch-ack",
		WorkerID:      "w-ack",
		LeaseID:       "lease-ack",
		FencingToken:  "tok-11",
		DaemonEpoch:   12,
		Chain:         newE2EChain(t, db, "ch-ack", actor.ActorID("agent:a")),
		WorkerActorID: "agent:a",
		Ledger:        store.NewLedger(db),
		NowFn:         now,
	})
	if err != nil {
		t.Fatal(err)
	}
	serveDone := make(chan error, 1)
	go func() { serveDone <- host.Serve(ctx) }()

	workerCodec := ipc.NewCodec(workerInR, workerOutW)
	handshake, _ := json.Marshal(ipc.HandshakePayload{LeaseID: "lease-ack"})
	if err := workerCodec.Write(ipc.Frame{ID: "h-1", Kind: ipc.KindHandshake, Payload: handshake}); err != nil {
		t.Fatal(err)
	}
	if frame, err := workerCodec.Read(); err != nil {
		t.Fatal(err)
	} else if frame.Kind != ipc.KindHandshakeAck {
		t.Fatalf("handshake ack kind=%s", frame.Kind)
	}

	pushDone := make(chan error, 1)
	go func() {
		pushDone <- host.PushTrigger(ctx, ipc.TriggerPayload{
			Envelope: *triggerEnvelope("env-ack", 42),
			Cursor:   42,
		})
	}()

	triggerFrame, err := workerCodec.Read()
	if err != nil {
		t.Fatal(err)
	}
	if triggerFrame.Kind != ipc.KindTrigger {
		t.Fatalf("trigger kind=%s want %s", triggerFrame.Kind, ipc.KindTrigger)
	}
	ackPayload, _ := json.Marshal(ipc.TriggerAckPayload{Accepted: true, Cursor: 42})
	if err := workerCodec.Write(ipc.Frame{
		ID:           triggerFrame.ID,
		Kind:         ipc.KindTriggerAck,
		ChannelID:    "ch-ack",
		WorkerID:     "w-ack",
		FencingToken: "tok-11",
		DaemonEpoch:  12,
		Payload:      ackPayload,
	}); err != nil {
		t.Fatal(err)
	}

	select {
	case err := <-pushDone:
		if err != nil {
			t.Fatalf("PushTrigger: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("PushTrigger did not return after ack")
	}

	cancel()
	_ = hostInR.Close()
	_ = hostOutW.Close()
	<-serveDone
}

func TestHost_PushTriggerReturnsNackError(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	dir := t.TempDir()
	db, err := store.OpenChannel(ctx, filepath.Join(dir, "ch.sqlite"), store.OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	hostInR, workerOutW := io.Pipe()
	workerInR, hostOutW := io.Pipe()
	defer func() {
		_ = hostInR.Close()
		_ = workerOutW.Close()
		_ = workerInR.Close()
		_ = hostOutW.Close()
	}()

	host, err := workerhost.NewHost(hostInR, hostOutW, workerhost.HostConfig{
		ChannelID:     "ch-nack",
		WorkerID:      "w-nack",
		LeaseID:       "lease-nack",
		FencingToken:  "tok-21",
		DaemonEpoch:   22,
		Chain:         newE2EChain(t, db, "ch-nack", actor.ActorID("agent:a")),
		WorkerActorID: "agent:a",
		Ledger:        store.NewLedger(db),
		NowFn:         now,
	})
	if err != nil {
		t.Fatal(err)
	}
	serveDone := make(chan error, 1)
	go func() { serveDone <- host.Serve(ctx) }()

	workerCodec := ipc.NewCodec(workerInR, workerOutW)
	handshake, _ := json.Marshal(ipc.HandshakePayload{LeaseID: "lease-nack"})
	if err := workerCodec.Write(ipc.Frame{ID: "h-1", Kind: ipc.KindHandshake, Payload: handshake}); err != nil {
		t.Fatal(err)
	}
	if _, err := workerCodec.Read(); err != nil {
		t.Fatal(err)
	}

	pushDone := make(chan error, 1)
	go func() {
		pushDone <- host.PushTrigger(ctx, ipc.TriggerPayload{
			Envelope: *triggerEnvelope("env-nack", 7),
			Cursor:   7,
		})
	}()

	triggerFrame, err := workerCodec.Read()
	if err != nil {
		t.Fatal(err)
	}
	ackPayload, _ := json.Marshal(ipc.TriggerAckPayload{
		Accepted: false,
		Cursor:   7,
		Reason:   "trigger_buffer_full",
	})
	if err := workerCodec.Write(ipc.Frame{
		ID:           triggerFrame.ID,
		Kind:         ipc.KindTriggerAck,
		ChannelID:    "ch-nack",
		WorkerID:     "w-nack",
		FencingToken: "tok-21",
		DaemonEpoch:  22,
		Payload:      ackPayload,
	}); err != nil {
		t.Fatal(err)
	}

	select {
	case err := <-pushDone:
		if err == nil {
			t.Fatal("PushTrigger returned nil for nack")
		}
		if !strings.Contains(err.Error(), "trigger_buffer_full") {
			t.Fatalf("PushTrigger error=%q", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("PushTrigger did not return after nack")
	}

	cancel()
	_ = hostInR.Close()
	_ = hostOutW.Close()
	<-serveDone
}

// TestFence_DaemonEpochMismatch covers codex review #10 directly:
//
//	a frame stamped with daemon_epoch=1 reaches a host expecting epoch=99
//	→ host emits fence_invalid → worker decodes to *FenceInvalidError.
func TestFence_DaemonEpochMismatch(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	dir := t.TempDir()
	db, _ := store.OpenChannel(ctx, filepath.Join(dir, "ch.sqlite"), store.OpenOptions{})
	defer func() { _ = db.Close() }()
	_ = store.NewMessages(db)
	led := store.NewLedger(db)
	chain := newE2EChain(t, db, "ch-1", actor.ActorID("agent:a"))

	in1, in2 := io.Pipe()
	out1, out2 := io.Pipe()

	host, _ := workerhost.NewHost(in1, out2, workerhost.HostConfig{
		ChannelID: "ch-1", WorkerID: "w-1", LeaseID: "lease-1",
		FencingToken: "tok-1", DaemonEpoch: 99,
		Chain: chain, WorkerActorID: "agent:a",
		Ledger: led, NowFn: now,
	})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = host.Serve(ctx)
	}()

	codec := ipc.NewCodec(out1, in2)
	envBytes, _ := json.Marshal(ipc.WriteMessagePayload{Envelope: message.Envelope{
		ID: "stale", TS: now(), TSReceived: now(), ChannelID: "ch-1",
		Sender: message.Sender{Kind: actor.KindAgent, ID: "agent:a"},
		Kind:   message.KindEvent, Type: "stale",
		Payload: json.RawMessage(`{}`), Visibility: message.VisibilityPublic,
		Audience: message.Audience{"agent:channel-agent"},
	}})
	if err := codec.Write(ipc.Frame{
		ID: "f-1", Kind: ipc.KindWriteMessage, ChannelID: "ch-1",
		FencingToken: "tok-1", DaemonEpoch: 1, // stale
		Payload: envBytes,
	}); err != nil {
		t.Fatal(err)
	}

	reply, err := codec.Read()
	if err != nil {
		t.Fatal(err)
	}
	if reply.Kind != ipc.KindFenceInvalid {
		t.Errorf("expected fence_invalid, got %s", reply.Kind)
	}

	if err := worker.FenceFromFrame(reply); err == nil {
		t.Error("FenceFromFrame should produce non-nil error")
	} else {
		var fe *worker.FenceInvalidError
		if !errors.As(err, &fe) {
			t.Errorf("expected *FenceInvalidError, got %T", err)
		} else if fe.ExpectedEpoch != 99 || fe.GotEpoch != 1 {
			t.Errorf("epoch values = expected=%d got=%d", fe.ExpectedEpoch, fe.GotEpoch)
		}
	}

	cancel()
	_ = in1.Close()
	_ = out2.Close()
	_ = in2.Close()
	_ = out1.Close()
	wg.Wait()
}

// TestInMemoryLeaseTable round-trip.
func TestInMemoryLeaseTable(t *testing.T) {
	tab := workerhost.NewInMemoryLeaseTable()
	tab.Put(workerhost.Lease{ID: "a", WorkerID: "w-1", FencingToken: "tok-1", DaemonEpoch: 1})
	if _, ok := tab.Get("a"); !ok {
		t.Error("Get after Put failed")
	}
	if list := tab.List(); len(list) != 1 {
		t.Errorf("List len=%d", len(list))
	}
	tab.Delete("a")
	if _, ok := tab.Get("a"); ok {
		t.Error("Get after Delete should fail")
	}
}
