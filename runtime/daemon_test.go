package runtime_test

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/wanpengxie/ActOS/kernel/actor"
	"github.com/wanpengxie/ActOS/kernel/channel"
	"github.com/wanpengxie/ActOS/kernel/daemonbus"
	"github.com/wanpengxie/ActOS/kernel/message"
	"github.com/wanpengxie/ActOS/kernel/placement"
	"github.com/wanpengxie/ActOS/runtime"
	"github.com/wanpengxie/ActOS/runtime/lifecycle"
	"github.com/wanpengxie/ActOS/runtime/store"
	"github.com/wanpengxie/ActOS/runtime/transit"
)

func now() int64 { return time.Now().UnixMilli() }

// TestDaemon_StartupPhases covers acceptance gate #1 (T3):
//
//	cmd/daemon equivalent assembles → scans channels/ → refreshes
//	daemon_epoch on every owned channel → advances to PhaseAcceptingNew.
func TestDaemon_StartupPhases(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	dataDir := t.TempDir()
	channelsDir := filepath.Join(dataDir, "channels")

	// Seed two existing channels with locks owned by "daemon-A" + epoch=1.
	for _, id := range []string{"ch-1", "ch-2"} {
		chDir := filepath.Join(channelsDir, id)
		if err := os.MkdirAll(chDir, 0o755); err != nil {
			t.Fatal(err)
		}
		dbPath := filepath.Join(chDir, "channel.sqlite")
		db, err := store.OpenChannel(ctx, dbPath, store.OpenOptions{})
		if err != nil {
			t.Fatal(err)
		}
		lock := store.NewChannelLock(db)
		if err := lock.Insert(ctx, store.ChannelLockRow{
			ChannelID:    channel.ID(id),
			FencingToken: 1, OwnerEpoch: 1,
			DaemonID: "daemon-A", DaemonEpoch: 1,
			AcquiredAt: now(), RefreshedAt: now(),
		}); err != nil {
			t.Fatal(err)
		}
		_ = db.Close()
	}

	cfg := runtime.DaemonConfig{
		DataDir:     dataDir,
		ChannelsDir: channelsDir,
		DaemonID:    "daemon-A",
		DaemonEpoch: 42, // fresh process
		UseMockBus:  true,
		NowFn:       now,
	}

	d, err := runtime.AssembleDaemon(ctx, cfg)
	if err != nil {
		t.Fatalf("AssembleDaemon: %v", err)
	}
	defer func() { _ = d.Close() }()

	if d.Phase() != lifecycle.PhaseUnstarted {
		t.Errorf("initial phase = %s", d.Phase())
	}

	if err := d.RunPhases(ctx); err != nil {
		t.Fatalf("RunPhases: %v", err)
	}
	if d.Phase() != lifecycle.PhaseAcceptingNew {
		t.Errorf("post-RunPhases phase = %s", d.Phase())
	}

	res := d.BootResult()
	if len(res.Local) != 2 {
		t.Fatalf("expected 2 local channels, got %d", len(res.Local))
	}
	if len(res.ReclaimAccepted) != 2 {
		t.Errorf("offline reclaim should accept all 2: %v", res.ReclaimAccepted)
	}

	// daemon_epoch should have been bumped to 42 on each owned channel.
	for _, c := range res.Local {
		if c.Lock.DaemonEpoch != 42 {
			t.Errorf("channel %s daemon_epoch=%d (expected 42 after RefreshDaemon)",
				c.ChannelID, c.Lock.DaemonEpoch)
		}
	}

	// Bus connected with epoch >= 1.
	if d.Transit() == nil {
		t.Error("Transit should be wired in mock-bus mode")
	}
	if d.Transit().Epoch() < 1 {
		t.Errorf("Connection epoch = %d", d.Transit().Epoch())
	}

	// Saga is usable post-boot.
	sagaCh := placement.CreateChannelRequest{
		ChannelID: "ch-new", CreateRequestID: "req-new",
		OwnerEpoch: 1, FencingToken: 1,
	}
	if _, err := d.Saga().Bootstrap(ctx, "ch-new", sagaCh); err != nil {
		t.Errorf("post-boot saga: %v", err)
	}
}

// TestDaemon_Phase3_DispatchesWriteMessage covers FIX-T2 acceptance #1
// (end-to-end): RunPhases starts the dispatcher goroutine, an inbound
// control.write_message frame is routed through the per-channel
// harness chain, and the daemon emits the corresponding
// control.write_message_ack frame. ctx cancel must drain all phase 3
// goroutines (no leak).
func TestDaemon_Phase3_DispatchesWriteMessage(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	dataDir := t.TempDir()
	channelsDir := filepath.Join(dataDir, "channels")
	chID := channel.ID("ch-write")
	chDir := filepath.Join(channelsDir, string(chID))
	if err := os.MkdirAll(chDir, 0o755); err != nil {
		t.Fatal(err)
	}
	dbPath := filepath.Join(chDir, "channel.sqlite")
	db, err := store.OpenChannel(ctx, dbPath, store.OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	// Seed channel_lock owned by daemon-A.
	lock := store.NewChannelLock(db)
	if err := lock.Insert(ctx, store.ChannelLockRow{
		ChannelID:    chID,
		FencingToken: 1, OwnerEpoch: 1,
		DaemonID: "daemon-A", DaemonEpoch: 1,
		AcquiredAt: now(), RefreshedAt: now(),
	}); err != nil {
		t.Fatal(err)
	}
	// Seed an actor row so the write_message handler accepts the caller.
	areg := store.NewActorRegistry(db)
	if err := areg.Insert(ctx, actor.Record{
		ID: "user:alice", Kind: message.SenderHuman,
		DisplayName: "Alice", CreatedAt: now(),
	}); err != nil {
		t.Fatal(err)
	}
	_ = db.Close()

	const secret = "phase3-secret"
	cfg := runtime.DaemonConfig{
		DataDir:           dataDir,
		ChannelsDir:       channelsDir,
		DaemonID:          "daemon-A",
		DaemonEpoch:       42,
		UseMockBus:        true,
		NowFn:             now,
		HumanCallerSecret: []byte(secret),
		SchedulerPeriod:   50 * time.Millisecond,
	}
	d, err := runtime.AssembleDaemon(ctx, cfg)
	if err != nil {
		t.Fatalf("AssembleDaemon: %v", err)
	}
	if err := d.RunPhases(ctx); err != nil {
		t.Fatalf("RunPhases: %v", err)
	}

	if !d.HasChannel(chID) {
		t.Fatalf("daemon did not register channel %s after phase 3", chID)
	}

	// Drive a control.write_message frame through the mock bus.
	bus := d.Bus()
	if bus == nil {
		t.Fatal("expected mock bus")
	}
	server := bus.ServerSide()

	ts := now()
	hc := transit.HumanCaller{
		UserID:           "u1",
		ActorIDInChannel: "user:alice",
		TS:               ts,
		Nonce:            "nonce-A",
	}
	hc.ServerToken = transit.SignHumanCaller(
		[]byte(secret), string(chID), hc.UserID, hc.ActorIDInChannel, hc.TS, hc.Nonce,
	)
	body := transit.WriteMessageBody{
		FrameID:     "frame-write-1",
		ChannelID:   string(chID),
		HumanCaller: hc,
		EnvelopePartial: message.Envelope{
			Type:       "human.text",
			Kind:       message.KindEvent,
			Payload:    json.RawMessage(`{"text":"hi"}`),
			Audience:   []string{"*"},
			Visibility: message.VisibilityPublic,
			TS:         ts,
		},
	}
	reqFrame, _ := transit.Encode("frame-srv-write",
		daemonbus.FrameTypeControlWriteMessage,
		"server", 0, ts, body)
	if err := server.SendToDaemon(ctx, reqFrame); err != nil {
		t.Fatal(err)
	}

	// Wait for the ack frame (skip any unrelated viewsync.push the
	// pusher may emit in the meantime — outbox starts empty so this
	// loop should see exactly one ack frame).
	var ack transit.WriteMessageAckBody
	deadline := time.After(5 * time.Second)
	for {
		select {
		case <-deadline:
			t.Fatal("daemon never emitted write_message_ack")
		default:
		}
		recvCtx, recvCancel := context.WithTimeout(ctx, 2*time.Second)
		f, err := server.RecvFromDaemon(recvCtx)
		recvCancel()
		if err != nil {
			t.Fatalf("recv ack: %v", err)
		}
		if f.FrameType != daemonbus.FrameTypeControlWriteMessageAck {
			continue
		}
		if err := transit.DecodePayload(f, &ack); err != nil {
			t.Fatal(err)
		}
		break
	}
	if !ack.Accepted {
		t.Fatalf("ack reject: reason=%s detail=%s", ack.RejectReason, ack.RejectDetail)
	}
	if ack.Seq != 1 {
		t.Errorf("ack.Seq=%d want 1", ack.Seq)
	}
	if ack.FrameID != body.FrameID {
		t.Errorf("ack.FrameID=%q want %q", ack.FrameID, body.FrameID)
	}

	// Graceful shutdown: Close must drain the dispatcher / pusher /
	// scheduler goroutines without leaking. We assert this by
	// requiring Close to return within a tight timeout.
	closeDone := make(chan error, 1)
	go func() { closeDone <- d.Close() }()
	select {
	case err := <-closeDone:
		if err != nil {
			t.Errorf("Close: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Close blocked — phase 3 goroutine leak")
	}
}

// TestDaemon_Phase3_ShutdownNoLeak verifies that even with no
// owned channels (empty channels/), phase 3 goroutines (dispatcher +
// scheduler) still wire up + tear down cleanly on ctx cancel.
func TestDaemon_Phase3_ShutdownNoLeak(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	dataDir := t.TempDir()
	cfg := runtime.DaemonConfig{
		DataDir:         dataDir,
		ChannelsDir:     filepath.Join(dataDir, "channels"),
		DaemonID:        "daemon-empty",
		DaemonEpoch:     1,
		UseMockBus:      true,
		NowFn:           now,
		SchedulerPeriod: 25 * time.Millisecond,
	}
	d, err := runtime.AssembleDaemon(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := d.RunPhases(ctx); err != nil {
		t.Fatal(err)
	}
	// Let the scheduler tick at least once.
	time.Sleep(60 * time.Millisecond)
	done := make(chan error, 1)
	go func() { done <- d.Close() }()
	select {
	case err := <-done:
		if err != nil {
			t.Errorf("Close: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Close blocked — goroutine leak")
	}
}
