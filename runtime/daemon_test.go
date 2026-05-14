package runtime_test

import (
	"context"
	"database/sql"
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

	// FIX-T3 acceptance #1: after harness accept the post-dispatch path
	// MUST stamp delivered_at (immediate, non-deferred envelope). Poll
	// briefly because the write/dispatch/MarkDelivered happens on the
	// dispatcher goroutine, which races with the ack send.
	{
		deadline := time.Now().Add(2 * time.Second)
		var got sql.NullInt64
		for time.Now().Before(deadline) {
			got, _ = queryDeliveredAt(ctx, dbPath, ack.MessageID)
			if got.Valid {
				break
			}
			time.Sleep(20 * time.Millisecond)
		}
		if !got.Valid {
			t.Error("immediate envelope: delivered_at never stamped — gateway dispatch did not run")
		}
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

// TestDaemon_FutureMessage_SchedulerDrains covers FIX-T3 acceptance:
// an envelope with `not_before > now` MUST NOT be delivered at write
// time; once `not_before <= now` the scheduler periodic scan picks it
// up, runs trigger.Gateway.Dispatch, and stamps `delivered_at` so
// subsequent scans skip it.
//
// We seed the future-message row directly via store.Messages.Append
// (the chain would refuse because the test rig doesn't model a full
// human caller for a `human.text` write with a custom not_before — the
// scheduler scan is upstream-agnostic so this path exercises the same
// gateway.Dispatch + MarkDelivered seam the harness adapter uses).
func TestDaemon_FutureMessage_SchedulerDrains(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	dataDir := t.TempDir()
	channelsDir := filepath.Join(dataDir, "channels")
	chID := channel.ID("ch-fut")
	chDir := filepath.Join(channelsDir, string(chID))
	if err := os.MkdirAll(chDir, 0o755); err != nil {
		t.Fatal(err)
	}
	dbPath := filepath.Join(chDir, "channel.sqlite")
	db, err := store.OpenChannel(ctx, dbPath, store.OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	// Seed the channel_lock (so phase 2 reclaim accepts the channel) +
	// two actors so audience=['*'] expands to a non-empty set + the
	// future-message row that the scheduler must drain.
	lock := store.NewChannelLock(db)
	if err := lock.Insert(ctx, store.ChannelLockRow{
		ChannelID:    chID,
		FencingToken: 1, OwnerEpoch: 1,
		DaemonID: "daemon-fut", DaemonEpoch: 1,
		AcquiredAt: now(), RefreshedAt: now(),
	}); err != nil {
		t.Fatal(err)
	}
	areg := store.NewActorRegistry(db)
	for _, rec := range []actor.Record{
		{ID: "user:alice", Kind: message.SenderHuman, CreatedAt: now()},
		{ID: "agent:beta", Kind: message.SenderAgent, CreatedAt: now()},
	} {
		if err := areg.Insert(ctx, rec); err != nil {
			t.Fatal(err)
		}
	}

	notBefore := now() + 250 // ms in the future
	msgs := store.NewMessages(db)
	if _, err := msgs.Append(ctx, &message.Envelope{
		ID:         "m-future",
		TS:         now(),
		ChannelID:  string(chID),
		Sender:     message.Sender{Kind: message.SenderHuman, ID: "user:alice"},
		Kind:       message.KindEvent,
		Type:       "human.text",
		Payload:    json.RawMessage(`{"text":"later"}`),
		Visibility: message.VisibilityPublic,
		Audience:   []string{"*"},
		NotBefore:  &notBefore,
	}); err != nil {
		t.Fatalf("seed future message: %v", err)
	}
	_ = db.Close()

	cfg := runtime.DaemonConfig{
		DataDir:         dataDir,
		ChannelsDir:     channelsDir,
		DaemonID:        "daemon-fut",
		DaemonEpoch:     42,
		UseMockBus:      true,
		NowFn:           now,
		SchedulerPeriod: 30 * time.Millisecond,
	}
	d, err := runtime.AssembleDaemon(ctx, cfg)
	if err != nil {
		t.Fatalf("AssembleDaemon: %v", err)
	}
	if err := d.RunPhases(ctx); err != nil {
		t.Fatalf("RunPhases: %v", err)
	}
	if !d.HasChannel(chID) {
		t.Fatalf("daemon did not register channel %s", chID)
	}

	// 1) Before not_before passes (let the scheduler tick a few times),
	// the row's delivered_at MUST still be NULL — that's the §5.3
	// future-message guarantee. We poll the daemon's own messages handle
	// via the exposed registry-friendly helper.
	time.Sleep(80 * time.Millisecond) // ~3 ticks; not_before is +250ms
	delivered, err := queryDeliveredAt(ctx, dbPath, "m-future")
	if err != nil {
		t.Fatalf("pre-not_before query: %v", err)
	}
	if delivered.Valid {
		t.Fatalf("delivered_at populated before not_before passed: %d", delivered.Int64)
	}

	// 2) Wait until well past not_before + at least one scheduler tick
	// so scanLongPending → gateway.Dispatch → MarkDelivered runs. We poll
	// with a deadline rather than sleep-and-pray.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		delivered, err = queryDeliveredAt(ctx, dbPath, "m-future")
		if err != nil {
			t.Fatalf("post-not_before query: %v", err)
		}
		if delivered.Valid {
			break
		}
		time.Sleep(30 * time.Millisecond)
	}
	if !delivered.Valid {
		t.Fatal("scheduler never marked future-message delivered after not_before passed")
	}
	if delivered.Int64 < notBefore {
		t.Errorf("delivered_at=%d < not_before=%d — scheduler ticked too early", delivered.Int64, notBefore)
	}

	// Clean shutdown.
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

// queryDeliveredAt opens the channel sqlite read-only and returns the
// delivered_at column for the row id. We open a fresh handle each call
// to avoid contention with the daemon's writer handle (WAL mode allows
// concurrent readers).
func queryDeliveredAt(ctx context.Context, dbPath, id string) (sql.NullInt64, error) {
	db, err := store.OpenChannel(ctx, dbPath, store.OpenOptions{SkipDDL: true})
	if err != nil {
		return sql.NullInt64{}, err
	}
	defer func() { _ = db.Close() }()
	row := db.QueryRowContext(ctx, `SELECT delivered_at FROM messages WHERE id=?`, id)
	var got sql.NullInt64
	if err := row.Scan(&got); err != nil {
		return sql.NullInt64{}, err
	}
	return got, nil
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
