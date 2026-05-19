package runtime_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/wanpengxie/ActOS/kernel/actor"
	kadapter "github.com/wanpengxie/ActOS/kernel/adapter"
	"github.com/wanpengxie/ActOS/kernel/channel"
	"github.com/wanpengxie/ActOS/kernel/daemonbus"
	"github.com/wanpengxie/ActOS/kernel/message"
	"github.com/wanpengxie/ActOS/kernel/placement"
	"github.com/wanpengxie/ActOS/runtime"
	"github.com/wanpengxie/ActOS/runtime/lifecycle"
	"github.com/wanpengxie/ActOS/runtime/store"
	"github.com/wanpengxie/ActOS/runtime/transit"
	"github.com/wanpengxie/ActOS/runtime/worker"
	"github.com/wanpengxie/ActOS/runtime/workerhost"
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
		ID: "user:alice", Kind: actor.KindHuman,
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
	// FIX-T8: stamp the daemon's current connection epoch so the
	// dispatcher's stale-frame guard accepts the frame.
	reqFrame, _ := transit.Encode("frame-srv-write",
		daemonbus.FrameTypeControlWriteMessage,
		"server", d.Transit().Epoch(), ts, body)
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
		{ID: "user:alice", Kind: actor.KindHuman, CreatedAt: now()},
		{ID: "agent:beta", Kind: actor.KindAgent, CreatedAt: now()},
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
		Sender:     message.Sender{Kind: actor.KindHuman, ID: "user:alice"},
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

// TestDaemon_LongPending_Scheduler_EmitsFailedTerminal covers the L1
// §6.4 long-pending fallback (T147 §B). For each request whose
// expires_at has elapsed without a terminal response the scheduler
// either synthesises a system-side failed terminal or skips, per the
// receiver-kind dispatch matrix:
//
//   - audience=[agent] / [system]      → emit reason=unanswered_timeout
//   - audience=[tool]                  → skip (adapter framework F3 timer)
//   - audience=[human]                 → skip (no baseline human SLA)
//   - audience=[deregistered actor]    → emit reason=receiver_unavailable
//   - audience=[never-registered actor]→ emit reason=receiver_unavailable
//
// The synthesised response is also asserted to be idempotent (re-running
// the scan does not double-emit thanks to the deterministic envelope id
// + harness step 0.5 dedupe).
func TestDaemon_LongPending_Scheduler_EmitsFailedTerminal(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	dataDir := t.TempDir()
	channelsDir := filepath.Join(dataDir, "channels")
	chID := channel.ID("ch-overdue")
	chDir := filepath.Join(channelsDir, string(chID))
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
		ChannelID:    chID,
		FencingToken: 1, OwnerEpoch: 1,
		DaemonID: "daemon-overdue", DaemonEpoch: 1,
		AcquiredAt: now(), RefreshedAt: now(),
	}); err != nil {
		t.Fatal(err)
	}

	// Seed the registry. We need:
	//   - system actor: signs the synthesised response.
	//   - agent:caller: the request originator (envelope.sender.id).
	//   - agent:beta:   agent receiver → unanswered_timeout.
	//   - tool:xhs:     tool receiver → skip.
	//   - user:alice:   human receiver → skip.
	//   - agent:gone:   inserted then deregistered → receiver_unavailable.
	// (audience=[system] is exercised via the system actor itself.)
	// (audience=[agent:ghost] is exercised by *not* registering it — the
	// store.NewMessages.Append path bypasses harness step 5 so the row
	// lands even when audience[0] is unknown.)
	areg := store.NewActorRegistry(db)
	for _, rec := range []actor.Record{
		{ID: actor.SystemActorID, Kind: actor.KindSystem, CreatedAt: now()},
		{ID: "agent:caller", Kind: actor.KindAgent, CreatedAt: now()},
		{ID: "agent:beta", Kind: actor.KindAgent, CreatedAt: now()},
		{ID: "tool:xhs", Kind: actor.KindTool, CreatedAt: now()},
		{ID: "user:alice", Kind: actor.KindHuman, CreatedAt: now()},
		{ID: "agent:gone", Kind: actor.KindAgent, CreatedAt: now()},
	} {
		if err := areg.Insert(ctx, rec); err != nil {
			t.Fatalf("seed actor %s: %v", rec.ID, err)
		}
	}
	// Deregister agent:gone so the scheduler sees a soft-deleted receiver.
	if err := areg.Deregister(ctx, "agent:gone", now()); err != nil {
		t.Fatal(err)
	}

	// Seed 6 requests, all already past expires_at (deadline = now - 1s).
	// We use "human.text" core type with kind=request — core types pass
	// step 4 without a type_registry hit. The raw store.Append path
	// bypasses harness (the harness would refuse kind=request with an
	// inactive / unknown receiver), which models a request emitted while
	// the receiver was active that later went silent.
	msgs := store.NewMessages(db)
	deadline := now() - 1000
	type seedCase struct {
		id           string
		audience     string
		expectEmit   bool
		expectReason message.TerminalFailureReason
	}
	cases := []seedCase{
		{id: "req-agent", audience: "agent:beta", expectEmit: true, expectReason: message.TerminalUnansweredTimeout},
		{id: "req-system", audience: string(actor.SystemActorID), expectEmit: true, expectReason: message.TerminalUnansweredTimeout},
		{id: "req-tool", audience: "tool:xhs", expectEmit: false},
		{id: "req-human", audience: "user:alice", expectEmit: false},
		{id: "req-deregistered", audience: "agent:gone", expectEmit: true, expectReason: message.TerminalReceiverUnavailable},
		{id: "req-unknown", audience: "agent:ghost", expectEmit: true, expectReason: message.TerminalReceiverUnavailable},
	}
	for _, c := range cases {
		env := &message.Envelope{
			ID:         c.id,
			TS:         now(),
			TSReceived: now(),
			ChannelID:  string(chID),
			Sender:     message.Sender{Kind: actor.KindAgent, ID: "agent:caller"},
			Kind:       message.KindRequest,
			Type:       "human.text",
			Payload:    json.RawMessage(`{"text":"please"}`),
			Visibility: message.VisibilityPublic,
			Audience:   []string{c.audience},
			ExpiresAt:  &deadline,
		}
		if _, err := msgs.Append(ctx, env); err != nil {
			t.Fatalf("seed %s: %v", c.id, err)
		}
	}
	_ = db.Close()

	cfg := runtime.DaemonConfig{
		DataDir:         dataDir,
		ChannelsDir:     channelsDir,
		DaemonID:        "daemon-overdue",
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

	// Poll until every expected emit has materialised. We assert the
	// FULL set in one shot to also catch "scheduler emitted for a
	// receiver it should have skipped" — every emit case must show up
	// AND every skip case must remain absent.
	expectedTerminalCount := 0
	for _, c := range cases {
		if c.expectEmit {
			expectedTerminalCount++
		}
	}
	deadline2 := time.Now().Add(3 * time.Second)
	var responses map[string]string
	for time.Now().Before(deadline2) {
		responses, err = queryTerminalResponses(ctx, dbPath)
		if err != nil {
			t.Fatalf("query responses: %v", err)
		}
		if len(responses) >= expectedTerminalCount {
			break
		}
		time.Sleep(30 * time.Millisecond)
	}
	if len(responses) != expectedTerminalCount {
		t.Fatalf("expected %d synthesised terminal responses, got %d: %+v",
			expectedTerminalCount, len(responses), responses)
	}

	for _, c := range cases {
		reason, has := responses[c.id]
		if c.expectEmit {
			if !has {
				t.Errorf("%s: expected emit reason=%s, got no terminal response", c.id, c.expectReason)
				continue
			}
			if reason != string(c.expectReason) {
				t.Errorf("%s: reason=%s want %s", c.id, reason, c.expectReason)
			}
		} else if has {
			t.Errorf("%s: scheduler should have SKIPPED but emitted reason=%s", c.id, reason)
		}
	}

	// Idempotency — let the scheduler tick again and assert no new
	// terminals show up.
	time.Sleep(120 * time.Millisecond)
	again, err := queryTerminalResponses(ctx, dbPath)
	if err != nil {
		t.Fatalf("query responses (re-tick): %v", err)
	}
	if len(again) != expectedTerminalCount {
		t.Errorf("idempotency: re-tick changed terminal count from %d to %d",
			expectedTerminalCount, len(again))
	}

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

// TestDaemon_DeviceTransit_InboundRoutesToPerChannelCallback covers
// T147 §A daemon-side wiring: when a device_transit.recv frame arrives
// at the daemonbus dispatcher, it must (1) decode the SendFrame to
// recover the routing key (channel_id), (2) look up the per-channel
// runtime, (3) hand the frame to that channel's *transit.DeviceTransit,
// and (4) the DeviceTransit invokes the closure set via
// ChannelHooks.SetDeviceCallback during OnChannelBoot.
//
// Verified end-to-end: frame on MockBus → daemon dispatcher → our
// recording callback observes the original payload bytes.
//
// Multi-channel routing is also asserted — a frame addressed to a
// channel the daemon doesn't own gets dropped without touching the
// callback (the production semantic is at-least-once; the server will
// retry once placement settles).
func TestDaemon_DeviceTransit_InboundRoutesToPerChannelCallback(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	dataDir := t.TempDir()
	channelsDir := filepath.Join(dataDir, "channels")
	chID := channel.ID("ch-dev")
	chDir := filepath.Join(channelsDir, string(chID))
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
		ChannelID:    chID,
		FencingToken: 1, OwnerEpoch: 1,
		DaemonID: "daemon-dev", DaemonEpoch: 1,
		AcquiredAt: now(), RefreshedAt: now(),
	}); err != nil {
		t.Fatal(err)
	}
	_ = db.Close()

	type observation struct {
		channelID string
		payload   string
	}
	observed := make(chan observation, 4)

	cfg := runtime.DaemonConfig{
		DataDir:         dataDir,
		ChannelsDir:     channelsDir,
		DaemonID:        "daemon-dev",
		DaemonEpoch:     1,
		UseMockBus:      true,
		NowFn:           now,
		SchedulerPeriod: 50 * time.Millisecond,
		OnChannelBoot: func(_ context.Context, h runtime.ChannelHooks) (func(context.Context) error, error) {
			// Defensive: DeviceTransit / SetDeviceCallback are populated by
			// the daemon when the channel has a transit client, which is
			// always true under UseMockBus.
			if h.DeviceTransit == nil {
				t.Errorf("OnChannelBoot: expected non-nil DeviceTransit on hooks for channel %s", h.ChannelID)
			}
			if h.SetDeviceCallback == nil {
				t.Errorf("OnChannelBoot: expected non-nil SetDeviceCallback on hooks for channel %s", h.ChannelID)
				return func(context.Context) error { return nil }, nil
			}
			h.SetDeviceCallback(func(_ context.Context, frame kadapter.SendFrame) error {
				observed <- observation{
					channelID: string(frame.ChannelID),
					payload:   string(frame.Payload),
				}
				return nil
			})
			return func(context.Context) error { return nil }, nil
		},
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

	srv := d.Bus().ServerSide()
	pushFrame := func(targetCh channel.ID, payload string) {
		t.Helper()
		body := kadapter.SendFrame{
			ChannelID:       targetCh,
			DeviceSessionID: "sess-1",
			Direction:       kadapter.DirectionFromDevice,
			RequestID:       "req-1",
			Payload:         []byte(payload),
		}
		frame, err := transit.Encode("frame-recv-"+string(targetCh),
			daemonbus.FrameTypeDeviceTransitRecv,
			"server", d.Transit().Epoch(), now(), body)
		if err != nil {
			t.Fatalf("encode device_transit.recv: %v", err)
		}
		if err := srv.SendToDaemon(ctx, frame); err != nil {
			t.Fatalf("SendToDaemon: %v", err)
		}
	}

	// 1) Frame targeting the owned channel — callback must fire with the
	// original payload bytes.
	pushFrame(chID, `{"correlation_id":"req-1","ok":true}`)
	select {
	case obs := <-observed:
		if obs.channelID != string(chID) {
			t.Errorf("callback observed channel_id=%s want %s", obs.channelID, chID)
		}
		if obs.payload != `{"correlation_id":"req-1","ok":true}` {
			t.Errorf("callback observed payload=%q", obs.payload)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("callback never fired for owned channel frame")
	}

	// 2) Frame targeting an unknown channel — dropped silently.
	pushFrame(channel.ID("ch-other"), `{"correlation_id":"req-x"}`)
	select {
	case obs := <-observed:
		t.Errorf("callback fired for unowned channel: %+v", obs)
	case <-time.After(200 * time.Millisecond):
		// Expected: no callback.
	}

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

// queryTerminalResponses opens the channel sqlite read-only and returns
// a map of request_id → response payload.reason for every terminal
// failed response present. Helper for the long-pending scheduler test.
func queryTerminalResponses(ctx context.Context, dbPath string) (map[string]string, error) {
	db, err := store.OpenChannel(ctx, dbPath, store.OpenOptions{SkipDDL: true})
	if err != nil {
		return nil, err
	}
	defer func() { _ = db.Close() }()
	rows, err := db.QueryContext(ctx,
		`SELECT COALESCE(parent_id,''), payload
		   FROM messages
		   WHERE kind='response' AND is_terminal=1 AND parent_id IS NOT NULL`)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	out := map[string]string{}
	for rows.Next() {
		var parentID, payload string
		if err := rows.Scan(&parentID, &payload); err != nil {
			return nil, err
		}
		var doc struct {
			Reason string `json:"reason"`
		}
		_ = json.Unmarshal([]byte(payload), &doc)
		out[parentID] = doc.Reason
	}
	return out, rows.Err()
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

// TestDaemon_Phase3_HeartbeatSender covers M1.6-T1 part A: after
// startPhase3 runs, the daemon must periodically emit
// control.heartbeat frames carrying the owned-channel snapshot. Without
// this, server placements drift to `stale` 90s after boot (the bug T0
// closing verification surfaced).
func TestDaemon_Phase3_HeartbeatSender(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	dataDir := t.TempDir()
	channelsDir := filepath.Join(dataDir, "channels")
	chID := channel.ID("ch-hb")
	chDir := filepath.Join(channelsDir, string(chID))
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
		ChannelID:    chID,
		FencingToken: 1, OwnerEpoch: 1,
		DaemonID: "daemon-hb", DaemonEpoch: 1,
		AcquiredAt: now(), RefreshedAt: now(),
	}); err != nil {
		t.Fatal(err)
	}
	_ = db.Close()

	cfg := runtime.DaemonConfig{
		DataDir:         dataDir,
		ChannelsDir:     channelsDir,
		DaemonID:        "daemon-hb",
		DaemonEpoch:     1,
		UseMockBus:      true,
		NowFn:           now,
		SchedulerPeriod: time.Second,
		HeartbeatPeriod: 25 * time.Millisecond,
	}
	d, err := runtime.AssembleDaemon(ctx, cfg)
	if err != nil {
		t.Fatalf("AssembleDaemon: %v", err)
	}
	if err := d.RunPhases(ctx); err != nil {
		t.Fatalf("RunPhases: %v", err)
	}
	t.Cleanup(func() { _ = d.Close() })

	server := d.Bus().ServerSide()

	// Collect at least 2 heartbeat frames within the timeout. Other
	// frame types may interleave (e.g. viewsync.push); we filter for
	// the heartbeat ones.
	recvCtx, recvCancel := context.WithTimeout(ctx, 2*time.Second)
	defer recvCancel()
	got := 0
	var firstBody transit.HeartbeatBody
	for got < 2 {
		f, err := server.RecvFromDaemon(recvCtx)
		if err != nil {
			t.Fatalf("only saw %d heartbeats before timeout: %v", got, err)
		}
		if f.FrameType != daemonbus.FrameTypeControlHeartbeat {
			continue
		}
		var body transit.HeartbeatBody
		if err := json.Unmarshal(f.Payload, &body); err != nil {
			t.Fatalf("decode heartbeat payload: %v", err)
		}
		if got == 0 {
			firstBody = body
		}
		got++
	}

	// The owned-channel snapshot must include the channel we seeded.
	found := false
	for _, id := range firstBody.Channels {
		if id == chID {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("heartbeat body.Channels=%v missing seeded channel %s", firstBody.Channels, chID)
	}
}

// TestDaemon_Phase3_ChannelAgent_Registered covers M1.6-T1 P2:
//   - bootChannel inserts the well-known agent:channel-agent actor row
//     into the per-channel registry (so trigger.Gateway audience expand
//     resolves wildcard envelopes to the agent target).
//   - bootChannel registers a deliverer handler for that id, so a
//     post-harness Dispatch reaches the agent layer. The P2 stub just
//     counts arrivals; P4 swaps in WorkerManager.OnTrigger via the same
//     Deliverer.Register seam.
func TestDaemon_Phase3_ChannelAgent_Registered(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	dataDir := t.TempDir()
	channelsDir := filepath.Join(dataDir, "channels")
	chID := channel.ID("ch-agent")
	chDir := filepath.Join(channelsDir, string(chID))
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
		ChannelID:    chID,
		FencingToken: 1, OwnerEpoch: 1,
		DaemonID: "daemon-agent", DaemonEpoch: 1,
		AcquiredAt: now(), RefreshedAt: now(),
	}); err != nil {
		t.Fatal(err)
	}
	areg := store.NewActorRegistry(db)
	if err := areg.Insert(ctx, actor.Record{
		ID: "user:alice", Kind: actor.KindHuman,
		DisplayName: "Alice", CreatedAt: now(),
	}); err != nil {
		t.Fatal(err)
	}
	_ = db.Close()

	const secret = "agent-secret"
	cfg := runtime.DaemonConfig{
		DataDir:           dataDir,
		ChannelsDir:       channelsDir,
		DaemonID:          "daemon-agent",
		DaemonEpoch:       1,
		UseMockBus:        true,
		NowFn:             now,
		HumanCallerSecret: []byte(secret),
		SchedulerPeriod:   50 * time.Millisecond,
		HeartbeatPeriod:   time.Second,
	}
	d, err := runtime.AssembleDaemon(ctx, cfg)
	if err != nil {
		t.Fatalf("AssembleDaemon: %v", err)
	}
	if err := d.RunPhases(ctx); err != nil {
		t.Fatalf("RunPhases: %v", err)
	}
	t.Cleanup(func() { _ = d.Close() })

	// (a) actor row exists post-boot — open the sqlite read-only and
	// query the registry directly to prove the Insert happened (the
	// daemon's own Lookup would conflate "ensureChannelAgent insert"
	// with "Lookup hit a residual row from a previous test run", since
	// t.TempDir guarantees a fresh tree it is in fact unambiguous, but
	// reading via Lookup also exercises the in-process registry).
	{
		db2, err := store.OpenChannel(ctx, dbPath, store.OpenOptions{})
		if err != nil {
			t.Fatalf("reopen channel sqlite: %v", err)
		}
		areg2 := store.NewActorRegistry(db2)
		rec, ok, err := areg2.Lookup(ctx, runtime.ChannelAgentID)
		_ = db2.Close()
		if err != nil {
			t.Fatalf("lookup channel-agent: %v", err)
		}
		if !ok {
			t.Fatalf("channel-agent actor row missing after bootChannel")
		}
		if rec.Kind != actor.KindAgent {
			t.Errorf("channel-agent kind=%q want %q", rec.Kind, actor.KindAgent)
		}
	}

	// (b) drive a write_message frame and assert the trigger counter
	// climbs — proves the deliverer handler is wired and reachable via
	// the post-harness gateway.Dispatch path.
	if got := d.ChannelAgentTriggerCount(chID); got != 0 {
		t.Fatalf("pre-drive counter = %d, want 0", got)
	}

	bus := d.Bus()
	server := bus.ServerSide()
	ts := now()
	hc := transit.HumanCaller{
		UserID:           "u1",
		ActorIDInChannel: "user:alice",
		TS:               ts,
		Nonce:            "nonce-agent",
	}
	hc.ServerToken = transit.SignHumanCaller(
		[]byte(secret), string(chID), hc.UserID, hc.ActorIDInChannel, hc.TS, hc.Nonce,
	)
	body := transit.WriteMessageBody{
		FrameID:     "frame-agent-1",
		ChannelID:   string(chID),
		HumanCaller: hc,
		EnvelopePartial: message.Envelope{
			Type:       "human.text",
			Kind:       message.KindEvent,
			Payload:    json.RawMessage(`{"text":"hi agent"}`),
			Audience:   []string{"*"},
			Visibility: message.VisibilityPublic,
			TS:         ts,
		},
	}
	reqFrame, _ := transit.Encode("frame-srv-agent",
		daemonbus.FrameTypeControlWriteMessage,
		"server", d.Transit().Epoch(), ts, body)
	if err := server.SendToDaemon(ctx, reqFrame); err != nil {
		t.Fatal(err)
	}

	// Wait for the ack — same drain shape as
	// TestDaemon_Phase3_DispatchesWriteMessage.
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
		var ack transit.WriteMessageAckBody
		if err := transit.DecodePayload(f, &ack); err != nil {
			t.Fatal(err)
		}
		if !ack.Accepted {
			t.Fatalf("ack reject: reason=%s detail=%s", ack.RejectReason, ack.RejectDetail)
		}
		break
	}

	// Dispatch races with the ack reply — poll briefly.
	pollDeadline := time.Now().Add(2 * time.Second)
	var got int64
	for time.Now().Before(pollDeadline) {
		got = d.ChannelAgentTriggerCount(chID)
		if got >= 1 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if got < 1 {
		t.Fatalf("channel-agent trigger counter = %d, want >= 1 (gateway.Dispatch did not reach handler)", got)
	}
}

// TestDaemon_Phase3_WorkerReply covers M1.6-T1 acceptance #2 + #3:
//
// (a) e2e — POST human.text → daemon harness chain → trigger gateway
//
//	dispatch → worker spawn → worker emits agent.text reply → query
//	channel.sqlite returns the reply row.
//
// (b) reuse — the second human.text in the same channel must hit the
//
//	SAME spawned worker (PipeSpawner spawn counter stays at 1).
//
// PipeSpawner runs an in-process worker.Runtime wired to MockBridge so
// the test stays hermetic (no need for ./bin/coagent-worker).
func TestDaemon_Phase3_WorkerReply(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	dataDir := t.TempDir()
	channelsDir := filepath.Join(dataDir, "channels")
	chID := channel.ID("ch-worker")
	chDir := filepath.Join(channelsDir, string(chID))
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
		ChannelID:    chID,
		FencingToken: 1, OwnerEpoch: 1,
		DaemonID: "daemon-worker", DaemonEpoch: 1,
		AcquiredAt: now(), RefreshedAt: now(),
	}); err != nil {
		t.Fatal(err)
	}
	areg := store.NewActorRegistry(db)
	if err := areg.Insert(ctx, actor.Record{
		ID: "user:alice", Kind: actor.KindHuman,
		DisplayName: "Alice", CreatedAt: now(),
	}); err != nil {
		t.Fatal(err)
	}
	_ = db.Close()

	// PipeSpawner injects a fresh worker.Runtime + MockBridge per
	// spawn. spawnCount is the reuse witness — it must stay 1 across
	// two consecutive human.text frames.
	spawnCount := new(atomic.Int64)
	spawner := &workerhost.PipeSpawner{
		WorkerFunc: func(ctx context.Context, leaseID string, _ []string, in io.Reader, out io.Writer) error {
			spawnCount.Add(1)
			bridge := worker.NewMockBridge()
			bridge.MaxTurns = 99 // big — manager re-use covers exit
			bridge.EnvelopeIDFn = func(workerID string, turn int) string {
				return "agent-reply-" + leaseID + "-" + itoa(turn)
			}
			rt, err := worker.New(worker.Config{
				LeaseID:        leaseID,
				In:             in,
				Out:            out,
				NowFn:          now,
				HeartbeatEvery: time.Hour, // suppress; test is short
				Bridge:         bridge,
			})
			if err != nil {
				return err
			}
			return rt.Run(ctx)
		},
	}

	const secret = "worker-secret"
	cfg := runtime.DaemonConfig{
		DataDir:           dataDir,
		ChannelsDir:       channelsDir,
		DaemonID:          "daemon-worker",
		DaemonEpoch:       1,
		UseMockBus:        true,
		NowFn:             now,
		HumanCallerSecret: []byte(secret),
		SchedulerPeriod:   50 * time.Millisecond,
		HeartbeatPeriod:   time.Second,
		WorkerSpawner:     spawner,
	}
	d, err := runtime.AssembleDaemon(ctx, cfg)
	if err != nil {
		t.Fatalf("AssembleDaemon: %v", err)
	}
	if err := d.RunPhases(ctx); err != nil {
		t.Fatalf("RunPhases: %v", err)
	}
	t.Cleanup(func() { _ = d.Close() })

	bus := d.Bus()
	server := bus.ServerSide()
	// sendHuman returns the daemon-allocated envelope id (canonical hash)
	// so the e2e check can look up the agent reply by parent_id.
	sendHuman := func(t *testing.T, nonce string) string {
		t.Helper()
		ts := now()
		hc := transit.HumanCaller{
			UserID:           "u1",
			ActorIDInChannel: "user:alice",
			TS:               ts,
			Nonce:            nonce,
		}
		hc.ServerToken = transit.SignHumanCaller(
			[]byte(secret), string(chID), hc.UserID, hc.ActorIDInChannel, hc.TS, hc.Nonce,
		)
		body := transit.WriteMessageBody{
			FrameID:     "frame-" + nonce,
			ChannelID:   string(chID),
			HumanCaller: hc,
			EnvelopePartial: message.Envelope{
				Type:       "human.text",
				Kind:       message.KindEvent,
				Payload:    json.RawMessage(`{"text":"hi-` + nonce + `"}`),
				Audience:   []string{"*"},
				Visibility: message.VisibilityPublic,
				TS:         ts,
			},
		}
		reqFrame, _ := transit.Encode("frame-srv-"+nonce,
			daemonbus.FrameTypeControlWriteMessage,
			"server", d.Transit().Epoch(), ts, body)
		if err := server.SendToDaemon(ctx, reqFrame); err != nil {
			t.Fatal(err)
		}
		deadline := time.After(5 * time.Second)
		for {
			select {
			case <-deadline:
				t.Fatal("write_message_ack timeout")
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
			var ack transit.WriteMessageAckBody
			if err := transit.DecodePayload(f, &ack); err != nil {
				t.Fatal(err)
			}
			if !ack.Accepted {
				t.Fatalf("ack reject: %s/%s", ack.RejectReason, ack.RejectDetail)
			}
			return ack.MessageID
		}
	}

	// === First human envelope — should spawn worker + collect reply.
	humanID1 := sendHuman(t, "n1")
	waitAgentReply(t, ctx, dbPath, humanID1)
	if got := spawnCount.Load(); got != 1 {
		t.Fatalf("after first trigger spawn count = %d want 1", got)
	}
	w1 := d.CurrentWorkerIDFor(chID)
	if w1 == "" {
		t.Fatal("worker id empty after first reply")
	}

	// === Second human envelope — same worker reused.
	humanID2 := sendHuman(t, "n2")
	waitAgentReply(t, ctx, dbPath, humanID2)
	if got := spawnCount.Load(); got != 1 {
		t.Fatalf("after second trigger spawn count = %d want 1 (worker should be reused)", got)
	}
	if w2 := d.CurrentWorkerIDFor(chID); w2 != w1 {
		t.Errorf("worker id changed across triggers: %q → %q", w1, w2)
	}
}

// waitAgentReply polls the channel sqlite for any agent.text envelope
// whose parent_id matches the supplied human envelope id. Times out at
// 5s — the spawn + IPC round-trip is ~50ms in CI; 5s buys headroom.
func waitAgentReply(t *testing.T, ctx context.Context, dbPath string, parentID string) {
	t.Helper()
	db, err := store.OpenChannel(ctx, dbPath, store.OpenOptions{})
	if err != nil {
		t.Fatalf("reopen sqlite: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	deadline := time.Now().Add(5 * time.Second)
	const q = `SELECT id, sender_id, COALESCE(parent_id,'')
	             FROM messages
	            WHERE type='agent.text' AND parent_id=?`
	for time.Now().Before(deadline) {
		row := db.QueryRowContext(ctx, q, parentID)
		var id, sender, parent string
		switch err := row.Scan(&id, &sender, &parent); err {
		case nil:
			if sender != "agent:channel-agent" {
				t.Fatalf("agent reply sender=%q want agent:channel-agent (id=%s)", sender, id)
			}
			return
		case sql.ErrNoRows:
			time.Sleep(30 * time.Millisecond)
		default:
			t.Fatalf("query agent reply: %v", err)
		}
	}
	// Dump all messages for diagnosis before failing.
	rows, derr := db.QueryContext(ctx, `SELECT id, type, sender_id, COALESCE(parent_id,'') FROM messages ORDER BY seq`)
	if derr == nil {
		defer func() { _ = rows.Close() }()
		for rows.Next() {
			var id, typ, sender, parent string
			_ = rows.Scan(&id, &typ, &sender, &parent)
			t.Logf("  row: id=%s type=%s sender=%s parent=%s", id, typ, sender, parent)
		}
	}
	t.Fatalf("agent.text reply for parent %q never appeared", parentID)
}

// itoa is a tiny helper to avoid pulling strconv just for the test's
// envelope id generator (test files already import a fair stack).
// TestDaemon_WorkerEnvForChannel_DomainPromptPlumbed covers M1.6-T5
// phase-3: when DaemonConfig.ChannelTemplates has an xhs-creator entry
// with a non-empty DomainPrompt, the daemon's per-channel WorkerEnv
// resolution carries both COAGENT_CHANNEL_TYPE and COAGENT_DOMAIN_PROMPT
// in slot order. Legacy / group channels produce only COAGENT_CHANNEL_TYPE
// (prompt omitted) so cmd.Env stays lean.
func TestDaemon_WorkerEnvForChannel_DomainPromptPlumbed(t *testing.T) {
	t.Parallel()
	tmp := t.TempDir()
	dataDir := filepath.Join(tmp, "data")
	channelsDir := filepath.Join(tmp, "channels")
	if err := os.MkdirAll(channelsDir, 0o755); err != nil {
		t.Fatal(err)
	}

	const xhsPrompt = "你是 xhs 内容创作 agent.\n禁止重复 publish。"
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	cfg := runtime.DaemonConfig{
		DataDir:     dataDir,
		ChannelsDir: channelsDir,
		DaemonID:    "daemon-prompt",
		DaemonEpoch: 1,
		UseMockBus:  true,
		NowFn:       now,
		ChannelTemplates: map[string]runtime.ChannelTemplate{
			"":            {},
			"group":       {},
			"xhs-creator": {DomainPrompt: xhsPrompt},
		},
	}
	d, err := runtime.AssembleDaemon(ctx, cfg)
	if err != nil {
		t.Fatalf("AssembleDaemon: %v", err)
	}
	t.Cleanup(func() { _ = d.Close() })

	t.Run("xhs-creator carries domain prompt", func(t *testing.T) {
		got := d.WorkerEnvForChannel("xhs-creator")
		if len(got) != 2 {
			t.Fatalf("len=%d want 2; got=%v", len(got), got)
		}
		if got[0] != "COAGENT_CHANNEL_TYPE=xhs-creator" {
			t.Errorf("env[0]=%q", got[0])
		}
		if got[1] != "COAGENT_DOMAIN_PROMPT="+xhsPrompt {
			t.Errorf("env[1]=%q want COAGENT_DOMAIN_PROMPT=<prompt>", got[1])
		}
	})

	t.Run("group channel omits prompt", func(t *testing.T) {
		got := d.WorkerEnvForChannel("group")
		if len(got) != 1 {
			t.Fatalf("len=%d want 1; got=%v", len(got), got)
		}
		if got[0] != "COAGENT_CHANNEL_TYPE=group" {
			t.Errorf("env[0]=%q want COAGENT_CHANNEL_TYPE=group", got[0])
		}
	})

	t.Run("legacy unset type still emits empty channel_type", func(t *testing.T) {
		got := d.WorkerEnvForChannel("")
		if len(got) != 1 || got[0] != "COAGENT_CHANNEL_TYPE=" {
			t.Errorf("legacy env=%v want [COAGENT_CHANNEL_TYPE=]", got)
		}
	})
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	neg := n < 0
	if neg {
		n = -n
	}
	buf := [20]byte{}
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}
