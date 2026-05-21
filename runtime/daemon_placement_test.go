package runtime_test

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/wanpengxie/ActOS/kernel/channel"
	"github.com/wanpengxie/ActOS/kernel/daemonbus"
	"github.com/wanpengxie/ActOS/kernel/placement"
	"github.com/wanpengxie/ActOS/runtime"
	"github.com/wanpengxie/ActOS/runtime/store"
	"github.com/wanpengxie/ActOS/runtime/transit"
)

// helper: start an in-process daemon (mock-bus) ready for control frame
// injection. Returns the daemon, its mock-bus server-side handle, and
// the channels directory for sqlite assertions.
func startDaemon(t *testing.T, ctx context.Context, daemonID string) (*runtime.Daemon, *transit.MockServer, string, string) {
	t.Helper()
	dataDir := t.TempDir()
	channelsDir := filepath.Join(dataDir, "channels")
	cfg := runtime.DaemonConfig{
		DataDir:         dataDir,
		ChannelsDir:     channelsDir,
		DaemonID:        daemonID,
		DaemonEpoch:     42,
		UseMockBus:      true,
		NowFn:           now,
		SchedulerPeriod: 50 * time.Millisecond,
	}
	d, err := runtime.AssembleDaemon(ctx, cfg)
	if err != nil {
		t.Fatalf("AssembleDaemon: %v", err)
	}
	if err := d.RunPhases(ctx); err != nil {
		t.Fatalf("RunPhases: %v", err)
	}
	return d, d.Bus().ServerSide(), dataDir, channelsDir
}

// sendCreateChannel injects a control.create_channel frame with the
// daemon's current connection epoch and waits up to 3s for the ACK.
func sendCreateChannel(
	t *testing.T,
	ctx context.Context,
	d *runtime.Daemon,
	srv *transit.MockServer,
	req placement.CreateChannelRequest,
) placement.CreateChannelAck {
	t.Helper()
	frame, err := transit.Encode("frame-create-"+string(req.ChannelID),
		daemonbus.FrameTypeControlCreateChannel,
		"server", d.Transit().Epoch(), now(), req)
	if err != nil {
		t.Fatalf("encode create: %v", err)
	}
	if err := srv.SendToDaemon(ctx, frame); err != nil {
		t.Fatalf("SendToDaemon: %v", err)
	}
	deadline := time.After(3 * time.Second)
	for {
		select {
		case <-deadline:
			t.Fatal("did not receive create_channel_ack within 3s")
		default:
		}
		recvCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		f, err := srv.RecvFromDaemon(recvCtx)
		cancel()
		if err != nil {
			t.Fatalf("RecvFromDaemon: %v", err)
		}
		if f.FrameKind != daemonbus.FrameTypeControlCreateChannelAck {
			continue
		}
		var ack placement.CreateChannelAck
		if err := transit.DecodePayload(f, &ack); err != nil {
			t.Fatalf("decode ack: %v", err)
		}
		return ack
	}
}

// TestDaemon_OnCreateChannel_FreshBootstrap covers T0.1 happy path:
// server pushes control.create_channel → daemon runs saga → writes
// channel_lock → mounts runtime → emits AckBound with all 5 match
// fields populated → channel.sqlite exists with actor_registry rows.
func TestDaemon_OnCreateChannel_FreshBootstrap(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	d, srv, dataDir, channelsDir := startDaemon(t, ctx, "daemon-A")
	defer func() { _ = d.Close() }()

	req := placement.CreateChannelRequest{
		ChannelID:       "ch-new",
		CreateRequestID: "req-fresh-1",
		InitialMembers: []placement.InitialMember{
			{MemberActorID: "user:alice", Kind: "human", DisplayName: "Alice"},
		},
	}
	ack := sendCreateChannel(t, ctx, d, srv, req)

	if ack.Status != placement.AckBound {
		t.Fatalf("ack.Status=%s reason=%s", ack.Status, ack.Reason)
	}
	if ack.ChannelID != req.ChannelID {
		t.Errorf("ack.ChannelID=%s want %s", ack.ChannelID, req.ChannelID)
	}
	if ack.CreateRequestID != req.CreateRequestID {
		t.Errorf("ack.CreateRequestID=%s want %s", ack.CreateRequestID, req.CreateRequestID)
	}
	// Daemon is the trust root for fencing (proto-foundation §3.3.3
	// Phase 2): owner_epoch=1 (fixed) and a fresh unguessable token.
	if ack.OwnerEpoch != 1 {
		t.Errorf("ack.OwnerEpoch=%d want 1 (Phase 2 fixed)", ack.OwnerEpoch)
	}
	if len(ack.FencingToken) != 32 {
		t.Errorf("ack.FencingToken=%q (len=%d) want 32-char hex (daemon-generated)",
			ack.FencingToken, len(ack.FencingToken))
	}
	if ack.DaemonID != "daemon-A" {
		t.Errorf("ack.DaemonID=%s want daemon-A", ack.DaemonID)
	}
	// Match() pre-check uses saga identifiers only — owner_epoch /
	// fencing_token are daemon outputs and not part of Match.
	want := placement.Placement{
		ChannelID:       req.ChannelID,
		DaemonID:        "daemon-A",
		State:           placement.StateCreating,
		OwnerEpoch:      0,
		FencingToken:    "",
		CreateRequestID: req.CreateRequestID,
	}
	if !ack.Match(want) {
		t.Errorf("ack does NOT match server placement (CAS would fail)")
	}
	// Capture the daemon-generated tuple for downstream assertions.
	daemonOwnerEpoch := ack.OwnerEpoch
	daemonFencingTok := ack.FencingToken

	// channel.sqlite physically present + DDL applied + actor_registry
	// has both system + alice rows.
	chDB := filepath.Join(channelsDir, string(req.ChannelID), "channel.sqlite")
	if _, err := os.Stat(chDB); err != nil {
		t.Fatalf("channel.sqlite missing: %v", err)
	}
	db, err := store.OpenChannel(ctx, chDB, store.OpenOptions{SkipDDL: true})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	reg := store.NewActorRegistry(db)
	if _, ok, _ := reg.Lookup(ctx, "system"); !ok {
		t.Error("system actor missing in actor_registry")
	}
	if _, ok, _ := reg.Lookup(ctx, "user:alice"); !ok {
		t.Error("user:alice missing in actor_registry")
	}
	// channel_lock row present with daemon-A and the
	// daemon-generated fencing tuple (NOT any server-supplied value —
	// the daemon is the trust root).
	lock := store.NewChannelLock(db)
	row, ok, err := lock.Get(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("channel_lock row missing after OnCreateChannel")
	}
	if row.DaemonID != "daemon-A" {
		t.Errorf("channel_lock.DaemonID=%q want daemon-A", row.DaemonID)
	}
	if row.OwnerEpoch != daemonOwnerEpoch || row.FencingToken != daemonFencingTok {
		t.Errorf("channel_lock fencing tuple = (epoch=%d, token=%q) want (epoch=%d, token=%q) (ack must match disk)",
			row.OwnerEpoch, row.FencingToken, daemonOwnerEpoch, daemonFencingTok)
	}

	// bootstrap_registry row marked completed.
	daemonDB := d.DaemonDB()
	var status string
	if err := daemonDB.QueryRowContext(ctx,
		`SELECT status FROM bootstrap_registry WHERE create_request_id=?`,
		string(req.CreateRequestID)).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != "completed" {
		t.Errorf("bootstrap_registry status=%q want completed", status)
	}

	// Daemon hot-mounted the channel.
	if !d.HasChannel(req.ChannelID) {
		t.Error("daemon did not mount the channel runtime after OnCreateChannel")
	}

	_ = dataDir // silence unused
}

// TestDaemon_OnCreateChannel_IdempotentReplay — replay with same tuple
// must AckBound again without double-bootstrap.
func TestDaemon_OnCreateChannel_IdempotentReplay(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	d, srv, _, _ := startDaemon(t, ctx, "daemon-A")
	defer func() { _ = d.Close() }()

	req := placement.CreateChannelRequest{
		ChannelID: "ch-idem", CreateRequestID: "req-idem",
	}
	a1 := sendCreateChannel(t, ctx, d, srv, req)
	if a1.Status != placement.AckBound {
		t.Fatalf("first ack=%s reason=%s", a1.Status, a1.Reason)
	}
	a2 := sendCreateChannel(t, ctx, d, srv, req)
	if a2.Status != placement.AckBound {
		t.Errorf("second ack=%s reason=%s (idempotent replay must AckBound)", a2.Status, a2.Reason)
	}
	// Idempotent replay MUST echo the same daemon-generated fencing
	// tuple from the original bootstrap — the daemon is the trust
	// root and the server's CAS depends on stable values across retries.
	if a2.OwnerEpoch != a1.OwnerEpoch || a2.FencingToken != a1.FencingToken {
		t.Errorf("idempotent replay returned a different fencing tuple: a1=(%d,%q) a2=(%d,%q)",
			a1.OwnerEpoch, a1.FencingToken, a2.OwnerEpoch, a2.FencingToken)
	}
}

// TestDaemon_OnCreateChannel_ConflictingRequestRejected — after a
// successful bind under create_request_id=req-A, a second create frame
// for the SAME channel_id with a DIFFERENT create_request_id (req-B)
// MUST be rejected. The daemon-side channel state is the trust root
// (proto-foundation §3.3.3): the server is not allowed to silently
// re-bootstrap a channel under a new saga id.
func TestDaemon_OnCreateChannel_ConflictingRequestRejected(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	d, srv, _, _ := startDaemon(t, ctx, "daemon-A")
	defer func() { _ = d.Close() }()

	bind := placement.CreateChannelRequest{
		ChannelID: "ch-high", CreateRequestID: "req-A",
	}
	if ack := sendCreateChannel(t, ctx, d, srv, bind); ack.Status != placement.AckBound {
		t.Fatalf("first bind=%s reason=%s", ack.Status, ack.Reason)
	}

	conflicting := bind
	conflicting.CreateRequestID = "req-B"
	ack := sendCreateChannel(t, ctx, d, srv, conflicting)
	if ack.Status != placement.AckRejected {
		t.Fatalf("conflicting create_request_id expected reject, got %s", ack.Status)
	}
	if ack.Reason != "create_request_id_mismatch" {
		t.Errorf("ack.Reason=%q want create_request_id_mismatch", ack.Reason)
	}
}

// TestDaemon_OnUnbindChannel covers T0.2: after the daemon mounts a
// channel via OnCreateChannel, a control.unbind_channel frame must
// drop the channel from d.HasChannel and emit unbind_channel_ack.
func TestDaemon_OnUnbindChannel(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	d, srv, _, channelsDir := startDaemon(t, ctx, "daemon-A")
	defer func() { _ = d.Close() }()

	chID := channel.ID("ch-unbind")
	req := placement.CreateChannelRequest{
		ChannelID: chID, CreateRequestID: "req-unb",
	}
	if ack := sendCreateChannel(t, ctx, d, srv, req); ack.Status != placement.AckBound {
		t.Fatalf("create=%s", ack.Status)
	}
	if !d.HasChannel(chID) {
		t.Fatal("channel not mounted after create")
	}

	body := struct {
		FrameID   string     `json:"frame_id"`
		ChannelID channel.ID `json:"channel_id"`
	}{FrameID: "unb-1", ChannelID: chID}
	frame, _ := transit.Encode("frame-unbind", daemonbus.FrameTypeControlUnbindChannel,
		"server", d.Transit().Epoch(), now(), body)
	if err := srv.SendToDaemon(ctx, frame); err != nil {
		t.Fatal(err)
	}

	// Drain frames until we see the ack.
	deadline := time.After(3 * time.Second)
	gotAck := false
	for !gotAck {
		select {
		case <-deadline:
			t.Fatal("no unbind ack within 3s")
		default:
		}
		recvCtx, c := context.WithTimeout(ctx, 1*time.Second)
		f, err := srv.RecvFromDaemon(recvCtx)
		c()
		if err != nil {
			t.Fatalf("recv: %v", err)
		}
		if f.FrameKind != daemonbus.FrameTypeControlUnbindChannelAck {
			continue
		}
		var ack struct {
			ChannelID channel.ID `json:"channel_id"`
			Status    string     `json:"status"`
		}
		if err := transit.DecodePayload(f, &ack); err != nil {
			t.Fatal(err)
		}
		if ack.Status != "unbound" {
			t.Errorf("ack.Status=%q want unbound", ack.Status)
		}
		if ack.ChannelID != chID {
			t.Errorf("ack.ChannelID=%q want %q", ack.ChannelID, chID)
		}
		gotAck = true
	}

	// Wait briefly for the dispatcher goroutine to remove the entry —
	// HasChannel is queried from the test goroutine.
	deadline2 := time.Now().Add(1 * time.Second)
	for time.Now().Before(deadline2) {
		if !d.HasChannel(chID) {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if d.HasChannel(chID) {
		t.Error("channel still mounted after unbind")
	}
	// Physical directory must NOT be deleted (unbind ≠ wipe).
	if _, err := os.Stat(filepath.Join(channelsDir, string(chID))); err != nil {
		t.Errorf("channel dir should still exist after unbind: %v", err)
	}
}

// TestDaemon_OnReclaimRejected_UnloadsChannel covers T0.4 reject leg:
// a reclaim_rejected frame for a mounted channel must drive the
// per-channel unload path.
func TestDaemon_OnReclaimRejected_UnloadsChannel(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	d, srv, _, _ := startDaemon(t, ctx, "daemon-A")
	defer func() { _ = d.Close() }()

	chID := channel.ID("ch-recl-rej")
	req := placement.CreateChannelRequest{
		ChannelID: chID, CreateRequestID: "req-recl",
	}
	if ack := sendCreateChannel(t, ctx, d, srv, req); ack.Status != placement.AckBound {
		t.Fatalf("create=%s", ack.Status)
	}

	// Inject reclaim_accepted with a rejected decision for chID.
	body := map[string]any{
		"daemon_id": "daemon-A",
		"decisions": []placement.ReclaimDecision{
			{ChannelID: chID, Accepted: false, Reason: "fencing mismatch"},
		},
	}
	frame, _ := transit.Encode("frame-recl", daemonbus.FrameTypeControlReclaimAccepted,
		"server", d.Transit().Epoch(), now(), body)
	if err := srv.SendToDaemon(ctx, frame); err != nil {
		t.Fatal(err)
	}

	// Wait for unload to take effect.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if !d.HasChannel(chID) {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if d.HasChannel(chID) {
		t.Error("channel still mounted after reclaim_rejected decision")
	}
}

// TestDaemon_OnReclaimAccepted_RecordsWatermark covers the accept leg
// — the bootstrap.Reconciler watermark must be stamped.
func TestDaemon_OnReclaimAccepted_RecordsWatermark(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	d, srv, _, _ := startDaemon(t, ctx, "daemon-A")
	defer func() { _ = d.Close() }()

	chID := channel.ID("ch-recl-ok")
	req := placement.CreateChannelRequest{
		ChannelID: chID, CreateRequestID: "req-recl-ok",
	}
	if ack := sendCreateChannel(t, ctx, d, srv, req); ack.Status != placement.AckBound {
		t.Fatalf("create=%s", ack.Status)
	}

	body := map[string]any{
		"daemon_id": "daemon-A",
		"decisions": []placement.ReclaimDecision{
			{ChannelID: chID, Accepted: true},
		},
	}
	frame, _ := transit.Encode("frame-recl-ok", daemonbus.FrameTypeControlReclaimAccepted,
		"server", d.Transit().Epoch(), now(), body)
	if err := srv.SendToDaemon(ctx, frame); err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if d.Reconciler().ReclaimAcceptedAt(chID) > 0 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if d.Reconciler().ReclaimAcceptedAt(chID) == 0 {
		t.Error("reconciler did not record accepted watermark")
	}
	if !d.HasChannel(chID) {
		t.Error("accepted reclaim should NOT unload the channel")
	}
}

// TestDaemon_OnHeartbeatAck_TracksWatermark covers T0.3: a
// control.heartbeat_ack frame must update the HeartbeatTracker.
func TestDaemon_OnHeartbeatAck_TracksWatermark(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	d, srv, _, _ := startDaemon(t, ctx, "daemon-A")
	defer func() { _ = d.Close() }()

	before := d.Heartbeat().LastAckAt()
	frame, _ := transit.Encode("frame-hb", daemonbus.FrameTypeControlHeartbeatAck,
		"server", d.Transit().Epoch(), now(), json.RawMessage(`{}`))
	if err := srv.SendToDaemon(ctx, frame); err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if d.Heartbeat().LastAckAt() > before {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if d.Heartbeat().LastAckAt() <= before {
		t.Error("HeartbeatTracker.LastAckAt did not advance after heartbeat_ack")
	}
	if d.Heartbeat().LastFrameID() != "frame-hb" {
		t.Errorf("HeartbeatTracker.LastFrameID=%q want frame-hb", d.Heartbeat().LastFrameID())
	}
}
