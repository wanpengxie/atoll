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

type channelCreatedEvent struct {
	Seq           int64
	ID            string
	TS            int64
	ChannelID     string
	SenderKind    string
	SenderID      string
	Kind          string
	Type          string
	Visibility    string
	Audience      []string
	CorrelationID string
	Payload       map[string]any
}

func loadChannelCreatedEvent(t *testing.T, ctx context.Context, db *sql.DB, chID channel.ID) channelCreatedEvent {
	t.Helper()
	var ev channelCreatedEvent
	var audienceRaw, payloadRaw []byte
	err := db.QueryRowContext(ctx, `
		SELECT seq, id, ts, channel_id, sender_kind, sender_id, kind, type,
		       visibility, audience, COALESCE(correlation_id, ''), payload
		FROM messages
		WHERE id = ?
	`, "system.channel.created:"+string(chID)).Scan(
		&ev.Seq, &ev.ID, &ev.TS, &ev.ChannelID, &ev.SenderKind, &ev.SenderID,
		&ev.Kind, &ev.Type, &ev.Visibility, &audienceRaw, &ev.CorrelationID, &payloadRaw,
	)
	if err != nil {
		t.Fatalf("query system.channel.created: %v", err)
	}
	if err := json.Unmarshal(audienceRaw, &ev.Audience); err != nil {
		t.Fatalf("decode created audience: %v", err)
	}
	if err := json.Unmarshal(payloadRaw, &ev.Payload); err != nil {
		t.Fatalf("decode created payload: %v", err)
	}
	return ev
}

func assertCanonicalChannelCreatedEvent(
	t *testing.T,
	ev channelCreatedEvent,
	chID channel.ID,
	daemonID string,
	ownerEpoch placement.OwnerEpoch,
) {
	t.Helper()
	if ev.Seq != 1 {
		t.Errorf("system.channel.created seq=%d want 1", ev.Seq)
	}
	if ev.ID != "system.channel.created:"+string(chID) {
		t.Errorf("created id=%q", ev.ID)
	}
	if ev.ChannelID != string(chID) {
		t.Errorf("created channel_id=%q want %q", ev.ChannelID, chID)
	}
	if ev.SenderKind != string(actor.KindSystem) || ev.SenderID != string(actor.SystemActorID) {
		t.Errorf("created sender=(%s,%s) want system actor", ev.SenderKind, ev.SenderID)
	}
	if ev.Kind != string(message.KindEvent) || ev.Type != "system.channel.created" {
		t.Errorf("created kind/type=(%s,%s)", ev.Kind, ev.Type)
	}
	if ev.Visibility != string(message.VisibilitySystem) {
		t.Errorf("created visibility=%q want system", ev.Visibility)
	}
	if len(ev.Audience) != 1 || ev.Audience[0] != string(message.AudienceWildcard) {
		t.Errorf("created audience=%v want [*]", ev.Audience)
	}
	if ev.CorrelationID != "" {
		t.Errorf("created correlation_id=%q want null", ev.CorrelationID)
	}
	if got := ev.Payload["channel_id"]; got != string(chID) {
		t.Errorf("payload.channel_id=%v want %s", got, chID)
	}
	if got := ev.Payload["daemon_id"]; got != daemonID {
		t.Errorf("payload.daemon_id=%v want %s", got, daemonID)
	}
	if got := ev.Payload["owner_epoch"]; got != float64(ownerEpoch) {
		t.Errorf("payload.owner_epoch=%v want %d", got, ownerEpoch)
	}
	if got := ev.Payload["created_at"]; got != float64(ev.TS) {
		t.Errorf("payload.created_at=%v want envelope ts %d", got, ev.TS)
	}
}

func sendCreateChannelFrame(
	t *testing.T,
	ctx context.Context,
	d *runtime.Daemon,
	srv *transit.MockServer,
	req placement.CreateChannelRequest,
) daemonbus.Frame {
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
		if f.FrameKind != daemonbus.FrameTypeControlCreateChannelAck &&
			f.FrameKind != daemonbus.FrameTypeControlRejectChannel {
			continue
		}
		return f
	}
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
	f := sendCreateChannelFrame(t, ctx, d, srv, req)
	if f.FrameKind != daemonbus.FrameTypeControlCreateChannelAck {
		t.Fatalf("expected create_channel_ack, got %s", f.FrameKind)
	}
	var ack placement.CreateChannelAck
	if err := transit.DecodePayload(f, &ack); err != nil {
		t.Fatalf("decode ack: %v", err)
	}
	return ack
}

func sendCreateChannelReject(
	t *testing.T,
	ctx context.Context,
	d *runtime.Daemon,
	srv *transit.MockServer,
	req placement.CreateChannelRequest,
) placement.RejectChannel {
	t.Helper()
	f := sendCreateChannelFrame(t, ctx, d, srv, req)
	if f.FrameKind != daemonbus.FrameTypeControlRejectChannel {
		t.Fatalf("expected reject_channel, got %s", f.FrameKind)
	}
	var rej placement.RejectChannel
	if err := transit.DecodePayload(f, &rej); err != nil {
		t.Fatalf("decode reject: %v", err)
	}
	return rej
}

func sendDaemonReclaim(
	t *testing.T,
	ctx context.Context,
	d *runtime.Daemon,
	srv *transit.MockServer,
	req placement.DaemonReclaimRequest,
) placement.ReclaimAccepted {
	t.Helper()
	frame, err := transit.Encode("frame-reclaim-"+string(req.ChannelID),
		daemonbus.FrameTypeControlDaemonReclaim,
		"server", d.Transit().Epoch(), now(), req)
	if err != nil {
		t.Fatalf("encode reclaim: %v", err)
	}
	if err := srv.SendToDaemon(ctx, frame); err != nil {
		t.Fatalf("SendToDaemon: %v", err)
	}
	deadline := time.After(3 * time.Second)
	for {
		select {
		case <-deadline:
			t.Fatal("did not receive reclaim result within 3s")
		default:
		}
		recvCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		f, err := srv.RecvFromDaemon(recvCtx)
		cancel()
		if err != nil {
			t.Fatalf("RecvFromDaemon: %v", err)
		}
		switch f.FrameKind {
		case daemonbus.FrameTypeControlReclaimAccepted:
			var ack placement.ReclaimAccepted
			if err := transit.DecodePayload(f, &ack); err != nil {
				t.Fatalf("decode reclaim accepted: %v", err)
			}
			return ack
		case daemonbus.FrameTypeControlReclaimRejected:
			var rej placement.ReclaimRejected
			if err := transit.DecodePayload(f, &rej); err != nil {
				t.Fatalf("decode reclaim rejected: %v", err)
			}
			t.Fatalf("reclaim rejected: %s", rej.Reason)
		}
	}
}

// TestDaemon_OnCreateChannel_FreshBootstrap covers T0.1 happy path:
// server pushes control.create_channel → daemon runs saga → writes
// channel_lock → mounts runtime → emits CreateChannelAccepted with all 5 match
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

	if ack.Result != placement.CreateChannelAccepted {
		t.Fatalf("ack.Result=%s", ack.Result)
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
	created := loadChannelCreatedEvent(t, ctx, db, req.ChannelID)
	assertCanonicalChannelCreatedEvent(t, created, req.ChannelID, "daemon-A", daemonOwnerEpoch)
	if _, ok := created.Payload["channel_type"]; ok {
		t.Errorf("payload.channel_type present for empty channel type: %v", created.Payload["channel_type"])
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
// must CreateChannelAccepted again without double-bootstrap.
func TestDaemon_OnCreateChannel_IdempotentReplay(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	d, srv, _, channelsDir := startDaemon(t, ctx, "daemon-A")
	defer func() { _ = d.Close() }()

	req := placement.CreateChannelRequest{
		ChannelID: "ch-idem", CreateRequestID: "req-idem",
	}
	a1 := sendCreateChannel(t, ctx, d, srv, req)
	if a1.Result != placement.CreateChannelAccepted {
		t.Fatalf("first ack=%s", a1.Result)
	}
	a2 := sendCreateChannel(t, ctx, d, srv, req)
	if a2.Result != placement.CreateChannelAccepted {
		t.Errorf("second ack=%s (idempotent replay must CreateChannelAccepted)", a2.Result)
	}
	// Idempotent replay MUST echo the same daemon-generated fencing
	// tuple from the original bootstrap — the daemon is the trust
	// root and the server's CAS depends on stable values across retries.
	if a2.OwnerEpoch != a1.OwnerEpoch || a2.FencingToken != a1.FencingToken {
		t.Errorf("idempotent replay returned a different fencing tuple: a1=(%d,%q) a2=(%d,%q)",
			a1.OwnerEpoch, a1.FencingToken, a2.OwnerEpoch, a2.FencingToken)
	}
	db, err := store.OpenChannel(ctx, filepath.Join(channelsDir, string(req.ChannelID), "channel.sqlite"), store.OpenOptions{SkipDDL: true})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	var count int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM messages WHERE type='system.channel.created'`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Errorf("system.channel.created count=%d want 1", count)
	}
	created := loadChannelCreatedEvent(t, ctx, db, req.ChannelID)
	assertCanonicalChannelCreatedEvent(t, created, req.ChannelID, "daemon-A", a1.OwnerEpoch)
}

// TestDaemon_OnCreateChannel_ReplayRepairsMissingCreatedEvent covers the
// crash window after channel_lock is durable but before the Layer 0
// system.channel.created event was appended.
func TestDaemon_OnCreateChannel_ReplayRepairsMissingCreatedEvent(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	d, srv, _, channelsDir := startDaemon(t, ctx, "daemon-A")
	defer func() { _ = d.Close() }()

	req := placement.CreateChannelRequest{
		ChannelID: "ch-created-repair", CreateRequestID: "req-created-repair",
	}
	sqlitePath, err := d.Saga().Bootstrap(ctx, req.ChannelID, req)
	if err != nil {
		t.Fatalf("pre-bootstrap saga: %v", err)
	}
	token, err := placement.NewFencingToken()
	if err != nil {
		t.Fatalf("fencing token: %v", err)
	}
	lockStore, err := d.OpenChannelLock(ctx, sqlitePath)
	if err != nil {
		t.Fatalf("open lock: %v", err)
	}
	ts := now()
	if err := lockStore.Insert(ctx, store.ChannelLockRow{
		ChannelID:    req.ChannelID,
		FencingToken: token,
		OwnerEpoch:   1,
		DaemonID:     "daemon-A",
		DaemonEpoch:  placement.DaemonEpoch(d.Transit().Epoch()),
		AcquiredAt:   ts,
		RefreshedAt:  ts,
	}); err != nil {
		t.Fatalf("insert lock: %v", err)
	}

	ack := sendCreateChannel(t, ctx, d, srv, req)
	if ack.Result != placement.CreateChannelAccepted {
		t.Fatalf("ack.Result=%s", ack.Result)
	}
	if ack.FencingToken != token {
		t.Errorf("ack.FencingToken=%q want repaired lock token %q", ack.FencingToken, token)
	}
	db, err := store.OpenChannel(ctx, filepath.Join(channelsDir, string(req.ChannelID), "channel.sqlite"), store.OpenOptions{SkipDDL: true})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	created := loadChannelCreatedEvent(t, ctx, db, req.ChannelID)
	assertCanonicalChannelCreatedEvent(t, created, req.ChannelID, "daemon-A", 1)
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
	if ack := sendCreateChannel(t, ctx, d, srv, bind); ack.Result != placement.CreateChannelAccepted {
		t.Fatalf("first bind=%s", ack.Result)
	}

	conflicting := bind
	conflicting.CreateRequestID = "req-B"
	rej := sendCreateChannelReject(t, ctx, d, srv, conflicting)
	if rej.Reason != "create_request_id_mismatch" {
		t.Errorf("reject reason=%q want create_request_id_mismatch", rej.Reason)
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
	if ack := sendCreateChannel(t, ctx, d, srv, req); ack.Result != placement.CreateChannelAccepted {
		t.Fatalf("create=%s", ack.Result)
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

// TestDaemon_OnHeldChannelsAckRejected_UnloadsChannel covers T0.4 reject leg:
// a held_channels_ack rejection for a mounted channel must drive the
// per-channel unload path.
func TestDaemon_OnHeldChannelsAckRejected_UnloadsChannel(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	d, srv, _, _ := startDaemon(t, ctx, "daemon-A")
	defer func() { _ = d.Close() }()

	chID := channel.ID("ch-recl-rej")
	req := placement.CreateChannelRequest{
		ChannelID: chID, CreateRequestID: "req-recl",
	}
	if ack := sendCreateChannel(t, ctx, d, srv, req); ack.Result != placement.CreateChannelAccepted {
		t.Fatalf("create=%s", ack.Result)
	}

	// Inject held_channels_ack with a rejected decision for chID.
	body := map[string]any{
		"daemon_id": "daemon-A",
		"decisions": []placement.HeldChannelsDecision{
			{ChannelID: chID, Accepted: false, Reason: "fencing mismatch"},
		},
	}
	frame, _ := transit.Encode("frame-held", daemonbus.FrameTypeControlHeldChannelsAck,
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
		t.Error("channel still mounted after held_channels_ack rejection")
	}
}

// TestDaemon_OnHeldChannelsAckAccepted_RecordsWatermark covers the accept leg
// — the bootstrap.Reconciler watermark must be stamped.
func TestDaemon_OnHeldChannelsAckAccepted_RecordsWatermark(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	d, srv, _, _ := startDaemon(t, ctx, "daemon-A")
	defer func() { _ = d.Close() }()

	chID := channel.ID("ch-recl-ok")
	req := placement.CreateChannelRequest{
		ChannelID: chID, CreateRequestID: "req-recl-ok",
	}
	if ack := sendCreateChannel(t, ctx, d, srv, req); ack.Result != placement.CreateChannelAccepted {
		t.Fatalf("create=%s", ack.Result)
	}

	body := map[string]any{
		"daemon_id": "daemon-A",
		"decisions": []placement.HeldChannelsDecision{
			{ChannelID: chID, Accepted: true},
		},
	}
	frame, _ := transit.Encode("frame-held-ok", daemonbus.FrameTypeControlHeldChannelsAck,
		"server", d.Transit().Epoch(), now(), body)
	if err := srv.SendToDaemon(ctx, frame); err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if d.Reconciler().HeldChannelAcceptedAt(chID) > 0 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if d.Reconciler().HeldChannelAcceptedAt(chID) == 0 {
		t.Error("reconciler did not record accepted watermark")
	}
	if !d.HasChannel(chID) {
		t.Error("accepted reclaim should NOT unload the channel")
	}
}

func TestDaemon_OnDaemonReclaim_EmitsCanonicalReclaimedPayload(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	d, srv, _, channelsDir := startDaemon(t, ctx, "daemon-A")
	defer func() { _ = d.Close() }()

	chID := channel.ID("ch-reclaimed-payload")
	createReq := placement.CreateChannelRequest{
		ChannelID: chID, CreateRequestID: "req-reclaimed-create",
	}
	createAck := sendCreateChannel(t, ctx, d, srv, createReq)
	if createAck.Result != placement.CreateChannelAccepted {
		t.Fatalf("create=%s", createAck.Result)
	}

	previousDaemon := placement.DaemonID("daemon-stale")
	reclaimReq := placement.DaemonReclaimRequest{
		ChannelID:           chID,
		CreateRequestID:     "req-reclaim-1",
		NewOwnerEpoch:       createAck.OwnerEpoch + 1,
		PreviousOwnerDaemon: &previousDaemon,
		PreviousState:       placement.ReclaimOriginStale,
	}
	reclaimAck := sendDaemonReclaim(t, ctx, d, srv, reclaimReq)
	if reclaimAck.NewOwnerEpoch != reclaimReq.NewOwnerEpoch {
		t.Fatalf("reclaim epoch=%d want %d", reclaimAck.NewOwnerEpoch, reclaimReq.NewOwnerEpoch)
	}

	dbPath := filepath.Join(channelsDir, string(chID), "channel.sqlite")
	db, err := store.OpenChannel(ctx, dbPath, store.OpenOptions{})
	if err != nil {
		t.Fatalf("open channel db: %v", err)
	}
	defer func() { _ = db.Close() }()

	var envelopeChannelID string
	var envelopeTS int64
	var rawPayload []byte
	err = db.QueryRowContext(ctx, `
		SELECT channel_id, ts, payload
		FROM messages
		WHERE type = 'system.placement.reclaimed'
	`).Scan(&envelopeChannelID, &envelopeTS, &rawPayload)
	if err != nil {
		t.Fatalf("query placement reclaimed event: %v", err)
	}
	if envelopeChannelID != string(chID) {
		t.Fatalf("envelope channel_id=%q want %q", envelopeChannelID, chID)
	}

	var payload map[string]any
	if err := json.Unmarshal(rawPayload, &payload); err != nil {
		t.Fatalf("decode reclaimed payload: %v", err)
	}
	want := map[string]any{
		"channel_id":               string(chID),
		"new_owner_daemon_id":      "daemon-A",
		"new_owner_epoch":          float64(reclaimReq.NewOwnerEpoch),
		"previous_owner_daemon_id": string(previousDaemon),
		"previous_owner_epoch":     float64(createAck.OwnerEpoch),
		"reclaimed_from_state":     string(placement.ReclaimOriginStale),
		"reclaimed_at":             float64(envelopeTS),
	}
	for key, wantValue := range want {
		if got := payload[key]; got != wantValue {
			t.Errorf("payload[%s]=%v want %v", key, got, wantValue)
		}
	}
	for _, oldKey := range []string{
		"previous_owner_daemon",
		"new_owner_daemon",
		"previous_state",
		"create_request_id",
	} {
		if _, ok := payload[oldKey]; ok {
			t.Errorf("payload unexpectedly contains old field %q", oldKey)
		}
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
