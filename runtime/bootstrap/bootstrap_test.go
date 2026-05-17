package bootstrap_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/wanpengxie/ActOS/kernel/channel"
	"github.com/wanpengxie/ActOS/kernel/placement"
	"github.com/wanpengxie/ActOS/runtime/bootstrap"
	"github.com/wanpengxie/ActOS/runtime/store"
)

func now() int64 { return time.Now().UnixMilli() }

func TestSaga_Bootstrap(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	channelsDir := filepath.Join(root, "channels")

	daemonDB, err := store.OpenDaemon(ctx, filepath.Join(root, "daemon.sqlite"), store.OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = daemonDB.Close() }()

	saga, err := bootstrap.NewSaga(bootstrap.SagaConfig{
		DaemonDB: daemonDB, ChannelsDir: channelsDir, NowFn: now,
	})
	if err != nil {
		t.Fatal(err)
	}

	path, err := saga.Bootstrap(ctx, channel.ID("ch-1"), placement.CreateChannelRequest{
		ChannelID:       "ch-1",
		CreateRequestID: "req-001",
		OwnerEpoch:      1, FencingToken: 1,
		InitialMembers: []placement.InitialMember{
			{ActorIDInChannel: "user:alice", Kind: "human", DisplayName: "Alice"},
		},
	})
	if err != nil {
		t.Fatalf("Bootstrap: %v", err)
	}
	if path == "" {
		t.Fatal("empty sqlite path")
	}

	// Channel sqlite exists.
	if _, err := os.Stat(path); err != nil {
		t.Errorf("channel sqlite missing: %v", err)
	}

	// bootstrap_registry row marked completed.
	var status string
	if err := daemonDB.QueryRowContext(ctx,
		`SELECT status FROM bootstrap_registry WHERE create_request_id=?`,
		"req-001",
	).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != "completed" {
		t.Errorf("bootstrap_registry status = %q", status)
	}

	// Channel sqlite has system + alice in actor_registry.
	chDB, err := store.OpenChannel(ctx, path, store.OpenOptions{SkipDDL: true})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = chDB.Close() }()
	reg := store.NewActorRegistry(chDB)
	if _, ok, _ := reg.Lookup(ctx, "system"); !ok {
		t.Error("system actor missing")
	}
	if _, ok, _ := reg.Lookup(ctx, "user:alice"); !ok {
		t.Error("alice actor missing")
	}
}

// TestReconciler_ReclaimWatermarks covers the M1.6-T0.4 watermark
// helpers added on Reconciler — Accept/Reject mutually exclusive,
// idempotent overwrites, missing channel returns zero values.
func TestReconciler_ReclaimWatermarks(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	daemonDB, _ := store.OpenDaemon(ctx, filepath.Join(root, "daemon.sqlite"), store.OpenOptions{})
	defer func() { _ = daemonDB.Close() }()

	rec, err := bootstrap.NewReconciler(daemonDB, now)
	if err != nil {
		t.Fatal(err)
	}

	if rec.ReclaimAcceptedAt("ch-x") != 0 {
		t.Error("initial AcceptedAt should be 0")
	}
	if _, ok := rec.ReclaimRejectedReason("ch-x"); ok {
		t.Error("initial RejectedReason should be unset")
	}

	rec.AcceptReclaim("ch-x")
	if rec.ReclaimAcceptedAt("ch-x") == 0 {
		t.Error("AcceptedAt not stamped")
	}
	// Reject supersedes accept.
	rec.RejectReclaim("ch-x", "stale")
	if rec.ReclaimAcceptedAt("ch-x") != 0 {
		t.Error("AcceptedAt should be cleared by Reject")
	}
	reason, ok := rec.ReclaimRejectedReason("ch-x")
	if !ok || reason != "stale" {
		t.Errorf("RejectedReason=%q ok=%v", reason, ok)
	}
	// Accept clears reject.
	rec.AcceptReclaim("ch-x")
	if _, ok := rec.ReclaimRejectedReason("ch-x"); ok {
		t.Error("Reject should be cleared by Accept")
	}
}

func TestReconciler_RollsBackInProgress(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	daemonDB, _ := store.OpenDaemon(ctx, filepath.Join(root, "daemon.sqlite"), store.OpenOptions{})
	defer func() { _ = daemonDB.Close() }()

	// Inject an in_progress row that mimics a crash mid-saga.
	channelDir := filepath.Join(root, "channels", "crashed")
	if err := os.MkdirAll(channelDir, 0o755); err != nil {
		t.Fatal(err)
	}
	_ = os.WriteFile(filepath.Join(channelDir, "channel.sqlite"), []byte("partial"), 0o644)
	if _, err := daemonDB.ExecContext(ctx, `INSERT INTO bootstrap_registry
		(create_request_id, channel_id, status, workdir_path, started_at)
		VALUES (?, ?, 'in_progress', ?, ?)`,
		"req-crash", "crashed", channelDir, now()); err != nil {
		t.Fatal(err)
	}

	rec, err := bootstrap.NewReconciler(daemonDB, now)
	if err != nil {
		t.Fatal(err)
	}
	rolled, err := rec.Run(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(rolled) != 1 || rolled[0].ChannelID != "crashed" {
		t.Fatalf("rolled = %+v", rolled)
	}
	// Workdir removed.
	if _, err := os.Stat(channelDir); !os.IsNotExist(err) {
		t.Errorf("workdir should be removed: %v", err)
	}
	// Status flipped.
	var status string
	_ = daemonDB.QueryRowContext(ctx, `SELECT status FROM bootstrap_registry WHERE create_request_id=?`, "req-crash").Scan(&status)
	if status != "rolled_back" {
		t.Errorf("status = %q", status)
	}
}
