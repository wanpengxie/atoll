package bootstrap_test

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
	khlog "github.com/wanpengxie/ActOS/kernel/log"
	"github.com/wanpengxie/ActOS/kernel/message"
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
		InitialMembers: []placement.InitialMember{
			{MemberActorID: "user:alice", Kind: "human", DisplayName: "Alice"},
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

	// bootstrap_registry row stays in_progress until the caller has inserted
	// channel_lock and invoked Complete.
	var status, phase string
	if err := daemonDB.QueryRowContext(ctx,
		`SELECT status, phase FROM bootstrap_registry WHERE create_request_id=?`,
		"req-001",
	).Scan(&status, &phase); err != nil {
		t.Fatal(err)
	}
	if status != "in_progress" || phase != "sent" {
		t.Errorf("bootstrap_registry status=%q phase=%q", status, phase)
	}
	if err := saga.Complete(ctx, "req-001"); err != nil {
		t.Fatalf("Complete: %v", err)
	}
	if err := daemonDB.QueryRowContext(ctx,
		`SELECT status, phase FROM bootstrap_registry WHERE create_request_id=?`,
		"req-001",
	).Scan(&status, &phase); err != nil {
		t.Fatal(err)
	}
	if status != "completed" || phase != "completed" {
		t.Errorf("bootstrap_registry after Complete status=%q phase=%q", status, phase)
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

	if rec.HeldChannelAcceptedAt("ch-x") != 0 {
		t.Error("initial AcceptedAt should be 0")
	}
	if _, ok := rec.HeldChannelRejectedReason("ch-x"); ok {
		t.Error("initial RejectedReason should be unset")
	}

	rec.AcceptHeldChannel("ch-x")
	if rec.HeldChannelAcceptedAt("ch-x") == 0 {
		t.Error("AcceptedAt not stamped")
	}
	// Reject supersedes accept.
	rec.RejectHeldChannel("ch-x", "stale")
	if rec.HeldChannelAcceptedAt("ch-x") != 0 {
		t.Error("AcceptedAt should be cleared by Reject")
	}
	reason, ok := rec.HeldChannelRejectedReason("ch-x")
	if !ok || reason != "stale" {
		t.Errorf("RejectedReason=%q ok=%v", reason, ok)
	}
	// Accept clears reject.
	rec.AcceptHeldChannel("ch-x")
	if _, ok := rec.HeldChannelRejectedReason("ch-x"); ok {
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

func TestReconciler_PreservesInProgressWithChannelLock(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	daemonDB, _ := store.OpenDaemon(ctx, filepath.Join(root, "daemon.sqlite"), store.OpenOptions{})
	defer func() { _ = daemonDB.Close() }()

	channelDir := filepath.Join(root, "channels", "locked")
	if err := os.MkdirAll(channelDir, 0o755); err != nil {
		t.Fatal(err)
	}
	chDB, err := store.OpenChannel(ctx, filepath.Join(channelDir, "channel.sqlite"), store.OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	token, err := placement.NewFencingToken()
	if err != nil {
		t.Fatal(err)
	}
	lock := store.NewChannelLock(chDB)
	if err := lock.Insert(ctx, store.ChannelLockRow{
		ChannelID:    "locked",
		FencingToken: token,
		OwnerEpoch:   1,
		DaemonID:     "daemon-A",
		DaemonEpoch:  1,
		AcquiredAt:   now(),
		RefreshedAt:  now(),
	}); err != nil {
		t.Fatal(err)
	}
	_ = chDB.Close()
	if _, err := daemonDB.ExecContext(ctx, `INSERT INTO bootstrap_registry
		(create_request_id, channel_id, status, workdir_path, started_at)
		VALUES (?, ?, 'in_progress', ?, ?)`,
		"req-locked", "locked", channelDir, now()); err != nil {
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
	if len(rolled) != 0 {
		t.Fatalf("rolled = %+v, want none for locked local channel", rolled)
	}
	if _, err := os.Stat(channelDir); err != nil {
		t.Fatalf("workdir should remain: %v", err)
	}
	var status string
	_ = daemonDB.QueryRowContext(ctx, `SELECT status FROM bootstrap_registry WHERE create_request_id=?`, "req-locked").Scan(&status)
	if status != "completed" {
		t.Errorf("status = %q want completed", status)
	}
}

func TestReconciler_RollsBackCompletedWithoutChannelLock(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	daemonDB, _ := store.OpenDaemon(ctx, filepath.Join(root, "daemon.sqlite"), store.OpenOptions{})
	defer func() { _ = daemonDB.Close() }()

	channelDir := filepath.Join(root, "channels", "completed-no-lock")
	if err := os.MkdirAll(channelDir, 0o755); err != nil {
		t.Fatal(err)
	}
	chDB, err := store.OpenChannel(ctx, filepath.Join(channelDir, "channel.sqlite"), store.OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	_ = chDB.Close()
	if _, err := daemonDB.ExecContext(ctx, `INSERT INTO bootstrap_registry
		(create_request_id, channel_id, status, workdir_path, started_at, completed_at)
		VALUES (?, ?, 'completed', ?, ?, ?)`,
		"req-completed-no-lock", "completed-no-lock", channelDir, now(), now()); err != nil {
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
	if len(rolled) != 1 || rolled[0].ChannelID != "completed-no-lock" {
		t.Fatalf("rolled = %+v", rolled)
	}
	if _, err := os.Stat(channelDir); !os.IsNotExist(err) {
		t.Errorf("completed/no-lock workdir should be removed: %v", err)
	}
	var status string
	_ = daemonDB.QueryRowContext(ctx, `SELECT status FROM bootstrap_registry WHERE create_request_id=?`, "req-completed-no-lock").Scan(&status)
	if status != "rolled_back" {
		t.Errorf("status = %q", status)
	}
}

func TestReconcile_LockWrittenButEventMissing_EnsuresEventBeforeCompleted(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	daemonDB, _ := store.OpenDaemon(ctx, filepath.Join(root, "daemon.sqlite"), store.OpenOptions{})
	defer func() { _ = daemonDB.Close() }()

	channelDir, chDB := createLockedChannel(t, ctx, root, "lock-event-missing")
	_ = chDB.Close()
	if _, err := daemonDB.ExecContext(ctx, `INSERT INTO bootstrap_registry
		(create_request_id, channel_id, status, phase, workdir_path, started_at)
		VALUES (?, ?, 'in_progress', 'partial_takeover', ?, ?)`,
		"req-lock-event-missing", "lock-event-missing", channelDir, now()); err != nil {
		t.Fatal(err)
	}

	rec, err := bootstrap.NewReconciler(daemonDB, now)
	if err != nil {
		t.Fatal(err)
	}
	called := false
	rec.SetEnsureSystemChannelCreatedEvent(func(ctx context.Context, channelID channel.ID, workdirPath string) error {
		called = true
		var status string
		if err := daemonDB.QueryRowContext(ctx, `SELECT status FROM bootstrap_registry WHERE create_request_id=?`,
			"req-lock-event-missing").Scan(&status); err != nil {
			return err
		}
		if status != "in_progress" {
			t.Fatalf("ensure hook ran after completion: status=%q", status)
		}
		return appendSystemChannelCreated(ctx, workdirPath, channelID)
	})

	rolled, err := rec.Run(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(rolled) != 0 {
		t.Fatalf("rolled=%+v want none", rolled)
	}
	if !called {
		t.Fatal("ensure hook was not called")
	}
	var status, phase string
	if err := daemonDB.QueryRowContext(ctx, `SELECT status, phase FROM bootstrap_registry WHERE create_request_id=?`,
		"req-lock-event-missing").Scan(&status, &phase); err != nil {
		t.Fatal(err)
	}
	if status != "completed" || phase != "completed" {
		t.Fatalf("registry status=%q phase=%q want completed/completed", status, phase)
	}
	if count := countCreatedEvents(t, ctx, channelDir); count != 1 {
		t.Fatalf("system.channel.created count=%d want 1", count)
	}
}

func TestReconcile_IdempotentEmit_NoOpIfEventAlreadyEmitted(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	daemonDB, _ := store.OpenDaemon(ctx, filepath.Join(root, "daemon.sqlite"), store.OpenOptions{})
	defer func() { _ = daemonDB.Close() }()

	channelDir, chDB := createLockedChannel(t, ctx, root, "lock-event-present")
	_ = chDB.Close()
	if err := appendSystemChannelCreated(ctx, channelDir, "lock-event-present"); err != nil {
		t.Fatal(err)
	}
	if _, err := daemonDB.ExecContext(ctx, `INSERT INTO bootstrap_registry
		(create_request_id, channel_id, status, phase, workdir_path, started_at)
		VALUES (?, ?, 'in_progress', 'partial_takeover', ?, ?)`,
		"req-lock-event-present", "lock-event-present", channelDir, now()); err != nil {
		t.Fatal(err)
	}

	rec, err := bootstrap.NewReconciler(daemonDB, now)
	if err != nil {
		t.Fatal(err)
	}
	rec.SetEnsureSystemChannelCreatedEvent(func(ctx context.Context, channelID channel.ID, workdirPath string) error {
		return appendSystemChannelCreated(ctx, workdirPath, channelID)
	})
	if _, err := rec.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if count := countCreatedEvents(t, ctx, channelDir); count != 1 {
		t.Fatalf("system.channel.created count=%d want idempotent single row", count)
	}
}

func createLockedChannel(t *testing.T, ctx context.Context, root, id string) (string, *sql.DB) {
	t.Helper()
	channelDir := filepath.Join(root, "channels", id)
	if err := os.MkdirAll(channelDir, 0o755); err != nil {
		t.Fatal(err)
	}
	chDB, err := store.OpenChannel(ctx, filepath.Join(channelDir, "channel.sqlite"), store.OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	token, err := placement.NewFencingToken()
	if err != nil {
		t.Fatal(err)
	}
	lock := store.NewChannelLock(chDB)
	if err := lock.Insert(ctx, store.ChannelLockRow{
		ChannelID:    channel.ID(id),
		FencingToken: token,
		OwnerEpoch:   1,
		DaemonID:     "daemon-A",
		DaemonEpoch:  1,
		AcquiredAt:   now(),
		RefreshedAt:  now(),
	}); err != nil {
		t.Fatal(err)
	}
	return channelDir, chDB
}

func appendSystemChannelCreated(ctx context.Context, workdirPath string, channelID channel.ID) error {
	sqlitePath := filepath.Join(workdirPath, "channel.sqlite")
	db, err := store.OpenChannel(ctx, sqlitePath, store.OpenOptions{SkipDDL: true})
	if err != nil {
		return err
	}
	defer func() { _ = db.Close() }()
	lock := store.NewChannelLock(db)
	row, ok, err := lock.Get(ctx)
	if err != nil {
		return err
	}
	if !ok {
		return os.ErrNotExist
	}
	payload, err := json.Marshal(map[string]any{
		"channel_id":  channelID,
		"daemon_id":   row.DaemonID,
		"owner_epoch": row.OwnerEpoch,
		"created_at":  now(),
	})
	if err != nil {
		return err
	}
	env := &message.Envelope{
		ID:         message.ID("system.channel.created:" + string(channelID)),
		TS:         now(),
		TSReceived: now(),
		ChannelID:  channelID,
		Sender:     message.Sender{Kind: actor.KindSystem, ID: actor.SystemActorID},
		Kind:       message.KindEvent,
		Type:       "system.channel.created",
		Payload:    payload,
		Visibility: message.VisibilitySystem,
		Audience:   message.Audience{message.AudienceWildcard},
	}
	if env.CanonicalHash, err = message.CanonicalHash(*env); err != nil {
		return err
	}
	res, err := store.NewMessagesWithLock(db, lock).Append(ctx, env, khlog.FencingTuple{
		Token: row.FencingToken,
		Epoch: row.DaemonEpoch,
	})
	if err != nil {
		return err
	}
	if res.Seq != 1 {
		return os.ErrInvalid
	}
	return nil
}

func countCreatedEvents(t *testing.T, ctx context.Context, workdirPath string) int {
	t.Helper()
	db, err := store.OpenChannel(ctx, filepath.Join(workdirPath, "channel.sqlite"), store.OpenOptions{SkipDDL: true})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	var count int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM messages WHERE type='system.channel.created'`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	return count
}
