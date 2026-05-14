package lifecycle_test

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/wanpengxie/ActOS/kernel/channel"
	"github.com/wanpengxie/ActOS/kernel/daemonbus"
	"github.com/wanpengxie/ActOS/kernel/placement"
	"github.com/wanpengxie/ActOS/runtime/lifecycle"
	"github.com/wanpengxie/ActOS/runtime/store"
)

// stubBootstrapper: opens the channel sqlite via store.OpenChannel and
// returns the path. crashAfter controls whether to inject a crash AFTER
// bootstrap but BEFORE ACK emission (failure injection #1) or AFTER ACK
// (failure injection #2 — modeled by the caller's emitAck stub).
type stubBootstrapper struct {
	root    string
	openErr error
}

func (s *stubBootstrapper) Bootstrap(ctx context.Context, id channel.ID, _ placement.CreateChannelRequest) (string, error) {
	if s.openErr != nil {
		return "", s.openErr
	}
	dir := filepath.Join(s.root, string(id))
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", err
	}
	dbPath := filepath.Join(dir, "channel.sqlite")
	db, err := store.OpenChannel(ctx, dbPath, store.OpenOptions{})
	if err != nil {
		return "", err
	}
	_ = db.Close()
	return dbPath, nil
}

type lockOpener struct {
	dbs map[string]*sql.DB
}

func newLockOpener() *lockOpener { return &lockOpener{dbs: map[string]*sql.DB{}} }

func (o *lockOpener) Open(ctx context.Context, path string) (*store.ChannelLock, error) {
	if db, ok := o.dbs[path]; ok {
		return store.NewChannelLock(db), nil
	}
	if _, err := os.Stat(path); err != nil {
		return nil, err
	}
	db, err := store.OpenChannel(ctx, path, store.OpenOptions{SkipDDL: true})
	if err != nil {
		return nil, err
	}
	o.dbs[path] = db
	return store.NewChannelLock(db), nil
}

func (o *lockOpener) Close() {
	for _, db := range o.dbs {
		_ = db.Close()
	}
}

func frameIDGen() func() string {
	var n atomic.Int64
	return func() string {
		i := n.Add(1)
		return "frame-" + atoi64(i)
	}
}

func atoi64(i int64) string {
	if i == 0 {
		return "0"
	}
	var buf [20]byte
	idx := len(buf)
	for i > 0 {
		idx--
		buf[idx] = byte('0' + i%10)
		i /= 10
	}
	return string(buf[idx:])
}

func now() int64 { return time.Now().UnixMilli() }

// TestCreate_FreshBootstrap covers acceptance gate #4 (T3):
//
//	mock server -> daemon.create_channel -> daemon ACK contains complete
//	field set -> mock server CAS active.
func TestCreate_FreshBootstrap(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	root := t.TempDir()
	channelsDir := filepath.Join(root, "channels")
	bootstrapper := &stubBootstrapper{root: channelsDir}
	opener := newLockOpener()
	defer opener.Close()

	var acks []placement.CreateChannelAck
	creator, err := lifecycle.NewCreator(lifecycle.CreatorConfig{
		DaemonID:     placement.DaemonID("daemon-A"),
		DaemonEpoch:  placement.DaemonEpoch(7),
		NowFn:        now,
		ChannelsDir:  channelsDir,
		Bootstrapper: bootstrapper,
		LockOpener:   opener.Open,
		FrameIDGen:   frameIDGen(),
		EmitAck: func(ctx context.Context, ack placement.CreateChannelAck) error {
			acks = append(acks, ack)
			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	req := placement.CreateChannelRequest{
		ChannelID:       channel.ID("ch-1"),
		CreateRequestID: placement.CreateRequestID("req-001"),
		OwnerEpoch:      placement.OwnerEpoch(1),
		FencingToken:    placement.FencingToken(1),
	}
	frame := daemonbus.Frame{
		FrameID:   "f-1",
		FrameType: daemonbus.FrameTypeControlCreateChannel,
	}
	if err := creator.HandleCreate(ctx, frame, req); err != nil {
		t.Fatalf("HandleCreate: %v", err)
	}

	if len(acks) != 1 {
		t.Fatalf("expected 1 ACK, got %d", len(acks))
	}
	ack := acks[0]
	// All 5 match-fields populated:
	if ack.FrameID != "f-1" {
		t.Errorf("ACK frame_id = %q", ack.FrameID)
	}
	if ack.ChannelID != "ch-1" {
		t.Errorf("ACK channel_id = %q", ack.ChannelID)
	}
	if ack.CreateRequestID != "req-001" {
		t.Errorf("ACK create_request_id = %q", ack.CreateRequestID)
	}
	if ack.OwnerEpoch != 1 || ack.FencingToken != 1 {
		t.Errorf("ACK owner/fencing = %d/%d", ack.OwnerEpoch, ack.FencingToken)
	}
	if ack.DaemonID != "daemon-A" || ack.DaemonEpoch != 7 {
		t.Errorf("ACK daemon = %s/%d", ack.DaemonID, ack.DaemonEpoch)
	}
	if ack.Status != placement.AckBound {
		t.Errorf("ACK status = %s", ack.Status)
	}

	// Mock-server-side CAS uses kernel/placement.CreateChannelAck.Match.
	serverRecord := placement.Placement{
		ChannelID:       "ch-1",
		DaemonID:        "daemon-A",
		State:           placement.StateCreating,
		OwnerEpoch:      1,
		FencingToken:    1,
		CreateRequestID: "req-001",
	}
	if !ack.Match(serverRecord) {
		t.Error("ACK should match server placement record (4-field match)")
	}

	// Local channel_lock row should now exist.
	sqlitePath := filepath.Join(channelsDir, "ch-1", "channel.sqlite")
	lock, _ := opener.Open(ctx, sqlitePath)
	row, ok, _ := lock.Get(ctx)
	if !ok {
		t.Fatal("channel_lock row missing after bootstrap")
	}
	if row.FencingToken != 1 || row.DaemonEpoch != 7 {
		t.Errorf("local lock row = %+v", row)
	}
}

// TestCreate_FailureBeforeACK covers acceptance gate #4 crash injection:
//
//	daemon crashes BEFORE emitting the ACK. The local channel_lock IS
//	persisted but the server never gets ACK → server times out and
//	transitions to orphan. We model "crash before ACK" by injecting an
//	error in EmitAck and verifying the lock row exists.
func TestCreate_FailureBeforeACK(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	root := t.TempDir()
	channelsDir := filepath.Join(root, "channels")
	bootstrapper := &stubBootstrapper{root: channelsDir}
	opener := newLockOpener()
	defer opener.Close()

	creator, _ := lifecycle.NewCreator(lifecycle.CreatorConfig{
		DaemonID:     placement.DaemonID("daemon-A"),
		DaemonEpoch:  placement.DaemonEpoch(1),
		NowFn:        now,
		ChannelsDir:  channelsDir,
		Bootstrapper: bootstrapper,
		LockOpener:   opener.Open,
		FrameIDGen:   frameIDGen(),
		EmitAck: func(ctx context.Context, ack placement.CreateChannelAck) error {
			return errors.New("crash: network down before ACK")
		},
	})

	req := placement.CreateChannelRequest{
		ChannelID:       channel.ID("ch-orphan"),
		CreateRequestID: placement.CreateRequestID("req-002"),
		OwnerEpoch:      placement.OwnerEpoch(1),
		FencingToken:    placement.FencingToken(1),
	}
	frame := daemonbus.Frame{FrameID: "f-2", FrameType: daemonbus.FrameTypeControlCreateChannel}

	err := creator.HandleCreate(ctx, frame, req)
	if err == nil {
		t.Error("expected emit error to propagate")
	}

	// Local lock row should still exist (bootstrap done before ACK emit).
	sqlitePath := filepath.Join(channelsDir, "ch-orphan", "channel.sqlite")
	lock, _ := opener.Open(ctx, sqlitePath)
	_, ok, _ := lock.Get(ctx)
	if !ok {
		t.Error("expected local lock to persist even on ACK emit failure (orphan recovery path)")
	}
}

// TestCreate_IdempotentReplay tests branch 3: same request twice → same ACK.
func TestCreate_IdempotentReplay(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	root := t.TempDir()
	channelsDir := filepath.Join(root, "channels")
	bootstrapper := &stubBootstrapper{root: channelsDir}
	opener := newLockOpener()
	defer opener.Close()

	var acks []placement.CreateChannelAck
	creator, _ := lifecycle.NewCreator(lifecycle.CreatorConfig{
		DaemonID:     placement.DaemonID("daemon-A"),
		DaemonEpoch:  placement.DaemonEpoch(1),
		NowFn:        now,
		ChannelsDir:  channelsDir,
		Bootstrapper: bootstrapper,
		LockOpener:   opener.Open,
		FrameIDGen:   frameIDGen(),
		EmitAck:      func(ctx context.Context, ack placement.CreateChannelAck) error { acks = append(acks, ack); return nil },
	})

	req := placement.CreateChannelRequest{
		ChannelID:       channel.ID("ch-idem"),
		CreateRequestID: placement.CreateRequestID("req-idem"),
		OwnerEpoch:      placement.OwnerEpoch(1),
		FencingToken:    placement.FencingToken(1),
	}
	frame := daemonbus.Frame{FrameID: "f-3", FrameType: daemonbus.FrameTypeControlCreateChannel}

	for i := 0; i < 2; i++ {
		if err := creator.HandleCreate(ctx, frame, req); err != nil {
			t.Fatalf("call %d: %v", i, err)
		}
	}
	if len(acks) != 2 {
		t.Fatalf("expected 2 acks, got %d", len(acks))
	}
	if acks[0].Status != placement.AckBound || acks[1].Status != placement.AckBound {
		t.Errorf("both acks should be bound: %s / %s", acks[0].Status, acks[1].Status)
	}
}

// TestCreate_StaleRequestRejected tests branch 4: local fencing newer than request.
func TestCreate_StaleRequestRejected(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	root := t.TempDir()
	channelsDir := filepath.Join(root, "channels")
	bootstrapper := &stubBootstrapper{root: channelsDir}
	opener := newLockOpener()
	defer opener.Close()

	var acks []placement.CreateChannelAck
	creator, _ := lifecycle.NewCreator(lifecycle.CreatorConfig{
		DaemonID:     placement.DaemonID("daemon-A"),
		DaemonEpoch:  placement.DaemonEpoch(1),
		NowFn:        now,
		ChannelsDir:  channelsDir,
		Bootstrapper: bootstrapper,
		LockOpener:   opener.Open,
		FrameIDGen:   frameIDGen(),
		EmitAck:      func(ctx context.Context, ack placement.CreateChannelAck) error { acks = append(acks, ack); return nil },
	})

	// First create with FencingToken=5.
	frame := daemonbus.Frame{FrameID: "f-4", FrameType: daemonbus.FrameTypeControlCreateChannel}
	_ = creator.HandleCreate(ctx, frame, placement.CreateChannelRequest{
		ChannelID: "ch-stale", CreateRequestID: "req", OwnerEpoch: 5, FencingToken: 5,
	})
	if acks[0].Status != placement.AckBound {
		t.Fatalf("first create should bind, got %s", acks[0].Status)
	}

	// Second create with older FencingToken=3.
	_ = creator.HandleCreate(ctx, frame, placement.CreateChannelRequest{
		ChannelID: "ch-stale", CreateRequestID: "req", OwnerEpoch: 3, FencingToken: 3,
	})
	if acks[1].Status != placement.AckRejected {
		t.Errorf("stale request should be rejected, got %s", acks[1].Status)
	}
}

// TestFencingChecker covers fencing.Validate happy + sad path.
func TestFencingChecker(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	dir := t.TempDir()
	dbPath := filepath.Join(dir, "ch.sqlite")
	db, _ := store.OpenChannel(ctx, dbPath, store.OpenOptions{})
	defer func() { _ = db.Close() }()
	lock := store.NewChannelLock(db)
	_ = lock.Insert(ctx, store.ChannelLockRow{
		ChannelID:    "ch-1",
		FencingToken: 1, OwnerEpoch: 1,
		DaemonID: "daemon-A", DaemonEpoch: 1,
		AcquiredAt: now(), RefreshedAt: now(),
	})

	chk, err := lifecycle.NewFencingChecker(lock)
	if err != nil {
		t.Fatal(err)
	}
	if err := chk.Validate(ctx, 1, 1); err != nil {
		t.Errorf("should pass: %v", err)
	}
	var mismatch *lifecycle.FenceMismatchError
	if err := chk.Validate(ctx, 1, 2); !errors.As(err, &mismatch) {
		t.Errorf("expected FenceMismatchError, got %v", err)
	}

	// After RefreshDaemon, the new daemon_epoch is required.
	_ = lock.RefreshDaemon(ctx, 99, now())
	if err := chk.Validate(ctx, 1, 1); !errors.As(err, &mismatch) {
		t.Errorf("after refresh old epoch should fail, got %v", err)
	}
	if err := chk.Validate(ctx, 1, 99); err != nil {
		t.Errorf("post-refresh validate should pass: %v", err)
	}
}

// TestBoot_LoadLocal verifies LoadLocal scans channels/ + refreshes
// daemon_epoch on each lock row.
func TestBoot_LoadLocal(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	root := t.TempDir()
	channelsDir := filepath.Join(root, "channels")

	// Seed two channels with locks owned by daemon-A.
	for _, id := range []string{"ch-1", "ch-2"} {
		dir := filepath.Join(channelsDir, id)
		_ = os.MkdirAll(dir, 0o755)
		dbPath := filepath.Join(dir, "channel.sqlite")
		db, err := store.OpenChannel(ctx, dbPath, store.OpenOptions{})
		if err != nil {
			t.Fatal(err)
		}
		lock := store.NewChannelLock(db)
		_ = lock.Insert(ctx, store.ChannelLockRow{
			ChannelID:    channel.ID(id),
			FencingToken: 1, OwnerEpoch: 1,
			DaemonID: "daemon-A", DaemonEpoch: 1,
			AcquiredAt: now(), RefreshedAt: now(),
		})
		_ = db.Close()
	}

	opener := newLockOpener()
	defer opener.Close()

	boot, err := lifecycle.NewBootstrapper(lifecycle.BootConfig{
		DaemonID:    "daemon-A",
		DaemonEpoch: placement.DaemonEpoch(42), // fresh daemon process
		NowFn:       now,
		ChannelsDir: channelsDir,
		LockOpener:  opener.Open,
	})
	if err != nil {
		t.Fatal(err)
	}
	if boot.Phase() != lifecycle.PhaseUnstarted {
		t.Errorf("initial phase = %s", boot.Phase())
	}

	local, err := boot.LoadLocal(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(local) != 2 {
		t.Fatalf("expected 2 local channels, got %d", len(local))
	}
	for _, c := range local {
		if !c.OwnedByUs {
			t.Errorf("%s should be owned by us", c.ChannelID)
		}
		if c.Lock.DaemonEpoch != 42 {
			t.Errorf("%s lock daemon_epoch = %d (expected 42 after refresh)", c.ChannelID, c.Lock.DaemonEpoch)
		}
	}
	if boot.Phase() != lifecycle.PhaseLoadingLocal {
		t.Errorf("phase after LoadLocal = %s", boot.Phase())
	}

	// Phase 2 (no EmitReclaim → offline path).
	res, err := boot.ReportReclaim(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(res.ReclaimAccepted) != 2 {
		t.Errorf("offline reclaim should accept all owned: %v", res.ReclaimAccepted)
	}

	boot.MarkRecovering()
	if boot.Phase() != lifecycle.PhaseRecovering {
		t.Errorf("phase = %s", boot.Phase())
	}
	boot.MarkAcceptingNew()
	if boot.Phase() != lifecycle.PhaseAcceptingNew {
		t.Errorf("phase = %s", boot.Phase())
	}
}

// TestUnloader exercises Register + Unload ordering.
func TestUnloader(t *testing.T) {
	u := lifecycle.NewUnloader()
	order := []string{}
	u.Register("ch-1", func() error { order = append(order, "a"); return nil })
	u.Register("ch-1", func() error { order = append(order, "b"); return nil })
	if err := u.Unload(context.Background(), "ch-1", lifecycle.UnloadIdle); err != nil {
		t.Fatal(err)
	}
	if len(order) != 2 || order[0] != "b" || order[1] != "a" {
		t.Errorf("LIFO order expected, got %v", order)
	}
	// Re-unload is a no-op.
	if err := u.Unload(context.Background(), "ch-1", lifecycle.UnloadIdle); err != nil {
		t.Errorf("idempotent unload err: %v", err)
	}
}
