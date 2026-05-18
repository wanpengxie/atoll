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

type completingBootstrapper struct {
	stubBootstrapper
	completed []string
}

func (s *completingBootstrapper) Complete(_ context.Context, createRequestID string) error {
	s.completed = append(s.completed, createRequestID)
	return nil
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

func TestCreate_CompletesBootstrapAfterLockInsert(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	root := t.TempDir()
	channelsDir := filepath.Join(root, "channels")
	bootstrapper := &completingBootstrapper{
		stubBootstrapper: stubBootstrapper{root: channelsDir},
	}
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
		ChannelID:       channel.ID("ch-complete"),
		CreateRequestID: placement.CreateRequestID("req-complete"),
		OwnerEpoch:      placement.OwnerEpoch(1),
		FencingToken:    placement.FencingToken(1),
	}
	if err := creator.HandleCreate(ctx, daemonbus.Frame{FrameID: "f-complete"}, req); err != nil {
		t.Fatalf("HandleCreate: %v", err)
	}
	if len(acks) != 1 || acks[0].Status != placement.AckBound {
		t.Fatalf("acks=%+v want one bound ack", acks)
	}
	if len(bootstrapper.completed) != 1 || bootstrapper.completed[0] != "req-complete" {
		t.Fatalf("completed=%v want [req-complete]", bootstrapper.completed)
	}
	sqlitePath := filepath.Join(channelsDir, "ch-complete", "channel.sqlite")
	lock, _ := opener.Open(ctx, sqlitePath)
	if _, ok, _ := lock.Get(ctx); !ok {
		t.Fatal("channel_lock row missing")
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

// TestCreate_HigherTokenRejected is the FIX-T4 regression for branch
// 2: when the daemon already has a local channel_lock row, a create
// request bringing a HIGHER fencing_token MUST be rejected (not
// silently UpgradeEpoch'd). The placement state machine is owned by
// the server — daemon must let the server reconcile loop drive the
// row through orphan → creating before producing the new ACK.
func TestCreate_HigherTokenRejected(t *testing.T) {
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

	// Phase 1: fresh bootstrap at token=3.
	frame := daemonbus.Frame{FrameID: "f-h1", FrameType: daemonbus.FrameTypeControlCreateChannel}
	if err := creator.HandleCreate(ctx, frame, placement.CreateChannelRequest{
		ChannelID: "ch-higher", CreateRequestID: "req-A", OwnerEpoch: 3, FencingToken: 3,
	}); err != nil {
		t.Fatalf("first create: %v", err)
	}
	if acks[0].Status != placement.AckBound {
		t.Fatalf("first create want bound, got %s", acks[0].Status)
	}

	// Phase 2: server reissues create with token=7 (would be a reclaim
	// upgrade). Daemon MUST reject — server reconcile must drive the
	// state machine, daemon never silently upgrades.
	if err := creator.HandleCreate(ctx, frame, placement.CreateChannelRequest{
		ChannelID: "ch-higher", CreateRequestID: "req-B", OwnerEpoch: 7, FencingToken: 7,
	}); err != nil {
		t.Fatalf("second create: %v", err)
	}
	if acks[1].Status != placement.AckRejected {
		t.Fatalf("higher-token create should be rejected, got %s", acks[1].Status)
	}
	if acks[1].Reason != "local_lock_stale_higher_token_received" {
		t.Errorf("reject reason=%q want local_lock_stale_higher_token_received", acks[1].Reason)
	}

	// Local lock row MUST remain at token=3 (no silent upgrade).
	sqlitePath := filepath.Join(channelsDir, "ch-higher", "channel.sqlite")
	lock, _ := opener.Open(ctx, sqlitePath)
	row, ok, _ := lock.Get(ctx)
	if !ok {
		t.Fatal("lock missing")
	}
	if row.FencingToken != 3 {
		t.Errorf("lock fencing_token=%d want 3 (no upgrade)", row.FencingToken)
	}
}

// TestCreate_EqualTokenOwnerEpochMismatch covers branch 3 tightening:
// fencing_token matches but owner_epoch differs → reject. (In practice
// fencing_token == owner_epoch by spec invariant, but a malformed
// server payload that violates the invariant must NOT silently bind.)
func TestCreate_EqualTokenOwnerEpochMismatch(t *testing.T) {
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

	frame := daemonbus.Frame{FrameID: "f-eq", FrameType: daemonbus.FrameTypeControlCreateChannel}
	// Bootstrap at (owner=5, token=5).
	if err := creator.HandleCreate(ctx, frame, placement.CreateChannelRequest{
		ChannelID: "ch-eq", CreateRequestID: "req", OwnerEpoch: 5, FencingToken: 5,
	}); err != nil {
		t.Fatalf("first: %v", err)
	}
	if acks[0].Status != placement.AckBound {
		t.Fatalf("first want bound got %s", acks[0].Status)
	}

	// Replay with same fencing_token but different owner_epoch → reject.
	if err := creator.HandleCreate(ctx, frame, placement.CreateChannelRequest{
		ChannelID: "ch-eq", CreateRequestID: "req", OwnerEpoch: 9, FencingToken: 5,
	}); err != nil {
		t.Fatalf("second: %v", err)
	}
	if acks[1].Status != placement.AckRejected {
		t.Errorf("owner_epoch mismatch should reject, got %s", acks[1].Status)
	}
	if acks[1].Reason != "owner_epoch_mismatch" {
		t.Errorf("reason=%q want owner_epoch_mismatch", acks[1].Reason)
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

// TestBoot_LoadOne covers the M1.6-T0.1.1 hot-path entry point —
// after saga.Bootstrap + channel_lock insert, LoadOne mounts the
// channel into the bootstrapper's in-memory view, refreshes
// daemon_epoch, and returns a LocalChannel. Unload removes it again.
func TestBoot_LoadOne(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	root := t.TempDir()
	channelsDir := filepath.Join(root, "channels")
	opener := newLockOpener()
	defer opener.Close()

	boot, err := lifecycle.NewBootstrapper(lifecycle.BootConfig{
		DaemonID:    "daemon-X",
		DaemonEpoch: placement.DaemonEpoch(11),
		NowFn:       now,
		ChannelsDir: channelsDir,
		LockOpener:  opener.Open,
	})
	if err != nil {
		t.Fatal(err)
	}

	// Seed a channel with an existing channel_lock owned by daemon-X.
	chID := channel.ID("ch-hot")
	dir := filepath.Join(channelsDir, string(chID))
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	dbPath := filepath.Join(dir, "channel.sqlite")
	db, err := store.OpenChannel(ctx, dbPath, store.OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.NewChannelLock(db).Insert(ctx, store.ChannelLockRow{
		ChannelID:    chID,
		FencingToken: 5, OwnerEpoch: 5,
		DaemonID: "daemon-X", DaemonEpoch: 1,
		AcquiredAt: now(), RefreshedAt: now(),
	}); err != nil {
		t.Fatal(err)
	}
	_ = db.Close()

	lc, err := boot.LoadOne(ctx, chID, dbPath)
	if err != nil {
		t.Fatalf("LoadOne: %v", err)
	}
	if lc.ChannelID != chID || lc.SQLitePath != dbPath || !lc.OwnedByUs {
		t.Errorf("LocalChannel = %+v", lc)
	}
	if lc.Lock.DaemonEpoch != 11 {
		t.Errorf("daemon_epoch not refreshed: %d", lc.Lock.DaemonEpoch)
	}

	// LoadOne should reject a channel whose lock row belongs to a
	// different daemon.
	otherDB := filepath.Join(channelsDir, "ch-other", "channel.sqlite")
	if err := os.MkdirAll(filepath.Dir(otherDB), 0o755); err != nil {
		t.Fatal(err)
	}
	odb, _ := store.OpenChannel(ctx, otherDB, store.OpenOptions{})
	if err := store.NewChannelLock(odb).Insert(ctx, store.ChannelLockRow{
		ChannelID:    "ch-other",
		FencingToken: 1, OwnerEpoch: 1,
		DaemonID: "daemon-Y", DaemonEpoch: 1,
		AcquiredAt: now(), RefreshedAt: now(),
	}); err != nil {
		t.Fatal(err)
	}
	_ = odb.Close()
	if _, err := boot.LoadOne(ctx, "ch-other", otherDB); err == nil {
		t.Error("LoadOne should reject channel owned by another daemon")
	}

	// Missing channel_lock row → ErrChannelUnbound.
	missingDB := filepath.Join(channelsDir, "ch-empty", "channel.sqlite")
	if err := os.MkdirAll(filepath.Dir(missingDB), 0o755); err != nil {
		t.Fatal(err)
	}
	mdb, _ := store.OpenChannel(ctx, missingDB, store.OpenOptions{})
	_ = mdb.Close()
	if _, err := boot.LoadOne(ctx, "ch-empty", missingDB); !errors.Is(err, lifecycle.ErrChannelUnbound) {
		t.Errorf("missing lock want ErrChannelUnbound, got %v", err)
	}

	// Unload drops the entry.
	boot.Unload(chID)
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
