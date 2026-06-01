package lifecycle_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/wanpengxie/ActOS/framework/multiuser/placement"
	"github.com/wanpengxie/ActOS/framework/multiuser/runtime/lifecycle"
	multistore "github.com/wanpengxie/ActOS/framework/multiuser/runtime/store"
	"github.com/wanpengxie/ActOS/kernel/channel"
	"github.com/wanpengxie/ActOS/runtime/store"
)

type lockOpener struct {
	dbs map[string]*sql.DB
}

func newLockOpener() *lockOpener { return &lockOpener{dbs: map[string]*sql.DB{}} }

func (o *lockOpener) Open(ctx context.Context, path string) (*multistore.ChannelLock, error) {
	if db, ok := o.dbs[path]; ok {
		return multistore.NewChannelLock(db), nil
	}
	if _, err := os.Stat(path); err != nil {
		return nil, err
	}
	db, err := store.OpenChannel(ctx, path, store.OpenOptions{SkipDDL: true})
	if err != nil {
		return nil, err
	}
	o.dbs[path] = db
	return multistore.NewChannelLock(db), nil
}

func (o *lockOpener) Close() {
	for _, db := range o.dbs {
		_ = db.Close()
	}
}

func now() int64 { return time.Now().UnixMilli() }

// TestFencingChecker covers fencing.Validate happy + sad path.
func TestFencingChecker(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	dir := t.TempDir()
	dbPath := filepath.Join(dir, "ch.sqlite")
	db, _ := store.OpenChannel(ctx, dbPath, store.OpenOptions{})
	defer func() { _ = db.Close() }()
	lock := multistore.NewChannelLock(db)
	_ = lock.Insert(ctx, multistore.ChannelLockRow{
		ChannelID:    "ch-1",
		FencingToken: "tok-1", OwnerEpoch: 1,
		DaemonID: "daemon-A", DaemonEpoch: 1,
		AcquiredAt: now(), RefreshedAt: now(),
	})

	chk, err := lifecycle.NewFencingChecker(lock)
	if err != nil {
		t.Fatal(err)
	}
	if err := chk.Validate(ctx, "tok-1", 1); err != nil {
		t.Errorf("should pass: %v", err)
	}
	var mismatch *lifecycle.FenceMismatchError
	if err := chk.Validate(ctx, "tok-1", 2); !errors.As(err, &mismatch) {
		t.Errorf("expected FenceMismatchError, got %v", err)
	}

	// After RefreshDaemon, the new daemon_epoch is required.
	_ = lock.RefreshDaemon(ctx, 99, now())
	if err := chk.Validate(ctx, "tok-1", 1); !errors.As(err, &mismatch) {
		t.Errorf("after refresh old epoch should fail, got %v", err)
	}
	if err := chk.Validate(ctx, "tok-1", 99); err != nil {
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
		lock := multistore.NewChannelLock(db)
		_ = lock.Insert(ctx, multistore.ChannelLockRow{
			ChannelID:    channel.ID(id),
			FencingToken: "tok-1", OwnerEpoch: 1,
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

	// Phase 2 (no EmitHeldChannelsReport -> offline path).
	res, err := boot.ReportHeldChannels(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(res.HeldAccepted) != 2 {
		t.Errorf("offline held-channel report should accept all owned: %v", res.HeldAccepted)
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

func TestBoot_LoadLocalQuarantinesCorruptChannelAndContinues(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	root := t.TempDir()
	channelsDir := filepath.Join(root, "channels")

	goodDir := filepath.Join(channelsDir, "ch-good")
	if err := os.MkdirAll(goodDir, 0o755); err != nil {
		t.Fatal(err)
	}
	goodDB := filepath.Join(goodDir, "channel.sqlite")
	db, err := store.OpenChannel(ctx, goodDB, store.OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	lock := multistore.NewChannelLock(db)
	if err := lock.Insert(ctx, multistore.ChannelLockRow{
		ChannelID:    "ch-good",
		FencingToken: "tok-good", OwnerEpoch: 1,
		DaemonID: "daemon-A", DaemonEpoch: 1,
		AcquiredAt: now(), RefreshedAt: now(),
	}); err != nil {
		t.Fatal(err)
	}
	_ = db.Close()

	badDir := filepath.Join(channelsDir, "ch-bad")
	if err := os.MkdirAll(badDir, 0o755); err != nil {
		t.Fatal(err)
	}
	badDB := filepath.Join(badDir, "channel.sqlite")
	if err := os.WriteFile(badDB, []byte("not sqlite"), 0o644); err != nil {
		t.Fatal(err)
	}

	var observed []lifecycle.QuarantinedChannel
	opener := newLockOpener()
	defer opener.Close()
	boot, err := lifecycle.NewBootstrapper(lifecycle.BootConfig{
		DaemonID:    "daemon-A",
		DaemonEpoch: 42,
		NowFn:       now,
		ChannelsDir: channelsDir,
		LockOpener:  opener.Open,
		OnQuarantine: func(_ context.Context, q lifecycle.QuarantinedChannel) {
			observed = append(observed, q)
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	local, err := boot.LoadLocal(ctx)
	if err != nil {
		t.Fatalf("LoadLocal should continue past corrupt channel: %v", err)
	}
	if len(local) != 1 || local[0].ChannelID != "ch-good" {
		t.Fatalf("local=%+v want only ch-good", local)
	}
	quarantined := boot.Quarantined()
	if len(quarantined) != 1 || quarantined[0].ChannelID != "ch-bad" {
		t.Fatalf("quarantined=%+v want ch-bad", quarantined)
	}
	if len(observed) != 1 || observed[0].ChannelID != "ch-bad" {
		t.Fatalf("observed quarantine=%+v want ch-bad", observed)
	}
	if _, err := os.Stat(badDB + ".quarantine.json"); err != nil {
		t.Fatalf("quarantine marker missing: %v", err)
	}

	res, err := boot.ReportHeldChannels(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(res.HeldAccepted) != 1 || res.HeldAccepted[0] != "ch-good" {
		t.Fatalf("HeldAccepted=%v want ch-good", res.HeldAccepted)
	}
	if len(res.HeldRejected) != 1 || res.HeldRejected[0] != "ch-bad" {
		t.Fatalf("HeldRejected=%v want ch-bad", res.HeldRejected)
	}
	if len(res.Quarantined) != 1 || res.Quarantined[0].ChannelID != "ch-bad" {
		t.Fatalf("BootResult.Quarantined=%+v want ch-bad", res.Quarantined)
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
	if err := multistore.NewChannelLock(db).Insert(ctx, multistore.ChannelLockRow{
		ChannelID:    chID,
		FencingToken: "tok-5", OwnerEpoch: 5,
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
	if err := multistore.NewChannelLock(odb).Insert(ctx, multistore.ChannelLockRow{
		ChannelID:    "ch-other",
		FencingToken: "tok-1", OwnerEpoch: 1,
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

// TestUnloaderConcurrent drives Register + Unload from many goroutines so the
// race detector can flag unsynchronized closeFns access. Register (channel
// load) and Unload (shutdown drain / stale / orphan handlers) genuinely run on
// separate goroutines in the daemon.
func TestUnloaderConcurrent(t *testing.T) {
	u := lifecycle.NewUnloader()
	const goroutines = 16
	const iters = 200
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for g := 0; g < goroutines; g++ {
		go func(g int) {
			defer wg.Done()
			for i := 0; i < iters; i++ {
				chID := channel.ID(fmt.Sprintf("ch-%d", (g+i)%4))
				u.Register(chID, func() error { return nil })
				_ = u.Unload(context.Background(), chID, lifecycle.UnloadIdle)
			}
		}(g)
	}
	wg.Wait()
}
