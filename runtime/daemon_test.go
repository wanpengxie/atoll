package runtime_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/coagent-ai/coagent/kernel/channel"
	"github.com/coagent-ai/coagent/kernel/placement"
	"github.com/coagent-ai/coagent/runtime"
	"github.com/coagent-ai/coagent/runtime/lifecycle"
	"github.com/coagent-ai/coagent/runtime/store"
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
