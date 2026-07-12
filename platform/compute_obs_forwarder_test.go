package platform

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/runtime/actorrt"
)

// safeLogBuffer is a mutex-guarded sink so the test goroutine can read the log
// while pump's goroutine writes it.
type safeLogBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *safeLogBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *safeLogBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

// TestCellObsForwarder_DropAccountAndWarn is the #6 regression: a saturated
// obs-forward queue no longer drops silently — every overflow bumps an atomic
// account (asserted exactly), and pump's periodic tick surfaces it as a Warn
// (never logged on the OnObs hot path). Filling the queue past its bound with
// no drainer makes the drop count deterministic (n − queue capacity).
func TestCellObsForwarder_DropAccountAndWarn(t *testing.T) {
	var logs safeLogBuffer
	f := newCellObsForwarder(slog.New(slog.NewTextHandler(&logs, nil)))
	f.reportEvery = 5 * time.Millisecond

	const n = obsForwardQueue + 100
	for i := 0; i < n; i++ {
		f.OnObs(context.Background(), actor.ActorID("x"), actorrt.Incarnation{}, actorrt.ObsKind("k"), nil)
	}
	if got, want := f.dropped.Load(), uint64(n-obsForwardQueue); got != want {
		t.Fatalf("dropped = %d, want %d (queue cap %d)", got, want, obsForwardQueue)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { f.pump(ctx); close(done) }()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if strings.Contains(logs.String(), "obs_forward_dropped") {
			break
		}
		time.Sleep(2 * time.Millisecond)
	}
	cancel()
	<-done

	text := logs.String()
	if !strings.Contains(text, "obs_forward_dropped") {
		t.Fatalf("pump never surfaced the drop account: %q", text)
	}
	if !strings.Contains(text, "dropped=100") {
		t.Fatalf("Warn missing the drop count: %q", text)
	}
}
