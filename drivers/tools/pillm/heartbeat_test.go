package pillm

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

func TestHeartbeatExistsWithoutProviderTokensAndJoinsBeforeTerminal(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var count atomic.Int64
	ticked := make(chan struct{}, 1)
	done := heartbeat(ctx, time.Millisecond, func() {
		count.Add(1)
		select {
		case ticked <- struct{}{}:
		default:
		}
	})
	select {
	case <-ticked:
	case <-time.After(time.Second):
		t.Fatal("no liveness while provider silent")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("heartbeat did not join")
	}
	n := count.Load()
	time.Sleep(5 * time.Millisecond)
	if count.Load() != n {
		t.Fatal("heartbeat after terminal join")
	}
}
