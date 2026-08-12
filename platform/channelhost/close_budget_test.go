package channelhost

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/platform/home"
	"github.com/wanpengxie/atoll/protocol/channel"
)

func TestCloseBudgetIncludesConvergenceJoinAndCanRetryJoin(t *testing.T) {
	h := newConvergingHost(t, newDesiredRegistry())
	c := h.convergence
	c.mu.Lock()
	c.started = true // model a worker parked in a lifecycle operation
	c.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	started := time.Now()
	err := h.Close(ctx)
	if !errors.Is(err, context.DeadlineExceeded) || !strings.Contains(err.Error(), "stop convergence") {
		t.Fatalf("Close error=%v, want accounted convergence timeout", err)
	}
	if elapsed := time.Since(started); elapsed > 300*time.Millisecond {
		t.Fatalf("Close exceeded bounded join: %v", elapsed)
	}

	// Cancellation is one-shot, joining is not: a later Close gets another
	// chance to observe the same worker's completion.
	close(c.done)
	if err := h.Close(context.Background()); err != nil {
		t.Fatalf("retry Close did not rejoin convergence: %v", err)
	}
}

func TestDestroyAndClosePassCallerBudgetToHomeShutdown(t *testing.T) {
	t.Run("destroy", func(t *testing.T) {
		h := newConvergingHost(t, newDesiredRegistry())
		id := channel.ID("slow-destroy")
		h.entries[id] = &entry{home: &home.Home{}, state: stateServing}
		h.shutdown = func(_ *home.Home, ctx context.Context) error {
			<-ctx.Done()
			return ctx.Err()
		}
		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()
		started := time.Now()
		err := h.Destroy(ctx, id)
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("Destroy error=%v, want deadline", err)
		}
		if elapsed := time.Since(started); elapsed > 300*time.Millisecond {
			t.Fatalf("Destroy ignored caller budget: %v", elapsed)
		}
		h.shutdown = func(*home.Home, context.Context) error { return nil }
		if err := h.Close(context.Background()); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("close", func(t *testing.T) {
		h := newConvergingHost(t, newDesiredRegistry())
		id := channel.ID("slow-close")
		h.entries[id] = &entry{home: &home.Home{}, state: stateServing}
		h.shutdown = func(_ *home.Home, ctx context.Context) error {
			<-ctx.Done()
			return ctx.Err()
		}
		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()
		started := time.Now()
		err := h.Close(ctx)
		if !errors.Is(err, context.DeadlineExceeded) || !strings.Contains(err.Error(), string(id)) {
			t.Fatalf("Close error=%v, want per-channel deadline account", err)
		}
		if elapsed := time.Since(started); elapsed > 300*time.Millisecond {
			t.Fatalf("Close ignored caller budget: %v", elapsed)
		}
		h.shutdown = func(*home.Home, context.Context) error { return nil }
		if err := h.Close(context.Background()); err != nil {
			t.Fatalf("retry Close: %v", err)
		}
	})
}
