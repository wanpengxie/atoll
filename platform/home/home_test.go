package home_test

import (
	"context"
	"path/filepath"
	"sync"
	"testing"

	"github.com/wanpengxie/atoll/platform/home"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
)

func TestHomeCloseConcurrentCompletionAndUnpublish(t *testing.T) {
	h := openTestHome(t)
	const callers = 12
	var wg sync.WaitGroup
	errs := make(chan error, callers)
	for range callers {
		wg.Add(1)
		go func() { defer wg.Done(); errs <- home.Shutdown(h) }()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("concurrent Close: %v", err)
		}
	}
	wake, unsubscribe := home.GatewaySubscribe(h)
	unsubscribe()
	select {
	case _, ok := <-wake:
		if ok {
			t.Fatal("Subscribe after Close returned open wake")
		}
	default:
		t.Fatal("Subscribe after Close did not return an already-closed wake")
	}
}

const testChannelID = channel.ID("test-home")

// openTestHome assembles a Home for testing.
func openTestHome(t *testing.T) *home.Home {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "home.sqlite")
	h, err := home.Open(completeHomeTestConfig(home.Config{ChannelID: testChannelID, DBPath: dbPath, Bootstrap: true}))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = home.Shutdown(h) })
	return h
}

// The kernel is a constant, not a member: Open registers no system row, so the
// membership projection has no facts to hand out for it. Its addressability is
// answered by the routing organ instead.
func TestView_ActorFacts_KernelIsNotAMember(t *testing.T) {
	h := openTestHome(t)
	facts, found, err := h.View().ActorFacts(context.Background(), actor.SystemActorID)
	if err != nil {
		t.Fatalf("ActorFacts: %v", err)
	}
	if found {
		t.Errorf("kernel answered as a member: %+v", facts)
	}
}

// TestOpen_RestartOverPersistentDB verifies a second Open over the SAME db file
// (a home restart) succeeds and restores the durable membership image.
func TestOpen_RestartOverPersistentDB(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "home.sqlite")
	h1, err := home.Open(completeHomeTestConfig(home.Config{ChannelID: testChannelID, DBPath: dbPath, Bootstrap: true, BootstrapOwnerPrincipal: "restart-owner"}))
	if err != nil {
		t.Fatalf("first Open: %v", err)
	}
	if err := home.Shutdown(h1); err != nil {
		t.Fatalf("Close: %v", err)
	}
	h2, err := home.Open(completeHomeTestConfig(home.Config{ChannelID: testChannelID, DBPath: dbPath, MustExistDB: true}))
	if err != nil {
		t.Fatalf("restart Open over existing DB: %v", err)
	}
	t.Cleanup(func() { _ = home.Shutdown(h2) })
	roster, err := h2.View().HumanRoster(context.Background())
	if err != nil {
		t.Fatalf("HumanRoster after restart: %v", err)
	}
	found := false
	for _, entry := range roster {
		if entry.Principal == "restart-owner" {
			found = true
		}
		if entry.ActorID == actor.SystemActorID {
			t.Errorf("kernel restored as a member row: %+v", entry)
		}
	}
	if !found {
		t.Errorf("durable member missing after restart: %+v", roster)
	}
}

// TestView_ReadVisibleAfterSeq_Empty verifies the public visible view on a
// fresh channel. Genesis system records are deliberately hidden.
func TestView_ReadVisibleAfterSeq_Empty(t *testing.T) {
	h := openTestHome(t)
	rows, _, err := h.View().ReadVisibleAfterSeq(context.Background(), 0, 100)
	if err != nil {
		t.Fatalf("ReadVisibleAfterSeq: %v", err)
	}
	if len(rows) != 0 {
		t.Fatalf("visible genesis rows=%d, want 0", len(rows))
	}
}
