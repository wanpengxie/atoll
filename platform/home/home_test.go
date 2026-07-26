package home_test

import (
	"context"
	"errors"
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
	if _, err := home.SystemOps(h).Admit(context.Background(), channel.AdmitRequest{
		Ref: "late-admit", Principal: "late",
	}); !isUnavailable(err) {
		t.Fatalf("Admit after Close = %v", err)
	}
	if _, err := home.SystemOps(h).Remove(context.Background(), channel.RemoveRequest{
		Ref: "late-remove", Target: "late", InitiatorActorID: "late",
	}); !isUnavailable(err) {
		t.Fatalf("Remove after Close = %v", err)
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

func isUnavailable(err error) bool {
	var opErr *channel.OperationError
	return errors.As(err, &opErr) && opErr.Code == channel.ErrCodeChannelUnavailable
}

const testChannelID = channel.ID("test-home")

// openTestHome assembles a Home for testing.
func openTestHome(t *testing.T) *home.Home {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "home.sqlite")
	h, err := home.Open(home.Config{CompositionResolver: emptyCompositionResolver{}, IntroductionResolver: emptyIntroductionResolver{}, ChannelID: testChannelID, DBPath: dbPath, Bootstrap: true})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = home.Shutdown(h) })
	return h
}

// The kernel is a constant, not a member: Open registers no system row, so the
// membership projection never contains it. Its addressability is answered by
// the routing organ instead.
func TestView_ActiveActors_ExcludesKernel(t *testing.T) {
	h := openTestHome(t)
	actors, err := h.View().ActiveActors(context.Background())
	if err != nil {
		t.Fatalf("ActiveActors: %v", err)
	}
	for _, a := range actors {
		if a.ID == actor.SystemActorID {
			t.Errorf("kernel appeared as a member row: %+v", a)
		}
	}
}

// TestAdmit_CellLessMember admits a human member (the pure-membership
// primitive, no cell) and confirms it surfaces in the actor roster. The record
// carries no transport binding at all — binding is a physical connection
// projection and left the value domain.
func TestAdmit_CellLessMember(t *testing.T) {
	h := openTestHome(t)
	ctx := context.Background()
	result, err := home.SystemOps(h).Admit(ctx, channel.AdmitRequest{
		Ref: "admit-alice", Principal: "alice",
	})
	if err != nil {
		t.Fatalf("Admit: %v", err)
	}
	id := result.ActorID
	actors, err := h.View().ActiveActors(ctx)
	if err != nil {
		t.Fatalf("ListActors: %v", err)
	}
	var found bool
	for _, a := range actors {
		if a.ID == id {
			found = true
			if a.Kind != actor.KindHuman || a.Principal != "alice" {
				t.Errorf("admitted member = %+v", a)
			}
		}
	}
	if !found {
		t.Fatalf("cell-less member %s not in roster", id)
	}
}

// TestOpen_RestartOverPersistentDB verifies a second Open over the SAME db file
// (a home restart) succeeds and restores the durable membership image.
func TestOpen_RestartOverPersistentDB(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "home.sqlite")
	h1, err := home.Open(home.Config{CompositionResolver: emptyCompositionResolver{}, IntroductionResolver: emptyIntroductionResolver{}, ChannelID: testChannelID, DBPath: dbPath, Bootstrap: true})
	if err != nil {
		t.Fatalf("first Open: %v", err)
	}
	if _, err := home.BootstrapOwner(context.Background(), h1, "restart-owner"); err != nil {
		t.Fatalf("seed owner: %v", err)
	}
	if err := home.Shutdown(h1); err != nil {
		t.Fatalf("Close: %v", err)
	}
	h2, err := home.Open(home.Config{CompositionResolver: emptyCompositionResolver{}, IntroductionResolver: emptyIntroductionResolver{}, ChannelID: testChannelID, DBPath: dbPath, MustExistDB: true})
	if err != nil {
		t.Fatalf("restart Open over existing DB: %v", err)
	}
	t.Cleanup(func() { _ = home.Shutdown(h2) })
	actors, err := h2.View().ActiveActors(context.Background())
	if err != nil {
		t.Fatalf("ListActors after restart: %v", err)
	}
	found := false
	for _, a := range actors {
		if a.Principal == "restart-owner" {
			found = true
		}
		if a.ID == actor.SystemActorID {
			t.Errorf("kernel restored as a member row: %+v", a)
		}
	}
	if !found {
		t.Errorf("durable member missing after restart: %+v", actors)
	}
}

// TestView_ReadVisibleAfterSeq_Empty verifies the public visible view on a
// fresh channel. Genesis system records are deliberately hidden.
func TestView_ReadVisibleAfterSeq_Empty(t *testing.T) {
	h := openTestHome(t)
	rows, _, err := h.View().ReadVisibleAfterSeq(context.Background(), channel.Reader{
		Principal: "observer", Mode: channel.ReaderObserver,
	}, 0, 100)
	if err != nil {
		t.Fatalf("ReadVisibleAfterSeq: %v", err)
	}
	if len(rows) != 0 {
		t.Fatalf("visible genesis rows=%d, want 0", len(rows))
	}
}
