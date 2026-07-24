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

// TestView_ActiveActors_IncludesSystem verifies the intrinsic system actor is
// registered by Open (genesis).
func TestView_ActiveActors_IncludesSystem(t *testing.T) {
	h := openTestHome(t)
	actors, err := h.View().ActiveActors(context.Background())
	if err != nil {
		t.Fatalf("ActiveActors: %v", err)
	}
	found := false
	for _, a := range actors {
		if a.ID == actor.SystemActorID && a.Kind == actor.KindSystem {
			found = true
		}
	}
	if !found {
		t.Errorf("system actor not in actors list: %+v", actors)
	}
}

// TestAdmit_CellLessMember admits a human member (the pure-membership primitive,
// no cell) and confirms it surfaces in the actor roster with no cell binding.
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
	var got *actor.Binding
	for _, a := range actors {
		if a.ID == id {
			b := a.Binding
			got = &b
		}
	}
	if got == nil {
		t.Fatalf("cell-less member %s not in roster", id)
	}
	if *got != "" {
		t.Errorf("cell-less member binding = %q, want empty (no cell)", *got)
	}
}

// TestOpen_RestartOverPersistentDB verifies the genesis seed is idempotent: a
// second Open over the SAME db file (a home restart) must succeed, not PK-conflict
// on re-seeding the intrinsic system actor. Before the Exists-guard this failed
// at bootstrap, taking down the whole restart-recovery path.
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
	// A normal reopen is legal only after bootstrap has published its owner.
	// Restart: re-open the same persistent channel DB — the system actor row
	// already exists, so the seed must no-op instead of failing.
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
		if a.ID == actor.SystemActorID {
			found = true
		}
	}
	if !found {
		t.Errorf("system actor missing after restart: %+v", actors)
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
