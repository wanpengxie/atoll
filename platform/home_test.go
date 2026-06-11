package platform_test

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/wanpengxie/ActOS/platform"
	"github.com/wanpengxie/ActOS/protocol/actor"
	"github.com/wanpengxie/ActOS/protocol/channel"
)

const testChannelID = channel.ID("test-home")

// openTestHome assembles a Home for testing.
func openTestHome(t *testing.T) *platform.Home {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "home.sqlite")
	h, err := platform.Open(platform.HomeConfig{ChannelID: testChannelID, DBPath: dbPath})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = h.Close() })
	return h
}

// TestView_MaxSeq_EmptyChannel verifies MaxSeq on a fresh channel is non-negative.
func TestView_MaxSeq_EmptyChannel(t *testing.T) {
	h := openTestHome(t)
	seq, err := h.View().MaxSeq(context.Background())
	if err != nil {
		t.Fatalf("MaxSeq: %v", err)
	}
	if seq < 0 {
		t.Errorf("MaxSeq = %d, want >= 0", seq)
	}
}

// TestView_ListActors_IncludesSystem verifies the intrinsic system actor is
// registered by Open (genesis).
func TestView_ListActors_IncludesSystem(t *testing.T) {
	h := openTestHome(t)
	actors, err := h.View().ListActors(context.Background())
	if err != nil {
		t.Fatalf("ListActors: %v", err)
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

// TestSpawn_PresencelessMember registers a human member with nil impl (membership
// only) and confirms it surfaces in the actor roster with no cell binding.
func TestSpawn_PresencelessMember(t *testing.T) {
	h := openTestHome(t)
	ctx := context.Background()
	id := actor.ActorID("user:alice")
	if err := h.Spawn(ctx, id, actor.KindHuman, nil); err != nil {
		t.Fatalf("Spawn(nil impl): %v", err)
	}
	actors, err := h.View().ListActors(ctx)
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
		t.Fatalf("presence-less member %s not in roster", id)
	}
	if *got != "" {
		t.Errorf("presence-less member binding = %q, want empty (no cell)", *got)
	}
}

// TestView_ReadAfterSeq_Empty verifies ReadAfterSeq on a fresh channel.
func TestView_ReadAfterSeq_Empty(t *testing.T) {
	h := openTestHome(t)
	rows, err := h.View().ReadAfterSeq(context.Background(), 0, 100)
	if err != nil {
		t.Fatalf("ReadAfterSeq: %v", err)
	}
	_ = rows // genesis events may be present; only error-freedom is asserted
}
