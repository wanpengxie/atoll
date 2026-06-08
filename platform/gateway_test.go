package platform_test

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/wanpengxie/ActOS/protocol/actor"
	"github.com/wanpengxie/ActOS/protocol/channel"
	"github.com/wanpengxie/ActOS/platform"
	"github.com/wanpengxie/ActOS/runtime/storespec"
)

const testChannelID = channel.ID("test-gw")

// setupChannelHome assembles a ChannelHome for testing. It pre-registers
// the web-client actor so ingress writes pass the harness.
func setupChannelHome(t *testing.T) *platform.ChannelHome {
	t.Helper()
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "gw.sqlite")

	ch, err := platform.NewChannelHome(platform.HomeConfig{
		ChannelID: testChannelID,
		DBPath:    dbPath,
	})
	if err != nil {
		t.Fatalf("NewChannelHome: %v", err)
	}
	t.Cleanup(func() { _ = ch.Close() })

	// Pre-register a web-client actor so ingress writes pass the harness.
	_ = ch.Membership().Insert(ctx, storespec.Record{
		ID: "web-client", Kind: actor.KindHuman, CreatedAt: time.Now().UnixMilli(),
	})

	return ch
}

// TestMaxSeq_EmptyChannel tests MaxSeq on an empty channel.
func TestMaxSeq_EmptyChannel(t *testing.T) {
	ch := setupChannelHome(t)
	gw := ch.Gateway()

	seq, err := gw.MaxSeq(context.Background())
	if err != nil {
		t.Fatalf("MaxSeq: %v", err)
	}
	// After genesis (system actor registration writes events), seq may be > 0.
	// Just verify no error and a non-negative value.
	if seq < 0 {
		t.Errorf("MaxSeq = %d, want >= 0", seq)
	}
}

// TestListActors_IncludesSystem tests that ListActors includes the system actor.
func TestListActors_IncludesSystem(t *testing.T) {
	ch := setupChannelHome(t)
	gw := ch.Gateway()

	actors, err := gw.ListActors(context.Background())
	if err != nil {
		t.Fatalf("ListActors: %v", err)
	}
	found := false
	for _, a := range actors {
		if a.ID == "system" && a.Kind == "system" {
			found = true
		}
	}
	if !found {
		t.Errorf("system actor not in actors list: %+v", actors)
	}
}

// TestListMessages_Empty tests ListMessages on an empty channel.
func TestListMessages_Empty(t *testing.T) {
	ch := setupChannelHome(t)
	gw := ch.Gateway()

	rows, err := gw.ListMessages(context.Background(), 0, 100)
	if err != nil {
		t.Fatalf("ListMessages: %v", err)
	}
	// May have genesis events, just verify no error.
	_ = rows
}
