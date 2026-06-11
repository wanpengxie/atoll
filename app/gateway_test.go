package app

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/wanpengxie/ActOS/platform"
	"github.com/wanpengxie/ActOS/protocol/channel"
)

const gwTestChannelID = channel.ID("test-gw")

// TestGateway_OverHomeCapabilities verifies the app-layer gateway composes
// correctly over a Home's Gate + View capability set: read paths (MaxSeq,
// ListActors, ListMessages) reach truth without error on a fresh channel.
func TestGateway_OverHomeCapabilities(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "gw.sqlite")
	home, err := platform.Open(platform.HomeConfig{ChannelID: gwTestChannelID, DBPath: dbPath})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = home.Close() })

	gw := homeGateway(gwTestChannelID, home)
	ctx := context.Background()

	if gw.ChannelID() != gwTestChannelID {
		t.Errorf("ChannelID = %q, want %q", gw.ChannelID(), gwTestChannelID)
	}
	if _, err := gw.MaxSeq(ctx); err != nil {
		t.Fatalf("MaxSeq: %v", err)
	}
	actors, err := gw.ListActors(ctx)
	if err != nil {
		t.Fatalf("ListActors: %v", err)
	}
	if len(actors) == 0 {
		t.Error("ListActors empty, want at least the system actor")
	}
	if _, err := gw.ListMessages(ctx, 0, 100); err != nil {
		t.Fatalf("ListMessages: %v", err)
	}
}
