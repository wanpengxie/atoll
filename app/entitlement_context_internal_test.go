package app

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/wanpengxie/atoll/platform/channelhost"
)

func TestEntitlementSnapshotHonorsCanceledContext(t *testing.T) {
	dir := t.TempDir()
	db, err := openTestAppDB(t, filepath.Join(dir, "app.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	a, err := New(Config{DB: db, HostFactory: func(deps channelhost.HomeDeps) (channelhost.LocalHost, error) {
		return channelhost.New(filepath.Join(dir, "channels"), deps)
	}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = a.Close() })
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, _, err := a.EntitlementSnapshot(ctx, "nobody"); !errors.Is(err, context.Canceled) {
		t.Fatalf("error=%v want context.Canceled", err)
	}
}
