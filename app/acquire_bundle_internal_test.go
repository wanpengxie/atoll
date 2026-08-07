package app

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/wanpengxie/atoll/protocol/channel"
)

func TestAcquireBundleTreatsDirectoryReadFailureAsUnavailable(t *testing.T) {
	db, err := openTestAppDB(t, filepath.Join(t.TempDir(), "app.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	a := &App{db: db}
	_, err = a.acquireBundle(context.Background(), channel.ID("channel-a"))
	if !errors.Is(err, errChannelUnavailable) {
		t.Fatalf("acquireBundle error = %v, want errChannelUnavailable", err)
	}
	if errors.Is(err, errChannelNotFound) {
		t.Fatalf("directory read failure collapsed to errChannelNotFound: %v", err)
	}
}
