package boot_test

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"

	"github.com/wanpengxie/atoll/platform/boot"
	"github.com/wanpengxie/atoll/platform/lagoon"
	"github.com/wanpengxie/atoll/protocol"
)

// An existing c0 row belongs to its owner: a later start must not rewrite its
// description or serving back to the carved defaults. Reconcile only carves
// c0 when it is missing.
func TestReconcileSystemLeavesExistingC0RowUntouched(t *testing.T) {
	ctx := context.Background()
	installed, err := boot.Ensure(ctx, boot.Config{ChannelDir: filepath.Join(t.TempDir(), "channels"), RootPassword: "root-pass"})
	if err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("sqlite", "file:"+installed.RegistryDBPath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `UPDATE channels SET description=?, serving=0 WHERE id=?`, "operator description", protocol.C0ChannelID); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	registry, err := lagoon.Open(installed.RegistryDBPath, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer registry.Close()
	if err := lagoon.NewRegistrar(registry, nil, nil).ReconcileSystem(ctx); err != nil {
		t.Fatal(err)
	}
	row, found, err := registry.GetChannelDesired(ctx, protocol.C0ChannelID)
	if err != nil || !found {
		t.Fatalf("c0 row after reconcile: found=%v err=%v", found, err)
	}
	if row.Description != "operator description" || row.Serving != 0 {
		t.Fatalf("reconcile rewrote existing c0 row: description=%q serving=%d", row.Description, row.Serving)
	}
}
