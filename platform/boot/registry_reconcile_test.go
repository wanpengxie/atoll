package boot_test

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"

	"github.com/wanpengxie/atoll/platform/boot"
	"github.com/wanpengxie/atoll/platform/channelspec"
	"github.com/wanpengxie/atoll/platform/lagoon"
)

// A start is never a calibrator: boot carves c0 once, and every later
// ReconcileSystem leaves the existing c0 row alone. The row is edited
// directly here because no word in the system may change c0's profile — the
// only writer that could drift it back is the start itself, which is exactly
// what this pins.
func TestReconcileSystemLeavesExistingC0RowUntouched(t *testing.T) {
	ctx := context.Background()
	installed, err := boot.Ensure(ctx, withClassDefaults(boot.Config{ChannelDir: filepath.Join(t.TempDir(), "channels"), RootPassword: "root-pass"}))
	if err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("sqlite", "file:"+installed.RegistryDBPath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `UPDATE channels SET description=?, serving=0 WHERE id=?`, "operator description", channelspec.C0ChannelID); err != nil {
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
	row, found, err := registry.GetChannelDesired(ctx, channelspec.C0ChannelID)
	if err != nil || !found {
		t.Fatalf("c0 row after reconcile: found=%v err=%v", found, err)
	}
	if row.Description != "operator description" || row.Serving != 0 {
		t.Fatalf("reconcile rewrote existing c0 row: description=%q serving=%d", row.Description, row.Serving)
	}
}

// A c0 genesis this build cannot read, or that is not c0's, refuses the
// start: there is no upgrade path before 1.0, the operator wipes and starts
// again. `{}` decodes but is nobody's genesis and must fail too.
func TestReconcileSystemRefusesUnreadableOrForeignC0Genesis(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct{ name, spec string }{
		{"empty object", `{}`},
		{"unknown field", `{"channel_id":"c0","type":"group","owner_principal":"root","created_at":1,"genesis_declarations":[],"profile":{"endpoints":{"x":{"receiver":"r"}}},"future_field":1}`},
		{"foreign channel", `{"channel_id":"other","type":"group","owner_principal":"root","created_at":1,"genesis_declarations":[],"profile":{"endpoints":{"x":{"receiver":"r"}}}}`},
		{"not json", `not json`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			installed, err := boot.Ensure(ctx, withClassDefaults(boot.Config{ChannelDir: filepath.Join(t.TempDir(), "channels"), RootPassword: "root-pass"}))
			if err != nil {
				t.Fatal(err)
			}
			db, err := sql.Open("sqlite", "file:"+installed.RegistryDBPath)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := db.ExecContext(ctx, `UPDATE channels SET spec_json=? WHERE id=?`, tc.spec, channelspec.C0ChannelID); err != nil {
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
			if err := lagoon.NewRegistrar(registry, nil, nil).ReconcileSystem(ctx); err == nil {
				t.Fatalf("reconcile accepted c0 genesis %q", tc.spec)
			}
		})
	}
}
