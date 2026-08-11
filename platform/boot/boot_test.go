package boot_test

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/platform/boot"
	"github.com/wanpengxie/atoll/platform/lagoon"
	"github.com/wanpengxie/atoll/protocol"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/runtime"
	"golang.org/x/crypto/bcrypt"

	_ "modernc.org/sqlite"
)

func TestEnsureInstallsRegistryAndPublishesMarkerLast(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	stamp := time.UnixMilli(1700000000000)
	result, err := boot.Ensure(ctx, boot.Config{ChannelDir: root, RootPassword: "root-pass", Now: func() time.Time { return stamp }})
	if err != nil {
		t.Fatal(err)
	}
	if !result.Installed || result.RootPassword != "root-pass" {
		t.Fatalf("result=%+v", result)
	}
	if boot.RegistryDDLCount() != 7 {
		t.Fatalf("registry DDL count=%d", boot.RegistryDDLCount())
	}
	db, err := sql.Open("sqlite", result.C0DBPath)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for _, table := range []string{"channels", "principals", "credentials", "decls", "decl_overlays", "devices", "bindings", "atoll_install"} {
		var n int
		if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?`, table).Scan(&n); err != nil || n != 1 {
			t.Fatalf("table %s count=%d err=%v", table, n, err)
		}
	}
	var installedAt int64
	if err := db.QueryRowContext(ctx, `SELECT installed_at FROM atoll_install WHERE id=1`).Scan(&installedAt); err != nil || installedAt != stamp.UnixMilli() {
		t.Fatalf("marker=%d err=%v", installedAt, err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	cs, err := runtime.OpenChannel(ctx, protocol.C0ChannelID, result.C0DBPath, runtime.OpenChannelOptions{MustExist: true})
	if err != nil {
		t.Fatalf("reopen c0 with registry tables: %v", err)
	}
	_ = cs.Close()
}

func TestEnsureGeneratesRootPasswordWhenNotSupplied(t *testing.T) {
	result, err := boot.Ensure(context.Background(), boot.Config{ChannelDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	if !result.Installed || result.RootPassword == "" {
		t.Fatalf("result=%+v", result)
	}
	db, err := sql.Open("sqlite", result.C0DBPath)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var hash string
	if err := db.QueryRow(`SELECT secret_hash FROM credentials WHERE principal_id='root'`).Scan(&hash); err != nil {
		t.Fatal(err)
	}
	if err := bcrypt.CompareHashAndPassword([]byte(hash), []byte(result.RootPassword)); err != nil {
		t.Fatal("generated password does not match installed credential")
	}
}

func TestEnsureMarkerMakesStartupReadOnlyForCredentials(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	first, err := boot.Ensure(ctx, boot.Config{ChannelDir: root, RootPassword: "first"})
	if err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("sqlite", first.C0DBPath)
	if err != nil {
		t.Fatal(err)
	}
	var before string
	if err := db.QueryRow(`SELECT secret_hash FROM credentials WHERE principal_id='root'`).Scan(&before); err != nil {
		t.Fatal(err)
	}
	_ = db.Close()
	second, err := boot.Ensure(ctx, boot.Config{ChannelDir: root, RootPassword: "second"})
	if err != nil {
		t.Fatal(err)
	}
	if second.Installed || second.RootPassword != "" {
		t.Fatalf("startup reran installer: %+v", second)
	}
	db, err = sql.Open("sqlite", second.C0DBPath)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var after string
	if err := db.QueryRow(`SELECT secret_hash FROM credentials WHERE principal_id='root'`).Scan(&after); err != nil {
		t.Fatal(err)
	}
	if before != after {
		t.Fatal("startup rewrote root credential")
	}
}

func TestEnsureRebuildsFileWithoutMarker(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	path, err := filepath.Abs(filepath.Join(root, "YzA.db"))
	if err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`CREATE TABLE partial(x INTEGER)`); err != nil {
		t.Fatal(err)
	}
	_ = db.Close()
	result, err := boot.Ensure(ctx, boot.Config{ChannelDir: root, RootPassword: "root-pass"})
	if err != nil {
		t.Fatal(err)
	}
	db, err = sql.Open("sqlite", result.C0DBPath)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var partial int
	if err := db.QueryRow(`SELECT COUNT(*) FROM sqlite_master WHERE name='partial'`).Scan(&partial); err != nil {
		t.Fatal(err)
	}
	if partial != 0 {
		t.Fatal("half-install residue survived rebuild")
	}
}

func TestStartupRepairsC0CompositionSeatsWithoutReinstalling(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	installed, err := boot.Ensure(ctx, boot.Config{ChannelDir: root, RootPassword: "root-pass"})
	if err != nil {
		t.Fatal(err)
	}
	cs, err := runtime.OpenChannel(ctx, protocol.C0ChannelID, installed.C0DBPath, runtime.OpenChannelOptions{MustExist: true})
	if err != nil {
		t.Fatal(err)
	}
	rows, err := cs.Actors.ListActive(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, row := range rows {
		if row.SourceDeclID == lagoon.RegistrarSeatDeclID {
			if err := cs.Actors.Deregister(ctx, []actor.ActorID{row.ID}, time.Now().UnixMilli()); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := cs.Close(); err != nil {
		t.Fatal(err)
	}
	result, err := boot.Ensure(ctx, boot.Config{ChannelDir: root, RootPassword: "must-not-be-used"})
	if err != nil {
		t.Fatal(err)
	}
	if result.Installed {
		t.Fatal("seat repair reran installation")
	}
	cs, err = runtime.OpenChannel(ctx, protocol.C0ChannelID, installed.C0DBPath, runtime.OpenChannelOptions{MustExist: true})
	if err != nil {
		t.Fatal(err)
	}
	defer cs.Close()
	rows, err = cs.Actors.ListActive(ctx)
	if err != nil {
		t.Fatal(err)
	}
	count := 0
	for _, row := range rows {
		if row.SourceDeclID == lagoon.RegistrarSeatDeclID {
			count++
		}
	}
	if count != 1 {
		t.Fatalf("active registrar seats=%d", count)
	}
}
