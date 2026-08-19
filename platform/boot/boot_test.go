package boot_test

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/platform/boot"
	"github.com/wanpengxie/atoll/platform/channelhost"
	"github.com/wanpengxie/atoll/platform/channelspec"
	"github.com/wanpengxie/atoll/platform/lagoon"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/runtime"
	"github.com/wanpengxie/atoll/runtime/storespec"
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
	if boot.RegistryDDLCount() != 8 {
		t.Fatalf("registry DDL count=%d", boot.RegistryDDLCount())
	}
	if filepath.Dir(result.RegistryDBPath) == filepath.Clean(root) {
		t.Fatalf("registry database lives inside channel directory: %s", result.RegistryDBPath)
	}
	if filepath.Dir(result.RegistryDBPath) != filepath.Dir(filepath.Clean(root)) {
		t.Fatalf("registry database parent=%s, want channel-dir sibling under %s", filepath.Dir(result.RegistryDBPath), filepath.Dir(filepath.Clean(root)))
	}
	db, err := sql.Open("sqlite", result.RegistryDBPath)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for _, table := range []string{"channels", "principals", "credentials", "decls", "decl_overlays", "channel_templates", "devices", "bindings", "atoll_install"} {
		var n int
		if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?`, table).Scan(&n); err != nil || n != 1 {
			t.Fatalf("table %s count=%d err=%v", table, n, err)
		}
	}
	for _, column := range []string{"description", "serving"} {
		var n int
		if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM pragma_table_info('channels') WHERE name=?`, column).Scan(&n); err != nil || n != 1 {
			t.Fatalf("channels.%s count=%d err=%v", column, n, err)
		}
	}
	var channelCount, principalCount int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM channels`).Scan(&channelCount); err != nil || channelCount != 2 {
		t.Fatalf("channels=%d err=%v", channelCount, err)
	}
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM principals WHERE id IN (?,?,?)`, channelspec.RootPrincipalID, channelspec.StewardPrincipalID, channelspec.GuestPrincipalID).Scan(&principalCount); err != nil || principalCount != 3 {
		t.Fatalf("system principals=%d err=%v", principalCount, err)
	}
	var lobbyServing int
	var lobbyDescription string
	if err := db.QueryRowContext(ctx, `SELECT serving,description FROM channels WHERE id=?`, channelspec.LobbyChannelID).Scan(&lobbyServing, &lobbyDescription); err != nil || lobbyServing != 0 || lobbyDescription == "" {
		t.Fatalf("lobby serving=%d description=%q err=%v", lobbyServing, lobbyDescription, err)
	}
	for _, id := range []channel.ID{channelspec.C0ChannelID, channelspec.LobbyChannelID} {
		var description, specRaw string
		var serving int
		if err := db.QueryRowContext(ctx, `SELECT description,serving,spec_json FROM channels WHERE id=?`, id).Scan(&description, &serving, &specRaw); err != nil {
			t.Fatal(err)
		}
		var spec lagoon.GenesisSpec
		if err := json.Unmarshal([]byte(specRaw), &spec); err != nil || spec.Profile.Description == nil || *spec.Profile.Description != description || spec.Profile.Serving == nil || *spec.Profile.Serving != serving {
			t.Fatalf("channel %s frozen profile=%+v row=(%q,%d) err=%v", id, spec.Profile, description, serving, err)
		}
		if len(spec.Profile.Endpoints) != 0 {
			t.Fatalf("channel %s unexpectedly froze endpoints=%d", id, len(spec.Profile.Endpoints))
		}
	}
	var privateSystemDecls int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM decls WHERE id IN (?,?) AND visibility='private' AND status='present'`, lagoon.SvcActorDeclID, lagoon.RegistrarDeclID).Scan(&privateSystemDecls); err != nil || privateSystemDecls != 2 {
		t.Fatalf("private system declarations=%d err=%v", privateSystemDecls, err)
	}
	var descriptionColumns int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM pragma_table_info('decls') WHERE name='description'`).Scan(&descriptionColumns); err != nil || descriptionColumns != 1 {
		t.Fatalf("decls.description columns=%d err=%v", descriptionColumns, err)
	}
	var installedAt int64
	if err := db.QueryRowContext(ctx, `SELECT installed_at FROM atoll_install WHERE id=1`).Scan(&installedAt); err != nil || installedAt != stamp.UnixMilli() {
		t.Fatalf("marker=%d err=%v", installedAt, err)
	}
	var localDeviceName string
	if err := db.QueryRowContext(ctx, `SELECT name FROM devices WHERE id=?`, channelspec.LocalDeviceID).Scan(&localDeviceName); err != nil {
		t.Fatal(err)
	}
	if err := lagoon.ValidateName(localDeviceName); err != nil {
		t.Fatalf("boot minted invalid local device name %q: %v", localDeviceName, err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	channelDB, err := sql.Open("sqlite", result.C0DBPath)
	if err != nil {
		t.Fatal(err)
	}
	defer channelDB.Close()
	for _, table := range []string{"channels", "principals", "credentials", "decls", "decl_overlays", "devices", "bindings", "atoll_install"} {
		var n int
		if err := channelDB.QueryRowContext(ctx, `SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?`, table).Scan(&n); err != nil || n != 0 {
			t.Fatalf("registry table %s leaked into c0: count=%d err=%v", table, n, err)
		}
	}
	if err := channelDB.Close(); err != nil {
		t.Fatal(err)
	}
	cs, err := runtime.OpenChannel(ctx, channelspec.C0ChannelID, result.C0DBPath, runtime.OpenChannelOptions{MustExist: true})
	if err != nil {
		t.Fatalf("reopen pure c0: %v", err)
	}
	_ = cs.Close()
	assertBootRoster(t, result.C0DBPath, []string{lagoon.RegistrarDeclID, lagoon.SvcActorDeclID, lagoon.StableBootstrapDeclID(channelspec.RootPrincipalID, "steward"), string(channelspec.LobbyChannelID)}, []string{channelspec.RootPrincipalID}, 5)
	lobbyPath, err := channelhost.DBPath(root, channelspec.LobbyChannelID)
	if err != nil {
		t.Fatal(err)
	}
	assertBootRoster(t, lobbyPath, []string{lagoon.SvcActorDeclID}, []string{channelspec.GuestPrincipalID}, 2)
}

func assertBootRoster(t *testing.T, path string, decls, principals []string, total int) {
	t.Helper()
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var count int
	if err := db.QueryRow(`SELECT COUNT(*) FROM actor_registry WHERE deregistered_at IS NULL`).Scan(&count); err != nil || count != total {
		t.Fatalf("%s active roster=%d want=%d err=%v", path, count, total, err)
	}
	for _, decl := range decls {
		var placement string
		if err := db.QueryRow(`SELECT placement FROM actor_registry WHERE source_decl_id=? AND deregistered_at IS NULL`, decl).Scan(&placement); err != nil {
			t.Fatalf("%s decl %s: %v", path, decl, err)
		}
		want := "server"
		if decl == lagoon.StableBootstrapDeclID(channelspec.RootPrincipalID, "steward") {
			want = "daemon"
		}
		if placement != want {
			t.Fatalf("%s decl %s placement=%s want=%s", path, decl, placement, want)
		}
	}
	for _, principal := range principals {
		var count int
		if err := db.QueryRow(`SELECT COUNT(*) FROM actor_registry WHERE principal=? AND deregistered_at IS NULL`, principal).Scan(&count); err != nil || count != 1 {
			t.Fatalf("%s principal %s count=%d err=%v", path, principal, count, err)
		}
	}
}

func TestEnsureGeneratesRootPasswordWhenNotSupplied(t *testing.T) {
	result, err := boot.Ensure(context.Background(), boot.Config{ChannelDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	if !result.Installed || result.RootPassword == "" {
		t.Fatalf("result=%+v", result)
	}
	db, err := sql.Open("sqlite", result.RegistryDBPath)
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
	db, err := sql.Open("sqlite", first.RegistryDBPath)
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
	db, err = sql.Open("sqlite", second.RegistryDBPath)
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

func TestEnsureRebuildsRegistryWithoutMarker(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	first, err := boot.Ensure(ctx, boot.Config{ChannelDir: root, RootPassword: "discarded"})
	if err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("sqlite", first.RegistryDBPath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`DELETE FROM atoll_install WHERE id=1`); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	result, err := boot.Ensure(ctx, boot.Config{ChannelDir: root, RootPassword: "root-pass"})
	if err != nil {
		t.Fatal(err)
	}
	if !result.Installed {
		t.Fatalf("result=%+v", result)
	}
}

func TestEnsureRebuildsBothDatabaseFamiliesWithoutInstallTable(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	c0Path, err := channelhost.DBPath(root, channelspec.C0ChannelID)
	if err != nil {
		t.Fatal(err)
	}
	registryPath := filepath.Join(filepath.Dir(root), "registry.db")
	for _, path := range []string{c0Path, registryPath} {
		db, err := sql.Open("sqlite", path)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := db.Exec(`CREATE TABLE partial(x INTEGER)`); err != nil {
			_ = db.Close()
			t.Fatal(err)
		}
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
		for _, suffix := range []string{"-wal", "-shm"} {
			if err := os.WriteFile(path+suffix, []byte("stale-sidecar"), 0o600); err != nil {
				t.Fatal(err)
			}
		}
	}

	result, err := boot.Ensure(ctx, boot.Config{ChannelDir: root, RootPassword: "root-pass"})
	if err != nil {
		t.Fatal(err)
	}
	if !result.Installed {
		t.Fatalf("result=%+v", result)
	}
	for _, path := range []string{result.C0DBPath, result.RegistryDBPath} {
		db, err := sql.Open("sqlite", path)
		if err != nil {
			t.Fatal(err)
		}
		var partial int
		if err := db.QueryRow(`SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='partial'`).Scan(&partial); err != nil {
			_ = db.Close()
			t.Fatal(err)
		}
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
		if partial != 0 {
			t.Fatalf("half-install residue survived rebuild in %s", path)
		}
	}
}

func TestEnsurePreservesUnreadableDatabase(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	path := filepath.Join(filepath.Dir(root), "registry.db")
	want := []byte("not a sqlite database\x00with existing registry data")
	if err := os.WriteFile(path, want, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := boot.Ensure(ctx, boot.Config{ChannelDir: root, RootPassword: "must-not-be-used"}); err == nil {
		t.Fatal("Ensure succeeded for an unreadable registry database")
	}
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("unreadable registry database changed: got %q, want %q", got, want)
	}
}

func TestRegistryAndC0WritersDoNotShareSQLiteLockDomain(t *testing.T) {
	ctx := context.Background()
	installed, err := boot.Ensure(ctx, boot.Config{ChannelDir: filepath.Join(t.TempDir(), "channels"), RootPassword: "root-pass"})
	if err != nil {
		t.Fatal(err)
	}
	registry, err := lagoon.Open(installed.RegistryDBPath, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer registry.Close()
	registrar := lagoon.NewRegistrar(registry, nil, nil)
	c0, err := runtime.OpenChannel(ctx, channelspec.C0ChannelID, installed.C0DBPath, runtime.OpenChannelOptions{MustExist: true})
	if err != nil {
		t.Fatal(err)
	}
	defer c0.Close()

	for i := 0; i < 64; i++ {
		start := make(chan struct{})
		done := make(chan error, 2)
		go func() {
			<-start
			done <- registrar.ReconcileSystem(ctx)
		}()
		go func(i int) {
			<-start
			_, err := c0.Log.Append(ctx, &message.Envelope{
				ID:         message.ID(fmt.Sprintf("lock-domain-%d", i)),
				TS:         int64(i + 1),
				TSReceived: int64(i + 1),
				ChannelID:  channelspec.C0ChannelID,
				Sender:     message.Sender{Kind: actor.KindHuman, ID: "root-writer"},
				Kind:       message.KindEvent,
				Type:       "test.concurrent_write",
				Payload:    json.RawMessage(`{}`),
				Visibility: message.VisibilityPublic,
				Audience:   message.Audience{actor.SystemActorID},
			}, false, storespec.AppendMetadata{})
			done <- err
		}(i)
		close(start)
		for writer := 0; writer < 2; writer++ {
			if err := <-done; err != nil {
				t.Fatalf("concurrent registry/c0 write %d: %v", i, err)
			}
		}
	}
}
