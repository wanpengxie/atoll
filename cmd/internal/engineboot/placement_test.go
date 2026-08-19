package engineboot

import (
	"context"
	"database/sql"
	"log/slog"
	"net/url"
	"path/filepath"
	"testing"

	_ "github.com/wanpengxie/atoll/drivers/agents/all"
	"github.com/wanpengxie/atoll/platform/channelhost"
	"github.com/wanpengxie/atoll/platform/channelspec"
	"github.com/wanpengxie/atoll/platform/lagoon"
)

func TestPeerPlacementIsServerWithoutCoreDeviceBindingAndCodexIsDaemon(t *testing.T) {
	channelDir := filepath.Join(t.TempDir(), "channels")
	eng, err := Boot(Config{ChannelDBDir: channelDir, Addr: "127.0.0.1:0", RootPassword: "test-root-password"}, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close(context.Background())

	registryURL := &url.URL{Scheme: "file", Path: filepath.Join(filepath.Dir(channelDir), "registry.db")}
	db, err := sql.Open("sqlite", registryURL.String()+"?mode=rw&_pragma=foreign_keys(1)&_pragma=busy_timeout(5000)")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`DELETE FROM bindings WHERE channel_id=?`, channelspec.C0ChannelID); err != nil {
		t.Fatal(err)
	}
	_ = db.Close()

	core, _ := eng.host.Acquire(channelspec.C0ChannelID)
	registrar := onlyDecl(t, core, lagoon.RegistrarDeclID)
	terminalValue(t, callMember(t, channelspec.C0ChannelID, core, channelspec.RootPrincipalID, registrar, string(lagoon.WordActorTemplateCreate), map[string]any{
		"id": "codex-placement", "name": "Codex Placement", "class": "codex", "config": map[string]any{}, "visibility": "public",
	}), nil)
	var created lagoon.ChannelCreateReply
	terminalValue(t, callMember(t, channelspec.C0ChannelID, core, channelspec.RootPrincipalID, registrar, string(lagoon.WordChannelCreate), map[string]any{
		"name": "placement", "recipe": map[string]any{"declarations": []any{map[string]any{"decl_id": "codex-placement"}}},
	}), &created)
	_ = waitBundle(t, eng, created.ChannelID)

	c0Path, err := channelhost.DBPath(channelDir, channelspec.C0ChannelID)
	if err != nil {
		t.Fatal(err)
	}
	assertActorPlacement(t, c0Path, string(created.ChannelID), "server", "")
	childPath, err := channelhost.DBPath(channelDir, created.ChannelID)
	if err != nil {
		t.Fatal(err)
	}
	assertActorPlacement(t, childPath, "codex-placement", "daemon", channelspec.LocalDeviceID)
}

func assertActorPlacement(t *testing.T, path, decl, placement, host string) {
	t.Helper()
	u := &url.URL{Scheme: "file", Path: path}
	db, err := sql.Open("sqlite", u.String()+"?mode=ro")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var gotPlacement, gotHost string
	if err := db.QueryRow(`SELECT placement,desired_host FROM actor_registry WHERE source_decl_id=? AND deregistered_at IS NULL`, decl).Scan(&gotPlacement, &gotHost); err != nil {
		t.Fatal(err)
	}
	if gotPlacement != placement || gotHost != host {
		t.Fatalf("decl=%s placement=(%s,%s) want=(%s,%s)", decl, gotPlacement, gotHost, placement, host)
	}
}
