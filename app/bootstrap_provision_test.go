package app_test

import (
	"context"
	_ "github.com/wanpengxie/atoll/drivers/agents/provider/codex"
	"os"
	"path/filepath"
	"testing"

	"github.com/wanpengxie/atoll/app"
	"github.com/wanpengxie/atoll/protocol/channel"
)

// TestProvisionLocalNodeConvergesIdempotently pins `atoll up`'s provisioning
// contract: one call makes a usable node (owner + token file + home channel
// with the owner enrolled + a local daemon bound to home), and a second call
// carrying the first run's identity claim CONVERGES onto the same rows — same
// channel, same daemon, same key — instead of erroring or minting duplicates.
func TestProvisionLocalNodeConvergesIdempotently(t *testing.T) {
	env := setupTestApp(t)
	dir := t.TempDir()
	tokenPath := filepath.Join(dir, "atoll-token")
	ctx := context.Background()

	first, err := env.app.ProvisionLocalNode(ctx, app.ProvisionSpec{TokenPath: tokenPath})
	if err != nil {
		t.Fatalf("first provision: %v", err)
	}
	if first.HomeChannelID == "" || first.DaemonID == "" || first.DaemonKey == "" {
		t.Fatalf("provision incomplete: %+v", first)
	}
	if info, err := os.Stat(tokenPath); err != nil || info.Mode().Perm() != 0o600 {
		t.Fatalf("token file: info=%v err=%v", info, err)
	}
	// Owner is enrolled in home by the ordinary creation path (creator
	// membership), not by any provisioning side door.
	if _, err := env.app.ResolvePrincipalForTest(first.HomeChannelID, first.OwnerID); err != nil {
		t.Fatalf("owner %s is not a member of home %s: %v", first.OwnerID, first.HomeChannelID, err)
	}

	// Reopen carries the persisted identity claim (what engineboot reads back
	// from the device home's identity file).
	second, err := env.app.ProvisionLocalNode(ctx, app.ProvisionSpec{
		TokenPath: tokenPath, DaemonID: first.DaemonID,
	})
	if err != nil {
		t.Fatalf("second provision must converge, got: %v", err)
	}
	if second.HomeChannelID != first.HomeChannelID {
		t.Fatalf("home channel not stable: %s then %s", first.HomeChannelID, second.HomeChannelID)
	}
	if second.DaemonID != first.DaemonID || second.DaemonKey != first.DaemonKey {
		t.Fatalf("local daemon not stable: %+v then %+v", first, second)
	}

	// Token rotation invariant: restart IS the rotation — after any number of
	// provisions the bootstrap owner has EXACTLY ONE live session.
	var sessions int
	if err := env.db.QueryRow(
		`SELECT COUNT(*) FROM sessions WHERE user_id = ?`, first.OwnerID,
	).Scan(&sessions); err != nil {
		t.Fatalf("count sessions: %v", err)
	}
	if sessions != 1 {
		t.Fatalf("bootstrap owner must hold exactly 1 live session after rotation, got %d", sessions)
	}
	decl, found, err := env.app.StableBootstrapCodexDeclarationForTest(first.OwnerID)
	if err != nil || !found {
		t.Fatalf("stable codex declaration: found=%v err=%v", found, err)
	}
	count, err := env.app.BootstrapCodexDeclarationCountForTest(first.OwnerID)
	if err != nil || count != 1 {
		t.Fatalf("codex declaration count=%d err=%v", count, err)
	}
	instances, err := env.app.DeclaredInstancesForTest(channel.ID(first.HomeChannelID), decl.ID)
	if err != nil || len(instances) != 1 {
		t.Fatalf("codex instances=%v err=%v", instances, err)
	}
	defaultAgent, found, err := env.app.DefaultAgentForTest(channel.ID(first.HomeChannelID))
	if err != nil || !found || defaultAgent != instances[0] {
		t.Fatalf("default=%q found=%v err=%v instances=%v", defaultAgent, found, err, instances)
	}
}

// TestProvisionLocalNodeNeverAdoptsByName pins the identity model's negative
// space: a user-created daemon that happens to be named "local-device" must
// NEVER be adopted as this installation's device (names are display-only), and
// a claim on a deleted row mints fresh instead of resurrecting.
func TestProvisionLocalNodeNeverAdoptsByName(t *testing.T) {
	env := setupTestApp(t)
	dir := t.TempDir()
	ctx := context.Background()

	first, err := env.app.ProvisionLocalNode(ctx, app.ProvisionSpec{
		TokenPath: filepath.Join(dir, "atoll-token"),
	})
	if err != nil {
		t.Fatalf("first provision: %v", err)
	}

	// A decoy: same owner, same display name, created through the ordinary
	// daemon path — exactly what a user might set up for a REMOTE machine.
	decoyID, decoyKey, err := env.app.CreateDaemonRowForTest(ctx, first.OwnerID, "local-device")
	if err != nil {
		t.Fatalf("create decoy daemon: %v", err)
	}

	// Reprovision WITHOUT a claim (identity file lost). It must mint a fresh
	// device — never adopt the decoy by its name.
	res, err := env.app.ProvisionLocalNode(ctx, app.ProvisionSpec{
		TokenPath: filepath.Join(dir, "atoll-token"),
	})
	if err != nil {
		t.Fatalf("reprovision: %v", err)
	}
	if res.DaemonID == decoyID || res.DaemonKey == decoyKey {
		t.Fatalf("provision adopted the name-colliding daemon %s — identity must go by claim, never by name", decoyID)
	}

	// A claim on a soft-deleted row mints fresh: the delete was authoritative.
	if _, err := env.db.Exec(
		`UPDATE daemons SET deleted_at = 1 WHERE id = ?`, res.DaemonID,
	); err != nil {
		t.Fatalf("soft-delete daemon: %v", err)
	}
	after, err := env.app.ProvisionLocalNode(ctx, app.ProvisionSpec{
		TokenPath: filepath.Join(dir, "atoll-token"), DaemonID: res.DaemonID,
	})
	if err != nil {
		t.Fatalf("provision after delete: %v", err)
	}
	if after.DaemonID == res.DaemonID {
		t.Fatalf("provision resurrected deleted daemon %s — the row is authoritative", res.DaemonID)
	}
}
