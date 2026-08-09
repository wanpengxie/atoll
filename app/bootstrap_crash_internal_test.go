package app

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/platform/channelhost"
	"github.com/wanpengxie/atoll/protocol/channel"
	"golang.org/x/crypto/bcrypt"
)

func TestProvisionLocalNodeConvergesCodexAfterEachCrashCut(t *testing.T) {
	for _, cut := range []string{"declaration", "introduce", "set-default-submit", "default-observed"} {
		t.Run(cut, func(t *testing.T) {
			a := newBootstrapCrashApp(t)
			ctx := context.Background()
			tokenPath := filepath.Join(t.TempDir(), "atoll-token")
			foundation := provisionFoundationForCrashCut(t, a, tokenPath)
			homeID := channel.ID(foundation.HomeChannelID)
			declID := stableBootstrapCodexDeclID(foundation.OwnerID)

			if _, err := a.createDeclarationCore(ctx, declID, bootstrapCodexName, foundation.OwnerID, "codex", nil, "private"); err != nil {
				t.Fatalf("cut A declaration: %v", err)
			}
			if cut != "declaration" {
				if _, err := forwardSysop(ctx, a, homeID, introduceCall(ctx, foundation.OwnerID, declID, nil)); err != nil {
					t.Fatalf("cut B introduce: %v", err)
				}
			}
			if cut == "set-default-submit" || cut == "default-observed" {
				bundle, err := a.acquireBundle(ctx, homeID)
				if err != nil {
					t.Fatalf("acquire home: %v", err)
				}
				if err := submitDefaultAgent(ctx, bundle, foundation.OwnerID, homeID, declID); err != nil {
					t.Fatalf("cut C submit: %v", err)
				}
			}
			if cut == "default-observed" {
				if err := a.convergeDefaultAgent(ctx, foundation.OwnerID, homeID, declID); err != nil {
					t.Fatalf("cut D observe: %v", err)
				}
			}

			// A process crash leaves the committed truth above in place. A fresh
			// provisioning pass must only fill the remaining predicates.
			result, err := a.ProvisionLocalNode(ctx, ProvisionSpec{TokenPath: tokenPath, DaemonID: foundation.DaemonID})
			if err != nil {
				t.Fatalf("converge after %s cut: %v", cut, err)
			}
			assertBootstrapCodexConverged(t, a, result)

			again, err := a.ProvisionLocalNode(ctx, ProvisionSpec{TokenPath: tokenPath, DaemonID: result.DaemonID})
			if err != nil {
				t.Fatalf("completed-state rerun after %s: %v", cut, err)
			}
			assertBootstrapCodexConverged(t, a, again)
		})
	}
}

func newBootstrapCrashApp(t *testing.T) *App {
	t.Helper()
	t.Cleanup(SetBcryptCostForTest(bcrypt.MinCost))
	root := t.TempDir()
	process, err := OpenProcessDB(filepath.Join(root, "app.db"), true)
	if err != nil {
		t.Fatal(err)
	}
	a, err := New(Config{
		DB: process.DB,
		HostFactory: func(deps channelhost.HomeDeps) (channelhost.LocalHost, error) {
			return channelhost.New(filepath.Join(root, "channels"), deps)
		},
	})
	if err != nil {
		_ = process.Close()
		t.Fatal(err)
	}
	a.Start()
	t.Cleanup(func() {
		_ = a.Close(context.Background())
		_ = process.Close()
	})
	return a
}

func provisionFoundationForCrashCut(t *testing.T, a *App, tokenPath string) ProvisionResult {
	t.Helper()
	ctx := context.Background()
	if _, err := BootstrapOwnerToken(ctx, a.db, tokenPath); err != nil {
		t.Fatal(err)
	}
	var owner string
	if err := a.db.QueryRowContext(ctx, `SELECT id FROM users WHERE email=?`, bootstrapOwnerEmail).Scan(&owner); err != nil {
		t.Fatal(err)
	}
	accepted, _, conflict, _, err := a.createGroupChannel(ctx, owner, homeChannelName, nil)
	if err != nil || conflict {
		t.Fatalf("create home: conflict=%v err=%v", conflict, err)
	}
	daemonID, daemonKey, err := a.claimOrMintDaemon(ctx, owner, "")
	if err != nil {
		t.Fatal(err)
	}
	var attachErr error
	for attempt := 0; attempt < 30; attempt++ {
		if _, attachErr = a.attachDaemonCore(ctx, owner, accepted.ID, daemonID); attachErr == nil {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if attachErr != nil {
		t.Fatal(attachErr)
	}
	return ProvisionResult{TokenPath: tokenPath, OwnerID: owner, HomeChannelID: string(accepted.ID), DaemonID: daemonID, DaemonKey: daemonKey}
}

func assertBootstrapCodexConverged(t *testing.T, a *App, result ProvisionResult) {
	t.Helper()
	declID := stableBootstrapCodexDeclID(result.OwnerID)
	var declarations int
	if err := a.db.QueryRow(`SELECT COUNT(*) FROM actor_decls WHERE id=? AND owner=? AND default_class='codex' AND deleted_at IS NULL`, declID, result.OwnerID).Scan(&declarations); err != nil || declarations != 1 {
		t.Fatalf("declarations=%d err=%v", declarations, err)
	}
	bundle, err := a.acquireBundle(context.Background(), channel.ID(result.HomeChannelID))
	if err != nil {
		t.Fatal(err)
	}
	instances, err := bundle.View().DeclaredInstances(context.Background(), declID)
	if err != nil || len(instances) != 1 {
		t.Fatalf("instances=%v err=%v", instances, err)
	}
	defaultAgent, found, err := bundle.View().DefaultAgent(context.Background())
	if err != nil || !found || defaultAgent != instances[0] {
		t.Fatalf("default=%q found=%v instances=%v err=%v", defaultAgent, found, instances, err)
	}
}
