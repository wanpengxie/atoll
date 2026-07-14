package app_test

// TestCreateChannelPokeAfterDirectoryCommit (连接模型勘误期 P1-4, 六轮终审): the creator's
// two Admits inside handleCreateChannel each fire a membership-change poke SYNCHRONOUSLY
// — before this test's fix, both fired while the `channels` directory row was still
// uncommitted, so a session poked at that instant re-resolved into the SAME stale
// "channel does not exist yet" answer (EntitlementSnapshot enumerates the directory
// table). This test proves the REAL write order end-to-end: a live gateway session is
// attached BEFORE the channel is created over real HTTP, then a submit naming the new
// channel is driven immediately — no manual resolver edit, no hand-called g.Poke — and
// must succeed within a short bound, proving the post-commit poke (not the 30s T_sweep
// backstop) is what converged it.

import (
	"net/http"
	"path/filepath"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/app"
	"github.com/wanpengxie/atoll/drivers/gateway"
	"github.com/wanpengxie/atoll/drivers/gateway/connector/web"
	"github.com/wanpengxie/atoll/platform/subjectgate"
	"golang.org/x/crypto/bcrypt"
)

func TestCreateChannelPokeAfterDirectoryCommit(t *testing.T) {
	t.Cleanup(app.SetBcryptCostForTest(bcrypt.MinCost))

	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "app.db")
	chDBDir := filepath.Join(tmpDir, "channels")

	db, err := openTestAppDB(t, dbPath)
	if err != nil {
		t.Fatalf("OpenDB: %v", err)
	}
	a, err := app.New(app.Config{DB: db, ChannelDBDir: chDBDir})
	if err != nil {
		db.Close()
		t.Fatalf("app.New: %v", err)
	}

	gw, err := gateway.New(gateway.Config{
		Routing:  a.ResolveRoutingForGateway,
		Resolver: testGatewayResolver(a),
	})
	if err != nil {
		t.Fatalf("gateway.New: %v", err)
	}
	gw.Start()
	a.SetGateway(web.New(gw))
	a.SetMembershipPoke(gw.Poke)
	t.Cleanup(func() {
		gw.Close()
		a.Close()
		db.Close()
	})

	env := &testEnv{handler: a.Handler(), app: a, tmpDir: tmpDir}

	regBody, cookies := register(t, env, "poke@example.com", "secret123", "Poke")
	userID, _ := regBody["id"].(string)
	if userID == "" {
		t.Fatal("register returned empty user id")
	}
	wsBody, cookies := createWorkspace(t, env, cookies, "poke-ws")
	wsID, _ := wsBody["id"].(string)
	_ = cookies

	// Attach the creator's gateway session BEFORE the channel exists — its first
	// (synchronous) reconcile sees an empty entitlement set, exactly the steady-state a
	// long-lived connection is in when the user goes on to create a new channel.
	s, err := gw.Attach(userID, nil)
	if err != nil {
		t.Fatalf("gw.Attach: %v", err)
	}
	s.StartFeed()
	defer s.Close()

	// Create the channel over REAL HTTP (the actual write order under test: two Admit
	// pokes pre-commit, then handleCreateChannel's post-commit poke).
	w := env.do(t, "POST", "/api/workspaces/"+wsID+"/channels", map[string]any{"name": "fresh"}, cookies)
	assertStatus(t, w, http.StatusCreated)
	chBody := respJSON(t, w)
	chID, _ := chBody["id"].(string)
	if chID == "" {
		t.Fatal("create channel returned empty id")
	}

	// Drive a submit naming the brand-new channel IMMEDIATELY (no sleep, no manual
	// g.Poke, no resolver hand-edit). It must stop being forbidden within a bound far
	// short of T_sweep (30s) — proving the post-commit poke drove convergence.
	deadline := time.After(2 * time.Second)
	tick := time.NewTicker(2 * time.Millisecond)
	defer tick.Stop()
	for {
		f, ferr := subjectgate.NewFrame(subjectgate.FrameSubmit, "poke-ref",
			subjectgate.SubmitPayload{ChannelID: chID, MsgType: "human.message"})
		if ferr != nil {
			t.Fatalf("NewFrame: %v", ferr)
		}
		resp := s.Upstream(f)
		if resp.Type != subjectgate.FrameError {
			break // resolved eligible — the submit reached the door (whatever it decides next is out of scope here).
		}
		var ep subjectgate.ErrorPayload
		_ = resp.DecodePayload(&ep)
		if ep.Code != subjectgate.CodeForbidden {
			// unavailable/closed etc. are not the bug under test — any non-forbidden
			// verdict proves the channel_id resolved to a known route.
			break
		}
		select {
		case <-tick.C:
			continue
		case <-deadline:
			t.Fatalf("channel %s did not become eligible within 2s of its HTTP creation — the creator's live session must subscribe on the post-commit poke (P1-4), not wait T_sweep=%s", chID, 30*time.Second)
		}
	}
}
