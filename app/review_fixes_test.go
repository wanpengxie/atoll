package app_test

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/platform/compute"
	"github.com/wanpengxie/atoll/protocol/channel"
)

// TestDeleteDaemonTombstonePersistFailureReturns5xx pins the daemon-delete fix: if the
// tombstone cannot reach durable storage, the handler must return 5xx (not a false
// ok) and leave the daemon intact — never silently drop the key while reporting
// success.
func TestDeleteDaemonTombstonePersistFailureReturns5xx(t *testing.T) {
	env := setupTestApp(t)
	_, cookies := register(t, env, "revoke@example.com", "secret123", "Owner")

	w := env.do(t, "POST", "/api/daemons", map[string]any{"name": "box"}, cookies)
	assertStatus(t, w, http.StatusCreated)
	daemonID := respJSON(t, w)["id"].(string)

	// Create a channel and bind the daemon. A failed realm tombstone transaction
	// must leave both the daemon and its independently authoritative channel binding.
	cookies2 := cookies
	chBody := env.do(t, "POST", "/api/channels", map[string]any{"name": "c"}, cookies2)
	assertStatus(t, chBody, http.StatusCreated)
	chID := respJSON(t, chBody)["id"].(string)
	w = env.do(t, "POST", "/api/channels/"+chID+"/daemons",
		map[string]any{"daemon_id": daemonID}, cookies2)
	assertStatus(t, w, http.StatusOK)

	// A normal schema trigger (not TEMP/connection-local) fails the authority
	// tombstone UPDATE itself — that single value write IS
	// the whole tombstone publication.
	if _, err := env.db.Exec(`CREATE TRIGGER fail_daemon_tombstone
		BEFORE UPDATE OF deleted_at ON daemons WHEN NEW.deleted_at IS NOT NULL
		BEGIN SELECT RAISE(ABORT, 'forced revoke failure'); END`); err != nil {
		t.Fatalf("install revoke trigger: %v", err)
	}
	defer env.db.Exec(`DROP TRIGGER IF EXISTS fail_daemon_tombstone`)
	w = env.do(t, "DELETE", "/api/daemons/"+daemonID, nil, cookies2)
	assertStatus(t, w, http.StatusInternalServerError)

	// The daemon must still exist — the tombstone was not persisted, so not reported ok.
	w = env.do(t, "GET", "/api/daemons", nil, cookies2)
	assertStatus(t, w, http.StatusOK)
	ds, _ := respJSON(t, w)["daemons"].([]any)
	found := false
	for _, d := range ds {
		if d.(map[string]any)["id"] == daemonID {
			found = true
		}
	}
	if !found {
		t.Fatalf("daemon deleted despite tombstone-persist failure (false ok)")
	}
	// And the daemon-channel binding must survive: the tx rolled back its FIRST
	// write, not left it half-applied (the whole point of the transaction fix).
	w = env.do(t, "GET", "/api/channels/"+chID+"/daemons", nil, cookies2)
	assertStatus(t, w, http.StatusOK)
	cds, _ := respJSON(t, w)["daemons"].([]any)
	bindingSurvived := false
	for _, d := range cds {
		if d.(map[string]any)["id"] == daemonID {
			bindingSurvived = true
		}
	}
	if !bindingSurvived {
		t.Fatal("channel binding was deleted despite the realm tombstone transaction rolling back")
	}
}

func TestDeleteDaemonRevokesDeviceAndKeepsBinding(t *testing.T) {
	env := setupTestApp(t)
	server := httptest.NewServer(env.handler)
	defer server.Close()
	_, cookies := register(t, env, "revoke-device@example.com", "secret123", "Owner")

	created := env.do(t, http.MethodPost, "/api/daemons", map[string]any{"name": "box"}, cookies)
	assertStatus(t, created, http.StatusCreated)
	daemonID := respJSON(t, created)["id"].(string)
	channelResponse := env.do(t, http.MethodPost, "/api/channels", map[string]any{"name": "c"}, cookies)
	assertStatus(t, channelResponse, http.StatusCreated)
	channelID := respJSON(t, channelResponse)["id"].(string)
	bound := env.do(t, http.MethodPost, "/api/channels/"+channelID+"/daemons",
		map[string]any{"daemon_id": daemonID}, cookies)
	assertStatus(t, bound, http.StatusOK)

	ctx, cancel := context.WithCancel(context.Background())
	runErr := make(chan error, 1)
	computeConfig := daemonComputeConfig(
		t,
		fmt.Sprintf("ws://%s/compute", server.Listener.Addr()),
		respJSON(t, created)["api_key"].(string),
		&e2eLinkPlan{chID: channel.ID(channelID)},
		nil,
	)
	go func() {
		runErr <- compute.Run(ctx, computeConfig)
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case <-runErr:
		case <-time.After(3 * time.Second):
		}
	})
	waitDaemonComposition(t, func() bool {
		listed := env.do(t, http.MethodGet, "/api/daemons", nil, cookies)
		if listed.Code != http.StatusOK {
			return false
		}
		for _, raw := range respJSON(t, listed)["daemons"].([]any) {
			daemon := raw.(map[string]any)
			if daemon["id"] == daemonID && daemon["online"] == true {
				return true
			}
		}
		return false
	}, "device carrier never became online")

	deleted := env.do(t, http.MethodDelete, "/api/daemons/"+daemonID, nil, cookies)
	assertStatus(t, deleted, http.StatusOK)
	body := respJSON(t, deleted)
	if body["authority_committed"] != true || body["convergence"] != "revoked" {
		t.Fatalf("delete response=%#v", body)
	}
	// Revocation is recorded on its own. There is no companion "the device never
	// confirmed" note any more: this host holds no projection of the device's
	// compartments, so there is nothing whose absence it could report on.
	diagnostics := body["diagnostics"].([]any)
	var sawRevoke bool
	for _, raw := range diagnostics {
		if raw.(map[string]any)["kind"] == "revoke" {
			sawRevoke = true
		}
	}
	if !sawRevoke {
		t.Fatalf("delete diagnostics=%#v, want the revoke record", diagnostics)
	}
	select {
	case err := <-runErr:
		if err == nil || !strings.Contains(err.Error(), "daemon revoked") {
			t.Fatalf("compute result=%v, want terminal daemon-revoked result", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("compute kept running after daemon revocation")
	}

	stillBound, err := env.app.DaemonBoundForTest(channel.ID(channelID), daemonID)
	if err != nil {
		t.Fatal(err)
	}
	if !stillBound {
		t.Fatalf("realm tombstone removed channel-local binding %q", daemonID)
	}
}

func TestComputeRejectsQueryCredentialAndMalformedBearer(t *testing.T) {
	env := setupTestApp(t)
	query := env.do(t, http.MethodGet, "/compute?key=must-not-enter-logs", nil, nil)
	assertStatus(t, query, http.StatusUnauthorized)

	missing := env.do(t, http.MethodGet, "/compute", nil, nil)
	assertStatus(t, missing, http.StatusBadRequest)
	malformed := env.doHeaders(t, http.MethodGet, "/compute", nil, nil, map[string]string{
		"Authorization": "Basic credentials",
	})
	assertStatus(t, malformed, http.StatusBadRequest)
	invalid := env.doHeaders(t, http.MethodGet, "/compute", nil, nil, map[string]string{
		"Authorization": "Bearer invalid",
	})
	assertStatus(t, invalid, http.StatusUnauthorized)
}

func TestChannelDaemonListIncludesBindingsOwnedByOtherMembers(t *testing.T) {
	env := setupTestApp(t)
	_, ownerCookies := register(t, env, "channel-owner@example.com", "secret123", "Owner")
	created := env.do(t, http.MethodPost, "/api/channels", map[string]any{"name": "shared"}, ownerCookies)
	assertStatus(t, created, http.StatusCreated)
	channelID := respJSON(t, created)["id"].(string)

	_, memberCookies := register(t, env, "daemon-owner@example.com", "secret123", "Member")
	joined := env.do(t, http.MethodPost, "/api/channels/"+channelID+"/join", nil, memberCookies)
	if joined.Code != http.StatusCreated && joined.Code != http.StatusOK {
		t.Fatalf("join status=%d body=%s", joined.Code, joined.Body.String())
	}
	daemon := env.do(t, http.MethodPost, "/api/daemons", map[string]any{"name": "member-box"}, memberCookies)
	assertStatus(t, daemon, http.StatusCreated)
	daemonID := respJSON(t, daemon)["id"].(string)
	bound := env.do(t, http.MethodPost, "/api/channels/"+channelID+"/daemons", map[string]any{"daemon_id": daemonID}, memberCookies)
	assertStatus(t, bound, http.StatusOK)

	listed := env.do(t, http.MethodGet, "/api/channels/"+channelID+"/daemons", nil, ownerCookies)
	assertStatus(t, listed, http.StatusOK)
	for _, raw := range respJSON(t, listed)["daemons"].([]any) {
		if raw.(map[string]any)["id"] == daemonID {
			return
		}
	}
	t.Fatalf("channel daemon list hid member-owned binding %s", daemonID)
}

func TestChannelDaemonListRejectsNonMember(t *testing.T) {
	env := setupTestApp(t)
	_, ownerCookies := register(t, env, "roster-owner@example.com", "secret123", "Owner")
	created := env.do(t, http.MethodPost, "/api/channels", map[string]any{"name": "private-roster"}, ownerCookies)
	assertStatus(t, created, http.StatusCreated)
	channelID := respJSON(t, created)["id"].(string)

	_, outsiderCookies := register(t, env, "roster-outsider@example.com", "secret123", "Outsider")
	listed := env.do(t, http.MethodGet, "/api/channels/"+channelID+"/daemons", nil, outsiderCookies)
	assertStatus(t, listed, http.StatusForbidden)
}

func TestActorDeclListUsesRelationIndexWhenChannelUnavailable(t *testing.T) {
	env := setupTestApp(t)
	setup := fullSetup(t, env)
	env.app.DropHomeForTest(channel.ID(setup.chID))

	listed := env.do(t, http.MethodGet, "/api/actor-decls", nil, setup.cookies)
	assertStatus(t, listed, http.StatusOK)
}
