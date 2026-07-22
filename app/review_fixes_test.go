package app_test

import (
	"net/http"
	"testing"
	"time"

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

// TestDeleteDaemonTombstonePullRemovesBindings proves a permanent realm value plus
// Home pull eventually removes the channel-local binding.
func TestDeleteDaemonTombstonePullRemovesBindings(t *testing.T) {
	env := setupTestApp(t)
	_, cookies := register(t, env, "kick@example.com", "secret123", "Owner")

	w := env.do(t, "POST", "/api/daemons", map[string]any{"name": "box"}, cookies)
	assertStatus(t, w, http.StatusCreated)
	daemonID := respJSON(t, w)["id"].(string)

	cookies2 := cookies
	chBody := env.do(t, "POST", "/api/channels", map[string]any{"name": "c"}, cookies2)
	assertStatus(t, chBody, http.StatusCreated)
	chID := respJSON(t, chBody)["id"].(string)
	w = env.do(t, "POST", "/api/channels/"+chID+"/daemons",
		map[string]any{"daemon_id": daemonID}, cookies2)
	assertStatus(t, w, http.StatusOK)

	// Delete commits the permanent daemon tombstone; the response reports authority
	// commitment separately from the bounded convergence observation.
	w = env.do(t, "DELETE", "/api/daemons/"+daemonID, nil, cookies2)
	assertStatus(t, w, http.StatusOK)
	first := respJSON(t, w)
	if first["authority_committed"] != true || (first["convergence"] != "observed_clear" && first["convergence"] != "convergence_pending") {
		t.Fatalf("first delete response=%#v", first)
	}
	w = env.do(t, "DELETE", "/api/daemons/"+daemonID, nil, cookies2)
	assertStatus(t, w, http.StatusOK)
	second := respJSON(t, w)
	if second["authority_committed"] != true || (second["convergence"] != "observed_clear" && second["convergence"] != "convergence_pending") {
		t.Fatalf("idempotent delete response=%#v", second)
	}

	// Daemon gone.
	w = env.do(t, "GET", "/api/daemons", nil, cookies2)
	assertStatus(t, w, http.StatusOK)
	ds, _ := respJSON(t, w)["daemons"].([]any)
	for _, d := range ds {
		if d.(map[string]any)["id"] == daemonID {
			t.Fatalf("daemon still present after delete")
		}
	}
	// Binding gone after Home's next pull (poked by the
	// delete handler; level semantics — poll, don't assume synchronous).
	deadline := time.Now().Add(5 * time.Second)
	for {
		w = env.do(t, "GET", "/api/channels/"+chID+"/daemons", nil, cookies2)
		assertStatus(t, w, http.StatusOK)
		cds, _ := respJSON(t, w)["daemons"].([]any)
		survived := false
		for _, d := range cds {
			if d.(map[string]any)["id"] == daemonID {
				survived = true
			}
		}
		if !survived {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("channel binding survived daemon tombstone convergence")
		}
		time.Sleep(20 * time.Millisecond)
	}
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

func TestActorDeclListFailsWhenChannelProjectionIsUnavailable(t *testing.T) {
	env := setupTestApp(t)
	setup := fullSetup(t, env)
	env.app.DropHomeForTest(channel.ID(setup.chID))

	listed := env.do(t, http.MethodGet, "/api/actor-decls", nil, setup.cookies)
	assertStatus(t, listed, http.StatusServiceUnavailable)
}
