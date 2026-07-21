package app_test

import (
	"net/http"
	"testing"
	"time"
)

// TestDeleteDaemon_RevokePersistFails_Returns5xx pins the daemon-delete fix: if the
// revocation cannot reach durable storage, the handler must return 5xx (not a false
// ok) and leave the daemon intact — never silently drop the key while reporting
// success.
func TestDeleteDaemon_RevokePersistFails_Returns5xx(t *testing.T) {
	env := setupTestApp(t)
	_, cookies := register(t, env, "revoke@example.com", "secret123", "Owner")

	w := env.do(t, "POST", "/api/daemons", map[string]any{"name": "box"}, cookies)
	assertStatus(t, w, http.StatusCreated)
	daemonID := respJSON(t, w)["id"].(string)

	// Create a channel and bind the daemon. A failed realm revocation transaction
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
	// the whole revocation publish.
	if _, err := env.db.Exec(`CREATE TRIGGER fail_daemon_revoke_job
		BEFORE UPDATE OF deleted_at ON daemons WHEN NEW.deleted_at IS NOT NULL
		BEGIN SELECT RAISE(ABORT, 'forced revoke failure'); END`); err != nil {
		t.Fatalf("install revoke trigger: %v", err)
	}
	defer env.db.Exec(`DROP TRIGGER IF EXISTS fail_daemon_revoke_job`)
	w = env.do(t, "DELETE", "/api/daemons/"+daemonID, nil, cookies2)
	assertStatus(t, w, http.StatusInternalServerError)

	// The daemon must still exist — revocation was not persisted, so not reported ok.
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
		t.Fatalf("daemon deleted despite revocation-persist failure (false ok)")
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
		t.Fatal("channel binding was deleted despite the realm revocation transaction rolling back")
	}
}

// TestDeleteDaemon_HappyPath_RemovesBindings proves the realm revocation job
// eventually enumerates live channels and removes their channel-local bindings.
func TestDeleteDaemon_HappyPath_RemovesBindings(t *testing.T) {
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

	// Delete commits the daemon revocation and durable fanout obligation.
	w = env.do(t, "DELETE", "/api/daemons/"+daemonID, nil, cookies2)
	assertStatus(t, w, http.StatusOK)
	w = env.do(t, "DELETE", "/api/daemons/"+daemonID, nil, cookies2)
	assertStatus(t, w, http.StatusOK)

	// Daemon gone.
	w = env.do(t, "GET", "/api/daemons", nil, cookies2)
	assertStatus(t, w, http.StatusOK)
	ds, _ := respJSON(t, w)["daemons"].([]any)
	for _, d := range ds {
		if d.(map[string]any)["id"] == daemonID {
			t.Fatalf("daemon still present after delete")
		}
	}
	// Binding gone after the convergence patrol's next visit (poked by the
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
			t.Fatalf("channel binding survived daemon revocation convergence")
		}
		time.Sleep(20 * time.Millisecond)
	}
}
