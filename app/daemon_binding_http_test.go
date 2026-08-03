package app_test

// Daemon↔channel binding over the HTTP face: attach/detach round-trip, the
// owner's plaintext api_key recovery, list visibility (member-owned bindings
// show to every member; non-members are refused), and the explicit 503 when
// the channel's serving image is unavailable.

import (
	"fmt"
	"net/http"
	"testing"

	"github.com/wanpengxie/atoll/protocol/channel"
)

func TestDaemonCreateAttachDetach(t *testing.T) {
	env := setupTestApp(t)
	s := fullSetup(t, env)

	daemonBody := createAndBindDaemon(t, env, s.chID, "test-daemon", s.cookies)
	daemonID := daemonBody["id"].(string)
	if daemonID == "" {
		t.Fatal("daemon id empty")
	}
	apiKey := daemonBody["api_key"].(string)
	if apiKey == "" {
		t.Fatal("daemon api_key empty")
	}
	owned := env.do(t, "GET", "/api/daemons", nil, s.cookies)
	assertStatus(t, owned, http.StatusOK)
	var recovered bool
	for _, item := range respJSON(t, owned)["daemons"].([]any) {
		daemon := item.(map[string]any)
		if daemon["id"] == daemonID {
			recovered = daemon["api_key"] == apiKey
		}
	}
	if !recovered {
		t.Fatal("owner daemon list did not recover the plaintext api_key")
	}

	// List channel daemons -- should contain the one we just created.
	w := env.do(t, "GET", fmt.Sprintf("/api/channels/%s/daemons", s.chID), nil, s.cookies)
	assertStatus(t, w, http.StatusOK)
	listBody := respJSON(t, w)
	daemons := listBody["daemons"].([]any)
	found := false
	for _, d := range daemons {
		m := d.(map[string]any)
		if m["id"] == daemonID {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("daemon %s not found in channel daemons list", daemonID)
	}

	// Detach daemon from channel.
	w = env.do(t, "DELETE", fmt.Sprintf("/api/channels/%s/daemons/%s", s.chID, daemonID), nil, s.cookies)
	assertStatus(t, w, http.StatusOK)

	// List channel daemons again -- should NOT contain the detached daemon.
	w = env.do(t, "GET", fmt.Sprintf("/api/channels/%s/daemons", s.chID), nil, s.cookies)
	assertStatus(t, w, http.StatusOK)
	listBody = respJSON(t, w)
	daemons = listBody["daemons"].([]any)
	for _, d := range daemons {
		m := d.(map[string]any)
		if m["id"] == daemonID {
			t.Fatalf("daemon %s should not be in channel daemons after detach", daemonID)
		}
	}
}

func TestDetachDaemonUnavailableChannelIsExplicit(t *testing.T) {
	env := setupTestApp(t)
	s := fullSetup(t, env)
	daemonID := createAndBindDaemon(t, env, s.chID, "count-failure-daemon", s.cookies)["id"].(string)
	if err := env.app.CloseHomeForTest(channel.ID(s.chID)); err != nil {
		t.Fatal(err)
	}
	w := env.do(t, "DELETE", fmt.Sprintf("/api/channels/%s/daemons/%s", s.chID, daemonID), nil, s.cookies)
	assertStatus(t, w, http.StatusServiceUnavailable)
	if body := respJSON(t, w); body["retry"] != "safe" {
		t.Fatalf("detach unknown=%v", body)
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
