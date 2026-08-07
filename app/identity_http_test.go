package app_test

// Identity HTTP face: register → login → me → create/list channels — the
// principal's first-session flow over the realm directory.

import (
	"net/http"
	"testing"
)

func TestRegisterLoginRealmChannel(t *testing.T) {
	env := setupTestApp(t)

	// 1. Register
	regBody, cookies := register(t, env, "alice@test.com", "pass1234", "Alice")
	userID := regBody["id"].(string)
	if userID == "" {
		t.Fatal("register returned empty user id")
	}
	if regBody["email"] != "alice@test.com" {
		t.Fatalf("register email mismatch: %v", regBody["email"])
	}

	// 2. Login
	loginBody, loginCookies := login(t, env, "alice@test.com", "pass1234")
	cookies = mergeCookies(cookies, loginCookies)
	if loginBody["id"] != userID {
		t.Fatalf("login user id mismatch: want %s got %v", userID, loginBody["id"])
	}

	// 3. GET /api/identity/me
	w := env.do(t, "GET", "/api/identity/me", nil, cookies)
	assertStatus(t, w, http.StatusOK)
	meBody := respJSON(t, w)
	if meBody["id"] != userID {
		t.Fatalf("me user id mismatch: want %s got %v", userID, meBody["id"])
	}
	if meBody["email"] != "alice@test.com" {
		t.Fatalf("me email mismatch: %v", meBody["email"])
	}

	// 4. Create channel directly in the realm directory.
	chBody, cookies := createChannel(t, env, cookies, "general")
	chID := chBody["id"].(string)
	if chID == "" {
		t.Fatal("channel id empty")
	}

	// 5. List realm channels.
	w = env.do(t, "GET", "/api/channels", nil, cookies)
	assertStatus(t, w, http.StatusOK)
	chListBody := respJSON(t, w)
	chList := chListBody["channels"].([]any)
	found := false
	for _, ch := range chList {
		m := ch.(map[string]any)
		if m["id"] == chID {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("channel %s not found in list: %v", chID, chList)
	}
}
