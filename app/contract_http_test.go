package app_test

import (
	"net/http"
	"testing"

	"github.com/wanpengxie/atoll/app/contract"
)

func TestContractMetaAndStrictRESTWrite(t *testing.T) {
	env := setupTestApp(t)

	meta := env.do(t, http.MethodGet, "/api/meta", nil, nil)
	assertStatus(t, meta, http.StatusOK)
	if got := respJSON(t, meta)["contract_version"]; got != contract.Version {
		t.Fatalf("contract version=%v want %s", got, contract.Version)
	}

	bad := env.do(t, http.MethodPost, "/api/identity/register", map[string]any{
		"email": "strict@example.com", "password": "secret", "display_name": "Strict",
		"unknown_field": true,
	}, nil)
	assertStatus(t, bad, http.StatusBadRequest)
	body := respJSON(t, bad)
	if body["code"] != string(contract.CodeBadPayload) || body["message"] == "" || body["error"] != nil {
		t.Fatalf("non-contract error response: %v", body)
	}
}

func TestRESTErrorHasOneContractShape(t *testing.T) {
	env := setupTestApp(t)
	response := env.do(t, http.MethodGet, "/api/channels", nil, nil)
	assertStatus(t, response, http.StatusUnauthorized)
	body := respJSON(t, response)
	if body["code"] != string(contract.CodeNotAuthenticated) || body["message"] == "" {
		t.Fatalf("bad error shape: %v", body)
	}
	for _, retired := range []string{"error", "error_code", "retry"} {
		if _, ok := body[retired]; ok {
			t.Fatalf("legacy field %q remains in %v", retired, body)
		}
	}
}

func TestBodylessWritesRejectPayloadBeforeSideEffects(t *testing.T) {
	env := setupTestApp(t)
	setup := fullSetup(t, env)

	join := env.do(t, http.MethodPost, "/api/channels/"+setup.chID+"/join", map[string]any{"unexpected": true}, setup.cookies)
	assertStatus(t, join, http.StatusBadRequest)
	if got := respJSON(t, join)["code"]; got != string(contract.CodeBadPayload) {
		t.Fatalf("join code=%v want %s", got, contract.CodeBadPayload)
	}

	deleted := env.do(t, http.MethodDelete, "/api/channels/"+setup.chID, map[string]any{"unexpected": true}, setup.cookies)
	assertStatus(t, deleted, http.StatusBadRequest)
	stillPresent := env.do(t, http.MethodGet, "/api/channels/"+setup.chID, nil, setup.cookies)
	assertStatus(t, stillPresent, http.StatusOK)

	logout := env.do(t, http.MethodPost, "/api/identity/logout", map[string]any{"unexpected": true}, setup.cookies)
	assertStatus(t, logout, http.StatusBadRequest)
	stillLoggedIn := env.do(t, http.MethodGet, "/api/identity/me", nil, setup.cookies)
	assertStatus(t, stillLoggedIn, http.StatusOK)
}
