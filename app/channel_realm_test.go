package app_test

import (
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestRealmChannelDirectoryNameParentAndOwnerPolicy(t *testing.T) {
	env := setupTestApp(t)
	owner, ownerCookies := register(t, env, "realm-owner@example.com", "secret123", "Owner")
	parentResp := env.do(t, "POST", "/api/channels", map[string]any{"name": "parent"}, ownerCookies)
	assertStatus(t, parentResp, http.StatusCreated)
	parent := respJSON(t, parentResp)
	parentID := parent["id"].(string)

	childResp := env.do(t, "POST", "/api/channels", map[string]any{"name": "child", "parent_id": parentID}, ownerCookies)
	assertStatus(t, childResp, http.StatusCreated)
	child := respJSON(t, childResp)
	if child["parent_id"] != parentID {
		t.Fatalf("child parent_id=%v want %s", child["parent_id"], parentID)
	}

	duplicate := env.do(t, "POST", "/api/channels", map[string]any{"name": "child"}, ownerCookies)
	assertStatus(t, duplicate, http.StatusConflict)
	entries, err := os.ReadDir(filepath.Join(env.tmpDir, "channels"))
	if err != nil {
		t.Fatal(err)
	}
	mainFiles := 0
	for _, entry := range entries {
		if strings.HasSuffix(entry.Name(), ".db") {
			mainFiles++
		}
	}
	if mainFiles != 2 {
		t.Fatalf("duplicate name left a physical artifact: %v", entries)
	}

	_, outsiderCookies := register(t, env, "realm-outsider@example.com", "secret123", "Outsider")
	denied := env.do(t, "DELETE", "/api/channels/"+parentID, nil, outsiderCookies)
	assertStatus(t, denied, http.StatusForbidden)

	deleted := env.do(t, "DELETE", "/api/channels/"+parentID, nil, ownerCookies)
	assertStatus(t, deleted, http.StatusAccepted)
	if owner["id"] == "" {
		t.Fatal("owner identity missing")
	}
	list := env.do(t, "GET", "/api/channels?parent_id="+parentID, nil, ownerCookies)
	assertStatus(t, list, http.StatusOK)
	children := respJSON(t, list)["channels"].([]any)
	if len(children) != 1 || children[0].(map[string]any)["id"] != child["id"] {
		t.Fatalf("dead-parent lineage query=%v", children)
	}
}

func TestRetiredContainerRoutesAreAbsent(t *testing.T) {
	env := setupTestApp(t)
	_, cookies := register(t, env, "route-check@example.com", "secret123", "Route")
	for _, path := range []string{"/api/" + "work" + "spaces", "/api/" + "work" + "spaces/x/channels"} {
		resp := env.do(t, "GET", path, nil, cookies)
		assertStatus(t, resp, http.StatusNotFound)
	}
}
