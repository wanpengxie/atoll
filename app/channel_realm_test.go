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
	invalidParent := env.do(t, "POST", "/api/channels", map[string]any{"name": "orphan-attempt", "parent_id": "missing-parent"}, ownerCookies)
	assertStatus(t, invalidParent, http.StatusConflict)
	var invalidRows int
	if err := env.db.QueryRow(`SELECT COUNT(*) FROM channels WHERE name='orphan-attempt'`).Scan(&invalidRows); err != nil || invalidRows != 0 {
		t.Fatalf("invalid parent rows=%d err=%v", invalidRows, err)
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
	assertStatus(t, deleted, http.StatusOK)
	if body := respJSON(t, deleted); body["changed"] != true {
		t.Fatalf("first delete=%v", body)
	}
	replayed := env.do(t, "DELETE", "/api/channels/"+parentID, nil, outsiderCookies)
	assertStatus(t, replayed, http.StatusOK)
	if body := respJSON(t, replayed); body["changed"] != false {
		t.Fatalf("retiring/absent delete replay=%v", body)
	}
	missing := env.do(t, "DELETE", "/api/channels/never-existed", nil, outsiderCookies)
	assertStatus(t, missing, http.StatusOK)
	if body := respJSON(t, missing); body["changed"] != false {
		t.Fatalf("absent delete=%v", body)
	}
	if owner["id"] == "" {
		t.Fatal("owner identity missing")
	}
	list := env.do(t, "GET", "/api/channels?parent_id="+parentID, nil, ownerCookies)
	assertStatus(t, list, http.StatusOK)
	children := respJSON(t, list)["channels"].([]any)
	if len(children) != 1 || children[0].(map[string]any)["id"] != child["id"] {
		t.Fatalf("dead-parent lineage query=%v", children)
	}
	assertStatus(t, env.do(t, "GET", "/api/channels/"+parentID, nil, ownerCookies), http.StatusNotFound)
	childDetail := env.do(t, "GET", "/api/channels/"+child["id"].(string), nil, ownerCookies)
	assertStatus(t, childDetail, http.StatusOK)
	if respJSON(t, childDetail)["parent_id"] != parentID {
		t.Fatalf("child lost historical parent after retirement: %s", childDetail.Body.String())
	}
	assertStatus(t, env.do(t, "GET", "/api/channels/"+child["id"].(string)+"/messages", nil, ownerCookies), http.StatusOK)
}

func TestRetiredContainerRoutesAreAbsent(t *testing.T) {
	env := setupTestApp(t)
	_, cookies := register(t, env, "route-check@example.com", "secret123", "Route")
	for _, path := range []string{"/api/" + "work" + "spaces", "/api/" + "work" + "spaces/x/channels"} {
		resp := env.do(t, "GET", path, nil, cookies)
		assertStatus(t, resp, http.StatusNotFound)
	}
}

func TestLifecycleResponsesExposeNoOperationProjection(t *testing.T) {
	env := setupTestApp(t)
	_, cookies := register(t, env, "operation-owner@example.com", "secret123", "Owner")
	created := env.do(t, "POST", "/api/channels", map[string]any{"name": "operation-channel"}, cookies)
	assertStatus(t, created, http.StatusCreated)
	body := respJSON(t, created)
	if _, exists := body["operation_id"]; exists {
		t.Fatalf("create leaked operation id: %v", body)
	}
	assertStatus(t, env.do(t, "GET", "/api/operations/anything", nil, cookies), http.StatusNotFound)
}
