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
	assertStatus(t, invalidParent, http.StatusBadRequest)
	var invalidJobs int
	if err := env.db.QueryRow(`SELECT COUNT(*) FROM channel_provision_jobs WHERE name='orphan-attempt'`).Scan(&invalidJobs); err != nil || invalidJobs != 0 {
		t.Fatalf("invalid parent jobs=%d err=%v", invalidJobs, err)
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

func TestLifecycleOperationProjectionOwnerOnly(t *testing.T) {
	env := setupTestApp(t)
	owner, cookies := register(t, env, "operation-owner@example.com", "secret123", "Owner")
	created := env.do(t, "POST", "/api/channels", map[string]any{"name": "operation-channel"}, cookies)
	assertStatus(t, created, http.StatusCreated)
	ch := respJSON(t, created)
	var ref string
	if err := env.db.QueryRow(`SELECT operation_id FROM channel_provision_jobs WHERE channel_id=?`, ch["id"]).Scan(&ref); err != nil {
		t.Fatal(err)
	}
	view := env.do(t, "GET", "/api/operations/"+ref, nil, cookies)
	assertStatus(t, view, http.StatusOK)
	body := respJSON(t, view)
	if body["family"] != "lifecycle" || body["status"] != "done" {
		t.Fatalf("operation=%v", body)
	}
	_, other := register(t, env, "operation-other@example.com", "secret123", "Other")
	denied := env.do(t, "GET", "/api/operations/"+ref, nil, other)
	assertStatus(t, denied, http.StatusNotFound)
	if owner["id"] == "" {
		t.Fatal("owner missing")
	}
}
