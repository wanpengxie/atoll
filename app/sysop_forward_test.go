package app_test

import (
	"net/http"
	"testing"

	"github.com/wanpengxie/atoll/protocol/channel"
)

func TestFiveSysopWordsUsePredicateBeforeQualification(t *testing.T) {
	env := setupTestApp(t)
	setup := fullSetup(t, env)
	createAndBindDaemon(t, env, setup.chID, "sysop-placement", setup.cookies)

	_, outsiderCookies := register(t, env, "sysop-outsider@example.com", "secret123", "Outsider")
	decl := env.do(t, http.MethodPost, "/api/actor-decls", map[string]any{
		"name": "sysop-target", "class": "go-kimi", "visibility": "public",
	}, setup.cookies)
	assertStatus(t, decl, http.StatusCreated)
	declID := respJSON(t, decl)["id"].(string)

	deniedIntroduce := env.do(t, http.MethodPost,
		"/api/channels/"+setup.chID+"/actors", map[string]any{"decl_id": declID}, outsiderCookies)
	assertStatus(t, deniedIntroduce, http.StatusForbidden)

	introduced := env.doHeaders(t, http.MethodPost,
		"/api/channels/"+setup.chID+"/actors", map[string]any{"decl_id": declID}, setup.cookies,
		map[string]string{"Idempotency-Key": "ignored-introduce-key"})
	assertStatus(t, introduced, http.StatusCreated)
	introducedBody := respJSON(t, introduced)
	target := introducedBody["actor_id"].(string)
	if introducedBody["changed"] != true {
		t.Fatalf("introduce=%v", introducedBody)
	}

	// Already-achieved introduce is accepted before the membership gate, even
	// for a caller that was forbidden while the predicate was false.
	introduceReplay := env.do(t, http.MethodPost,
		"/api/channels/"+setup.chID+"/actors", map[string]any{"decl_id": declID}, outsiderCookies)
	assertStatus(t, introduceReplay, http.StatusOK)
	if body := respJSON(t, introduceReplay); body["changed"] != false {
		t.Fatalf("introduce replay=%v", body)
	}

	deniedRemove := env.do(t, http.MethodDelete,
		"/api/channels/"+setup.chID+"/actors/"+target, nil, outsiderCookies)
	assertStatus(t, deniedRemove, http.StatusForbidden)
	removed := env.do(t, http.MethodDelete,
		"/api/channels/"+setup.chID+"/actors/"+target, nil, setup.cookies)
	assertStatus(t, removed, http.StatusOK)
	if body := respJSON(t, removed); body["changed"] != true {
		t.Fatalf("remove=%v", body)
	}
	removeReplay := env.do(t, http.MethodDelete,
		"/api/channels/"+setup.chID+"/actors/"+target, nil, outsiderCookies)
	assertStatus(t, removeReplay, http.StatusOK)
	if body := respJSON(t, removeReplay); body["changed"] != false {
		t.Fatalf("remove replay=%v", body)
	}

	daemon := env.do(t, http.MethodPost, "/api/daemons",
		map[string]any{"name": "sysop-daemon"}, setup.cookies)
	assertStatus(t, daemon, http.StatusCreated)
	daemonID := respJSON(t, daemon)["id"].(string)
	attached := env.do(t, http.MethodPost, "/api/channels/"+setup.chID+"/daemons",
		map[string]any{"daemon_id": daemonID}, setup.cookies)
	assertStatus(t, attached, http.StatusOK)
	if body := respJSON(t, attached); body["changed"] != true {
		t.Fatalf("attach=%v", body)
	}
	attachReplay := env.do(t, http.MethodPost, "/api/channels/"+setup.chID+"/daemons",
		map[string]any{"daemon_id": daemonID}, setup.cookies)
	assertStatus(t, attachReplay, http.StatusOK)
	if body := respJSON(t, attachReplay); body["changed"] != false {
		t.Fatalf("attach replay=%v", body)
	}
	detached := env.do(t, http.MethodDelete,
		"/api/channels/"+setup.chID+"/daemons/"+daemonID, nil, setup.cookies)
	assertStatus(t, detached, http.StatusOK)
	if body := respJSON(t, detached); body["changed"] != true {
		t.Fatalf("detach=%v", body)
	}
	detachReplay := env.do(t, http.MethodDelete,
		"/api/channels/"+setup.chID+"/daemons/"+daemonID, nil, outsiderCookies)
	assertStatus(t, detachReplay, http.StatusOK)
	if body := respJSON(t, detachReplay); body["changed"] != false {
		t.Fatalf("detach replay=%v", body)
	}
}

func TestJoinReminderIgnoresIdempotencyKey(t *testing.T) {
	env := setupTestApp(t)
	_, ownerCookies := register(t, env, "join-owner@example.com", "secret123", "Owner")
	channelBody, _ := createChannel(t, env, ownerCookies, "join-reminder")
	channelID := channelBody["id"].(string)
	member, memberCookies := register(t, env, "join-member@example.com", "secret123", "Member")

	first := env.doHeaders(t, http.MethodPost, "/api/channels/"+channelID+"/join", nil, memberCookies,
		map[string]string{"Idempotency-Key": "same"})
	assertStatus(t, first, http.StatusCreated)
	firstBody := respJSON(t, first)
	if firstBody["changed"] != true || firstBody["actor_id"] == "" {
		t.Fatalf("join=%v", firstBody)
	}
	if _, err := env.db.Exec(`DELETE FROM principal_channels WHERE channel_id=? AND principal=?`,
		channelID, member["id"]); err != nil {
		t.Fatal(err)
	}
	replay := env.doHeaders(t, http.MethodPost, "/api/channels/"+channelID+"/join", nil, memberCookies,
		map[string]string{"Idempotency-Key": "different-and-ignored"})
	assertStatus(t, replay, http.StatusOK)
	replayBody := respJSON(t, replay)
	if replayBody["changed"] != false || replayBody["actor_id"] != firstBody["actor_id"] {
		t.Fatalf("join replay=%v", replayBody)
	}
	var repaired int
	if err := env.db.QueryRow(`SELECT COUNT(*) FROM principal_channels
		WHERE channel_id=? AND actor_id=?`, channelID, firstBody["actor_id"]).Scan(&repaired); err != nil {
		t.Fatal(err)
	}
	if repaired != 1 {
		t.Fatal("join reminder did not repair the missing relation row")
	}
}

func TestSysopUnknownReturnsSafeRetryWithoutOperation(t *testing.T) {
	env := setupTestApp(t)
	owner, cookies := register(t, env, "unknown-owner@example.com", "secret123", "Owner")
	chID, err := env.app.CreateHalfBuiltChannelForTest(owner["id"].(string), "unknown-channel")
	if err != nil {
		t.Fatal(err)
	}
	response := env.do(t, http.MethodPost, "/api/channels/"+chID+"/join", nil, cookies)
	assertStatus(t, response, http.StatusServiceUnavailable)
	body := respJSON(t, response)
	if body["will_retry"] != true || body["code"] != "result_unknown" {
		t.Fatalf("unknown response=%v", body)
	}
	if _, exists := body["operation_id"]; exists {
		t.Fatalf("operation leaked into response=%v", body)
	}
}

func TestRemoveProtectedActorIsTerminalConflict(t *testing.T) {
	env := setupTestApp(t)
	setup := fullSetup(t, env)
	response := env.do(t, http.MethodDelete,
		"/api/channels/"+setup.chID+"/actors/"+string(setup.actorID), nil, setup.cookies)
	assertStatus(t, response, http.StatusConflict)
	if _, _, err := env.app.ActorFactsForTest(channel.ID(setup.chID), setup.actorID); err != nil {
		t.Fatal(err)
	}
}
