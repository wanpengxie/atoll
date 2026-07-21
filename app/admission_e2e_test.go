package app_test

import (
	"net/http"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/protocol/channel"
)

func TestHTTPRemoveUsesPrincipalAccountAndActorInitiator(t *testing.T) {
	env := setupTestApp(t)
	setup := fullSetup(t, env)
	createAndBindDaemon(t, env, setup.chID, "remove-host", setup.cookies)
	decl := env.do(t, http.MethodPost, "/api/actor-decls", map[string]any{
		"name": "removable", "class": "go-kimi", "visibility": "public",
	}, setup.cookies)
	assertStatus(t, decl, http.StatusCreated)
	declID := respJSON(t, decl)["id"].(string)
	introduced := env.do(t, http.MethodPost, "/api/channels/"+setup.chID+"/actors", map[string]any{"decl_id": declID}, setup.cookies)
	assertStatus(t, introduced, http.StatusCreated)
	target := respJSON(t, introduced)["actor_id"].(string)

	_, outsiderCookies := register(t, env, "remove-outsider@example.com", "secret123", "Outsider")
	denied := env.do(t, http.MethodDelete, "/api/channels/"+setup.chID+"/actors/"+target, nil, outsiderCookies)
	assertStatus(t, denied, http.StatusForbidden)

	removed := env.doHeaders(t, http.MethodDelete, "/api/channels/"+setup.chID+"/actors/"+target, nil, setup.cookies,
		map[string]string{"Idempotency-Key": "remove-once"})
	assertStatus(t, removed, http.StatusOK)
	body := respJSON(t, removed)
	ref := body["operation_id"].(string)
	if ref == "" || body["status"] != "done" {
		t.Fatalf("remove response=%v", body)
	}
	status := env.do(t, http.MethodGet, "/api/operations/"+ref, nil, setup.cookies)
	assertStatus(t, status, http.StatusOK)
	if got := respJSON(t, status); got["op"] != "remove" || got["status"] != "done" {
		t.Fatalf("remove operation=%v", got)
	}
	if _, err := env.app.ResolveSourceForTest(setup.chID, declID); err == nil {
		t.Fatal("removed declaration actor remained active")
	}

	protected := env.do(t, http.MethodDelete, "/api/channels/"+setup.chID+"/actors/"+string(setup.actorID), nil, setup.cookies)
	assertStatus(t, protected, http.StatusConflict)
}

func TestAdmissionJoinIdempotencyAndOperationProjection(t *testing.T) {
	env := setupTestApp(t)
	_, ownerCookies := register(t, env, "admission-owner@example.com", "secret123", "Owner")
	channelBody, ownerCookies := createChannel(t, env, ownerCookies, "admission-channel")
	channelID := channelBody["id"].(string)
	_, joinerCookies := register(t, env, "admission-joiner@example.com", "secret123", "Joiner")

	headers := map[string]string{"Idempotency-Key": "join-once"}
	first := env.doHeaders(t, http.MethodPost, "/api/channels/"+channelID+"/join", nil, joinerCookies, headers)
	assertStatus(t, first, http.StatusCreated)
	firstBody := respJSON(t, first)
	ref := firstBody["operation_id"].(string)
	actorID := firstBody["actor_id"].(string)
	if ref == "" || actorID == "" || firstBody["created"] != true {
		t.Fatalf("first join=%v", firstBody)
	}

	replay := env.doHeaders(t, http.MethodPost, "/api/channels/"+channelID+"/join", nil, joinerCookies, headers)
	assertStatus(t, replay, http.StatusCreated)
	replayBody := respJSON(t, replay)
	if replayBody["operation_id"] != ref || replayBody["actor_id"] != actorID {
		t.Fatalf("idempotent replay changed terminal: first=%v replay=%v", firstBody, replayBody)
	}

	status := env.do(t, http.MethodGet, "/api/operations/"+ref, nil, joinerCookies)
	assertStatus(t, status, http.StatusOK)
	statusBody := respJSON(t, status)
	if statusBody["family"] != "admission" || statusBody["status"] != "done" || statusBody["op"] != "join" {
		t.Fatalf("operation projection=%v", statusBody)
	}
	forbidden := env.do(t, http.MethodGet, "/api/operations/"+ref, nil, ownerCookies)
	assertStatus(t, forbidden, http.StatusForbidden)
	decl := env.do(t, http.MethodPost, "/api/actor-decls", map[string]any{"name": "different-request", "class": "go-kimi"}, joinerCookies)
	assertStatus(t, decl, http.StatusCreated)
	conflict := env.doHeaders(t, http.MethodPost, "/api/channels/"+channelID+"/actors", map[string]any{"decl_id": respJSON(t, decl)["id"]}, joinerCookies, headers)
	assertStatus(t, conflict, http.StatusConflict)

	// A fresh ref for the same admission is business-idempotent and reports the
	// existing membership rather than minting a second actor.
	secondRef := env.do(t, http.MethodPost, "/api/channels/"+channelID+"/join", nil, joinerCookies)
	assertStatus(t, secondRef, http.StatusOK)
	secondBody := respJSON(t, secondRef)
	if secondBody["actor_id"] != actorID || secondBody["created"] != false {
		t.Fatalf("business replay=%v", secondBody)
	}
}

func TestAdmissionIdempotencyKeyRejectsCrossChannelReuse(t *testing.T) {
	env := setupTestApp(t)
	_, ownerCookies := register(t, env, "xchan-owner@example.com", "secret123", "Owner")
	channelA, ownerCookies := createChannel(t, env, ownerCookies, "xchan-a")
	channelB, _ := createChannel(t, env, ownerCookies, "xchan-b")
	_, joinerCookies := register(t, env, "xchan-joiner@example.com", "secret123", "Joiner")

	headers := map[string]string{"Idempotency-Key": "shared-key"}
	first := env.doHeaders(t, http.MethodPost, "/api/channels/"+channelA["id"].(string)+"/join", nil, joinerCookies, headers)
	assertStatus(t, first, http.StatusCreated)

	// Same requester + same idempotency key, different channel: the reused key
	// must conflict, never silently return channel A's operation for channel B.
	cross := env.doHeaders(t, http.MethodPost, "/api/channels/"+channelB["id"].(string)+"/join", nil, joinerCookies, headers)
	assertStatus(t, cross, http.StatusConflict)
}

func TestAdmissionPendingBecomesUnresolvedWhenChannelRetires(t *testing.T) {
	env := setupTestApp(t)
	_, ownerCookies := register(t, env, "unknown-owner@example.com", "secret123", "Owner")
	channelBody, _ := createChannel(t, env, ownerCookies, "unknown-channel")
	channelID := channelBody["id"].(string)
	_, joinerCookies := register(t, env, "unknown-joiner@example.com", "secret123", "Joiner")
	if err := env.app.CloseHomeForTest(channel.ID(channelID)); err != nil {
		t.Fatal(err)
	}

	response := env.do(t, http.MethodPost, "/api/channels/"+channelID+"/join", nil, joinerCookies)
	assertStatus(t, response, http.StatusAccepted)
	ref := respJSON(t, response)["operation_id"].(string)
	if _, err := env.db.Exec(`DELETE FROM channels WHERE id=?`, channelID); err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		var status, code string
		if err := env.db.QueryRow(`SELECT status,COALESCE(error_code,'') FROM channel_admission_operations WHERE operation_id=?`, ref).Scan(&status, &code); err != nil {
			t.Fatal(err)
		}
		if status == "unresolved" {
			if code != "channel_retired" {
				t.Fatalf("unresolved code=%q", code)
			}
			return
		}
		if status == "rejected" {
			t.Fatalf("result-unknown operation was forged as rejected (%s)", code)
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("pending admission did not converge to unresolved")
}
