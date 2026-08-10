package app_test

import (
	"bufio"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/protocol/channel"
)

func TestChannelDetailIsDesiredWhileObserverContentRequiresSpaceTool(t *testing.T) {
	env := setupTestApp(t)
	owner := fullSetup(t, env)
	_, outsiderCookies := register(t, env, "observer-http@example.com", "secret123", "Observer")

	for _, path := range []string{
		"/api/channels/" + owner.chID + "/messages",
		"/api/channels/" + owner.chID + "/resources",
	} {
		assertStatus(t, env.do(t, http.MethodGet, path, nil, outsiderCookies), http.StatusOK)
	}
	if err := env.app.RemoveSpaceToolForTest(channel.ID(owner.chID)); err != nil {
		t.Fatal(err)
	}
	detail := env.do(t, http.MethodGet, "/api/channels/"+owner.chID, nil, outsiderCookies)
	assertStatus(t, detail, http.StatusOK)
	for _, path := range []string{
		"/api/channels/" + owner.chID + "/messages",
		"/api/channels/" + owner.chID + "/resources",
	} {
		response := env.do(t, http.MethodGet, path, nil, outsiderCookies)
		assertStatus(t, response, http.StatusConflict)
		if !strings.Contains(response.Body.String(), "capability_unavailable") {
			t.Fatalf("observer failure body=%s", response.Body.String())
		}
	}
	assertStatus(t, env.do(t, http.MethodGet, "/api/channels/"+owner.chID+"/messages", nil, owner.cookies), http.StatusOK)
	assertStatus(t, env.do(t, http.MethodGet, "/api/channels/"+owner.chID+"/resources", nil, owner.cookies), http.StatusOK)
}

func openObserverStream(t *testing.T, srv *httptest.Server, chID string, cookies []*http.Cookie) (*http.Response, *bufio.Scanner) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	t.Cleanup(cancel)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL+"/api/channels/"+chID+"/observe", nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, cookie := range cookies {
		req.AddCookie(cookie)
	}
	resp, err := srv.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK {
		resp.Body.Close()
		t.Fatalf("observe status=%d", resp.StatusCode)
	}
	t.Cleanup(func() { _ = resp.Body.Close() })
	return resp, bufio.NewScanner(resp.Body)
}

func waitSSETerminal(t *testing.T, scanner *bufio.Scanner, want string) {
	t.Helper()
	seenEvent := false
	for scanner.Scan() {
		line := scanner.Text()
		if line == "event: terminated" {
			seenEvent = true
		}
		if seenEvent && strings.HasPrefix(line, "data: ") && strings.Contains(line, `"type":"terminated"`) && strings.Contains(line, `"code":"`+want+`"`) {
			return
		}
	}
	t.Fatalf("SSE closed without terminal %q: %v", want, scanner.Err())
}

func TestObserverSSETerminatesOnCapabilityRemovalAndJoin(t *testing.T) {
	t.Run("space tool removed", func(t *testing.T) {
		env := setupTestApp(t)
		owner := fullSetup(t, env)
		_, observer := register(t, env, "observer-sse-remove@example.com", "secret123", "Observer")
		srv := httptest.NewServer(env.app.Handler())
		defer srv.Close()
		_, scanner := openObserverStream(t, srv, owner.chID, observer)
		if err := env.app.RemoveSpaceToolForTest(channel.ID(owner.chID)); err != nil {
			t.Fatal(err)
		}
		waitSSETerminal(t, scanner, "capability_unavailable")
	})

	t.Run("observer joins", func(t *testing.T) {
		env := setupTestApp(t)
		owner := fullSetup(t, env)
		_, observer := register(t, env, "observer-sse-join@example.com", "secret123", "Observer")
		srv := httptest.NewServer(env.app.Handler())
		defer srv.Close()
		_, scanner := openObserverStream(t, srv, owner.chID, observer)
		joined := env.do(t, http.MethodPost, "/api/channels/"+owner.chID+"/join", nil, observer)
		if joined.Code != http.StatusCreated && joined.Code != http.StatusOK {
			t.Fatalf("join status=%d body=%s", joined.Code, joined.Body.String())
		}
		waitSSETerminal(t, scanner, "now_member")
	})
}

func waitSSEMessage(t *testing.T, scanner *bufio.Scanner, id string) string {
	t.Helper()
	seenEvent := false
	for scanner.Scan() {
		line := scanner.Text()
		if line == "event: message" {
			seenEvent = true
		}
		if seenEvent && strings.HasPrefix(line, "data: ") && strings.Contains(line, `"id":"`+id+`"`) {
			return line
		}
	}
	t.Fatalf("SSE closed before message %q: %v", id, scanner.Err())
	return ""
}

func TestObserverSSEBackfillsThenStreamsLiveMessages(t *testing.T) {
	env := setupTestApp(t)
	owner := fullSetup(t, env)
	_, observer := register(t, env, "observer-sse-tail@example.com", "secret123", "Observer")
	srv := httptest.NewServer(env.app.Handler())
	defer srv.Close()
	ownerWS := dialWS(t, srv, owner.cookies, owner.chID, 0)
	defer ownerWS.close()

	first := ownerWS.sendMessage(map[string]any{
		"id": "observer-history", "channel_id": owner.chID, "msg_type": "observer.history",
		"kind": "request", "audience": []string{string(owner.boostID)}, "visibility": "public", "payload": map[string]any{},
	})
	if first["type"] != "ack" {
		t.Fatalf("history submit=%v", first)
	}
	_, scanner := openObserverStream(t, srv, owner.chID, observer)
	waitSSEMessage(t, scanner, "observer-history")

	second := ownerWS.sendMessage(map[string]any{
		"id": "observer-live", "channel_id": owner.chID, "msg_type": "observer.live",
		"kind": "request", "audience": []string{string(owner.boostID)}, "visibility": "public", "payload": map[string]any{},
	})
	if second["type"] != "ack" {
		t.Fatalf("live submit=%v", second)
	}
	waitSSEMessage(t, scanner, "observer-live")
}

func visibleMessageIDs(t *testing.T, response *httptest.ResponseRecorder) map[string]bool {
	t.Helper()
	assertStatus(t, response, http.StatusOK)
	var body struct {
		Messages []struct {
			Envelope struct {
				ID string `json:"id"`
			} `json:"envelope"`
		} `json:"messages"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode messages: %v body=%s", err, response.Body.String())
	}
	ids := make(map[string]bool, len(body.Messages))
	for _, row := range body.Messages {
		ids[row.Envelope.ID] = true
	}
	return ids
}

func TestPublicMessageHTTPIsSharedAndPrivateIsRejected(t *testing.T) {
	env := setupTestApp(t)
	owner := fullSetup(t, env)
	bobCookies, bobID := addSecondMember(t, env, owner, "visible-bob@example.com")
	carolCookies, _ := addSecondMember(t, env, owner, "visible-carol@example.com")
	_, observerCookies := register(t, env, "visible-observer@example.com", "secret123", "Observer")
	srv := httptest.NewServer(env.app.Handler())
	defer srv.Close()
	ownerWS := dialWS(t, srv, owner.cookies, owner.chID, 0)
	defer ownerWS.close()
	_, scanner := openObserverStream(t, srv, owner.chID, observerCookies)
	private := ownerWS.sendMessage(map[string]any{
		"id": "visible-private", "channel_id": owner.chID, "msg_type": "visible.private",
		"kind": "event", "audience": []string{string(bobID)}, "visibility": "private", "payload": map[string]any{},
	})
	if private["type"] != "error" || private["error"] != "bad_payload" {
		t.Fatalf("private submit must be permanent bad_payload: %v", private)
	}
	public := ownerWS.sendMessage(map[string]any{
		"id": "visible-public-empty-audience", "channel_id": owner.chID, "msg_type": "visible.public",
		"kind": "event", "visibility": "public", "payload": map[string]any{},
	})
	if public["type"] != "ack" {
		t.Fatalf("public submit=%v", public)
	}
	wsEnvelope := ownerWS.waitTail(func(env map[string]any) bool {
		return env["id"] == "visible-public-empty-audience"
	}, 3*time.Second)
	if audience, ok := wsEnvelope["audience"].([]any); !ok || len(audience) != 0 {
		t.Fatalf("ws audience=%#v, want []", wsEnvelope["audience"])
	}
	if line := waitSSEMessage(t, scanner, "visible-public-empty-audience"); !strings.Contains(line, `"audience":[]`) {
		t.Fatalf("SSE audience is not canonical []: %s", line)
	}

	path := "/api/channels/" + owner.chID + "/messages?limit=500"
	ownerPage := env.do(t, http.MethodGet, path, nil, owner.cookies)
	ownerIDs := visibleMessageIDs(t, ownerPage)
	bobIDs := visibleMessageIDs(t, env.do(t, http.MethodGet, path, nil, bobCookies))
	carolIDs := visibleMessageIDs(t, env.do(t, http.MethodGet, path, nil, carolCookies))
	observerIDs := visibleMessageIDs(t, env.do(t, http.MethodGet, path, nil, observerCookies))
	for who, ids := range map[string]map[string]bool{"owner": ownerIDs, "bob": bobIDs, "carol": carolIDs, "observer": observerIDs} {
		if !ids["visible-public-empty-audience"] {
			t.Fatalf("public missing for %s: %v", who, ids)
		}
	}
	if ownerIDs["visible-private"] || bobIDs["visible-private"] || carolIDs["visible-private"] || observerIDs["visible-private"] {
		t.Fatal("rejected private message reached history")
	}
	var body struct {
		Messages []struct {
			Envelope struct {
				ID       string          `json:"id"`
				Audience json.RawMessage `json:"audience"`
			} `json:"envelope"`
		} `json:"messages"`
	}
	if err := json.Unmarshal(ownerPage.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	for _, row := range body.Messages {
		if row.Envelope.ID == "visible-public-empty-audience" {
			if string(row.Envelope.Audience) != "[]" {
				t.Fatalf("HTTP audience=%s, want []", row.Envelope.Audience)
			}
			return
		}
	}
	t.Fatal("empty-audience event missing from HTTP page")
}
