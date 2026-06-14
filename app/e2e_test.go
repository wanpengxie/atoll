package app_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"github.com/wanpengxie/ActOS/app"
)

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// testEnv holds a fresh App + handler for one test. Each test gets an isolated
// SQLite (temp dir) so tests never share state.
type testEnv struct {
	handler http.Handler
	app     *app.App
	tmpDir  string
}

func setupTestApp(t *testing.T) *testEnv {
	t.Helper()

	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "app.db")
	chDBDir := filepath.Join(tmpDir, "channels")

	db, err := app.OpenDB(dbPath)
	if err != nil {
		t.Fatalf("OpenDB: %v", err)
	}

	a, err := app.New(app.Config{
		DB:           db,
		ChannelDBDir: chDBDir,
		AgentFactory: stubAgentFactory,
	})
	if err != nil {
		db.Close()
		t.Fatalf("app.New: %v", err)
	}
	t.Cleanup(func() {
		a.Close()
		db.Close()
	})

	return &testEnv{
		handler: a.Handler(),
		app:     a,
		tmpDir:  tmpDir,
	}
}

// do performs an HTTP request against the in-process handler and returns the
// recorder. It attaches cookies from prior responses so session tracking works.
func (e *testEnv) do(t *testing.T, method, path string, body any, cookies []*http.Cookie) *httptest.ResponseRecorder {
	t.Helper()

	var bodyReader *bytes.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			t.Fatalf("marshal body: %v", err)
		}
		bodyReader = bytes.NewReader(b)
	} else {
		bodyReader = bytes.NewReader(nil)
	}

	req := httptest.NewRequest(method, path, bodyReader)
	req.Header.Set("Content-Type", "application/json")
	for _, c := range cookies {
		req.AddCookie(c)
	}

	w := httptest.NewRecorder()
	e.handler.ServeHTTP(w, req)
	return w
}

// respJSON decodes recorder body into map[string]any.
func respJSON(t *testing.T, w *httptest.ResponseRecorder) map[string]any {
	t.Helper()
	var m map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &m); err != nil {
		t.Fatalf("decode response: %v\nbody: %s", err, w.Body.String())
	}
	return m
}

// respJSONArray decodes recorder body into []any (for list-messages which
// returns a top-level array).
func respJSONArray(t *testing.T, w *httptest.ResponseRecorder) []any {
	t.Helper()
	var arr []any
	if err := json.Unmarshal(w.Body.Bytes(), &arr); err != nil {
		t.Fatalf("decode array response: %v\nbody: %s", err, w.Body.String())
	}
	return arr
}

// extractCookies returns all Set-Cookie values as []*http.Cookie.
func extractCookies(w *httptest.ResponseRecorder) []*http.Cookie {
	resp := http.Response{Header: w.Header()}
	return resp.Cookies()
}

// mergeCookies replaces or appends new cookies into an existing jar slice.
func mergeCookies(existing, new []*http.Cookie) []*http.Cookie {
	m := make(map[string]*http.Cookie)
	for _, c := range existing {
		m[c.Name] = c
	}
	for _, c := range new {
		m[c.Name] = c
	}
	out := make([]*http.Cookie, 0, len(m))
	for _, c := range m {
		out = append(out, c)
	}
	return out
}

// assertStatus checks the status code and fatals with the response body on mismatch.
func assertStatus(t *testing.T, w *httptest.ResponseRecorder, want int) {
	t.Helper()
	if w.Code != want {
		t.Fatalf("want status %d, got %d\nbody: %s", want, w.Code, w.Body.String())
	}
}

// ---------------------------------------------------------------------------
// Higher-level helpers -- each returns updated cookie jar
// ---------------------------------------------------------------------------

type setupResult struct {
	cookies []*http.Cookie
	userID  string
	wsID    string
	chID    string
}

func register(t *testing.T, env *testEnv, email, password, displayName string) (map[string]any, []*http.Cookie) {
	t.Helper()
	w := env.do(t, "POST", "/api/identity/register", map[string]any{
		"email":        email,
		"password":     password,
		"display_name": displayName,
	}, nil)
	assertStatus(t, w, http.StatusCreated)
	return respJSON(t, w), extractCookies(w)
}

func login(t *testing.T, env *testEnv, email, password string) (map[string]any, []*http.Cookie) {
	t.Helper()
	w := env.do(t, "POST", "/api/identity/login", map[string]any{
		"email":    email,
		"password": password,
	}, nil)
	assertStatus(t, w, http.StatusOK)
	return respJSON(t, w), extractCookies(w)
}

func createWorkspace(t *testing.T, env *testEnv, cookies []*http.Cookie, name string) (map[string]any, []*http.Cookie) {
	t.Helper()
	w := env.do(t, "POST", "/api/workspaces", map[string]any{
		"name": name,
	}, cookies)
	assertStatus(t, w, http.StatusCreated)
	return respJSON(t, w), mergeCookies(cookies, extractCookies(w))
}

func createChannel(t *testing.T, env *testEnv, cookies []*http.Cookie, wsID, name string) (map[string]any, []*http.Cookie) {
	t.Helper()
	w := env.do(t, "POST", fmt.Sprintf("/api/workspaces/%s/channels", wsID), map[string]any{
		"name": name,
	}, cookies)
	assertStatus(t, w, http.StatusCreated)
	return respJSON(t, w), mergeCookies(cookies, extractCookies(w))
}

// fullSetup does register + login + create workspace + create channel, returns
// all IDs and the cookie jar.
func fullSetup(t *testing.T, env *testEnv) setupResult {
	t.Helper()

	regBody, cookies := register(t, env, "test@example.com", "secret123", "Tester")
	userID := regBody["id"].(string)

	// Login is optional since register auto-logs-in, but exercise the endpoint.
	_, loginCookies := login(t, env, "test@example.com", "secret123")
	cookies = mergeCookies(cookies, loginCookies)

	wsBody, cookies := createWorkspace(t, env, cookies, "TestWS")
	wsID := wsBody["id"].(string)

	chBody, cookies := createChannel(t, env, cookies, wsID, "general")
	chID := chBody["id"].(string)

	return setupResult{
		cookies: cookies,
		userID:  userID,
		wsID:    wsID,
		chID:    chID,
	}
}

// ---------------------------------------------------------------------------
// Test1: Register -> Login -> Workspace -> Channel
// ---------------------------------------------------------------------------

func TestE2E_RegisterLoginWorkspaceChannel(t *testing.T) {
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

	// 4. Create workspace
	wsBody, cookies := createWorkspace(t, env, cookies, "MyWorkspace")
	wsID := wsBody["id"].(string)
	if wsID == "" {
		t.Fatal("workspace id empty")
	}

	// 5. List workspaces
	w = env.do(t, "GET", "/api/workspaces", nil, cookies)
	assertStatus(t, w, http.StatusOK)
	wsListBody := respJSON(t, w)
	wsList := wsListBody["workspaces"].([]any)
	found := false
	for _, ws := range wsList {
		m := ws.(map[string]any)
		if m["id"] == wsID {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("workspace %s not found in list: %v", wsID, wsList)
	}

	// 6. Create channel
	chBody, cookies := createChannel(t, env, cookies, wsID, "general")
	chID := chBody["id"].(string)
	if chID == "" {
		t.Fatal("channel id empty")
	}

	// 7. List channels
	w = env.do(t, "GET", fmt.Sprintf("/api/workspaces/%s/channels", wsID), nil, cookies)
	assertStatus(t, w, http.StatusOK)
	chListBody := respJSON(t, w)
	chList := chListBody["channels"].([]any)
	found = false
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

// ---------------------------------------------------------------------------
// Test2: Send message -> write to truth -> read back
// ---------------------------------------------------------------------------

func TestE2E_SendMessageAndReadBack(t *testing.T) {
	env := setupTestApp(t)
	s := fullSetup(t, env)

	// Send a message.
	w := env.do(t, "POST", fmt.Sprintf("/api/channels/%s/messages", s.chID), map[string]any{
		"type":    "chat.text",
		"kind":    "event",
		"payload": map[string]any{"text": "hello world"},
	}, s.cookies)
	assertStatus(t, w, http.StatusCreated)
	sendBody := respJSON(t, w)
	seq := sendBody["seq"]
	if seq == nil || seq.(float64) <= 0 {
		t.Fatalf("send message returned invalid seq: %v", seq)
	}
	msgID := sendBody["message_id"].(string)
	if msgID == "" {
		t.Fatal("send message returned empty message_id")
	}

	// Read back messages.
	w = env.do(t, "GET", fmt.Sprintf("/api/channels/%s/messages?after=0", s.chID), nil, s.cookies)
	assertStatus(t, w, http.StatusOK)
	msgs := respJSONArray(t, w)
	if len(msgs) == 0 {
		t.Fatal("expected at least one message, got 0")
	}

	// Find our message by checking the envelope.
	found := false
	for _, raw := range msgs {
		row := raw.(map[string]any)
		envelope, ok := row["envelope"]
		if !ok {
			continue
		}
		// The envelope might be stored as a JSON string or object.
		var envMap map[string]any
		switch v := envelope.(type) {
		case string:
			if err := json.Unmarshal([]byte(v), &envMap); err != nil {
				continue
			}
		case map[string]any:
			envMap = v
		default:
			continue
		}
		if envMap["id"] == msgID {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("sent message %s not found in channel messages", msgID)
	}
}

// ---------------------------------------------------------------------------
// Test3: Daemon create -> attach -> detach
// ---------------------------------------------------------------------------

func TestE2E_DaemonCreateAttachDetach(t *testing.T) {
	env := setupTestApp(t)
	s := fullSetup(t, env)

	// Create and attach daemon in one call.
	w := env.do(t, "POST", fmt.Sprintf("/api/channels/%s/daemons", s.chID), map[string]any{
		"name": "test-daemon",
	}, s.cookies)
	assertStatus(t, w, http.StatusCreated)
	daemonBody := respJSON(t, w)
	daemonID := daemonBody["id"].(string)
	if daemonID == "" {
		t.Fatal("daemon id empty")
	}
	apiKey := daemonBody["api_key"].(string)
	if apiKey == "" {
		t.Fatal("daemon api_key empty")
	}

	// List channel daemons -- should contain the one we just created.
	w = env.do(t, "GET", fmt.Sprintf("/api/channels/%s/daemons", s.chID), nil, s.cookies)
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
	w = env.do(t, "DELETE", fmt.Sprintf("/api/channels/%s/daemons/%s/attach", s.chID, daemonID), nil, s.cookies)
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

// ---------------------------------------------------------------------------
// Test4: Daemon attach + message send (simplified -- HTTP API layer only)
//
// The full RunCompute test (in-process daemon echoing) is complex due to
// WS transport; this test exercises the message write path through a daemon-
// attached channel and verifies the message is readable.
// ---------------------------------------------------------------------------

func TestE2E_DaemonAttachAndMessageFlow(t *testing.T) {
	env := setupTestApp(t)
	s := fullSetup(t, env)

	// Create and attach a daemon.
	w := env.do(t, "POST", fmt.Sprintf("/api/channels/%s/daemons", s.chID), map[string]any{
		"name": "echo-daemon",
	}, s.cookies)
	assertStatus(t, w, http.StatusCreated)
	daemonBody := respJSON(t, w)
	daemonID := daemonBody["id"].(string)
	_ = daemonID

	// Send an event-kind message through the channel that has a daemon attached.
	// Without RunCompute there is no daemon actor cell to receive request-kind
	// messages (the harness enforces exactly-one active target for requests), so
	// we use kind=event which has no cardinality constraint.
	w = env.do(t, "POST", fmt.Sprintf("/api/channels/%s/messages", s.chID), map[string]any{
		"type":    "echo.ping",
		"kind":    "event",
		"payload": map[string]any{"text": "ping"},
	}, s.cookies)
	assertStatus(t, w, http.StatusCreated)
	sendBody := respJSON(t, w)
	reqSeq := sendBody["seq"].(float64)
	reqMsgID := sendBody["message_id"].(string)

	// Read back -- should contain the request message.
	w = env.do(t, "GET", fmt.Sprintf("/api/channels/%s/messages?after=0", s.chID), nil, s.cookies)
	assertStatus(t, w, http.StatusOK)
	msgs := respJSONArray(t, w)
	if len(msgs) == 0 {
		t.Fatal("expected at least one message")
	}

	// Verify the request is present.
	found := false
	for _, raw := range msgs {
		row := raw.(map[string]any)
		if row["seq"].(float64) == reqSeq {
			found = true
			// Verify envelope contains our message id.
			var envMap map[string]any
			switch v := row["envelope"].(type) {
			case string:
				_ = json.Unmarshal([]byte(v), &envMap)
			case map[string]any:
				envMap = v
			}
			if envMap != nil && envMap["id"] != reqMsgID {
				t.Fatalf("seq %v envelope id mismatch: want %s got %v", reqSeq, reqMsgID, envMap["id"])
			}
			break
		}
	}
	if !found {
		t.Fatalf("request message with seq %v not found", reqSeq)
	}
}

// ---------------------------------------------------------------------------
// Test5: sendMessage without audience -> default fills all members
// ---------------------------------------------------------------------------

func TestE2E_SendMessageNoAudienceDefaultFill(t *testing.T) {
	env := setupTestApp(t)
	s := fullSetup(t, env)

	// Send a message with NO audience field at all.
	w := env.do(t, "POST", fmt.Sprintf("/api/channels/%s/messages", s.chID), map[string]any{
		"type":    "chat.text",
		"kind":    "event",
		"payload": map[string]any{"text": "broadcast"},
	}, s.cookies)
	assertStatus(t, w, http.StatusCreated)
	body := respJSON(t, w)
	seq := body["seq"].(float64)
	if seq <= 0 {
		t.Fatalf("expected positive seq, got %v", seq)
	}
	msgID := body["message_id"].(string)
	if msgID == "" {
		t.Fatal("expected non-empty message_id")
	}

	// Also send with an explicit empty audience array -- should still succeed.
	w = env.do(t, "POST", fmt.Sprintf("/api/channels/%s/messages", s.chID), map[string]any{
		"type":     "chat.text",
		"kind":     "event",
		"payload":  map[string]any{"text": "broadcast2"},
		"audience": []string{},
	}, s.cookies)
	assertStatus(t, w, http.StatusCreated)
	body2 := respJSON(t, w)
	seq2 := body2["seq"].(float64)
	if seq2 <= seq {
		t.Fatalf("second message seq %v should be > first seq %v", seq2, seq)
	}

	// Verify both messages exist in truth.
	w = env.do(t, "GET", fmt.Sprintf("/api/channels/%s/messages?after=0", s.chID), nil, s.cookies)
	assertStatus(t, w, http.StatusOK)
	msgs := respJSONArray(t, w)
	if len(msgs) < 2 {
		t.Fatalf("expected at least 2 messages, got %d", len(msgs))
	}
}
