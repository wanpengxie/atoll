package app_test

// harness_test.go is the app_test suite harness: one isolated App per test
// (fresh SQLite in a temp dir), the in-process HTTP driver, and the shared
// register/login/channel/daemon setup helpers every black-box test builds on.
// It holds no tests of its own.

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync"
	"testing"

	"github.com/wanpengxie/atoll/app"
	"github.com/wanpengxie/atoll/drivers/gateway"
	"github.com/wanpengxie/atoll/drivers/gateway/connector/web"
	"github.com/wanpengxie/atoll/platform"
	"github.com/wanpengxie/atoll/platform/channelhost"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
	"golang.org/x/crypto/bcrypt"
)

// authenticatedTestPlan is the daemon-side factory source for live runs: it
// resolves per actor at body-build time, exactly the production shape. A body
// can only be built for a spec the daemon Host's desired serves, and that
// desired is the pulled plan — so "only what the server published gets built"
// holds by construction, with no plan snapshot to maintain here.
func authenticatedTestPlan(decls []platform.ActorDecl) *testFactorySource {
	factories := make(map[actor.ActorID]platform.ActorFactory, len(decls))
	for _, d := range decls {
		factories[d.ID] = d.Factory
	}
	return &testFactorySource{factories: factories}
}

type testFactorySource struct {
	mu        sync.Mutex
	factories map[actor.ActorID]platform.ActorFactory
}

func (p *testFactorySource) BuildClass(
	id actor.ActorID,
	_ string,
	_ json.RawMessage,
) (platform.ActorFactory, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	f, ok := p.factories[id]
	return f, ok
}

func truthRowsForTest(t *testing.T, env *testEnv, chID string) []any {
	t.Helper()
	rows, err := env.app.MessagesForTest(channel.ID(chID))
	if err != nil {
		t.Fatal(err)
	}
	out := make([]any, 0, len(rows))
	for _, row := range rows {
		b, err := json.Marshal(row.Envelope)
		if err != nil {
			t.Fatal(err)
		}
		var envelope any
		if err := json.Unmarshal(b, &envelope); err != nil {
			t.Fatal(err)
		}
		out = append(out, map[string]any{
			"seq": float64(row.Seq), "is_terminal": row.IsTerminal, "envelope": envelope,
		})
	}
	return out
}

// testEnv holds a fresh App + handler for one test. Each test gets an isolated
// SQLite (temp dir) so tests never share state.
type testEnv struct {
	handler http.Handler
	app     *app.App
	db      *sql.DB
	tmpDir  string
}

// testGatewayResolver reproduces cmd/server's app→gateway entitlement DTO bridge
// (连接模型勘误期 §3.2: app → drivers is fenced, so the assembly root — here the test
// harness — maps the app's own DTO into gateway.Route).
func testGatewayResolver(a *app.App) gateway.EntitlementResolver {
	return gateway.ResolverFunc(func(ctx context.Context, principal string) ([]gateway.Route, []channel.ID, error) {
		routes, failed, err := a.EntitlementSnapshot(ctx, principal)
		if err != nil {
			return nil, nil, err
		}
		gr := make([]gateway.Route, 0, len(routes))
		for _, r := range routes {
			gr = append(gr, gateway.Route{
				Channel: r.Channel, Bundle: r.Bundle, SubjectID: r.SubjectID,
			})
		}
		return gr, failed, nil
	})
}

func setupTestApp(t *testing.T) *testEnv {
	t.Helper()

	// Drop the password work factor for the suite: every fullSetup does a
	// register+login, and under -race a DefaultCost hash+compare burns ~1.7s
	// of pure CPU per test — the suite's single biggest time sink (owner
	// 2026-07-13). MinCost (~1ms) tests the same code path.
	t.Cleanup(app.SetBcryptCostForTest(bcrypt.MinCost))

	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "app.db")
	chDBDir := filepath.Join(tmpDir, "channels")

	db, err := openTestAppDB(t, dbPath)
	if err != nil {
		t.Fatalf("OpenDB: %v", err)
	}

	a, err := app.New(app.Config{
		DB: db,
		HostFactory: func(deps channelhost.HomeDeps) (channelhost.LocalHost, error) {
			return channelhost.New(chDBDir, deps)
		},
	})
	if err != nil {
		db.Close()
		t.Fatalf("app.New: %v", err)
	}
	testAgentBuilder = stubAgentFactory

	// gateway 期 S3: wire the real human-ingress connector into the app test harness,
	// exactly as cmd/server does (the app cannot construct it — app → drivers is
	// fenced — so the assembly-root wiring is reproduced here in the harness).
	gw, err := gateway.New(gateway.Config{
		Resolver: testGatewayResolver(a),
	})
	if err != nil {
		t.Fatalf("gateway.New: %v", err)
	}
	gw.Start()
	a.SetGateway(web.New(gw))
	a.SetMembershipPoke(gw.Poke)

	t.Cleanup(func() {
		// gateway先静默 (关站全序: stop presence loop, close sessions, drain) BEFORE the homes it drives close, then close
		// the app (joins every home's reconcile ticker goroutine, which reads
		// testAgentBuilder in its build path) BEFORE nil-ing the global — else a
		// still-running ticker races the write under -race.
		gw.Close()
		a.Close(context.Background())
		testAgentBuilder = nil
		db.Close()
	})

	return &testEnv{
		handler: a.Handler(),
		app:     a,
		db:      db,
		tmpDir:  tmpDir,
	}
}

// do performs an HTTP request against the in-process handler and returns the
// recorder. It attaches cookies from prior responses so session tracking works.
func (e *testEnv) do(t *testing.T, method, path string, body any, cookies []*http.Cookie) *httptest.ResponseRecorder {
	return e.doHeaders(t, method, path, body, cookies, nil)
}

func (e *testEnv) doHeaders(t *testing.T, method, path string, body any, cookies []*http.Cookie, headers map[string]string) *httptest.ResponseRecorder {
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
	for key, value := range headers {
		req.Header.Set(key, value)
	}
	for _, c := range cookies {
		req.AddCookie(c)
	}

	w := httptest.NewRecorder()
	e.handler.ServeHTTP(w, req)
	return w
}

func createAndBindDaemon(t *testing.T, env *testEnv, chID, name string, cookies []*http.Cookie) map[string]any {
	t.Helper()
	created := env.do(t, http.MethodPost, "/api/daemons", map[string]any{"name": name}, cookies)
	assertStatus(t, created, http.StatusCreated)
	body := respJSON(t, created)
	bound := env.do(t, http.MethodPost, "/api/channels/"+chID+"/daemons", map[string]any{"daemon_id": body["id"]}, cookies)
	assertStatus(t, bound, http.StatusOK)
	return body
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
	chID    string
	actorID actor.ActorID
	boostID actor.ActorID
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

func createChannel(t *testing.T, env *testEnv, cookies []*http.Cookie, name string) (map[string]any, []*http.Cookie) {
	t.Helper()
	w := env.do(t, "POST", "/api/channels", map[string]any{
		"name": name,
	}, cookies)
	assertStatus(t, w, http.StatusCreated)
	body := respJSON(t, w)
	return body, mergeCookies(cookies, extractCookies(w))
}

// fullSetup does register + login + create channel, returning
// all IDs and the cookie jar.
func fullSetup(t *testing.T, env *testEnv) setupResult {
	t.Helper()

	regBody, cookies := register(t, env, "test@example.com", "secret123", "Tester")
	userID := regBody["id"].(string)

	// Login is optional since register auto-logs-in, but exercise the endpoint.
	_, loginCookies := login(t, env, "test@example.com", "secret123")
	cookies = mergeCookies(cookies, loginCookies)

	chBody, cookies := createChannel(t, env, cookies, "general")
	chID := chBody["id"].(string)
	actorID, _ := env.app.ResolvePrincipalForTest(chID, userID)
	boostID, _ := env.app.ResolveSourceForTest(chID, "sys:boost")

	return setupResult{
		cookies: cookies,
		userID:  userID,
		chID:    chID,
		actorID: actorID,
		boostID: boostID,
	}
}
