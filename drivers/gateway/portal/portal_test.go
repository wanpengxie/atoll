package portal

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"testing/fstest"
	"time"

	"github.com/wanpengxie/atoll/drivers/gateway"
	"github.com/wanpengxie/atoll/platform/dataplane"
	"github.com/wanpengxie/atoll/platform/lagoon"
	"github.com/wanpengxie/atoll/platform/obs"
	"github.com/wanpengxie/atoll/protocol/access"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
)

type filePlaneStub struct {
	channelID channel.ID
	caller    actor.ActorID
	ticket    string
	mode      access.Operation
	body      []byte
	name      string
	err       error
}

type obsPlaneStub struct {
	principal, path, query string
	answer                 obs.Observation
	err                    error
}

func (s *obsPlaneStub) Pull(_ context.Context, principal, path, query string) (obs.Observation, error) {
	s.principal, s.path, s.query = principal, path, query
	return s.answer, s.err
}

func (f *filePlaneStub) Resolve(channel.ID, actor.ActorID, string) (dataplane.Ticket, error) {
	return dataplane.Ticket{}, errors.New("unused")
}
func (f *filePlaneStub) ServeExchange(context.Context, channel.ID, io.ReadWriteCloser) {}
func (f *filePlaneStub) OpenTransfer(context.Context, channel.ID, actor.ActorID, string, access.Operation) (io.ReadWriteCloser, error) {
	return nil, errors.New("unused")
}
func (f *filePlaneStub) TicketFile(channel.ID, actor.ActorID, string) (string, bool) {
	return f.name, f.name != ""
}
func (f *filePlaneStub) ServeHTTP(_ context.Context, ch channel.ID, caller actor.ActorID, ticket string, mode access.Operation, dst io.Writer, src io.Reader) error {
	f.channelID, f.caller, f.ticket, f.mode = ch, caller, ticket, mode
	if f.err != nil {
		return f.err
	}
	if mode == access.OpRead {
		_, _ = dst.Write(f.body)
	} else {
		f.body, _ = io.ReadAll(src)
	}
	return nil
}

func TestRegisterRejectsMissingFieldsAndTrailingJSON(t *testing.T) {
	p := New(Config{ContractVersion: "test", Sessions: gateway.NewSessionStore()})
	for _, body := range []string{`{"email":""}`, `{"email":"a@example.test","password":"x"} {}`} {
		req := httptest.NewRequest(http.MethodPost, "/api/identity/register", strings.NewReader(body))
		rec := httptest.NewRecorder()
		p.ServeHTTP(rec, req)
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("body=%q status=%d response=%s", body, rec.Code, rec.Body.String())
		}
	}
}

func TestFallbacksReturnContractJSONWithoutCORS(t *testing.T) {
	p := New(Config{ContractVersion: "test", Sessions: gateway.NewSessionStore()})
	closed := map[string]bool{
		string(lagoon.CodeInvalidArgs): true, string(lagoon.CodeNotFound): true,
		string(lagoon.CodeConflictExists): true, string(lagoon.CodePermissionDenied): true,
		string(lagoon.CodeReserved): true, string(lagoon.CodeResultUnknown): true,
	}
	for _, tc := range []struct {
		name   string
		method string
		path   string
		status int
	}{
		{name: "unknown path", method: http.MethodGet, path: "/missing", status: http.StatusNotFound},
		{name: "wrong method", method: http.MethodGet, path: "/api/identity/login", status: http.StatusMethodNotAllowed},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			p.ServeHTTP(rec, httptest.NewRequest(tc.method, tc.path, nil))
			if rec.Code != tc.status {
				t.Fatalf("status=%d want=%d body=%s", rec.Code, tc.status, rec.Body.String())
			}
			if got := rec.Header().Get("Content-Type"); got != "application/json" {
				t.Fatalf("content-type=%q", got)
			}
			for _, header := range []string{"Access-Control-Allow-Origin", "Access-Control-Allow-Headers", "Access-Control-Allow-Methods"} {
				if got := rec.Header().Get(header); got != "" {
					t.Fatalf("%s=%q", header, got)
				}
			}
			var body map[string]string
			if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
				t.Fatalf("body is not JSON: %v (%s)", err, rec.Body.String())
			}
			if !closed[body["code"]] {
				t.Fatalf("code=%q is outside the closed set", body["code"])
			}
		})
	}
}

// The one membership this entrance's tests are about: alice is human:alice:7 in
// c0 and nothing anywhere else.
func filePortal(t *testing.T, plane *filePlaneStub) (*Portal, *gateway.SessionStore) {
	t.Helper()
	gw, err := gateway.New(gateway.Config{Resolver: gateway.ResolverFunc(
		func(_ context.Context, principal string) ([]gateway.Route, []channel.ID, error) {
			if principal != "alice" {
				return nil, nil, nil
			}
			return []gateway.Route{{Channel: "c0", SubjectID: "human:alice:7"}}, nil, nil
		})})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = gw.Close() })
	sessions := gateway.NewSessionStore()
	return New(Config{DataPlane: plane, Gateway: gw, Sessions: sessions, ContractVersion: "test"}), sessions
}

// fileRequest is a transfer by an authenticated person, the only kind this
// entrance answers.
func fileRequest(t *testing.T, sessions *gateway.SessionStore, principal, method, target string, body string) *http.Request {
	t.Helper()
	req := httptest.NewRequest(method, target, strings.NewReader(body))
	req.AddCookie(&http.Cookie{Name: sessionCookie, Value: sessions.Mint(principal, time.Minute)})
	return req
}

// A transfer names its channel and its ticket, and nothing else: the direction
// comes from the method, the download name comes from the ticket, and the actor
// is looked up rather than stated.
func TestFileGetIsNamedByItsTicketAlone(t *testing.T) {
	plane := &filePlaneStub{body: []byte("payload"), name: "report final.pdf"}
	p, sessions := filePortal(t, plane)
	rec := httptest.NewRecorder()
	p.ServeHTTP(rec, fileRequest(t, sessions, "alice", http.MethodGet, "/files?channel_id=c0&t=ticket-a", ""))
	if rec.Code != http.StatusOK || rec.Body.String() != "payload" {
		t.Fatalf("status=%d body=%q", rec.Code, rec.Body.String())
	}
	if plane.ticket != "ticket-a" || plane.mode != access.OpRead {
		t.Fatalf("plane call=(%q,%q)", plane.ticket, plane.mode)
	}
	if got := rec.Header().Get("Content-Disposition"); !strings.Contains(got, "report%20final.pdf") {
		t.Fatalf("content-disposition=%q", got)
	}
}

func TestFilePutStreamsThroughRedeemer(t *testing.T) {
	plane := &filePlaneStub{}
	p, sessions := filePortal(t, plane)
	rec := httptest.NewRecorder()
	p.ServeHTTP(rec, fileRequest(t, sessions, "alice", http.MethodPut, "/files?channel_id=c0&t=write-ticket", "bytes"))
	if rec.Code != http.StatusNoContent || string(plane.body) != "bytes" || plane.mode != access.OpWrite {
		t.Fatalf("status=%d body=%q mode=%q", rec.Code, plane.body, plane.mode)
	}
	if plane.ticket != "write-ticket" {
		t.Fatalf("ticket=%q", plane.ticket)
	}
}

// This entrance is where an outside identity becomes an inside one. A session
// names a principal, which is a claim an outsider can make and therefore must be
// authenticated; the plane is driven with an actor, which the runtime mints and
// nobody claims. So what the plane must be told is the actor that principal IS
// in the named channel — not the principal, and not anything the request says
// about itself.
func TestAFileTransferIsPerformedAsTheActorThePrincipalIsInThatChannel(t *testing.T) {
	plane := &filePlaneStub{body: []byte("payload")}
	p, sessions := filePortal(t, plane)

	rec := httptest.NewRecorder()
	p.ServeHTTP(rec, fileRequest(t, sessions, "alice", http.MethodPut, "/files?channel_id=c0&t=write-ticket", "bytes"))
	if plane.channelID != "c0" || plane.caller != "human:alice:7" {
		t.Fatalf("plane was driven as (%q,%q)", plane.channelID, plane.caller)
	}

	// A principal with no membership in the named channel has no actor there, so
	// there is nothing it could be doing this as.
	plane.channelID, plane.caller, plane.ticket = "", "", ""
	rec = httptest.NewRecorder()
	p.ServeHTTP(rec, fileRequest(t, sessions, "mallory", http.MethodPut, "/files?channel_id=c0&t=write-ticket", "bytes"))
	if rec.Code != http.StatusForbidden {
		t.Fatalf("non-member status=%d", rec.Code)
	}

	// And with no session at all there is no claim to resolve in the first place.
	for _, method := range []string{http.MethodGet, http.MethodPut} {
		rec := httptest.NewRecorder()
		p.ServeHTTP(rec, httptest.NewRequest(method, "/files?channel_id=c0&t=stolen-ticket", strings.NewReader("bytes")))
		if rec.Code != http.StatusUnauthorized {
			t.Fatalf("%s with no session status=%d", method, rec.Code)
		}
	}
	if plane.caller != "" || plane.ticket != "" {
		t.Fatalf("plane was reached without a resolved actor: caller=%q ticket=%q", plane.caller, plane.ticket)
	}
}

// A ticket's scope is (channel, actor), so a transfer that names neither its
// channel nor its ticket is not a weaker request — it is not one.
func TestAFileTransferMustNameItsChannelAndItsTicket(t *testing.T) {
	plane := &filePlaneStub{body: []byte("payload")}
	p, sessions := filePortal(t, plane)
	for _, target := range []string{"/files", "/files?channel_id=c0", "/files?t=ticket-a"} {
		for _, method := range []string{http.MethodGet, http.MethodPut} {
			rec := httptest.NewRecorder()
			p.ServeHTTP(rec, fileRequest(t, sessions, "alice", method, target, "bytes"))
			if rec.Code != http.StatusBadRequest {
				t.Fatalf("%s %s status=%d", method, target, rec.Code)
			}
		}
	}
	if plane.caller != "" || plane.ticket != "" {
		t.Fatalf("plane was reached: caller=%q ticket=%q", plane.caller, plane.ticket)
	}
}

func TestObsRouteAuthenticatesCookieAndForwardsRawAddressAndQuery(t *testing.T) {
	sessions := gateway.NewSessionStore()
	token := sessions.Mint("alice", time.Minute)
	plane := &obsPlaneStub{answer: obs.Observation{Subject: "channel/c/part/profile", Kind: "profile", Complete: true, Items: []obs.Item{}}}
	p := New(Config{Sessions: sessions, Obs: plane, ContractVersion: "test"})
	req := httptest.NewRequest(http.MethodGet, "/obs/channel/c%2Fpart/profile?parent_id=raw%2Fvalue", nil)
	req.AddCookie(&http.Cookie{Name: sessionCookie, Value: token})
	rec := httptest.NewRecorder()
	p.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}
	if plane.principal != "alice" || plane.path != "/obs/channel/c%2Fpart/profile" || plane.query != "parent_id=raw%2Fvalue" {
		t.Fatalf("pull args=(%q,%q,%q)", plane.principal, plane.path, plane.query)
	}
}

func TestObsRouteAcceptsBearerSessionAndRejectsUnauthenticated(t *testing.T) {
	sessions := gateway.NewSessionStore()
	token := sessions.Mint("alice", time.Minute)
	plane := &obsPlaneStub{answer: obs.Observation{Items: []obs.Item{}}}
	p := New(Config{Sessions: sessions, Obs: plane, ContractVersion: "test"})

	req := httptest.NewRequest(http.MethodGet, "/obs/space/decls", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	rec := httptest.NewRecorder()
	p.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK || plane.principal != "alice" {
		t.Fatalf("bearer status=%d principal=%q", rec.Code, plane.principal)
	}

	unauth := httptest.NewRecorder()
	p.ServeHTTP(unauth, httptest.NewRequest(http.MethodGet, "/obs/space/decls", nil))
	if unauth.Code != http.StatusUnauthorized || !strings.Contains(unauth.Body.String(), `"code":"not_authenticated"`) {
		t.Fatalf("unauth status=%d body=%s", unauth.Code, unauth.Body.String())
	}
}

func TestObsTypedFailureHTTPMatrix(t *testing.T) {
	tests := []struct {
		kind   obs.ErrorKind
		status int
		code   string
	}{
		{obs.ErrBadAddress, 400, "invalid_args"},
		{obs.ErrUnknownKind, 400, "invalid_args"},
		{obs.ErrBadQuery, 400, "invalid_args"},
		{obs.ErrUnauthed, 401, "not_authenticated"},
		{obs.ErrForbidden, 403, "permission_denied"},
		{obs.ErrNotServing, 503, "unavailable"},
		{obs.ErrTimeout, 503, "unavailable"},
		{obs.ErrOverloaded, 503, "unavailable"},
		{obs.ErrInternal, 500, "internal_error"},
	}
	for _, test := range tests {
		t.Run(string(test.kind), func(t *testing.T) {
			sessions := gateway.NewSessionStore()
			token := sessions.Mint("alice", time.Minute)
			p := New(Config{Sessions: sessions, Obs: &obsPlaneStub{err: &obs.Error{Kind: test.kind, Detail: "detail"}}, ContractVersion: "test"})
			req := httptest.NewRequest(http.MethodGet, "/obs/space/decls", nil)
			req.AddCookie(&http.Cookie{Name: sessionCookie, Value: token})
			rec := httptest.NewRecorder()
			p.ServeHTTP(rec, req)
			if rec.Code != test.status || !strings.Contains(rec.Body.String(), `"code":"`+test.code+`"`) {
				t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
			}
		})
	}
}

func TestObsWrongMethodNeverEntersPlane(t *testing.T) {
	plane := &obsPlaneStub{}
	p := New(Config{Obs: plane, ContractVersion: "test"})
	rec := httptest.NewRecorder()
	p.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/obs/space/decls", nil))
	if rec.Code != http.StatusMethodNotAllowed || plane.path != "" {
		t.Fatalf("status=%d plane.path=%q", rec.Code, plane.path)
	}
}

type blockingObsRegistry struct {
	entered chan struct{}
	exited  chan struct{}
}

func (r *blockingObsRegistry) PrincipalPresent(ctx context.Context, _ string) (bool, error) {
	close(r.entered)
	<-ctx.Done()
	close(r.exited)
	return false, ctx.Err()
}
func (*blockingObsRegistry) Channels(context.Context, *string) ([]obs.Row, bool, error) {
	return nil, false, errors.New("unexpected")
}
func (*blockingObsRegistry) Channel(context.Context, string) (obs.Row, bool, error) {
	return obs.Row{}, false, errors.New("unexpected")
}
func (*blockingObsRegistry) Principals(context.Context) ([]obs.Row, bool, error) {
	return nil, false, errors.New("unexpected")
}
func (*blockingObsRegistry) Daemons(context.Context) ([]obs.Row, bool, error) {
	return nil, false, errors.New("unexpected")
}
func (*blockingObsRegistry) Decls(context.Context) ([]obs.Row, bool, error) {
	return nil, false, errors.New("unexpected")
}

type recordingObsPlane struct {
	plane *obs.Plane
	done  chan error
}

func (p recordingObsPlane) Pull(ctx context.Context, principal, path, query string) (obs.Observation, error) {
	answer, err := p.plane.Pull(ctx, principal, path, query)
	p.done <- err
	return answer, err
}

func TestObsHTTPContextCancellationReachesBlockingRegistry(t *testing.T) {
	registry := &blockingObsRegistry{entered: make(chan struct{}), exited: make(chan struct{})}
	recorded := recordingObsPlane{plane: obs.New(obs.Config{Registry: registry}), done: make(chan error, 1)}
	sessions := gateway.NewSessionStore()
	token := sessions.Mint("alice", time.Minute)
	p := New(Config{Sessions: sessions, Obs: recorded, ContractVersion: "test"})

	ctx, cancel := context.WithCancel(context.Background())
	req := httptest.NewRequest(http.MethodGet, "/obs/space/decls", nil).WithContext(ctx)
	req.AddCookie(&http.Cookie{Name: sessionCookie, Value: token})
	rec := httptest.NewRecorder()
	handlerDone := make(chan struct{})
	go func() {
		p.ServeHTTP(rec, req)
		close(handlerDone)
	}()
	<-registry.entered
	cancel()
	select {
	case <-registry.exited:
	case <-time.After(time.Second):
		t.Fatal("registry did not stop after HTTP context cancellation")
	}
	select {
	case err := <-recorded.done:
		var typed *obs.Error
		if !errors.As(err, &typed) || typed.Kind != obs.ErrCanceled {
			t.Fatalf("plane error=%v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("plane did not return after cancellation")
	}
	select {
	case <-handlerDone:
	case <-time.After(time.Second):
		t.Fatal("HTTP handler did not return after cancellation")
	}
	if rec.Body.Len() != 0 || rec.Header().Get("Content-Type") != "" {
		t.Fatalf("canceled handler wrote response: headers=%v body=%s", rec.Header(), rec.Body.String())
	}
}

// The UI shares the entrance with the API because it addresses the node over
// relative paths. Sharing an origin is not sharing a namespace: /api stays the
// machine's, and answers as the machine even when nothing is there.
func TestTheUIAnswersOnlyForPathsTheNodeDidNotClaim(t *testing.T) {
	ui := fstest.MapFS{
		"index.html":    {Data: []byte("<!doctype html>page")},
		"assets/app.js": {Data: []byte("console.log(1)")},
	}
	p := New(Config{ContractVersion: "test", Sessions: gateway.NewSessionStore(), Web: ui})
	for _, tc := range []struct {
		name   string
		method string
		path   string
		status int
		body   string
	}{
		{name: "root is the page", method: http.MethodGet, path: "/", status: http.StatusOK, body: "<!doctype html>page"},
		{name: "a built asset is itself", method: http.MethodGet, path: "/assets/app.js", status: http.StatusOK, body: "console.log(1)"},
		{name: "a UI route the browser resolves gets the page", method: http.MethodGet, path: "/channel/c0", status: http.StatusOK, body: "<!doctype html>page"},
		{name: "a missing file is a missing file", method: http.MethodGet, path: "/assets/gone.js", status: http.StatusNotFound},
		{name: "an unknown API path stays the machine's", method: http.MethodGet, path: "/api/nothing", status: http.StatusNotFound},
		{name: "only reads reach the UI", method: http.MethodPost, path: "/channel/c0", status: http.StatusNotFound},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			p.ServeHTTP(rec, httptest.NewRequest(tc.method, tc.path, nil))
			if rec.Code != tc.status {
				t.Fatalf("status=%d want=%d body=%s", rec.Code, tc.status, rec.Body.String())
			}
			if tc.body != "" && rec.Body.String() != tc.body {
				t.Fatalf("body=%q want=%q", rec.Body.String(), tc.body)
			}
			if tc.status == http.StatusNotFound {
				if got := rec.Header().Get("Content-Type"); got != "application/json" {
					t.Fatalf("a refusal must stay machine-readable: content-type=%q body=%s", got, rec.Body.String())
				}
			}
		})
	}
}

// `root` is what the person who installed the node calls that account, and the
// node invented the domain, so logging in completes it. Only logging in:
// registration refuses a bare name outright (see the register handler), which is
// what keeps the two spellings from ever naming different accounts.
func TestLoggingInReadsABareNameAsAnAccountTheNodeCarved(t *testing.T) {
	for _, tc := range []struct{ typed, means string }{
		{typed: "root", means: "root@atoll.local"},
		{typed: "  root  ", means: "root@atoll.local"},
		{typed: "guest", means: "guest@atoll.local"},
		// 已经指名了域的，原样——节点不是 principal 的唯一来源
		{typed: "alice@example.com", means: "alice@example.com"},
		{typed: "root@atoll.local", means: "root@atoll.local"},
		// 空的仍然是空的：缺不缺账号由入口自己判，恒不由这里补出一个来
		{typed: "", means: ""},
		{typed: "   ", means: ""},
	} {
		if got := qualifyLoginName(tc.typed); got != tc.means {
			t.Fatalf("qualifyLoginName(%q)=%q want %q", tc.typed, got, tc.means)
		}
	}
}

// A bare name must never become a stored account: logging in would look for it
// under the node's domain and never find it. So the entrance refuses instead of
// completing — the refusal happens before anything is written.
func TestRegisteringRefusesANameWithoutADomain(t *testing.T) {
	p := New(Config{ContractVersion: "test", Sessions: gateway.NewSessionStore()})
	for _, typed := range []string{"root2", "alice", "  bob  "} {
		rec := httptest.NewRecorder()
		body := strings.NewReader(`{"email":"` + strings.TrimSpace(typed) + `","password":"longenough"}`)
		p.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/api/identity/register", body))
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("register %q: status=%d want 400 body=%s", typed, rec.Code, rec.Body.String())
		}
		if !strings.Contains(rec.Body.String(), "full address") {
			t.Fatalf("register %q: 拒绝信息要说清该怎么改：%s", typed, rec.Body.String())
		}
	}
}
