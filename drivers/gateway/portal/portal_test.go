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
	"time"

	"github.com/wanpengxie/atoll/drivers/gateway"
	"github.com/wanpengxie/atoll/platform/dataplane"
	"github.com/wanpengxie/atoll/platform/lagoon"
	"github.com/wanpengxie/atoll/platform/obs"
	"github.com/wanpengxie/atoll/protocol/access"
	"github.com/wanpengxie/atoll/protocol/channel"
)

type filePlaneStub struct {
	principal string
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

func (f *filePlaneStub) Resolve(channel.ID, string) (dataplane.Ticket, error) {
	return dataplane.Ticket{}, errors.New("unused")
}
func (f *filePlaneStub) ServeExchange(context.Context, channel.ID, io.ReadWriteCloser) {}
func (f *filePlaneStub) TicketFile(string, string) (string, bool)                     { return f.name, f.name != "" }
func (f *filePlaneStub) ServeHTTP(_ context.Context, principal, ticket string, mode access.Operation, dst io.Writer, src io.Reader) error {
	f.principal, f.ticket, f.mode = principal, ticket, mode
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

// fileRequest is a transfer by an authenticated person, the only kind this
// entrance answers.
func fileRequest(t *testing.T, sessions *gateway.SessionStore, principal, method, target string, body string) *http.Request {
	t.Helper()
	req := httptest.NewRequest(method, target, strings.NewReader(body))
	req.AddCookie(&http.Cookie{Name: sessionCookie, Value: sessions.Mint(principal, time.Minute)})
	return req
}

// A transfer carries its ticket and nothing else. The direction comes from the
// method, and the name a browser saves the download under comes from the ticket
// too — the client never spells out where the bytes live, so there is no second
// place for the two sides to disagree about how to spell it.
func TestFileGetIsNamedByItsTicketAlone(t *testing.T) {
	sessions := gateway.NewSessionStore()
	plane := &filePlaneStub{body: []byte("payload"), name: "report final.pdf"}
	p := New(Config{DataPlane: plane, Sessions: sessions, ContractVersion: "test"})
	rec := httptest.NewRecorder()
	p.ServeHTTP(rec, fileRequest(t, sessions, "alice", http.MethodGet, "/files?t=ticket-a", ""))
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
	sessions := gateway.NewSessionStore()
	plane := &filePlaneStub{}
	p := New(Config{DataPlane: plane, Sessions: sessions, ContractVersion: "test"})
	rec := httptest.NewRecorder()
	p.ServeHTTP(rec, fileRequest(t, sessions, "alice", http.MethodPut, "/files?t=write-ticket", "bytes"))
	if rec.Code != http.StatusNoContent || string(plane.body) != "bytes" || plane.mode != access.OpWrite {
		t.Fatalf("status=%d body=%q mode=%q", rec.Code, plane.body, plane.mode)
	}
	if plane.ticket != "write-ticket" {
		t.Fatalf("ticket=%q", plane.ticket)
	}
}

// Bytes landing on a channel's disk are an operation by somebody, so this
// entrance establishes who — exactly as /ws and /obs do. A ticket says what was
// granted, never who is holding it, so an unauthenticated request must not be
// able to finish somebody else's transfer by presenting one.
func TestAFileTransferIsPerformedAsTheAuthenticatedPerson(t *testing.T) {
	sessions := gateway.NewSessionStore()
	plane := &filePlaneStub{body: []byte("payload")}
	p := New(Config{DataPlane: plane, Sessions: sessions, ContractVersion: "test"})

	rec := httptest.NewRecorder()
	p.ServeHTTP(rec, fileRequest(t, sessions, "alice", http.MethodPut, "/files?t=write-ticket", "bytes"))
	if plane.principal != "alice" {
		t.Fatalf("plane was told principal=%q", plane.principal)
	}

	plane.principal, plane.ticket, plane.body = "", "", nil
	for _, method := range []string{http.MethodGet, http.MethodPut} {
		rec := httptest.NewRecorder()
		p.ServeHTTP(rec, httptest.NewRequest(method, "/files?t=stolen-ticket", strings.NewReader("bytes")))
		if rec.Code != http.StatusUnauthorized {
			t.Fatalf("%s with no session status=%d", method, rec.Code)
		}
	}
	if plane.ticket != "" || plane.body != nil {
		t.Fatalf("plane was reached without a session: ticket=%q body=%q", plane.ticket, plane.body)
	}
}

// The ticket is the other half: a session says who, a ticket says what was
// granted, and neither alone reaches the plane.
func TestFileTransferWithoutATicketNeverReachesThePlane(t *testing.T) {
	sessions := gateway.NewSessionStore()
	plane := &filePlaneStub{body: []byte("payload")}
	p := New(Config{DataPlane: plane, Sessions: sessions, ContractVersion: "test"})
	for _, method := range []string{http.MethodGet, http.MethodPut} {
		rec := httptest.NewRecorder()
		p.ServeHTTP(rec, fileRequest(t, sessions, "alice", method, "/files", "bytes"))
		if rec.Code != http.StatusForbidden {
			t.Fatalf("%s without a ticket status=%d", method, rec.Code)
		}
	}
	if plane.principal != "" || plane.ticket != "" {
		t.Fatalf("plane was reached: principal=%q ticket=%q", plane.principal, plane.ticket)
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
