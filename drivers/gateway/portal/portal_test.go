package portal

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/wanpengxie/atoll/drivers/gateway"
	"github.com/wanpengxie/atoll/platform/dataplane"
	"github.com/wanpengxie/atoll/platform/lagoon"
	"github.com/wanpengxie/atoll/protocol/access"
	"github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/protocol/resource"
)

type submitterStub struct {
	reply lagoon.Reply
	err   error
}

type filePlaneStub struct {
	address resource.ResourceID
	ticket  string
	mode    access.Operation
	body    []byte
	err     error
}

func (f *filePlaneStub) Resolve(channel.ID, string) (dataplane.Ticket, error) {
	return dataplane.Ticket{}, errors.New("unused")
}
func (f *filePlaneStub) ResolveLocal(string, channel.ID, string) (dataplane.Ticket, error) {
	return dataplane.Ticket{}, errors.New("unused")
}
func (f *filePlaneStub) ServeExchange(context.Context, channel.ID, io.ReadWriteCloser) {}
func (f *filePlaneStub) ServeHTTP(_ context.Context, address resource.ResourceID, ticket string, mode access.Operation, dst io.Writer, src io.Reader) error {
	f.address, f.ticket, f.mode = address, ticket, mode
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

func (s submitterStub) Submit(context.Context, lagoon.SubmitIn) (lagoon.Reply, error) {
	return lagoon.Reply{}, errors.New("unexpected authenticated submit")
}
func (s submitterStub) SubmitApplication(context.Context, lagoon.Word, any) (lagoon.Reply, error) {
	return s.reply, s.err
}

func TestRegisterConflictNeverMintsSession(t *testing.T) {
	sessions := gateway.NewSessionStore()
	p := New(Config{ContractVersion: "test", Submitter: submitterStub{err: &lagoon.Error{Code: lagoon.CodeConflictExists}}, Sessions: sessions})
	req := httptest.NewRequest(http.MethodPost, "/api/identity/register", strings.NewReader(`{"id":"alice","email":"alice@example.test","password":"secret"}`))
	rec := httptest.NewRecorder()
	p.ServeHTTP(rec, req)
	if rec.Code != http.StatusConflict {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}
	if len(rec.Result().Cookies()) != 0 {
		t.Fatalf("conflict minted cookies: %+v", rec.Result().Cookies())
	}
}

func TestRegisterSuccessMintsInMemorySession(t *testing.T) {
	sessions := gateway.NewSessionStore()
	p := New(Config{ContractVersion: "test", Submitter: submitterStub{reply: lagoon.Reply{Value: json.RawMessage(`{"id":"alice"}`)}}, Sessions: sessions})
	req := httptest.NewRequest(http.MethodPost, "/api/identity/register", strings.NewReader(`{"id":"alice","email":"alice@example.test","password":"secret"}`))
	rec := httptest.NewRecorder()
	p.ServeHTTP(rec, req)
	if rec.Code != http.StatusCreated {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}
	cookies := rec.Result().Cookies()
	if len(cookies) != 1 {
		t.Fatalf("cookies=%+v", cookies)
	}
	principal, ok := sessions.Verify(cookies[0].Value)
	if !ok || principal != "alice" {
		t.Fatalf("session=(%q,%v)", principal, ok)
	}
}

func TestRegisterMissingReplyValueNeverMintsSession(t *testing.T) {
	sessions := gateway.NewSessionStore()
	p := New(Config{ContractVersion: "test", Submitter: submitterStub{reply: lagoon.Reply{}}, Sessions: sessions})
	req := httptest.NewRequest(http.MethodPost, "/api/identity/register", strings.NewReader(`{"email":"alice@example.test","password":"secret"}`))
	rec := httptest.NewRecorder()
	p.ServeHTTP(rec, req)
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}
	if len(rec.Result().Cookies()) != 0 {
		t.Fatalf("missing reply value minted cookies: %+v", rec.Result().Cookies())
	}
}

func TestRegisterAllowsUnknownFieldsButRejectsTrailingDocument(t *testing.T) {
	p := New(Config{ContractVersion: "test", Submitter: submitterStub{reply: lagoon.Reply{Value: json.RawMessage(`{"id":"alice"}`)}}, Sessions: gateway.NewSessionStore()})
	for _, tc := range []struct {
		name string
		body string
		want int
	}{
		{name: "unknown field", body: `{"email":"alice@example.test","password":"secret","future_option":true}`, want: http.StatusCreated},
		{name: "trailing document", body: `{"email":"alice@example.test","password":"secret"} {}`, want: http.StatusBadRequest},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			p.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/api/identity/register", strings.NewReader(tc.body)))
			if rec.Code != tc.want {
				t.Fatalf("status=%d want=%d body=%s", rec.Code, tc.want, rec.Body.String())
			}
		})
	}
}

func TestRegisterRejectsMissingFieldsAndTrailingJSON(t *testing.T) {
	p := New(Config{ContractVersion: "test", Submitter: submitterStub{}, Sessions: gateway.NewSessionStore()})
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
	p := New(Config{ContractVersion: "test", Submitter: submitterStub{}, Sessions: gateway.NewSessionStore()})
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

func TestFileRouteUsesOneCanonicalEncodedAddress(t *testing.T) {
	address := "daemon://host/docs/report%20final.pdf"
	plane := &filePlaneStub{body: []byte("payload")}
	p := New(Config{DataPlane: plane, ContractVersion: "test"})
	req := httptest.NewRequest(http.MethodGet, "/files/"+url.PathEscape(address)+"?t=ticket-a", nil)
	rec := httptest.NewRecorder()
	p.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK || rec.Body.String() != "payload" {
		t.Fatalf("status=%d body=%q", rec.Code, rec.Body.String())
	}
	if plane.address != resource.ResourceID(address) || plane.ticket != "ticket-a" || plane.mode != access.OpRead {
		t.Fatalf("plane call=(%q,%q,%q)", plane.address, plane.ticket, plane.mode)
	}
	if got := rec.Header().Get("Content-Disposition"); !strings.Contains(got, "report%20final.pdf") {
		t.Fatalf("content-disposition=%q", got)
	}

	for _, raw := range []string{
		"/files/daemon://host/x?t=ticket-a",
		"/files/" + strings.ReplaceAll(url.PathEscape(address), "%2F", "%2f") + "?t=ticket-a",
	} {
		rec := httptest.NewRecorder()
		p.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, raw, nil))
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("non-canonical %q status=%d", raw, rec.Code)
		}
	}
}

func TestFilePutStreamsThroughRedeemer(t *testing.T) {
	address := "daemon://host/docs/new.bin"
	plane := &filePlaneStub{}
	p := New(Config{DataPlane: plane, ContractVersion: "test"})
	rec := httptest.NewRecorder()
	p.ServeHTTP(rec, httptest.NewRequest(http.MethodPut, "/files/"+url.PathEscape(address)+"?t=write-ticket", strings.NewReader("bytes")))
	if rec.Code != http.StatusNoContent || string(plane.body) != "bytes" || plane.mode != access.OpWrite {
		t.Fatalf("status=%d body=%q mode=%q", rec.Code, plane.body, plane.mode)
	}
}
