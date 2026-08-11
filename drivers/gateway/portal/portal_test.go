package portal

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/wanpengxie/atoll/drivers/gateway"
	"github.com/wanpengxie/atoll/platform/lagoon"
)

type submitterStub struct {
	reply lagoon.Reply
	err   error
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
	p := New(Config{ContractVersion: "test", Submitter: submitterStub{reply: lagoon.Reply{Value: lagoon.PrincipalRow{ID: "alice"}}}, Sessions: sessions})
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
