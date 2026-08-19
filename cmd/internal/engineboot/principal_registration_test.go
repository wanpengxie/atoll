package engineboot

import (
	"bytes"
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/platform/channelspec"
)

func TestGuestRegistrationConflictsNeverMintSessions(t *testing.T) {
	eng, err := Boot(Config{ChannelDBDir: filepath.Join(t.TempDir(), "channels"), Addr: "127.0.0.1:0", RootPassword: "test-root-password", OpenRegistration: true}, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close(context.Background())

	register := func(body string) *httptest.ResponseRecorder {
		t.Helper()
		req := httptest.NewRequest(http.MethodPost, "/api/identity/register", bytes.NewBufferString(body))
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()
		eng.handler.ServeHTTP(rec, req)
		return rec
	}
	assertConflictWithoutCookie := func(label string, rec *httptest.ResponseRecorder) {
		t.Helper()
		if rec.Code != http.StatusConflict {
			t.Fatalf("%s status=%d body=%s", label, rec.Code, rec.Body.String())
		}
		if cookies := rec.Header().Values("Set-Cookie"); len(cookies) != 0 {
			t.Fatalf("%s minted cookies=%v", label, cookies)
		}
	}

	assertConflictWithoutCookie("root email", register(`{"id":"root-copy","email":"root@atoll.local","password":"anything","display_name":"Root"}`))
	first := register(`{"id":"repeat-user","email":"repeat@example.test","password":"secret","display_name":"Repeat"}`)
	if first.Code != http.StatusCreated || len(first.Header().Values("Set-Cookie")) == 0 {
		t.Fatalf("first registration status=%d cookies=%v body=%s", first.Code, first.Header().Values("Set-Cookie"), first.Body.String())
	}
	assertConflictWithoutCookie("same registration", register(`{"id":"repeat-user","email":"repeat@example.test","password":"different","display_name":"Repeat"}`))

	if _, ok, err := eng.registry.GetPrincipalStatus(context.Background(), channelspec.RootPrincipalID); err != nil || !ok {
		t.Fatalf("root disappeared ok=%v err=%v", ok, err)
	}
}

// Registration is node policy and closed by default: c0 exposes no
// principal-create endpoint to the lobby, so the guest can neither see it on
// c0's card nor call it, and the portal answers 403 registration_closed
// without minting a session. Login stays open.
func TestRegistrationClosedByDefaultIsInvisibleAndRefused(t *testing.T) {
	eng, err := Boot(Config{ChannelDBDir: filepath.Join(t.TempDir(), "channels"), Addr: "127.0.0.1:0", RootPassword: "test-root-password"}, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close(context.Background())
	deadline := time.Now().Add(5 * time.Second)
	for {
		if _, ok := eng.host.Acquire(channelspec.LobbyChannelID); ok {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("lobby unavailable")
		}
		time.Sleep(20 * time.Millisecond)
	}
	req := httptest.NewRequest(http.MethodPost, "/api/identity/register", bytes.NewBufferString(`{"id":"alice","email":"alice@example.test","password":"secret"}`))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	eng.handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusForbidden || !strings.Contains(rec.Body.String(), "registration_closed") || rec.Header().Get("Set-Cookie") != "" {
		t.Fatalf("closed registration: status=%d body=%s cookie=%q", rec.Code, rec.Body.String(), rec.Header().Get("Set-Cookie"))
	}
	if _, found, _ := eng.registry.GetPrincipalStatus(context.Background(), "alice"); found {
		t.Fatal("closed registration still created a principal")
	}
	login := httptest.NewRequest(http.MethodPost, "/api/identity/login", bytes.NewBufferString(`{"email":"root@atoll.local","password":"test-root-password"}`))
	login.Header.Set("Content-Type", "application/json")
	rec = httptest.NewRecorder()
	eng.handler.ServeHTTP(rec, login)
	if rec.Code != http.StatusOK {
		t.Fatalf("login with registration closed: status=%d body=%s", rec.Code, rec.Body.String())
	}
}
