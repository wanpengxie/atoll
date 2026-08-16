package engineboot

import (
	"bytes"
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"github.com/wanpengxie/atoll/protocol"
)

func TestGuestRegistrationConflictsNeverMintSessions(t *testing.T) {
	eng, err := Boot(Config{ChannelDBDir: filepath.Join(t.TempDir(), "channels"), Addr: "127.0.0.1:0", RootPassword: "test-root-password"}, slog.New(slog.DiscardHandler))
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

	if _, ok, err := eng.registry.GetPrincipalStatus(context.Background(), protocol.RootPrincipalID); err != nil || !ok {
		t.Fatalf("root disappeared ok=%v err=%v", ok, err)
	}
}
