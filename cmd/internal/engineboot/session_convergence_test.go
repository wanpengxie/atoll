package engineboot

import (
	"bytes"
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/platform/lagoon"
	"github.com/wanpengxie/atoll/platform/subjectgate"
	"github.com/wanpengxie/atoll/protocol"
	"github.com/wanpengxie/atoll/protocol/channel"
)

func TestRegistrarCommitPokeConvergesAnExistingSession(t *testing.T) {
	eng, err := Boot(Config{ChannelDBDir: filepath.Join(t.TempDir(), "channels"), Addr: "127.0.0.1:0", RootPassword: "test-root-password"}, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close(context.Background())
	req := httptest.NewRequest(http.MethodPost, "/api/identity/register", bytes.NewBufferString(`{"id":"poke-user","email":"poke@example.test","password":"secret"}`))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	eng.handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusCreated {
		t.Fatalf("register status=%d body=%s", rec.Code, rec.Body.String())
	}
	var home channel.ID
	rows, err := eng.registry.ListPresentChannels(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for _, row := range rows {
		if row.OwnerPrincipal == "poke-user" {
			home = row.ID
		}
	}
	if home == "" {
		t.Fatal("registered home missing")
	}
	session, err := eng.gateway.Attach("poke-user", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()
	session.StartFeed()
	probe, err := subjectgate.NewFrame(subjectgate.FrameCancel, "poke-probe", subjectgate.CancelPayload{ChannelID: string(home), ReqID: "missing"})
	if err != nil {
		t.Fatal(err)
	}
	if code := subjectgateErrorCode(t, session.Upstream(probe)); code == subjectgate.CodeForbidden {
		t.Fatal("present principal session lacked its home route")
	}
	core, _ := eng.host.Acquire(protocol.C0ChannelID)
	registrar := onlyDecl(t, core, lagoon.RegistrarSeatDeclID)
	terminalValue(t, callMember(t, protocol.C0ChannelID, core, protocol.RootPrincipalID, registrar, string(lagoon.WordPrincipalRetire), map[string]any{"principal_id": "poke-user"}), nil)

	timer := time.NewTimer(2 * time.Second)
	defer timer.Stop()
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for {
		if code := subjectgateErrorCode(t, session.Upstream(probe)); code == subjectgate.CodeForbidden {
			return
		}
		select {
		case <-timer.C:
			t.Fatal("session did not converge from registrar onCommit poke")
		case <-ticker.C:
		}
	}
}

func subjectgateErrorCode(t *testing.T, frame subjectgate.Frame) string {
	t.Helper()
	if frame.Type != subjectgate.FrameError {
		return ""
	}
	var payload subjectgate.ErrorPayload
	if err := frame.DecodePayload(&payload); err != nil {
		t.Fatal(err)
	}
	return payload.Code
}
