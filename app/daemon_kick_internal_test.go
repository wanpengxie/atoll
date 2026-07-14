package app

import (
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"github.com/wanpengxie/atoll/protocol/channel"
)

// A link is registered before its attach frame is accepted, while IsAttached
// intentionally remains false. Revocation must still execute its first Kick;
// the derived observation is only useful after the write, as a convergence
// check. This is the exact publication seam the old precondition skipped.
func TestKickDaemonConvergeKicksRegisteredLinkBeforeAttachedPublication(t *testing.T) {
	dir := t.TempDir()
	db, err := openTestAppDB(t, filepath.Join(dir, "app.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	a, err := New(Config{
		DB:           db,
		ChannelDBDir: filepath.Join(dir, "channels"),
		Logger:       slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = a.Close() })

	chID := channel.ID("kick-before-attached-publication")
	h, err := a.createHome(chID, filepath.Join(dir, "kick-channel.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h.ServeAttach(w, r, "daemon-1")
	}))
	t.Cleanup(srv.Close)

	conn, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(srv.URL, "http"), nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	// ServeAttach registers the link before reading an attach frame. Leave the
	// peer deliberately half-attached, so View.IsAttached remains false while a
	// real Kick target exists.
	time.Sleep(50 * time.Millisecond)
	if h.View().IsAttached("daemon-1") {
		t.Fatal("half-attached link unexpectedly published IsAttached")
	}
	a.kickDaemonConverge(chID, "daemon-1")

	_ = conn.SetReadDeadline(time.Now().Add(time.Second))
	_, _, err = conn.ReadMessage()
	if err == nil {
		t.Fatal("registered half-attached link survived kick convergence")
	}
	if timeout, ok := err.(interface{ Timeout() bool }); ok && timeout.Timeout() {
		t.Fatal("kick was skipped because IsAttached had not yet been published")
	}
}
