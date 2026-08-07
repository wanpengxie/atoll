package app_test

// Half-built channel handling over the HTTP face: a desired row whose physical
// image has not converged yet (the ordinary post-acceptance build window) must
// stay deletable, and reads against it must answer honestly instead of
// fabricating or panicking.

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/wanpengxie/atoll/protocol/channel"
)

// TestHalfBuiltChannel_DeleteSucceeds pins that delete authority is realm-side
// owner policy and must remain reachable when the channel-local image is
// incomplete.
func TestHalfBuiltChannel_DeleteSucceeds(t *testing.T) {
	env := setupTestApp(t)
	srv := httptest.NewServer(env.app.Handler())
	t.Cleanup(srv.Close)
	s := fullSetup(t, env)

	chID, err := env.app.CreateHalfBuiltChannelForTest(s.userID, "half-built")
	if err != nil {
		t.Fatalf("CreateHalfBuiltChannelForTest: %v", err)
	}

	w := env.do(t, "DELETE", "/api/channels/"+chID, nil, s.cookies)
	if w.Code != http.StatusOK {
		t.Fatalf("delete half-built channel: want 200, got %d (%s)", w.Code, w.Body.String())
	}
	// Gone from the directory now.
	if g := env.do(t, "GET", "/api/channels/"+chID, nil, s.cookies); g.Code != http.StatusNotFound {
		t.Fatalf("deleted channel GET: want 404, got %d", g.Code)
	}
}

// TestHalfBuiltChannel_OpenClearError pins that opening a half-built channel
// gives a clear, non-panic response. Cross-membrane reads require the channel's
// realm-tool capability, which a half-built image cannot provide.
func TestHalfBuiltChannel_OpenClearError(t *testing.T) {
	env := setupTestApp(t)
	srv := httptest.NewServer(env.app.Handler())
	t.Cleanup(srv.Close)
	s := fullSetup(t, env)

	chID, err := env.app.CreateHalfBuiltChannelForTest(s.userID, "half-built-open")
	if err != nil {
		t.Fatalf("CreateHalfBuiltChannelForTest: %v", err)
	}

	// Desired directory detail is available even while physical state has not
	// converged; only the derived default_agent field is omitted.
	if g := env.do(t, "GET", "/api/channels/"+chID, nil, s.cookies); g.Code != http.StatusOK {
		t.Fatalf("GET half-built channel: want 200, got %d (%s)", g.Code, g.Body.String())
	} else if _, present := respJSON(t, g)["default_agent"]; present {
		t.Fatalf("half-built channel fabricated default_agent: %s", g.Body.String())
	}
	// The absent local image is reported honestly rather than fabricated.
	if _, err := env.app.HumanRosterForTest(channel.ID(chID)); err == nil {
		t.Fatal("half-built channel unexpectedly exposed a local registry")
	}

	// Opening the ws: a non-channel-member (no Admit ever landed) may tail but a
	// write frame is refused with a clear forbidden error — never a panic (not_member
	// retired 连接模型勘误期: eligibility refusal is uniformly forbidden, 表①).
	c := dialWS(t, srv, s.cookies, chID, 0)
	defer c.close()
	ack := c.sendMessage(map[string]any{
		"msg_type": "chat.text", "kind": "event",
		"payload": map[string]any{"text": "anyone home?"},
	})
	if ack["type"] != "error" || ack["error"] != "forbidden" {
		t.Fatalf("write to half-built channel: want error forbidden, got %v", ack)
	}
}
