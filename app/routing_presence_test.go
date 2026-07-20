package app_test

import (
	"net/http/httptest"
	"testing"
	"time"
)

// TestRouting_DefaultAgentReady pins the membrane routing contract for the
// always-on default floor. Carrier recovery itself is a platform/home concern.
func TestRouting_DefaultAgentReady(t *testing.T) {
	env := setupTestApp(t)
	srv := httptest.NewServer(env.app.Handler())
	t.Cleanup(srv.Close)
	s := fullSetup(t, env)
	chID := s.chID

	c := dialWS(t, srv, s.cookies, chID, 0)
	defer c.close()

	// Every channel is created with the agent:boost floor as its default (a live
	// server-embedded cell). No-audience send routes to it (ack).
	deadline := time.Now().Add(2 * time.Second)
	for {
		ack := c.sendMessage(map[string]any{"msg_type": "chat.text", "payload": map[string]any{"text": "hi"}})
		if ack["type"] == "ack" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("live default did not become ready, last frame %v", ack)
		}
		time.Sleep(10 * time.Millisecond)
	}

}
