package app_test

import (
	"net/http/httptest"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/protocol/channel"
)

// TestRouting_DefaultAgentRecoversAfterCarrierCrash pins the new liveness
// contract: an always-on declared identity that loses its local carrier is
// restarted by the Home supervisor and routing resumes without changing the
// durable default target.
func TestRouting_DefaultAgentRecoversAfterCarrierCrash(t *testing.T) {
	env := setupTestApp(t)
	srv := httptest.NewServer(env.app.Handler())
	t.Cleanup(srv.Close)
	s := fullSetup(t, env)
	chID := s.chID

	c := dialWS(t, srv, s.cookies, chID, 0)
	defer c.close()

	// Every channel is created with the agent:boost floor as its default (a live
	// server-embedded cell). No-audience send routes to it (ack).
	ack := c.sendMessage(map[string]any{"msg_type": "chat.text", "payload": map[string]any{"text": "hi"}})
	if ack["type"] != "ack" {
		t.Fatalf("live default: want ack, got %v", ack)
	}

	// Kill the boost floor's cell — the channel's default_agent still POINTS at it
	// (channels table unchanged), it is simply no longer embodied.
	if err := env.app.KillCellForTest(channel.ID(chID), s.boostID); err != nil {
		t.Fatalf("kill cell: %v", err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for {
		ack2 := c.sendMessage(map[string]any{"msg_type": "chat.text", "payload": map[string]any{"text": "hi again"}})
		if ack2["type"] == "ack" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("default did not recover, last frame %v", ack2)
		}
		time.Sleep(10 * time.Millisecond)
	}
}
