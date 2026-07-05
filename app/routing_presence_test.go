package app_test

import (
	"fmt"
	"net/http"
	"testing"

	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
)

// TestRouting_DeadDefaultAgentIs503 (S3, DoD): no-audience routing judges the
// default agent by PRESENCE (View.Stat), not membership (ListActors). A live
// default routes (201); once its cell is killed the same send is 503 — never the
// old silent 201 into a black hole.
func TestRouting_DeadDefaultAgentIs503(t *testing.T) {
	env := setupTestApp(t)
	s := fullSetup(t, env)
	chID := s.chID

	// Every channel is created with the agent:boost floor as its default (a live
	// server-embedded cell). No-audience send routes to it (201).
	w := env.do(t, "POST", fmt.Sprintf("/api/channels/%s/messages", chID),
		map[string]any{"type": "chat.text", "payload": map[string]any{"text": "hi"}}, s.cookies)
	assertStatus(t, w, http.StatusCreated)

	// Kill the boost floor's cell — the channel's default_agent still POINTS at it
	// (channels table unchanged), it is simply no longer embodied.
	if err := env.app.KillCellForTest(channel.ID(chID), actor.ActorID("agent:boost")); err != nil {
		t.Fatalf("kill cell: %v", err)
	}

	// Brain-dead default AND dead boost floor → the same send is now 503, not the
	// old silent 201 into a black hole (this is the ListActors→Stat fix).
	w = env.do(t, "POST", fmt.Sprintf("/api/channels/%s/messages", chID),
		map[string]any{"type": "chat.text", "payload": map[string]any{"text": "hi again"}}, s.cookies)
	assertStatus(t, w, http.StatusServiceUnavailable)
}

// TestHomeUnavailable_TwoState (A10, DoD): home==nil splits honestly by whether
// the directory (channels table) knows the channel — absent → 404 (permanent),
// present-but-not-open → 503 "channel unavailable" (retryable). One handler, two
// states, never conflated.
func TestHomeUnavailable_TwoState(t *testing.T) {
	env := setupTestApp(t)
	s := fullSetup(t, env)

	// (i) directory has no such channel → 404.
	w := env.do(t, "GET", "/api/channels/does-not-exist/messages", nil, s.cookies)
	assertStatus(t, w, http.StatusNotFound)

	// (ii) directory HAS the channel but its home is not open → 503 unavailable.
	env.app.DropHomeForTest(channel.ID(s.chID))
	w = env.do(t, "GET", fmt.Sprintf("/api/channels/%s/messages", s.chID), nil, s.cookies)
	assertStatus(t, w, http.StatusServiceUnavailable)
	if got := respJSON(t, w)["error"]; got != "channel unavailable" {
		t.Fatalf("503 body error = %q, want %q", got, "channel unavailable")
	}
}
