package e2e

import (
	"net/http"
	"testing"
	"time"
)

func TestPortalIdentitySessionAndMessageRoundTrip(t *testing.T) {
	h := newHarness(t)

	user := newAPIClient(t, h.base)
	registered := user.register("portal-e2e-user", "portal@example.test", "portal-password")
	if registered["id"] != "portal-e2e-user" {
		t.Fatalf("register=%v", registered)
	}
	user.request(http.MethodPost, "/api/identity/logout", nil, http.StatusOK)
	loggedIn := user.login("portal@example.test", "portal-password")
	if loggedIn["id"] != "portal-e2e-user" {
		t.Fatalf("login=%v", loggedIn)
	}
	// The authenticated websocket itself is part of the identity loop. Its
	// channel-blind attach is the only operation possible until the portal
	// exposes this new principal's random home channel coordinate.
	userWS := dialWS(t, h.base, user.cookieHeader(), map[string]int64{})
	userWS.close()

	_, rootWS := rootClient(t, h, map[string]int64{c0ChannelID: 0})
	messageID := rootWS.submit(c0ChannelID, "e2e.note", "event", nil, map[string]any{"text": "portal-round-trip"})
	echo := rootWS.awaitEnvelope(func(envelope map[string]any) bool {
		return envelope["id"] == messageID && envelope["type"] == "e2e.note"
	}, 15*time.Second)
	payload, _ := echo["payload"].(map[string]any)
	if payload["text"] != "portal-round-trip" {
		t.Fatalf("feed echo=%v", echo)
	}
}
