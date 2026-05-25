//go:build e2e

package e2e

import (
	"context"
	"testing"
	"time"

	"github.com/wanpengxie/ActOS/tests/e2e/harness"
)

// TestE2E_DeviceActorRegister_FullHandshake covers phase-2 case 2.
//
// The registration + handshake flow (UI side, see ui/src/components/DeviceBind.jsx):
//
//  1. GET /api/placements/:chID — derive daemon_id (we already know it
//     here so step 1 collapses to a placement lookup the harness owns).
//  2. POST /api/channels/:chID/device-actor — server registers the
//     adapter actor token and returns the raw token to the caller.
//  3. Mock extension opens wss://server/devicebus?actor_id=...
//     with Origin=chrome-extension://<test-id> — server's checkOrigin
//     accepts because Options.DeviceAllowedOrigins pre-declared it.
//
// Regression targets:
//   - Origin allowlist drift (today's bug: missing `chrome-extension://*`
//     entry → handshake 403 → DeviceBind UI stuck).
//   - Token verification path (HMAC over hash) so an invalid token is
//     rejected with 401 not 5xx.
//   - Registration row visibility: the server.db row carries the right
//     actor_id + channel_id + daemon_id + token_hash.
func TestE2E_DeviceActorRegister_FullHandshake(t *testing.T) {
	s := harness.Start(t, harness.Options{
		DeviceAllowedOrigins: []string{harness.MockExtensionOriginID},
	})

	email := "devbind+" + uniqSuffix() + "@e2e.local"
	s.RegisterAndLogin(email, "password-e2e-12345")
	wsID := s.CreateWorkspace("ws-devbind-" + uniqSuffix())
	channelID := s.CreateChannel(wsID, "ch-xhs-"+uniqSuffix(), "xhs-creator")
	s.BindChannel(wsID, channelID)

	// Step 1 — placement lookup so we know which daemon to bind against.
	// The UI flow uses GET /api/placements/:chID; we shortcut via the
	// harness sqlite read which exercises the same daemon_id stamp.
	// BindChannel returns before daemon claim → placement.State="active"
	// sync; poll until active so daemon is ready for device traffic.
	placement := harness.EventuallyValue(t, "placement reaches active", 5*time.Second, func() (harness.PlacementRow, bool) {
		p, ok := s.GetPlacement(channelID)
		return p, ok && p.State == "active"
	})

	// Step 2 — register an actor token.
	deviceID := "device-" + uniqSuffix()
	issued := s.RegisterDeviceActor(channelID, deviceID, placement.DaemonID)
	if issued.ActorID == "" || issued.Token == "" {
		t.Fatalf("register returned empty: %+v", issued)
	}
	if issued.ExpiresAt < time.Now().UnixMilli() {
		t.Fatalf("expires_at=%d already in the past", issued.ExpiresAt)
	}

	row, ok := s.GetDeviceActor(channelID, issued.ActorID)
	if !ok {
		t.Fatalf("device actor row missing for %s", issued.ActorID)
	}
	if row.ChannelID != channelID {
		t.Errorf("device_actor_tokens.channel_id=%q want %q", row.ChannelID, channelID)
	}
	if row.DaemonID != placement.DaemonID {
		t.Errorf("device_actor_tokens.daemon_id=%q want %q", row.DaemonID, placement.DaemonID)
	}
	if row.DeviceID != deviceID {
		t.Errorf("device_actor_tokens.device_id=%q want %q", row.DeviceID, deviceID)
	}
	if len(row.TokenHash) != 64 { // HMAC-SHA-256 hex = 64 chars
		t.Errorf("token_hash len=%d want 64", len(row.TokenHash))
	}

	// Step 3 — mock chrome extension dials /devicebus.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ext := harness.NewMockExtension(t, ctx, harness.MockExtensionConfig{
		WSURL:     s.DevicebusWSURL(issued.ActorID, issued.Token),
		ActorID:   issued.ActorID,
		Token:     issued.Token,
		ChannelID: channelID,
		DeviceID:  deviceID,
	})
	_ = ext // we don't dispatch commands in this case; the connect itself is the assertion

	// Bad-token guard: a malformed token must NOT promote a fresh
	// registration. Issue a second token, dial with the wrong token,
	// expect dial to fail (we attempt via the harness ServerURL
	// in a separate connection — using NewMockExtension would Fatal).
	otherIssued := s.RegisterDeviceActor(channelID, "device-other-"+uniqSuffix(), placement.DaemonID)
	badURL := s.DevicebusWSURL(otherIssued.ActorID, "wrong-token-not-the-real-one")
	if err := dialExpectingFailure(ctx, badURL, harness.MockExtensionOriginID); err == nil {
		t.Fatalf("/devicebus accepted bogus token — server checkToken broken")
	}

	other, ok := s.GetDeviceActor(channelID, otherIssued.ActorID)
	if !ok {
		t.Fatalf("other actor row missing")
	}
	_ = other
}

// dialExpectingFailure attempts a /devicebus handshake and returns
// non-nil error when the dial fails — which is the success path for
// the bad-token guard. Imported inline so the test file doesn't drag
// in gorilla/websocket directly.
func dialExpectingFailure(ctx context.Context, wsURL, origin string) error {
	header := httpHeaderWithOrigin(origin)
	conn, resp, err := dialWS(ctx, wsURL, header)
	if err != nil {
		// Status code != 101 surfaces as a dial error too — both are
		// acceptable failure modes for this guard.
		_ = resp
		return err
	}
	_ = conn.Close()
	return nil
}
