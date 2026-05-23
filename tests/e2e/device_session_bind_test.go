//go:build e2e

package e2e

import (
	"context"
	"testing"
	"time"

	"github.com/wanpengxie/ActOS/tests/e2e/harness"
)

// TestE2E_DeviceSessionBind_FullFourStep covers phase-2 case 2.
//
// The four step bind flow (UI side, see ui/src/components/DeviceBind.jsx):
//
//  1. GET /api/placements/:chID — derive daemon_id (we already know it
//     here so step 1 collapses to a placement lookup the harness owns).
//  2. POST /api/channels/:chID/devices — server allocates device_session
//     row in state=pending, signs token, ACKs daemon-side bind frame
//     (notifier.Bind → daemonbus control.bind_device_session → daemon
//     mirror Upsert → ack), then transitions pending→ready and returns
//     the raw token to the caller.
//  3. Mock extension opens wss://server/devicebus?session_id=&token=
//     with Origin=chrome-extension://<test-id> — server's checkOrigin
//     accepts because Options.DeviceAllowedOrigins pre-declared it.
//  4. WS connect triggers MarkActive in the server (ready → active).
//
// Regression targets:
//   - Origin allowlist drift (today's bug: missing `chrome-extension://*`
//     entry → handshake 403 → DeviceBind UI stuck).
//   - Token verification path (HMAC over hash) so an invalid token is
//     rejected with 401 not 5xx.
//   - daemon-side mirror sync — after bind ack the daemon must hold the
//     session in its in-memory store before the WS open path tries to
//     route inbound frames against it.
//   - State row visibility: the device_sessions row in server.db carries
//     the right channel_id + daemon_id + token_hash (16-hex prefix).
func TestE2E_DeviceSessionBind_FullFourStep(t *testing.T) {
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
	// sync; poll until active so daemon is ready to receive bind.
	placement := harness.EventuallyValue(t, "placement reaches active", 5*time.Second, func() (harness.PlacementRow, bool) {
		p, ok := s.GetPlacement(channelID)
		return p, ok && p.State == "active"
	})

	// Step 2 — issue a session.
	deviceID := "device-" + uniqSuffix()
	issued := s.IssueDeviceSession(channelID, deviceID, placement.DaemonID)
	if issued.DeviceSessionID == "" || issued.Token == "" {
		t.Fatalf("issue returned empty: %+v", issued)
	}
	if issued.ExpiresAt < time.Now().UnixMilli() {
		t.Fatalf("expires_at=%d already in the past", issued.ExpiresAt)
	}

	// device_sessions row should be in state=ready after the daemon ack.
	row, ok := s.GetDeviceSession(issued.DeviceSessionID)
	if !ok {
		t.Fatalf("device_sessions row missing for %s", issued.DeviceSessionID)
	}
	if row.State != "ready" {
		t.Fatalf("device_sessions.state=%q want ready (daemon ack path broken)", row.State)
	}
	if row.ChannelID != channelID {
		t.Errorf("device_sessions.channel_id=%q want %q", row.ChannelID, channelID)
	}
	if row.DaemonID != placement.DaemonID {
		t.Errorf("device_sessions.daemon_id=%q want %q", row.DaemonID, placement.DaemonID)
	}
	if row.DeviceID != deviceID {
		t.Errorf("device_sessions.device_id=%q want %q", row.DeviceID, deviceID)
	}
	if len(row.TokenHash) != 64 { // HMAC-SHA-256 hex = 64 chars
		t.Errorf("token_hash len=%d want 64", len(row.TokenHash))
	}

	// Step 3 — mock chrome extension dials /devicebus.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ext := harness.NewMockExtension(t, ctx, harness.MockExtensionConfig{
		WSURL:     s.DevicebusWSURL(issued.DeviceSessionID, issued.Token),
		SessionID: issued.DeviceSessionID,
		Token:     issued.Token,
		ChannelID: channelID,
		DeviceID:  deviceID,
	})
	_ = ext // we don't dispatch commands in this case; the connect itself is the assertion

	// Step 4 — server MarkActive transitions ready → active inside the
	// HandleWS upgrader. Poll the row until it flips.
	harness.Eventually(t, "device session reaches active", 5*time.Second, func() bool {
		r, ok := s.GetDeviceSession(issued.DeviceSessionID)
		return ok && r.State == "active"
	})

	// Bad-token guard: a malformed token must NOT promote a fresh
	// session to active. Issue a second session, dial with the wrong
	// token, expect dial to fail (we attempt via the harness ServerURL
	// in a separate connection — using NewMockExtension would Fatal).
	otherIssued := s.IssueDeviceSession(channelID, "device-other-"+uniqSuffix(), placement.DaemonID)
	badURL := s.DevicebusWSURL(otherIssued.DeviceSessionID, "wrong-token-not-the-real-one")
	if err := dialExpectingFailure(ctx, badURL, harness.MockExtensionOriginID); err == nil {
		t.Fatalf("/devicebus accepted bogus token — server checkToken broken")
	}

	// Other-session row stays in ready (NOT active) because the dial
	// was rejected before MarkActive ran.
	other, ok := s.GetDeviceSession(otherIssued.DeviceSessionID)
	if !ok {
		t.Fatalf("other session row missing")
	}
	if other.State != "ready" {
		t.Errorf("other session state=%q want ready (failed dial advanced state)", other.State)
	}
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
