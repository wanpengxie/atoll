package devicebus_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"

	"github.com/wanpengxie/ActOS/kernel/channel"
	"github.com/wanpengxie/ActOS/kernel/placement"
	"github.com/wanpengxie/ActOS/server/devicebus"
	"github.com/wanpengxie/ActOS/server/identity"
	"github.com/wanpengxie/ActOS/server/store"
)

func newSvc(t *testing.T, clock func() time.Time) *devicebus.Service {
	t.Helper()
	ctx := context.Background()
	db, err := store.Open(ctx, filepath.Join(t.TempDir(), "d.db"), store.OpenOptions{})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	svc := devicebus.NewService(db, devicebus.Config{
		TokenSecret: "secret",
		TokenTTL:    1 * time.Hour,
	})
	if clock != nil {
		svc = svc.WithClock(clock)
	}
	return svc
}

func TestIssueAndLifecycle(t *testing.T) {
	t.Parallel()
	svc := newSvc(t, nil)
	ctx := context.Background()

	res, err := svc.IssueSession(ctx, devicebus.IssueInput{
		DeviceID: "dev-A", DeviceType: "xhs",
		ChannelID: channel.ID("ch-X"), UserID: "u1",
		DaemonID: placement.DaemonID("d1"),
	})
	if err != nil {
		t.Fatalf("Issue: %v", err)
	}
	if res.Token == "" || res.Session.ID == "" {
		t.Fatal("empty token / id")
	}
	if res.Session.State != devicebus.StatePending {
		t.Errorf("state=%q want pending", res.Session.State)
	}

	// Bound (daemon ACK).
	if err := svc.MarkBound(ctx, res.Session.ID); err != nil {
		t.Fatalf("MarkBound: %v", err)
	}
	row, _ := svc.Get(ctx, res.Session.ID)
	if row.State != devicebus.StateReady {
		t.Errorf("post-bound state=%q want ready", row.State)
	}

	// Token validation succeeds while ready.
	if _, err := svc.ValidateToken(ctx, res.Session.ID, res.Token); err != nil {
		t.Fatalf("ValidateToken: %v", err)
	}

	// Wrong token fails.
	if _, err := svc.ValidateToken(ctx, res.Session.ID, "wrong"); err != devicebus.ErrTokenInvalid {
		t.Errorf("wrong token err=%v want ErrTokenInvalid", err)
	}

	// Active.
	if err := svc.MarkActive(ctx, res.Session.ID); err != nil {
		t.Fatalf("MarkActive: %v", err)
	}
	row, _ = svc.Get(ctx, res.Session.ID)
	if row.State != devicebus.StateActive {
		t.Errorf("post-active state=%q", row.State)
	}
	if err := svc.MarkActive(ctx, res.Session.ID); err != nil {
		t.Fatalf("idempotent active: %v", err)
	}

	// Offline → Active round trip.
	if err := svc.MarkOffline(ctx, res.Session.ID); err != nil {
		t.Fatalf("MarkOffline: %v", err)
	}
	row, _ = svc.Get(ctx, res.Session.ID)
	if row.State != devicebus.StateOffline {
		t.Errorf("state=%q want offline", row.State)
	}
	if err := svc.MarkActive(ctx, res.Session.ID); err != nil {
		t.Fatalf("re-active: %v", err)
	}
	row, _ = svc.Get(ctx, res.Session.ID)
	if row.State != devicebus.StateActive {
		t.Errorf("re-active state=%q want active", row.State)
	}

	// Revoke -> terminal.
	if err := svc.Revoke(ctx, res.Session.ID); err != nil {
		t.Fatalf("Revoke: %v", err)
	}
	row, _ = svc.Get(ctx, res.Session.ID)
	if row.State != devicebus.StateRevoked {
		t.Errorf("post-revoke state=%q", row.State)
	}
	if _, err := svc.ValidateToken(ctx, res.Session.ID, res.Token); err != devicebus.ErrSessionRevoked {
		t.Errorf("revoked validate err=%v want ErrSessionRevoked", err)
	}
}

func TestValidateTokenRejectsPendingSession(t *testing.T) {
	t.Parallel()
	svc := newSvc(t, nil)
	ctx := context.Background()

	res, err := svc.IssueSession(ctx, devicebus.IssueInput{
		DeviceID: "dev-A", ChannelID: "ch-X", UserID: "u1", DaemonID: "d1",
	})
	if err != nil {
		t.Fatalf("Issue: %v", err)
	}
	if _, err := svc.ValidateToken(ctx, res.Session.ID, res.Token); !errors.Is(err, devicebus.ErrSessionNotReady) {
		t.Fatalf("ValidateToken pending err=%v want ErrSessionNotReady", err)
	}
}

func TestExpireDueSessions(t *testing.T) {
	t.Parallel()
	clock := &fakeClock{now: time.Unix(1_700_000_000, 0)}
	svc := newSvc(t, clock.Now)
	ctx := context.Background()

	res, err := svc.IssueSession(ctx, devicebus.IssueInput{
		DeviceID: "dev-A", ChannelID: "ch-X", UserID: "u1", DaemonID: "d1",
	})
	if err != nil {
		t.Fatalf("Issue: %v", err)
	}
	if err := svc.MarkBound(ctx, res.Session.ID); err != nil {
		t.Fatalf("MarkBound: %v", err)
	}

	// Advance past TokenTTL.
	clock.now = clock.now.Add(2 * time.Hour)
	if err := svc.ExpireDueSessions(ctx); err != nil {
		t.Fatalf("Expire: %v", err)
	}
	row, _ := svc.Get(ctx, res.Session.ID)
	if row.State != devicebus.StateExpired {
		t.Errorf("state=%q want expired", row.State)
	}
}

func TestDefaultTokenTTLIsThirtyDays(t *testing.T) {
	t.Parallel()
	clock := &fakeClock{now: time.Unix(1_700_000_000, 0)}
	ctx := context.Background()
	db, err := store.Open(ctx, filepath.Join(t.TempDir(), "d.db"), store.OpenOptions{})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	svc := devicebus.NewService(db, devicebus.Config{TokenSecret: "secret"}).WithClock(clock.Now)

	res, err := svc.IssueSession(ctx, devicebus.IssueInput{
		DeviceID: "dev-A", ChannelID: "ch-X", UserID: "u1", DaemonID: "d1",
	})
	if err != nil {
		t.Fatalf("Issue: %v", err)
	}
	want := clock.now.Add(30 * 24 * time.Hour).UnixMilli()
	if res.Session.ExpiresAt != want {
		t.Fatalf("ExpiresAt=%d want %d", res.Session.ExpiresAt, want)
	}
}

func TestIssueSessionReplacesSameDeviceAndCapsUserChannel(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	db, err := store.Open(ctx, filepath.Join(t.TempDir(), "d.db"), store.OpenOptions{})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	svc := devicebus.NewService(db, devicebus.Config{
		TokenSecret:               "secret",
		TokenTTL:                  time.Hour,
		MaxSessionsPerUserChannel: 2,
	})

	first, err := svc.IssueSession(ctx, devicebus.IssueInput{
		DeviceID: "dev-A", ChannelID: "ch-X", UserID: "u1", DaemonID: "d1",
	})
	if err != nil {
		t.Fatalf("Issue first: %v", err)
	}
	second, err := svc.IssueSession(ctx, devicebus.IssueInput{
		DeviceID: "dev-A", ChannelID: "ch-X", UserID: "u1", DaemonID: "d1",
	})
	if err != nil {
		t.Fatalf("Issue replacement: %v", err)
	}
	if len(second.ReplacedSessions) != 1 || second.ReplacedSessions[0].ID != first.Session.ID {
		t.Fatalf("ReplacedSessions=%+v want first session", second.ReplacedSessions)
	}
	row, err := svc.Get(ctx, first.Session.ID)
	if err != nil {
		t.Fatalf("Get first: %v", err)
	}
	if row.State != devicebus.StateRevoked {
		t.Fatalf("first state=%q want revoked", row.State)
	}

	if _, err := svc.IssueSession(ctx, devicebus.IssueInput{
		DeviceID: "dev-B", ChannelID: "ch-X", UserID: "u1", DaemonID: "d1",
	}); err != nil {
		t.Fatalf("Issue second device: %v", err)
	}
	if _, err := svc.IssueSession(ctx, devicebus.IssueInput{
		DeviceID: "dev-C", ChannelID: "ch-X", UserID: "u1", DaemonID: "d1",
	}); !errors.Is(err, devicebus.ErrSessionLimitExceeded) {
		t.Fatalf("Issue over cap err=%v want ErrSessionLimitExceeded", err)
	}
}

func TestHandleIssueReturnsTokenWithNoStoreHeaders(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	db, err := store.Open(ctx, filepath.Join(t.TempDir(), "routes.db"), store.OpenOptions{})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	idSvc := identity.NewService(db, identity.Config{
		SessionSecret: "test-session",
		BcryptCost:    4,
		NotifyCode:    func(email, code string, purpose identity.VerificationPurpose) {},
	})
	if _, err := idSvc.Register(ctx, identity.RegisterInput{
		Email: "device@example.com", Password: "topsecret123",
	}); err != nil {
		t.Fatalf("Register: %v", err)
	}
	login, err := idSvc.Login(ctx, identity.LoginInput{Email: "device@example.com", Password: "topsecret123"})
	if err != nil {
		t.Fatalf("Login: %v", err)
	}

	svc := devicebus.NewService(db, devicebus.Config{TokenSecret: "secret", TokenTTL: time.Hour})
	svc.SetAccessAuthorizer(allowAllAuthorizer{})
	svc.SetBindNotifier(fakeBindNotifier{})

	gin.SetMode(gin.TestMode)
	r := gin.New()
	api := r.Group("/api")
	api.Use(idSvc.AuthMiddleware())
	svc.RegisterRoutes(api)

	req, _ := http.NewRequest(http.MethodPost, "/api/channels/ch-X/devices",
		strings.NewReader(`{"device_id":"dev-A","device_type":"xhs","daemon_id":"d1"}`))
	req.Header.Set("Content-Type", "application/json")
	req.AddCookie(&http.Cookie{Name: identity.CookieName, Value: login.Token})
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)
	if rec.Code != http.StatusCreated {
		t.Fatalf("status=%d want 201 body=%s", rec.Code, rec.Body.String())
	}
	if got := rec.Header().Get("Cache-Control"); got != "no-store" {
		t.Fatalf("Cache-Control=%q want no-store", got)
	}
	if got := rec.Header().Get("Pragma"); got != "no-cache" {
		t.Fatalf("Pragma=%q want no-cache", got)
	}
	if !strings.Contains(rec.Body.String(), `"token"`) {
		t.Fatalf("issue response should include one-time raw token: %s", rec.Body.String())
	}
}

func TestAllStatesClosedSet(t *testing.T) {
	t.Parallel()
	if got := len(devicebus.AllStates); got != 6 {
		t.Errorf("len=%d want 6", got)
	}
}

func TestHandleWSRejectsPendingBeforeUpgrade(t *testing.T) {
	t.Parallel()
	svc := newSvc(t, nil)
	ctx := context.Background()
	res, err := svc.IssueSession(ctx, devicebus.IssueInput{
		DeviceID: "dev-A", ChannelID: "ch-X", UserID: "u1", DaemonID: "d1",
	})
	if err != nil {
		t.Fatalf("Issue: %v", err)
	}

	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.GET("/devicebus", svc.HandleWS(noopForwarder{}))
	srv := httptest.NewServer(r)
	defer srv.Close()

	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http") + "/devicebus?session_id=" + res.Session.ID
	dialer := deviceWSDialer(res.Token)
	ws, resp, err := dialer.Dial(wsURL, nil)
	if err == nil {
		_ = ws.Close()
		t.Fatal("pending session upgraded successfully")
	}
	if resp == nil {
		t.Fatalf("dial response nil: %v", err)
	}
	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("status=%d want 409", resp.StatusCode)
	}
}

func TestHandleWSRejectsNonAllowlistedOrigin(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	db, err := store.Open(ctx, filepath.Join(t.TempDir(), "d.db"), store.OpenOptions{})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	svc := devicebus.NewService(db, devicebus.Config{
		TokenSecret:    "secret",
		TokenTTL:       time.Hour,
		AllowedOrigins: []string{"https://allowed.example"},
	})
	res, err := svc.IssueSession(ctx, devicebus.IssueInput{
		DeviceID: "dev-A", ChannelID: "ch-X", UserID: "u1", DaemonID: "d1",
	})
	if err != nil {
		t.Fatalf("Issue: %v", err)
	}
	if err := svc.MarkBound(ctx, res.Session.ID); err != nil {
		t.Fatalf("MarkBound: %v", err)
	}

	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.GET("/devicebus", svc.HandleWS(noopForwarder{}))
	srv := httptest.NewServer(r)
	defer srv.Close()

	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http") + "/devicebus?session_id=" + res.Session.ID
	header := http.Header{}
	header.Set("Origin", "https://evil.example")
	dialer := deviceWSDialer(res.Token)
	ws, resp, err := dialer.Dial(wsURL, header)
	if err == nil {
		_ = ws.Close()
		t.Fatal("dial with non-allowlisted Origin succeeded")
	}
	if resp == nil {
		t.Fatalf("dial response nil: %v", err)
	}
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("status=%d want 403", resp.StatusCode)
	}
}

func TestHandleWSSubprotocolParserFailClosed(t *testing.T) {
	t.Parallel()
	svc := newSvc(t, nil)
	ctx := context.Background()
	res, err := svc.IssueSession(ctx, devicebus.IssueInput{
		DeviceID: "dev-A", ChannelID: "ch-X", UserID: "u1", DaemonID: "d1",
	})
	if err != nil {
		t.Fatalf("Issue: %v", err)
	}
	if err := svc.MarkBound(ctx, res.Session.ID); err != nil {
		t.Fatalf("MarkBound: %v", err)
	}

	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.GET("/devicebus", svc.HandleWS(noopForwarder{}))
	srv := httptest.NewServer(r)
	defer srv.Close()

	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http") + "/devicebus?session_id=" + res.Session.ID
	cases := []struct {
		name      string
		protocols []string
	}{
		{
			name:      "duplicate token slot",
			protocols: []string{"coagent.device.v1", "token." + res.Token, "token.other"},
		},
		{
			name:      "unknown slot",
			protocols: []string{"coagent.device.v1", "token." + res.Token, "unknown.slot"},
		},
		{
			name:      "duplicate real protocol",
			protocols: []string{"coagent.device.v1", "coagent.device.v1", "token." + res.Token},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dialer := deviceWSDialerWithSubprotocols(tc.protocols...)
			ws, resp, err := dialer.Dial(wsURL, nil)
			if err == nil {
				_ = ws.Close()
				t.Fatalf("dial succeeded for malformed subprotocols: %v", tc.protocols)
			}
			if resp == nil {
				t.Fatalf("dial response nil: %v", err)
			}
			if resp.StatusCode != http.StatusBadRequest {
				t.Fatalf("status=%d want 400", resp.StatusCode)
			}
		})
	}
}

// TestIssueResultCarriesFingerprint covers T147 phase-4b — the issue
// path returns a non-empty TokenFingerprint sized to TokenFingerprintLength
// so the gateway can ship it into the daemon-side mirror without
// re-hashing the raw token.
func TestIssueResultCarriesFingerprint(t *testing.T) {
	t.Parallel()
	svc := newSvc(t, nil)
	ctx := context.Background()

	res, err := svc.IssueSession(ctx, devicebus.IssueInput{
		DeviceID: "dev-A", ChannelID: "ch-X", UserID: "u1", DaemonID: "d1",
	})
	if err != nil {
		t.Fatalf("Issue: %v", err)
	}
	if got := len(res.TokenFingerprint); got != devicebus.TokenFingerprintLength {
		t.Errorf("len(fingerprint)=%d want %d", got, devicebus.TokenFingerprintLength)
	}
	// Fingerprint is stable across re-derivation (HMAC of raw token →
	// prefix). Re-issue with the same token should yield the same prefix.
	// Issue a second session — different sessions MUST have different
	// fingerprints because the raw tokens differ.
	res2, err := svc.IssueSession(ctx, devicebus.IssueInput{
		DeviceID: "dev-B", ChannelID: "ch-X", UserID: "u1", DaemonID: "d1",
	})
	if err != nil {
		t.Fatalf("Issue 2: %v", err)
	}
	if res.TokenFingerprint == res2.TokenFingerprint {
		t.Error("two distinct sessions produced identical fingerprints")
	}
}

type fakeClock struct{ now time.Time }

func (f *fakeClock) Now() time.Time { return f.now }

type noopForwarder struct{}

func (noopForwarder) ForwardDeviceFrame(context.Context, devicebus.DeviceFrame) error { return nil }

type allowAllAuthorizer struct{}

func (allowAllAuthorizer) AuthorizeChannelAccess(context.Context, string, string) error { return nil }

type fakeBindNotifier struct{}

func (fakeBindNotifier) Bind(context.Context, devicebus.BindInput) error { return nil }

func (fakeBindNotifier) Unbind(context.Context, devicebus.UnbindInput) error { return nil }

type recordingForwarder struct {
	mu     sync.Mutex
	frames []devicebus.DeviceFrame
	ch     chan struct{}
}

func newRecordingForwarder() *recordingForwarder {
	return &recordingForwarder{ch: make(chan struct{}, 8)}
}

func (f *recordingForwarder) ForwardDeviceFrame(_ context.Context, frame devicebus.DeviceFrame) error {
	f.mu.Lock()
	f.frames = append(f.frames, frame)
	f.mu.Unlock()
	select {
	case f.ch <- struct{}{}:
	default:
	}
	return nil
}

func (f *recordingForwarder) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.frames)
}

func (f *recordingForwarder) waitCount(t *testing.T, want int) {
	t.Helper()
	deadline := time.After(2 * time.Second)
	for {
		if f.count() >= want {
			return
		}
		select {
		case <-f.ch:
		case <-deadline:
			t.Fatalf("forwarded frame count=%d want >= %d", f.count(), want)
		}
	}
}

// deviceWSDialer returns a gorilla dialer that offers the v4 device
// subprotocol slots (real proto + token slot) on the handshake. Per
// impl-layer3 §6.5.1 (R5-14) the token rides in Sec-WebSocket-Protocol,
// not the URL query.
func deviceWSDialer(token string) *websocket.Dialer {
	return deviceWSDialerWithSubprotocols(
		"coagent.device.v1",
		"token."+token,
	)
}

func deviceWSDialerWithSubprotocols(protocols ...string) *websocket.Dialer {
	d := *websocket.DefaultDialer
	d.Subprotocols = protocols
	return &d
}

// TestHandleWS_IdleDeviceTrippedByReadDeadline mirrors the daemonbus
// keepalive regression test: a device WS that connects then ignores
// server pings (no pong replies, no business reads) must be reaped
// within ~IdleReadTimeout. Otherwise the gateway holds a dead
// devicebus.Connection in s.sessions forever and any daemon→device
// push will silently no-op.
func TestHandleWS_IdleDeviceTrippedByReadDeadline(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	db, err := store.Open(ctx, filepath.Join(t.TempDir(), "d.db"), store.OpenOptions{})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	svc := devicebus.NewService(db, devicebus.Config{
		TokenSecret:        "secret",
		TokenTTL:           time.Hour,
		AllowMissingOrigin: true,
		PingCadence:        100 * time.Millisecond,
		IdleReadTimeout:    500 * time.Millisecond,
		PingWriteTimeout:   250 * time.Millisecond,
	})
	res, err := svc.IssueSession(ctx, devicebus.IssueInput{
		DeviceID: "dev-idle", ChannelID: "ch-X", UserID: "u1", DaemonID: "d1",
	})
	if err != nil {
		t.Fatalf("Issue: %v", err)
	}
	if err := svc.MarkBound(ctx, res.Session.ID); err != nil {
		t.Fatalf("MarkBound: %v", err)
	}

	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.GET("/devicebus", svc.HandleWS(noopForwarder{}))
	srv := httptest.NewServer(r)
	defer srv.Close()

	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http") + "/devicebus?session_id=" + res.Session.ID
	dialer := deviceWSDialer(res.Token)
	ws, _, err := dialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer func() { _ = ws.Close() }()

	// Silence the client: no pong replies, no reads after dial.
	ws.SetPingHandler(func(string) error { return nil })

	// Poll for the session to flip away from active. MarkOffline runs
	// in the HandleWS defer; once the server-side read loop unwedges
	// it will run.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		got, err := svc.Get(ctx, res.Session.ID)
		if err != nil {
			t.Fatalf("GetSession: %v", err)
		}
		if got.State != devicebus.StateActive {
			return // success — server marked it offline (or similar)
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("server did not reap silent device WS within 3s — idle read deadline not enforced")
}

func TestHandleWS_RevokeClosesLiveConnectionBeforeForward(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	db, err := store.Open(ctx, filepath.Join(t.TempDir(), "d.db"), store.OpenOptions{})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	svc := devicebus.NewService(db, devicebus.Config{
		TokenSecret:        "secret",
		TokenTTL:           time.Hour,
		AllowMissingOrigin: true,
	})
	res, err := svc.IssueSession(ctx, devicebus.IssueInput{
		DeviceID: "dev-race", ChannelID: "ch-X", UserID: "u1", DaemonID: "d1",
	})
	if err != nil {
		t.Fatalf("Issue: %v", err)
	}
	if err := svc.MarkBound(ctx, res.Session.ID); err != nil {
		t.Fatalf("MarkBound: %v", err)
	}

	gin.SetMode(gin.TestMode)
	r := gin.New()
	fwd := newRecordingForwarder()
	r.GET("/devicebus", svc.HandleWS(fwd))
	srv := httptest.NewServer(r)
	defer srv.Close()

	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http") + "/devicebus?session_id=" + res.Session.ID
	ws, _, err := deviceWSDialer(res.Token).Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer func() { _ = ws.Close() }()

	if err := ws.WriteJSON(devicebus.DeviceFrame{TransitSeq: 1}); err != nil {
		t.Fatalf("write pre-revoke frame: %v", err)
	}
	fwd.waitCount(t, 1)

	if err := svc.Revoke(ctx, res.Session.ID); err != nil {
		t.Fatalf("Revoke: %v", err)
	}
	if err := svc.SendFrameToDevice(ctx, res.Session.ID, devicebus.DeviceFrame{}); !errors.Is(err, devicebus.ErrSessionNotFound) {
		t.Fatalf("SendFrameToDevice after revoke err=%v want ErrSessionNotFound", err)
	}

	_ = ws.WriteJSON(devicebus.DeviceFrame{TransitSeq: 2})
	time.Sleep(150 * time.Millisecond)
	if got := fwd.count(); got != 1 {
		t.Fatalf("post-revoke frame forwarded; count=%d want 1", got)
	}
	_ = ws.SetReadDeadline(time.Now().Add(time.Second))
	if _, _, err := ws.ReadMessage(); err == nil {
		t.Fatal("revoked device WS remained readable/open")
	}
}

func TestHandleWS_RechecksExpiredSessionBeforeForward(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	clock := &fakeClock{now: time.Unix(1_700_000_000, 0)}
	db, err := store.Open(ctx, filepath.Join(t.TempDir(), "d.db"), store.OpenOptions{})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	svc := devicebus.NewService(db, devicebus.Config{
		TokenSecret:        "secret",
		TokenTTL:           time.Hour,
		AllowMissingOrigin: true,
	}).WithClock(clock.Now)
	res, err := svc.IssueSession(ctx, devicebus.IssueInput{
		DeviceID: "dev-exp", ChannelID: "ch-X", UserID: "u1", DaemonID: "d1",
	})
	if err != nil {
		t.Fatalf("Issue: %v", err)
	}
	if err := svc.MarkBound(ctx, res.Session.ID); err != nil {
		t.Fatalf("MarkBound: %v", err)
	}

	gin.SetMode(gin.TestMode)
	r := gin.New()
	fwd := newRecordingForwarder()
	r.GET("/devicebus", svc.HandleWS(fwd))
	srv := httptest.NewServer(r)
	defer srv.Close()

	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http") + "/devicebus?session_id=" + res.Session.ID
	ws, _, err := deviceWSDialer(res.Token).Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer func() { _ = ws.Close() }()

	clock.now = clock.now.Add(2 * time.Hour)
	if err := svc.ExpireDueSessions(ctx); err != nil {
		t.Fatalf("ExpireDueSessions: %v", err)
	}
	if err := ws.WriteJSON(devicebus.DeviceFrame{TransitSeq: 1}); err != nil {
		t.Fatalf("write expired frame: %v", err)
	}
	time.Sleep(150 * time.Millisecond)
	if got := fwd.count(); got != 0 {
		t.Fatalf("expired session frame forwarded; count=%d want 0", got)
	}
	_ = ws.SetReadDeadline(time.Now().Add(time.Second))
	if _, _, err := ws.ReadMessage(); err == nil {
		t.Fatal("expired device WS remained readable/open")
	}
}
