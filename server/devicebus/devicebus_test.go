package devicebus_test

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/wanpengxie/ActOS/kernel/channel"
	"github.com/wanpengxie/ActOS/kernel/placement"
	"github.com/wanpengxie/ActOS/server/devicebus"
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
	if _, err := svc.ValidateToken(ctx, res.Session.ID, res.Token); err != devicebus.ErrSessionExpired {
		t.Errorf("revoked validate err=%v want ErrSessionExpired", err)
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

func TestAllStatesClosedSet(t *testing.T) {
	t.Parallel()
	if got := len(devicebus.AllStates); got != 6 {
		t.Errorf("len=%d want 6", got)
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
