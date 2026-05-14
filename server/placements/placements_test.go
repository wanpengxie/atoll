package placements_test

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/coagent-ai/coagent/kernel/channel"
	"github.com/coagent-ai/coagent/kernel/placement"
	"github.com/coagent-ai/coagent/server/placements"
	"github.com/coagent-ai/coagent/server/store"
)

type clock struct{ now time.Time }

func (c *clock) Now() time.Time { return c.now }

func newSvc(t *testing.T, c *clock) *placements.Service {
	t.Helper()
	ctx := context.Background()
	db, err := store.Open(ctx, filepath.Join(t.TempDir(), "p.db"), store.OpenOptions{})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	svc := placements.NewService(db, placements.Config{
		GracePeriod:      30 * time.Second,
		CreateTimeout:    10 * time.Second,
		HeartbeatTimeout: 20 * time.Second,
		ReconcileTick:    50 * time.Millisecond,
	}).WithClock(c.Now)
	return svc
}

// TestCreatingToActiveHappyPath exercises the L2 §1.4.11.3 step 1+5 path —
// Reserve → daemon ACK matches → CASActivate succeeds → state=active.
func TestCreatingToActiveHappyPath(t *testing.T) {
	t.Parallel()
	c := &clock{now: time.Unix(1_700_000_000, 0)}
	svc := newSvc(t, c)
	ctx := context.Background()

	p, req, err := svc.Reserve(ctx, channel.ID("ch-1"), placement.DaemonID("d-1"), 7, nil)
	if err != nil {
		t.Fatalf("Reserve: %v", err)
	}
	if p.State != placement.StateCreating {
		t.Errorf("state=%q want creating", p.State)
	}
	if req.ChannelID != p.ChannelID || req.OwnerEpoch != p.OwnerEpoch || req.FencingToken != p.FencingToken {
		t.Errorf("create request mismatch: %+v vs %+v", req, p)
	}

	ack := placement.CreateChannelAck{
		FrameID: "f-1", ChannelID: p.ChannelID, CreateRequestID: p.CreateRequestID,
		OwnerEpoch: p.OwnerEpoch, FencingToken: p.FencingToken,
		DaemonID: p.DaemonID, DaemonEpoch: 1, Status: placement.AckBound,
	}
	ok, err := svc.Activate(ctx, ack, 7)
	if err != nil {
		t.Fatalf("Activate: %v", err)
	}
	if !ok {
		t.Fatal("Activate ok=false on matching ACK")
	}

	got, _, _ := svc.Get(ctx, p.ChannelID)
	if got.State != placement.StateActive {
		t.Errorf("post-Activate state=%q want active", got.State)
	}
	if got.ActivatedAt == 0 {
		t.Errorf("activated_at not set")
	}
}

// TestACKMismatchRejected covers codex #3/#4 — each of the 4
// match-fields is permuted; CAS must fail and the row must stay
// in 'creating'.
func TestACKMismatchRejected(t *testing.T) {
	t.Parallel()
	c := &clock{now: time.Unix(1_700_000_000, 0)}
	svc := newSvc(t, c)
	ctx := context.Background()

	p, _, err := svc.Reserve(ctx, channel.ID("ch-2"), placement.DaemonID("d-1"), 1, nil)
	if err != nil {
		t.Fatalf("Reserve: %v", err)
	}

	baseAck := placement.CreateChannelAck{
		ChannelID: p.ChannelID, CreateRequestID: p.CreateRequestID,
		OwnerEpoch: p.OwnerEpoch, FencingToken: p.FencingToken,
		DaemonID: p.DaemonID, Status: placement.AckBound,
	}

	mutations := []struct {
		name string
		fn   func(a placement.CreateChannelAck) placement.CreateChannelAck
	}{
		{"wrong create_request_id", func(a placement.CreateChannelAck) placement.CreateChannelAck { a.CreateRequestID = "bogus"; return a }},
		{"wrong owner_epoch", func(a placement.CreateChannelAck) placement.CreateChannelAck { a.OwnerEpoch++; return a }},
		{"wrong fencing_token", func(a placement.CreateChannelAck) placement.CreateChannelAck { a.FencingToken++; return a }},
		{"wrong daemon_id", func(a placement.CreateChannelAck) placement.CreateChannelAck { a.DaemonID = "other"; return a }},
		{"rejected status", func(a placement.CreateChannelAck) placement.CreateChannelAck { a.Status = placement.AckRejected; return a }},
	}
	for _, m := range mutations {
		m := m
		t.Run(m.name, func(t *testing.T) {
			ok, err := svc.Activate(ctx, m.fn(baseAck), 1)
			if err != nil {
				t.Fatalf("Activate: %v", err)
			}
			if ok {
				t.Errorf("expected CAS to lose; got ok=true")
			}
		})
	}

	got, _, _ := svc.Get(ctx, p.ChannelID)
	if got.State != placement.StateCreating {
		t.Errorf("post-mutation state=%q want creating", got.State)
	}
}

// TestCreateTimeoutOrphan covers reconcile's creating→orphan path.
// Heartbeat-timeout active→stale is suppressed by cold-start grace
// (T1.7), proven by TestColdStartGrace below.
func TestCreateTimeoutOrphan(t *testing.T) {
	t.Parallel()
	c := &clock{now: time.Unix(1_700_000_000, 0)}
	svc := newSvc(t, c)
	ctx := context.Background()
	svc.MarkStartedAt()

	p, _, err := svc.Reserve(ctx, channel.ID("ch-3"), placement.DaemonID("d-1"), 1, nil)
	if err != nil {
		t.Fatalf("Reserve: %v", err)
	}

	// Advance past CreateTimeout AND past GracePeriod.
	c.now = c.now.Add(60 * time.Second)

	if err := svc.ReconcileOnce(ctx); err != nil {
		t.Fatalf("ReconcileOnce: %v", err)
	}

	got, _, _ := svc.Get(ctx, p.ChannelID)
	if got.State != placement.StateOrphan {
		t.Errorf("state=%q want orphan", got.State)
	}
}

// TestColdStartGrace asserts the L2 §11 + T1.7 cold-start grace:
// active rows whose last_heartbeat_at is stale MUST NOT transition
// to stale inside the GracePeriod window. After the grace expires
// the same row DOES transition.
func TestColdStartGrace(t *testing.T) {
	t.Parallel()
	c := &clock{now: time.Unix(1_700_000_000, 0)}
	svc := newSvc(t, c)
	ctx := context.Background()
	svc.MarkStartedAt()

	// Reserve + activate at t=0.
	p, _, err := svc.Reserve(ctx, channel.ID("ch-4"), placement.DaemonID("d-1"), 1, nil)
	if err != nil {
		t.Fatalf("Reserve: %v", err)
	}
	ack := placement.CreateChannelAck{
		ChannelID: p.ChannelID, CreateRequestID: p.CreateRequestID,
		OwnerEpoch: p.OwnerEpoch, FencingToken: p.FencingToken,
		DaemonID: p.DaemonID, Status: placement.AckBound,
	}
	if ok, err := svc.Activate(ctx, ack, 1); err != nil || !ok {
		t.Fatalf("Activate ok=%v err=%v", ok, err)
	}

	// Advance past HeartbeatTimeout (20s) but stay inside GracePeriod (30s).
	c.now = c.now.Add(25 * time.Second)
	if err := svc.ReconcileOnce(ctx); err != nil {
		t.Fatalf("Reconcile inside grace: %v", err)
	}
	got, _, _ := svc.Get(ctx, p.ChannelID)
	if got.State != placement.StateActive {
		t.Fatalf("inside grace state=%q want active (no stale yet)", got.State)
	}

	// Advance past GracePeriod (30s) — now stale should fire.
	c.now = c.now.Add(10 * time.Second)
	if err := svc.ReconcileOnce(ctx); err != nil {
		t.Fatalf("Reconcile post-grace: %v", err)
	}
	got, _, _ = svc.Get(ctx, p.ChannelID)
	if got.State != placement.StateStale {
		t.Errorf("post-grace state=%q want stale", got.State)
	}
}

// TestReserveCollision covers L2 §1.4.11.3 step 1 — INSERT must fail
// when channel_id already has a placement row.
func TestReserveCollision(t *testing.T) {
	t.Parallel()
	c := &clock{now: time.Unix(1_700_000_000, 0)}
	svc := newSvc(t, c)
	ctx := context.Background()

	if _, _, err := svc.Reserve(ctx, channel.ID("ch-5"), placement.DaemonID("d-1"), 1, nil); err != nil {
		t.Fatalf("Reserve #1: %v", err)
	}
	_, _, err := svc.Reserve(ctx, channel.ID("ch-5"), placement.DaemonID("d-1"), 1, nil)
	var existsErr *placement.ErrPlacementExists
	if !errors.As(err, &existsErr) {
		t.Errorf("err=%v want ErrPlacementExists", err)
	}
}

// TestReclaim covers L2 §1.4.11.4 — daemon reconnects + reports the
// (fencing_token, owner_epoch) it has on disk; server accepts when
// the tuple matches the active placement.
func TestReclaim(t *testing.T) {
	t.Parallel()
	c := &clock{now: time.Unix(1_700_000_000, 0)}
	svc := newSvc(t, c)
	ctx := context.Background()
	svc.MarkStartedAt()

	p, _, err := svc.Reserve(ctx, channel.ID("ch-6"), placement.DaemonID("d-1"), 1, nil)
	if err != nil {
		t.Fatalf("Reserve: %v", err)
	}
	ack := placement.CreateChannelAck{
		ChannelID: p.ChannelID, CreateRequestID: p.CreateRequestID,
		OwnerEpoch: p.OwnerEpoch, FencingToken: p.FencingToken,
		DaemonID: p.DaemonID, Status: placement.AckBound,
	}
	if ok, _ := svc.Activate(ctx, ack, 1); !ok {
		t.Fatalf("Activate failed")
	}

	// Matching reclaim accepted.
	got, err := svc.AcceptReclaim(ctx, p.ChannelID, placement.ReclaimChannel{
		ChannelID: p.ChannelID, FencingToken: p.FencingToken, OwnerEpoch: p.OwnerEpoch,
	}, 9)
	if err != nil || !got {
		t.Fatalf("AcceptReclaim ok=%v err=%v", got, err)
	}

	// Mismatched reclaim rejected.
	got, err = svc.AcceptReclaim(ctx, p.ChannelID, placement.ReclaimChannel{
		ChannelID: p.ChannelID, FencingToken: 9999, OwnerEpoch: 9999,
	}, 10)
	if err != nil {
		t.Fatalf("AcceptReclaim mismatch err: %v", err)
	}
	if got {
		t.Errorf("AcceptReclaim mismatch ok=true; expected false")
	}
}
