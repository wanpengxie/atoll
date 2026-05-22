package placements_test

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/wanpengxie/ActOS/kernel/channel"
	kerneldaemonbus "github.com/wanpengxie/ActOS/kernel/daemonbus"
	"github.com/wanpengxie/ActOS/kernel/placement"
	"github.com/wanpengxie/ActOS/server/placements"
	"github.com/wanpengxie/ActOS/server/store"
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
	if p.EnteredStateAt != p.CreatedAt || p.EnteredStateAt == 0 {
		t.Errorf("creating entered_state_at=%d created_at=%d", p.EnteredStateAt, p.CreatedAt)
	}
	if req.ChannelID != p.ChannelID || req.CreateRequestID != p.CreateRequestID {
		t.Errorf("create request mismatch: %+v vs %+v", req, p)
	}
	// proto-foundation §3.3.3 Phase 1: server reserves with epoch=0 /
	// empty fencing_token; the daemon is the trust root.
	if p.OwnerEpoch != 0 || p.FencingToken != "" {
		t.Errorf("Phase 1 placement should have epoch=0 / token=\"\"; got epoch=%d token=%q",
			p.OwnerEpoch, p.FencingToken)
	}

	// Phase 2 ack carries the daemon-generated fencing tuple.
	const daemonEpoch placement.OwnerEpoch = 1
	const daemonTok placement.FencingToken = "daemon-generated-tok"
	ack := placement.CreateChannelAck{
		FrameID: "f-1", ChannelID: p.ChannelID, CreateRequestID: p.CreateRequestID,
		OwnerEpoch: daemonEpoch, FencingToken: daemonTok,
		DaemonID: p.DaemonID, DaemonEpoch: 1, Result: placement.CreateChannelAccepted,
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
	if got.EnteredStateAt != got.ActivatedAt {
		t.Errorf("entered_state_at=%d want activated_at=%d", got.EnteredStateAt, got.ActivatedAt)
	}
	// Phase 3 CAS must persist the daemon-supplied fencing tuple.
	if got.OwnerEpoch != daemonEpoch || got.FencingToken != daemonTok {
		t.Errorf("post-Activate epoch/token = %d/%q want %d/%q (Phase 3 CAS should write ack values)",
			got.OwnerEpoch, got.FencingToken, daemonEpoch, daemonTok)
	}
}

// TestACKMismatchRejected verifies the Phase 3 CAS predicate
// (channel_id + create_request_id + daemon_id + state='creating')
// plus the protocol obligation that the ack carry a non-empty
// fencing_token (impl-layer2 §3.2.2). Mutating any predicate field —
// or stripping the fencing_token / setting Status=rejected — must
// leave the placement row in 'creating'.
//
// Note: owner_epoch / fencing_token are daemon outputs, not CAS
// predicates, so mutating them alone (when otherwise valid) does NOT
// fail the CAS. Their integrity is enforced by the daemon-side
// channel_lock — separate test surface.
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
		OwnerEpoch: 1, FencingToken: "daemon-tok",
		DaemonID: p.DaemonID, Result: placement.CreateChannelAccepted,
	}

	mutations := []struct {
		name string
		fn   func(a placement.CreateChannelAck) placement.CreateChannelAck
	}{
		{"wrong create_request_id", func(a placement.CreateChannelAck) placement.CreateChannelAck { a.CreateRequestID = "bogus"; return a }},
		{"wrong daemon_id", func(a placement.CreateChannelAck) placement.CreateChannelAck { a.DaemonID = "other"; return a }},
		{"empty fencing_token", func(a placement.CreateChannelAck) placement.CreateChannelAck { a.FencingToken = ""; return a }},
		{"missing accepted result", func(a placement.CreateChannelAck) placement.CreateChannelAck {
			a.Result = ""
			return a
		}},
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
	if got.EnteredStateAt != c.now.UnixMilli() {
		t.Errorf("entered_state_at=%d want %d", got.EnteredStateAt, c.now.UnixMilli())
	}
}

// TestColdStartGrace asserts the L2 §11 + T1.7 row-local grace:
// active rows whose last_heartbeat_at is stale MUST NOT transition
// to stale inside their entered_state_at GracePeriod window even when
// the process-level start time is already older than the grace. After
// the row-local grace expires the same row DOES transition.
func TestColdStartGrace(t *testing.T) {
	t.Parallel()
	c := &clock{now: time.Unix(1_700_000_000, 0)}
	svc := newSvc(t, c)
	ctx := context.Background()
	svc.WithStartedAt(c.now.Add(-time.Hour))

	// Reserve + activate at t=0. Daemon supplies the fencing tuple in
	// the ack per proto-foundation §3.3.3 Phase 2.
	p, _, err := svc.Reserve(ctx, channel.ID("ch-4"), placement.DaemonID("d-1"), 1, nil)
	if err != nil {
		t.Fatalf("Reserve: %v", err)
	}
	ack := placement.CreateChannelAck{
		ChannelID: p.ChannelID, CreateRequestID: p.CreateRequestID,
		OwnerEpoch: 1, FencingToken: "daemon-tok",
		DaemonID: p.DaemonID, Result: placement.CreateChannelAccepted,
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

func TestResolveDaemonForChannelActiveOnly(t *testing.T) {
	t.Parallel()
	c := &clock{now: time.Unix(1_700_000_000, 0)}
	svc := newSvc(t, c)
	ctx := context.Background()

	p, _, err := svc.Reserve(ctx, channel.ID("ch-resolve"), placement.DaemonID("d-owner"), 1, nil)
	if err != nil {
		t.Fatalf("Reserve: %v", err)
	}
	if daemonID, ok, err := svc.ResolveDaemonForChannel(ctx, p.ChannelID); err != nil || ok || daemonID != "" {
		t.Fatalf("creating ResolveDaemonForChannel daemon=%q ok=%v err=%v", daemonID, ok, err)
	}

	ack := placement.CreateChannelAck{
		ChannelID: p.ChannelID, CreateRequestID: p.CreateRequestID,
		OwnerEpoch: 1, FencingToken: "daemon-tok",
		DaemonID: p.DaemonID, Result: placement.CreateChannelAccepted,
	}
	if ok, err := svc.Activate(ctx, ack, 1); err != nil || !ok {
		t.Fatalf("Activate ok=%v err=%v", ok, err)
	}
	daemonID, ok, err := svc.ResolveDaemonForChannel(ctx, p.ChannelID)
	if err != nil || !ok || daemonID != p.DaemonID {
		t.Fatalf("active ResolveDaemonForChannel daemon=%q ok=%v err=%v", daemonID, ok, err)
	}

	c.now = c.now.Add(40 * time.Second)
	if err := svc.Store().MarkStale(ctx, p.ChannelID, c.now.UnixMilli()); err != nil {
		t.Fatalf("MarkStale: %v", err)
	}
	if daemonID, ok, err := svc.ResolveDaemonForChannel(ctx, p.ChannelID); err != nil || ok || daemonID != "" {
		t.Fatalf("stale ResolveDaemonForChannel daemon=%q ok=%v err=%v", daemonID, ok, err)
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
// the tuple matches the active placement (with daemon_id pinned).
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
	const daemonEpoch placement.OwnerEpoch = 1
	const daemonTok placement.FencingToken = "daemon-tok-6"
	ack := placement.CreateChannelAck{
		ChannelID: p.ChannelID, CreateRequestID: p.CreateRequestID,
		OwnerEpoch: daemonEpoch, FencingToken: daemonTok,
		DaemonID: p.DaemonID, Result: placement.CreateChannelAccepted,
	}
	if ok, _ := svc.Activate(ctx, ack, 1); !ok {
		t.Fatalf("Activate failed")
	}

	// Matching reclaim accepted — daemon presents the tuple it
	// generated during bootstrap (persisted into placement by CASActivate).
	c.now = c.now.Add(5 * time.Second)
	reclaimAt := c.now.UnixMilli()
	got, reason, err := svc.AcceptHeldChannel(ctx, p.ChannelID, p.DaemonID, placement.HeldChannel{
		ChannelID: p.ChannelID, FencingToken: daemonTok, OwnerEpoch: daemonEpoch,
	}, 9)
	if err != nil || !got || reason != "" {
		t.Fatalf("AcceptHeldChannel ok=%v reason=%q err=%v", got, reason, err)
	}
	reclaimed, _, err := svc.Get(ctx, p.ChannelID)
	if err != nil {
		t.Fatalf("Get reclaimed: %v", err)
	}
	if reclaimed.EnteredStateAt != reclaimAt || reclaimed.LastHeartbeatAt != reclaimAt {
		t.Errorf("reclaim timestamps entered=%d heartbeat=%d want %d", reclaimed.EnteredStateAt, reclaimed.LastHeartbeatAt, reclaimAt)
	}

	// Mismatched (epoch / token) reclaim rejected.
	got, reason, err = svc.AcceptHeldChannel(ctx, p.ChannelID, p.DaemonID, placement.HeldChannel{
		ChannelID: p.ChannelID, FencingToken: "tok-9999", OwnerEpoch: 9999,
	}, 10)
	if err != nil {
		t.Fatalf("AcceptHeldChannel mismatch err: %v", err)
	}
	if got || reason != "" {
		t.Errorf("AcceptHeldChannel mismatch ok=%v reason=%q; expected false empty reason", got, reason)
	}
}

// TestReclaimHijackDifferentDaemonID is the FIX-T4 regression: a
// different daemon presenting the SAME (channel_id, fencing_token,
// owner_epoch) tuple MUST be rejected. Without the daemon_id pin in
// the SQL WHERE clause the hostile daemon would silently inherit
// ownership; with the fix the CAS finds zero rows and returns
// ok=false (no error).
func TestReclaimHijackDifferentDaemonID(t *testing.T) {
	t.Parallel()
	c := &clock{now: time.Unix(1_700_000_000, 0)}
	svc := newSvc(t, c)
	ctx := context.Background()
	svc.MarkStartedAt()

	p, _, err := svc.Reserve(ctx, channel.ID("ch-hijack"), placement.DaemonID("d-owner"), 1, nil)
	if err != nil {
		t.Fatalf("Reserve: %v", err)
	}
	const daemonEpoch placement.OwnerEpoch = 1
	const daemonTok placement.FencingToken = "daemon-tok-hijack"
	ack := placement.CreateChannelAck{
		ChannelID: p.ChannelID, CreateRequestID: p.CreateRequestID,
		OwnerEpoch: daemonEpoch, FencingToken: daemonTok,
		DaemonID: p.DaemonID, Result: placement.CreateChannelAccepted,
	}
	if ok, _ := svc.Activate(ctx, ack, 1); !ok {
		t.Fatalf("Activate failed")
	}

	// The original owner reclaims with the correct tuple → accepted.
	ok1, reason1, err := svc.AcceptHeldChannel(ctx, p.ChannelID, p.DaemonID, placement.HeldChannel{
		ChannelID: p.ChannelID, FencingToken: daemonTok, OwnerEpoch: daemonEpoch,
	}, 7)
	if err != nil || !ok1 || reason1 != "" {
		t.Fatalf("owner reclaim ok=%v reason=%q err=%v", ok1, reason1, err)
	}

	// A different daemon presents an identical (epoch, token) tuple →
	// MUST be rejected (no row update).
	ok2, reason2, err := svc.AcceptHeldChannel(ctx, p.ChannelID, placement.DaemonID("d-attacker"), placement.HeldChannel{
		ChannelID: p.ChannelID, FencingToken: daemonTok, OwnerEpoch: daemonEpoch,
	}, 8)
	if err != nil {
		t.Fatalf("hijack reclaim err: %v", err)
	}
	if ok2 || reason2 != "" {
		t.Fatalf("hijack reclaim ok=%v reason=%q — daemon_id pin missing in SQL WHERE", ok2, reason2)
	}

	// Sanity: the row's daemon_id should still be the original owner.
	got, _, err := svc.Get(ctx, p.ChannelID)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.DaemonID != p.DaemonID {
		t.Errorf("post-hijack daemon_id=%q want %q (ownership leaked)", got.DaemonID, p.DaemonID)
	}
}

func TestAcceptHeldChannel_StaleRejected_RequiresReclaim(t *testing.T) {
	t.Parallel()
	c := &clock{now: time.Unix(1_700_000_000, 0)}
	svc := newSvc(t, c)
	ctx := context.Background()

	p, _, err := svc.Reserve(ctx, channel.ID("ch-stale-held"), placement.DaemonID("d-owner"), 1, nil)
	if err != nil {
		t.Fatalf("Reserve: %v", err)
	}
	if ok, err := svc.Activate(ctx, placement.CreateChannelAck{
		ChannelID:       p.ChannelID,
		CreateRequestID: p.CreateRequestID,
		OwnerEpoch:      1,
		FencingToken:    "tok-stale-held",
		DaemonID:        p.DaemonID,
		Result:          placement.CreateChannelAccepted,
	}, 1); err != nil || !ok {
		t.Fatalf("Activate ok=%v err=%v", ok, err)
	}
	if err := svc.Store().MarkStale(ctx, p.ChannelID, c.Now().UnixMilli()); err != nil {
		t.Fatalf("MarkStale: %v", err)
	}

	ok, reason, err := svc.AcceptHeldChannel(ctx, p.ChannelID, p.DaemonID, placement.HeldChannel{
		ChannelID:    p.ChannelID,
		FencingToken: "tok-stale-held",
		OwnerEpoch:   1,
	}, 2)
	if err != nil {
		t.Fatalf("AcceptHeldChannel: %v", err)
	}
	if ok || reason != placement.HeldChannelStaleRequiresReclaim {
		t.Fatalf("AcceptHeldChannel ok=%v reason=%q want stale reclaim reject", ok, reason)
	}
	got, _, err := svc.Get(ctx, p.ChannelID)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.State != placement.StateStale || got.OwnerEpoch != 1 || got.FencingToken != "tok-stale-held" {
		t.Fatalf("stale row mutated: state=%q epoch=%d token=%q", got.State, got.OwnerEpoch, got.FencingToken)
	}

	reserved, req, reclaimOK, err := svc.ReserveReclaim(ctx, p.ChannelID, placement.DaemonID("d-new"), 3)
	if err != nil || !reclaimOK {
		t.Fatalf("ReserveReclaim ok=%v err=%v", reclaimOK, err)
	}
	if reserved.State != placement.StateCreating || reserved.OwnerEpoch != 2 || reserved.FencingToken != "" {
		t.Fatalf("ReserveReclaim state=%q epoch=%d token=%q", reserved.State, reserved.OwnerEpoch, reserved.FencingToken)
	}
	if ok, err := svc.ActivateReclaim(ctx, placement.ReclaimAccepted{
		ChannelID:       p.ChannelID,
		CreateRequestID: req.CreateRequestID,
		NewOwnerEpoch:   req.NewOwnerEpoch,
		FencingToken:    "tok-after-reclaim",
	}, placement.DaemonID("d-new"), 3); err != nil || !ok {
		t.Fatalf("ActivateReclaim ok=%v err=%v", ok, err)
	}
	got, _, err = svc.Get(ctx, p.ChannelID)
	if err != nil {
		t.Fatalf("Get post reclaim: %v", err)
	}
	if got.State != placement.StateActive || got.OwnerEpoch != 2 || got.FencingToken != "tok-after-reclaim" {
		t.Fatalf("post reclaim state=%q epoch=%d token=%q", got.State, got.OwnerEpoch, got.FencingToken)
	}
}

func TestPlacementSaga_BootstrapReserve_PhaseTracked(t *testing.T) {
	t.Parallel()
	c := &clock{now: time.Unix(1_700_000_000, 0)}
	svc := newSvc(t, c)
	ctx := context.Background()

	p, _, err := svc.Reserve(ctx, channel.ID("ch-saga-bootstrap"), placement.DaemonID("d-1"), 1, nil)
	if err != nil {
		t.Fatalf("Reserve: %v", err)
	}
	saga, found, err := svc.Store().SagaForCreateRequest(ctx, placements.SagaKindBootstrapReserve, p.ChannelID, p.CreateRequestID)
	if err != nil {
		t.Fatalf("SagaForCreateRequest: %v", err)
	}
	if !found {
		t.Fatal("bootstrap reserve saga missing")
	}
	if saga.Phase != placements.SagaPhaseSent || saga.ExpectedAckFrameKind != string(kerneldaemonbus.FrameTypeControlCreateChannelAck) {
		t.Fatalf("saga phase=%q expected_ack=%q", saga.Phase, saga.ExpectedAckFrameKind)
	}

	if ok, err := svc.Activate(ctx, placement.CreateChannelAck{
		ChannelID:       p.ChannelID,
		CreateRequestID: p.CreateRequestID,
		OwnerEpoch:      1,
		FencingToken:    "tok-saga-bootstrap",
		DaemonID:        p.DaemonID,
		Result:          placement.CreateChannelAccepted,
	}, 1); err != nil || !ok {
		t.Fatalf("Activate ok=%v err=%v", ok, err)
	}
	saga, found, err = svc.Store().GetSaga(ctx, saga.SagaID)
	if err != nil || !found {
		t.Fatalf("GetSaga found=%v err=%v", found, err)
	}
	if saga.Phase != placements.SagaPhaseCompleted || saga.TerminalStatus != "accepted" {
		t.Fatalf("completed saga phase=%q terminal=%q", saga.Phase, saga.TerminalStatus)
	}
}

func TestPlacementSaga_ReclaimReserve_PhaseTracked(t *testing.T) {
	t.Parallel()
	c := &clock{now: time.Unix(1_700_000_000, 0)}
	svc := newSvc(t, c)
	ctx := context.Background()

	p, _, err := svc.Reserve(ctx, channel.ID("ch-saga-reclaim"), placement.DaemonID("d-old"), 1, nil)
	if err != nil {
		t.Fatalf("Reserve: %v", err)
	}
	if ok, err := svc.Activate(ctx, placement.CreateChannelAck{
		ChannelID:       p.ChannelID,
		CreateRequestID: p.CreateRequestID,
		OwnerEpoch:      1,
		FencingToken:    "tok-old-saga",
		DaemonID:        p.DaemonID,
		Result:          placement.CreateChannelAccepted,
	}, 1); err != nil || !ok {
		t.Fatalf("Activate ok=%v err=%v", ok, err)
	}
	if err := svc.Store().MarkStale(ctx, p.ChannelID, c.Now().UnixMilli()); err != nil {
		t.Fatalf("MarkStale: %v", err)
	}

	reserved, req, ok, err := svc.ReserveReclaim(ctx, p.ChannelID, placement.DaemonID("d-new"), 2)
	if err != nil || !ok {
		t.Fatalf("ReserveReclaim ok=%v err=%v", ok, err)
	}
	saga, found, err := svc.Store().SagaForCreateRequest(ctx, placements.SagaKindReclaimReserve, reserved.ChannelID, reserved.CreateRequestID)
	if err != nil {
		t.Fatalf("SagaForCreateRequest: %v", err)
	}
	if !found {
		t.Fatal("reclaim reserve saga missing")
	}
	if saga.Phase != placements.SagaPhaseSent || saga.ExpectedAckFrameKind != string(kerneldaemonbus.FrameTypeControlReclaimAccepted) {
		t.Fatalf("saga phase=%q expected_ack=%q", saga.Phase, saga.ExpectedAckFrameKind)
	}

	if ok, err := svc.ActivateReclaim(ctx, placement.ReclaimAccepted{
		ChannelID:       reserved.ChannelID,
		CreateRequestID: req.CreateRequestID,
		NewOwnerEpoch:   req.NewOwnerEpoch,
		FencingToken:    "tok-new-saga",
	}, placement.DaemonID("d-new"), 2); err != nil || !ok {
		t.Fatalf("ActivateReclaim ok=%v err=%v", ok, err)
	}
	saga, found, err = svc.Store().GetSaga(ctx, saga.SagaID)
	if err != nil || !found {
		t.Fatalf("GetSaga found=%v err=%v", found, err)
	}
	if saga.Phase != placements.SagaPhaseCompleted || saga.TerminalStatus != "accepted" {
		t.Fatalf("completed reclaim saga phase=%q terminal=%q", saga.Phase, saga.TerminalStatus)
	}
}

func TestPlacementSaga_MidSagaCrash_RecoveryByPhase(t *testing.T) {
	t.Parallel()
	c := &clock{now: time.Unix(1_700_000_000, 0)}
	svc := newSvc(t, c)
	ctx := context.Background()
	svc.MarkStartedAt()

	p, _, err := svc.Reserve(ctx, channel.ID("ch-saga-crash"), placement.DaemonID("d-1"), 1, nil)
	if err != nil {
		t.Fatalf("Reserve: %v", err)
	}
	saga, found, err := svc.Store().SagaForCreateRequest(ctx, placements.SagaKindBootstrapReserve, p.ChannelID, p.CreateRequestID)
	if err != nil || !found {
		t.Fatalf("bootstrap saga found=%v err=%v", found, err)
	}
	if err := svc.Store().MarkSagaPhase(ctx, saga.SagaID, placements.SagaPhasePartialTakeover, c.Now().UnixMilli()); err != nil {
		t.Fatalf("MarkSagaPhase: %v", err)
	}

	c.now = c.now.Add(11 * time.Second)
	if err := svc.ReconcileOnce(ctx); err != nil {
		t.Fatalf("ReconcileOnce: %v", err)
	}
	got, _, err := svc.Get(ctx, p.ChannelID)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.State != placement.StateOrphan {
		t.Fatalf("placement state=%q want orphan", got.State)
	}
	saga, found, err = svc.Store().GetSaga(ctx, saga.SagaID)
	if err != nil || !found {
		t.Fatalf("GetSaga found=%v err=%v", found, err)
	}
	if saga.Phase != placements.SagaPhaseAbandoned || saga.AbandonmentReason == "" {
		t.Fatalf("saga phase=%q reason=%q want abandoned by recovery", saga.Phase, saga.AbandonmentReason)
	}
}

func TestPlacementSaga_TimeoutAbandonment(t *testing.T) {
	t.Parallel()
	c := &clock{now: time.Unix(1_700_000_000, 0)}
	svc := newSvc(t, c)
	ctx := context.Background()

	saga, err := svc.Store().StartSaga(ctx, placements.StartSagaInput{
		SagaID:               "manual-timeout",
		ChannelID:            "ch-saga-timeout",
		CreateRequestID:      "req-timeout",
		OwnerEpoch:           1,
		DaemonID:             "d-1",
		SagaKind:             placements.SagaKindBootstrapReserve,
		Phase:                placements.SagaPhaseAwaitingAck,
		SentAt:               c.Now().Add(-time.Minute).UnixMilli(),
		ExpectedAckFrameKind: string(kerneldaemonbus.FrameTypeControlCreateChannelAck),
		NowMs:                c.Now().UnixMilli(),
	})
	if err != nil {
		t.Fatalf("StartSaga: %v", err)
	}
	if err := svc.Store().AbandonTimedOutSagas(ctx, c.Now().Add(-10*time.Second).UnixMilli(), "phase_timeout", c.Now().UnixMilli()); err != nil {
		t.Fatalf("AbandonTimedOutSagas: %v", err)
	}
	got, found, err := svc.Store().GetSaga(ctx, saga.SagaID)
	if err != nil || !found {
		t.Fatalf("GetSaga found=%v err=%v", found, err)
	}
	if got.Phase != placements.SagaPhaseAbandoned || got.TerminalStatus != "timeout" || got.AbandonmentReason != "phase_timeout" {
		t.Fatalf("timeout saga phase=%q terminal=%q reason=%q", got.Phase, got.TerminalStatus, got.AbandonmentReason)
	}
}

// TestReserve_DoesNotPreGenerateFencing asserts the trust-root
// direction defined by proto-foundation §3.3.3: server Reserve writes
// owner_epoch=0 and an empty fencing_token. The daemon-side Phase 2
// bootstrap is the trust root for fencing — it generates the
// unguessable token and returns it in the ack. The CreateChannelRequest
// carries no fencing fields (impl-layer2 §3.2.1).
func TestReserve_DoesNotPreGenerateFencing(t *testing.T) {
	t.Parallel()
	c := &clock{now: time.Unix(1_700_000_000, 0)}
	svc := newSvc(t, c)
	ctx := context.Background()

	p, req, err := svc.Reserve(ctx, channel.ID("ch-A"), placement.DaemonID("d-1"), 1, nil)
	if err != nil {
		t.Fatalf("Reserve: %v", err)
	}

	// Phase 1 placement carries epoch=0 / token="".
	if p.OwnerEpoch != 0 {
		t.Errorf("placement.OwnerEpoch=%d want 0 (daemon is the trust root)", p.OwnerEpoch)
	}
	if p.FencingToken != "" {
		t.Errorf("placement.FencingToken=%q want empty (daemon is the trust root)", p.FencingToken)
	}
	// CreateRequestID must still be allocated by the server (saga id).
	if req.CreateRequestID == "" || req.CreateRequestID != p.CreateRequestID {
		t.Errorf("request.CreateRequestID=%q placement.CreateRequestID=%q want non-empty equal",
			req.CreateRequestID, p.CreateRequestID)
	}
}

// TestActivate_WritesDaemonFencingTuple asserts the Phase 3 CAS reads
// owner_epoch / fencing_token FROM the daemon's ack and persists them
// into the placement row (proto-foundation §3.3.3 Phase 3 +
// impl-layer2 §3.2.3).
func TestActivate_WritesDaemonFencingTuple(t *testing.T) {
	t.Parallel()
	c := &clock{now: time.Unix(1_700_000_000, 0)}
	svc := newSvc(t, c)
	ctx := context.Background()

	p, _, err := svc.Reserve(ctx, channel.ID("ch-write-ack"), placement.DaemonID("d-1"), 7, nil)
	if err != nil {
		t.Fatalf("Reserve: %v", err)
	}

	const daemonEpoch placement.OwnerEpoch = 1
	const daemonTok placement.FencingToken = "01234567890abcdef01234567890abcd"
	ack := placement.CreateChannelAck{
		FrameID: "f-write-ack", ChannelID: p.ChannelID,
		CreateRequestID: p.CreateRequestID,
		OwnerEpoch:      daemonEpoch, FencingToken: daemonTok,
		DaemonID: p.DaemonID, DaemonEpoch: 1, Result: placement.CreateChannelAccepted,
	}
	ok, err := svc.Activate(ctx, ack, 7)
	if err != nil || !ok {
		t.Fatalf("Activate ok=%v err=%v", ok, err)
	}
	got, _, _ := svc.Get(ctx, p.ChannelID)
	if got.State != placement.StateActive {
		t.Errorf("state=%q want active", got.State)
	}
	if got.OwnerEpoch != daemonEpoch {
		t.Errorf("OwnerEpoch=%d want %d (from ack)", got.OwnerEpoch, daemonEpoch)
	}
	if got.FencingToken != daemonTok {
		t.Errorf("FencingToken=%q want %q (from ack)", got.FencingToken, daemonTok)
	}
}

// TestActivate_RejectsEmptyFencingToken asserts the server refuses to
// CAS when the daemon-side ack omits the fencing token (impl-layer2
// §3.2.2 marks it required on the accept path). The placement row
// stays in 'creating' so the reconcile loop can drive it to orphan.
func TestActivate_RejectsEmptyFencingToken(t *testing.T) {
	t.Parallel()
	c := &clock{now: time.Unix(1_700_000_000, 0)}
	svc := newSvc(t, c)
	ctx := context.Background()

	p, _, err := svc.Reserve(ctx, channel.ID("ch-empty-tok"), placement.DaemonID("d-1"), 1, nil)
	if err != nil {
		t.Fatalf("Reserve: %v", err)
	}
	ack := placement.CreateChannelAck{
		ChannelID: p.ChannelID, CreateRequestID: p.CreateRequestID,
		OwnerEpoch: 1, FencingToken: "", // ← protocol violation: missing
		DaemonID: p.DaemonID, Result: placement.CreateChannelAccepted,
	}
	ok, err := svc.Activate(ctx, ack, 1)
	if err != nil {
		t.Fatalf("Activate err=%v", err)
	}
	if ok {
		t.Errorf("Activate ok=true but ack lacked fencing_token; CAS must reject")
	}
	got, _, _ := svc.Get(ctx, p.ChannelID)
	if got.State != placement.StateCreating {
		t.Errorf("state=%q want creating (CAS should leave row untouched)", got.State)
	}
}

func TestServerInitiatedReclaimReserveAndActivate(t *testing.T) {
	t.Parallel()
	c := &clock{now: time.Unix(1_700_000_000, 0)}
	svc := newSvc(t, c)
	ctx := context.Background()

	p, _, err := svc.Reserve(ctx, channel.ID("ch-reclaim-srv"), placement.DaemonID("d-old"), 1, nil)
	if err != nil {
		t.Fatalf("Reserve: %v", err)
	}
	if ok, err := svc.Activate(ctx, placement.CreateChannelAck{
		ChannelID:       p.ChannelID,
		CreateRequestID: p.CreateRequestID,
		OwnerEpoch:      1,
		FencingToken:    "tok-old",
		DaemonID:        p.DaemonID,
		Result:          placement.CreateChannelAccepted,
	}, 1); err != nil || !ok {
		t.Fatalf("Activate ok=%v err=%v", ok, err)
	}
	if err := svc.Store().MarkStale(ctx, p.ChannelID, c.Now().UnixMilli()); err != nil {
		t.Fatalf("MarkStale: %v", err)
	}

	reserved, req, ok, err := svc.ReserveReclaim(ctx, p.ChannelID, placement.DaemonID("d-new"), 7)
	if err != nil || !ok {
		t.Fatalf("ReserveReclaim ok=%v err=%v", ok, err)
	}
	if reserved.State != placement.StateCreating {
		t.Fatalf("reserved state=%q want creating", reserved.State)
	}
	if reserved.OwnerEpoch != 2 || req.NewOwnerEpoch != 2 {
		t.Fatalf("owner_epoch reserved=%d req=%d want 2", reserved.OwnerEpoch, req.NewOwnerEpoch)
	}
	if req.PreviousOwnerDaemon == nil || *req.PreviousOwnerDaemon != placement.DaemonID("d-old") {
		t.Fatalf("previous_owner_daemon=%v want d-old", req.PreviousOwnerDaemon)
	}
	if req.PreviousState != placement.ReclaimOriginStale {
		t.Fatalf("previous_state=%q want stale", req.PreviousState)
	}

	ok, err = svc.ActivateReclaim(ctx, placement.ReclaimAccepted{
		ChannelID:       p.ChannelID,
		CreateRequestID: req.CreateRequestID,
		NewOwnerEpoch:   req.NewOwnerEpoch,
		FencingToken:    "tok-new",
	}, placement.DaemonID("d-new"), 7)
	if err != nil || !ok {
		t.Fatalf("ActivateReclaim ok=%v err=%v", ok, err)
	}
	got, _, err := svc.Get(ctx, p.ChannelID)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.State != placement.StateActive || got.DaemonID != placement.DaemonID("d-new") {
		t.Fatalf("post-reclaim state=%q daemon=%q", got.State, got.DaemonID)
	}
	if got.OwnerEpoch != 2 || got.FencingToken != "tok-new" {
		t.Fatalf("post-reclaim epoch=%d token=%q", got.OwnerEpoch, got.FencingToken)
	}
}

func TestValidatePushFencingByPlacementState(t *testing.T) {
	t.Parallel()
	c := &clock{now: time.Unix(1_700_000_000, 0)}
	svc := newSvc(t, c)
	ctx := context.Background()

	active, _, err := svc.Reserve(ctx, channel.ID("ch-push-active"), placement.DaemonID("d-active"), 1, nil)
	if err != nil {
		t.Fatalf("Reserve active: %v", err)
	}
	if ok, err := svc.Activate(ctx, placement.CreateChannelAck{
		ChannelID:       active.ChannelID,
		CreateRequestID: active.CreateRequestID,
		OwnerEpoch:      1,
		FencingToken:    "tok-active",
		DaemonID:        active.DaemonID,
		Result:          placement.CreateChannelAccepted,
	}, 1); err != nil || !ok {
		t.Fatalf("Activate active ok=%v err=%v", ok, err)
	}
	assertPush := func(name string, channelID channel.ID, daemonID placement.DaemonID, ownerEpoch placement.OwnerEpoch, fencingToken placement.FencingToken, want bool) {
		t.Helper()
		got, err := svc.ValidatePushFencing(ctx, channelID, daemonID, ownerEpoch, fencingToken)
		if err != nil {
			t.Fatalf("%s ValidatePushFencing: %v", name, err)
		}
		if got != want {
			t.Fatalf("%s ValidatePushFencing=%v want %v", name, got, want)
		}
	}
	assertPush("active matching", active.ChannelID, active.DaemonID, 1, "tok-active", true)
	assertPush("active wrong token", active.ChannelID, active.DaemonID, 1, "tok-wrong", false)

	reclaimSource, _, err := svc.Reserve(ctx, channel.ID("ch-push-reclaim"), placement.DaemonID("d-old"), 1, nil)
	if err != nil {
		t.Fatalf("Reserve reclaim source: %v", err)
	}
	if ok, err := svc.Activate(ctx, placement.CreateChannelAck{
		ChannelID:       reclaimSource.ChannelID,
		CreateRequestID: reclaimSource.CreateRequestID,
		OwnerEpoch:      1,
		FencingToken:    "tok-old",
		DaemonID:        reclaimSource.DaemonID,
		Result:          placement.CreateChannelAccepted,
	}, 1); err != nil || !ok {
		t.Fatalf("Activate reclaim source ok=%v err=%v", ok, err)
	}
	if err := svc.Store().MarkStale(ctx, reclaimSource.ChannelID, c.Now().UnixMilli()); err != nil {
		t.Fatalf("MarkStale reclaim source: %v", err)
	}
	creating, _, ok, err := svc.ReserveReclaim(ctx, reclaimSource.ChannelID, placement.DaemonID("d-new"), 2)
	if err != nil || !ok {
		t.Fatalf("ReserveReclaim ok=%v err=%v", ok, err)
	}
	if creating.State != placement.StateCreating || creating.FencingToken != "" {
		t.Fatalf("ReserveReclaim state=%q token=%q want creating with empty token", creating.State, creating.FencingToken)
	}
	assertPush("creating reclaim candidate", creating.ChannelID, placement.DaemonID("d-new"), creating.OwnerEpoch, "tok-daemon-generated", true)
	assertPush("creating empty token", creating.ChannelID, placement.DaemonID("d-new"), creating.OwnerEpoch, "", false)
	assertPush("creating wrong daemon", creating.ChannelID, placement.DaemonID("d-other"), creating.OwnerEpoch, "tok-daemon-generated", false)
	assertPush("creating old epoch", creating.ChannelID, placement.DaemonID("d-new"), creating.OwnerEpoch-1, "tok-daemon-generated", false)

	stale, _, err := svc.Reserve(ctx, channel.ID("ch-push-stale"), placement.DaemonID("d-stale"), 1, nil)
	if err != nil {
		t.Fatalf("Reserve stale: %v", err)
	}
	if ok, err := svc.Activate(ctx, placement.CreateChannelAck{
		ChannelID:       stale.ChannelID,
		CreateRequestID: stale.CreateRequestID,
		OwnerEpoch:      1,
		FencingToken:    "tok-stale",
		DaemonID:        stale.DaemonID,
		Result:          placement.CreateChannelAccepted,
	}, 1); err != nil || !ok {
		t.Fatalf("Activate stale ok=%v err=%v", ok, err)
	}
	if err := svc.Store().MarkStale(ctx, stale.ChannelID, c.Now().UnixMilli()); err != nil {
		t.Fatalf("MarkStale: %v", err)
	}
	assertPush("stale rejected", stale.ChannelID, stale.DaemonID, 1, "tok-stale", false)

	orphan, _, err := svc.Reserve(ctx, channel.ID("ch-push-orphan"), placement.DaemonID("d-orphan"), 1, nil)
	if err != nil {
		t.Fatalf("Reserve orphan: %v", err)
	}
	if err := svc.Store().MarkOrphan(ctx, orphan.ChannelID, c.Now().UnixMilli()); err != nil {
		t.Fatalf("MarkOrphan: %v", err)
	}
	assertPush("orphan rejected", orphan.ChannelID, orphan.DaemonID, orphan.OwnerEpoch, "tok-orphan", false)
	assertPush("missing rejected", channel.ID("ch-push-missing"), placement.DaemonID("d-missing"), 1, "tok-missing", false)
}

func TestObserveHeartbeatPlacementDiffActions(t *testing.T) {
	t.Parallel()
	c := &clock{now: time.Unix(1_700_000_000, 0)}
	svc := newSvc(t, c)
	ctx := context.Background()

	p, _, err := svc.Reserve(ctx, channel.ID("ch-ok"), placement.DaemonID("d-1"), 1, nil)
	if err != nil {
		t.Fatalf("Reserve ok: %v", err)
	}
	if ok, err := svc.Activate(ctx, placement.CreateChannelAck{
		ChannelID:       p.ChannelID,
		CreateRequestID: p.CreateRequestID,
		OwnerEpoch:      1,
		FencingToken:    "tok-ok",
		DaemonID:        p.DaemonID,
		Result:          placement.CreateChannelAccepted,
	}, 1); err != nil || !ok {
		t.Fatalf("Activate ok=%v err=%v", ok, err)
	}
	other, _, err := svc.Reserve(ctx, channel.ID("ch-other"), placement.DaemonID("d-2"), 1, nil)
	if err != nil {
		t.Fatalf("Reserve other: %v", err)
	}
	if ok, err := svc.Activate(ctx, placement.CreateChannelAck{
		ChannelID:       other.ChannelID,
		CreateRequestID: other.CreateRequestID,
		OwnerEpoch:      1,
		FencingToken:    "tok-other",
		DaemonID:        other.DaemonID,
		Result:          placement.CreateChannelAccepted,
	}, 1); err != nil || !ok {
		t.Fatalf("Activate other ok=%v err=%v", ok, err)
	}
	stale, _, err := svc.Reserve(ctx, channel.ID("ch-stale"), placement.DaemonID("d-1"), 1, nil)
	if err != nil {
		t.Fatalf("Reserve stale: %v", err)
	}
	if ok, err := svc.Activate(ctx, placement.CreateChannelAck{
		ChannelID:       stale.ChannelID,
		CreateRequestID: stale.CreateRequestID,
		OwnerEpoch:      1,
		FencingToken:    "tok-stale",
		DaemonID:        stale.DaemonID,
		Result:          placement.CreateChannelAccepted,
	}, 1); err != nil || !ok {
		t.Fatalf("Activate stale ok=%v err=%v", ok, err)
	}
	if err := svc.Store().MarkStale(ctx, stale.ChannelID, c.Now().UnixMilli()); err != nil {
		t.Fatalf("MarkStale: %v", err)
	}

	diff, err := svc.ObserveHeartbeat(ctx, placement.DaemonID("d-1"), []placement.HeartbeatHeldChannel{
		{ChannelID: "ch-ok", OwnerEpoch: 1, FencingToken: "tok-ok"},
		{ChannelID: "ch-ok", OwnerEpoch: 1, FencingToken: "wrong"},
		{ChannelID: "ch-other", OwnerEpoch: 1, FencingToken: "tok-other"},
		{ChannelID: "ch-stale", OwnerEpoch: 1, FencingToken: "tok-stale"},
		{ChannelID: "ch-missing", OwnerEpoch: 1, FencingToken: "tok-missing"},
	})
	if err != nil {
		t.Fatalf("ObserveHeartbeat: %v", err)
	}
	want := []placement.PlacementDiffAction{
		placement.PlacementDiffActionOK,
		placement.PlacementDiffActionReclaimPending,
		// daemon_id mismatch routes to reclaim_pending by impl-layer2 spec.
		placement.PlacementDiffActionReclaimPending,
		placement.PlacementDiffActionReclaimPending,
		placement.PlacementDiffActionDirectoryMissing,
	}
	if len(diff) != len(want) {
		t.Fatalf("diff len=%d want %d", len(diff), len(want))
	}
	for i, action := range want {
		if diff[i].Action != action {
			t.Fatalf("diff[%d].Action=%q want %q (diff=%+v)", i, diff[i].Action, action, diff)
		}
	}
}
