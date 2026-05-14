package placements

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/wanpengxie/ActOS/kernel/channel"
	"github.com/wanpengxie/ActOS/kernel/placement"
)

// Config bundles reconcile knobs (T1.7).
type Config struct {
	GracePeriod      time.Duration // cold-start window during which active→stale is suppressed
	CreateTimeout    time.Duration // creating → orphan when this elapses
	HeartbeatTimeout time.Duration // active → stale when last_heartbeat_at exceeds this
	ReconcileTick    time.Duration // how often to run a sweep (default 5s in prod, faster in tests)
}

// Service bundles SQLStore + reconcile loop + the helper that
// constructs CreateChannelRequest frames on behalf of the gateway.
type Service struct {
	store *SQLStore
	cfg   Config
	now   func() time.Time
	mu    sync.Mutex
	// startedAt is set on first Reconcile call (or RunReconcile) to
	// implement the T1.7 cold-start grace.
	startedAt time.Time
	hasStart  bool
}

// NewService builds a Service.
func NewService(db *sql.DB, cfg Config) *Service {
	if cfg.GracePeriod <= 0 {
		cfg.GracePeriod = 60 * time.Second
	}
	if cfg.CreateTimeout <= 0 {
		cfg.CreateTimeout = 30 * time.Second
	}
	if cfg.HeartbeatTimeout <= 0 {
		cfg.HeartbeatTimeout = 90 * time.Second
	}
	if cfg.ReconcileTick <= 0 {
		cfg.ReconcileTick = 5 * time.Second
	}
	return &Service{
		store: NewSQLStore(db),
		cfg:   cfg,
		now:   time.Now,
	}
}

// WithClock overrides the clock (tests).
func (s *Service) WithClock(now func() time.Time) *Service {
	s.now = now
	return s
}

// WithStartedAt overrides the cold-start anchor (tests use this to
// skip / extend the grace period).
func (s *Service) WithStartedAt(t time.Time) *Service {
	s.mu.Lock()
	s.startedAt = t
	s.hasStart = true
	s.mu.Unlock()
	return s
}

// Store exposes the SQLStore (composition root + tests use this).
func (s *Service) Store() *SQLStore { return s.store }

// Reserve performs L2 §1.4.11.3 step 1 — generate a new
// (owner_epoch, fencing_token, create_request_id) triple and INSERT
// the row in 'creating'. Callers pick the daemonID upstream.
//
// Returns the (Placement, CreateChannelRequest) pair so the gateway
// can immediately ship the control.create_channel frame to daemon.
//
// This is the M1.5 demo-friendly entry point — the federation /
// tenancy reservation columns introduced by m1.5-tickets §T10 are
// left at their zero value (NULL in sqlite). Callers that need to
// populate them MUST use ReserveWith.
func (s *Service) Reserve(
	ctx context.Context,
	channelID channel.ID,
	daemonID placement.DaemonID,
	connectionEpoch placement.ConnectionEpoch,
	initialMembers []placement.InitialMember,
) (placement.Placement, placement.CreateChannelRequest, error) {
	return s.ReserveWith(ctx, channelID, daemonID, connectionEpoch, initialMembers, ReserveOptions{})
}

// ReserveWith is the federation / tenancy-aware variant of Reserve.
// It threads ReserveOptions (TenantID / HostActorID / FederatedOrigin)
// through the placement record without changing the state machine.
//
// M1.5 demo flows can keep calling Reserve; this entry point is for
// M1.4 channel-as-actor + M2+ federation / SaaS callers that need to
// populate the reservation columns at insert time.
func (s *Service) ReserveWith(
	ctx context.Context,
	channelID channel.ID,
	daemonID placement.DaemonID,
	connectionEpoch placement.ConnectionEpoch,
	initialMembers []placement.InitialMember,
	opts ReserveOptions,
) (placement.Placement, placement.CreateChannelRequest, error) {
	now := s.now().UnixMilli()
	epoch := placement.OwnerEpoch(now)

	p := placement.Placement{
		ChannelID:             channelID,
		DaemonID:              daemonID,
		State:                 placement.StateCreating,
		OwnerEpoch:            epoch,
		FencingToken:          placement.FencingToken(epoch),
		CreateRequestID:       placement.CreateRequestID(uuid.NewString()),
		DaemonConnectionEpoch: connectionEpoch,
		CreatedAt:             now,
		// Federation / tenancy reservation per m1.5-tickets §T10.
		// Zero values land as NULL in sqlite via nullableString.
		HostActorID:     opts.HostActorID,
		FederatedOrigin: opts.FederatedOrigin,
		TenantID:        tenantOrDefault(opts.TenantID),
	}
	out, err := s.store.Reserve(ctx, p)
	if err != nil {
		return placement.Placement{}, placement.CreateChannelRequest{}, err
	}

	req := placement.CreateChannelRequest{
		ChannelID:                     out.ChannelID,
		CreateRequestID:               out.CreateRequestID,
		OwnerEpoch:                    out.OwnerEpoch,
		FencingToken:                  out.FencingToken,
		DaemonConnectionEpochExpected: out.DaemonConnectionEpoch,
		InitialMembers:                initialMembers,
	}
	return out, req, nil
}

// Activate runs L2 §1.4.11.3 step 5 on a daemon ACK. It validates
// the ACK's status before invoking the SQL CAS (ack.Match is also
// checked client-side as a fast pre-check + diagnostic).
//
// Returns (ok, nil) when the CAS succeeded; (false, nil) when the
// CAS lost — the gateway should treat this as ACK rejected, and let
// reconcile transition the row to orphan via create_timeout.
func (s *Service) Activate(
	ctx context.Context,
	ack placement.CreateChannelAck,
	newConnectionEpoch placement.ConnectionEpoch,
) (bool, error) {
	if ack.Status != placement.AckBound {
		return false, nil
	}
	cur, ok, err := s.store.Get(ctx, ack.ChannelID)
	if err != nil {
		return false, err
	}
	if !ok {
		return false, fmt.Errorf("placements: activate unknown channel %q", ack.ChannelID)
	}
	if !ack.Match(cur) {
		// 5-field pre-check failure — short-circuit the SQL CAS;
		// reconcile path will eventually transition to orphan.
		return false, nil
	}
	return s.store.CASActivate(ctx, ack, newConnectionEpoch, s.now().UnixMilli())
}

// Heartbeat refreshes last_heartbeat_at on a control.heartbeat
// frame. Used by daemonbus dispatch path.
func (s *Service) Heartbeat(ctx context.Context, channelID channel.ID, daemonID placement.DaemonID) error {
	return s.store.Heartbeat(ctx, channelID, daemonID, s.now().UnixMilli())
}

// AcceptReclaim runs the reclaim CAS (L2 §1.4.11.4 step 2).
func (s *Service) AcceptReclaim(
	ctx context.Context,
	channelID channel.ID,
	req placement.ReclaimChannel,
	newConnectionEpoch placement.ConnectionEpoch,
) (bool, error) {
	return s.store.AcceptReclaim(ctx, channelID, req, newConnectionEpoch, s.now().UnixMilli())
}

// Get is a thin pass-through.
func (s *Service) Get(ctx context.Context, channelID channel.ID) (placement.Placement, bool, error) {
	return s.store.Get(ctx, channelID)
}

// ListByDaemon is a thin pass-through.
func (s *Service) ListByDaemon(ctx context.Context, daemonID placement.DaemonID) ([]placement.Placement, error) {
	return s.store.ListByDaemon(ctx, daemonID)
}

// ListByState is a thin pass-through.
func (s *Service) ListByState(ctx context.Context, state placement.State) ([]placement.Placement, error) {
	return s.store.ListByState(ctx, state)
}

// MarkStartedAt anchors the cold-start clock (RunReconcile calls
// this automatically; tests can call it manually).
func (s *Service) MarkStartedAt() {
	s.mu.Lock()
	if !s.hasStart {
		s.startedAt = s.now()
		s.hasStart = true
	}
	s.mu.Unlock()
}

// ReconcileOnce runs a single reconcile sweep — creating→orphan
// (always) + active→stale (only after grace).
func (s *Service) ReconcileOnce(ctx context.Context) error {
	s.MarkStartedAt()
	now := s.now()
	cutoffCreate := now.Add(-s.cfg.CreateTimeout).UnixMilli()
	cutoffHeart := now.Add(-s.cfg.HeartbeatTimeout).UnixMilli()

	// 1) creating + created_at < cutoffCreate → orphan (no grace).
	creating, err := s.store.ListByState(ctx, placement.StateCreating)
	if err != nil {
		return err
	}
	for _, p := range creating {
		if p.CreatedAt > cutoffCreate {
			continue
		}
		if err := s.store.MarkOrphan(ctx, p.ChannelID, now.UnixMilli()); err != nil {
			return err
		}
	}

	// 2) active + last_heartbeat_at < cutoffHeart → stale, only
	//    AFTER cold-start grace has elapsed.
	s.mu.Lock()
	start := s.startedAt
	s.mu.Unlock()
	if now.Sub(start) < s.cfg.GracePeriod {
		return nil
	}

	active, err := s.store.ListByState(ctx, placement.StateActive)
	if err != nil {
		return err
	}
	for _, p := range active {
		// last_heartbeat_at == 0 means we never saw one — that
		// shouldn't happen because CASActivate sets it, but be
		// defensive.
		if p.LastHeartbeatAt == 0 {
			continue
		}
		if p.LastHeartbeatAt > cutoffHeart {
			continue
		}
		if err := s.store.MarkStale(ctx, p.ChannelID, now.UnixMilli()); err != nil {
			return err
		}
	}
	return nil
}

// RunReconcile loops ReconcileOnce until ctx is cancelled. Anchors
// the cold-start clock at first call.
func (s *Service) RunReconcile(ctx context.Context) {
	s.MarkStartedAt()
	ticker := time.NewTicker(s.cfg.ReconcileTick)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := s.ReconcileOnce(ctx); err != nil && !errors.Is(err, context.Canceled) {
				// Reconcile errors are non-fatal — log but keep going.
				fmt.Printf("[placements] reconcile error: %v\n", err)
			}
		}
	}
}
