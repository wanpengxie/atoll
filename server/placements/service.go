package placements

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/rs/zerolog"

	"github.com/wanpengxie/ActOS/kernel/channel"
	kerneldaemonbus "github.com/wanpengxie/ActOS/kernel/daemonbus"
	"github.com/wanpengxie/ActOS/kernel/placement"
	"github.com/wanpengxie/ActOS/pkg/requestctx"
	"github.com/wanpengxie/ActOS/server/channelaccess"
)

// Config bundles reconcile knobs (T1.7).
type Config struct {
	GracePeriod      time.Duration // cold-start window during which active→stale is suppressed
	CreateTimeout    time.Duration // creating → orphan when this elapses
	HeartbeatTimeout time.Duration // active → stale when last_heartbeat_at exceeds this
	ReconcileTick    time.Duration // how often to run a sweep (default 5s in prod, faster in tests)
	Logger           *zerolog.Logger
}

// Service bundles SQLStore + reconcile loop + the helper that
// constructs CreateChannelRequest frames on behalf of the gateway.
type Service struct {
	store *SQLStore
	cfg   Config
	now   func() time.Time
	log   zerolog.Logger
	mu    sync.Mutex
	// startedAt is set on first Reconcile call (or RunReconcile) to
	// implement the T1.7 cold-start grace.
	startedAt time.Time
	hasStart  bool

	reclaimMu      sync.RWMutex
	reclaimHandler func(context.Context, placement.Placement) error

	accessMu sync.RWMutex
	access   channelaccess.Authorizer
}

// ChannelDaemonResolver is the narrow daemonbus dependency for
// resolving a channel's current active owner. The placement package
// remains the sole server owner of channel_placements SQL.
type ChannelDaemonResolver interface {
	ResolveDaemonForChannel(ctx context.Context, channelID channel.ID) (placement.DaemonID, bool, error)
}

// DaemonLoadReader exposes placement-owned load accounting to schedulers.
type DaemonLoadReader interface {
	ActiveOrCreatingCountsByDaemon(ctx context.Context) (map[placement.DaemonID]int, error)
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
	log := zerolog.Nop()
	if cfg.Logger != nil {
		log = *cfg.Logger
	}
	return &Service{
		store: NewSQLStore(db),
		cfg:   cfg,
		now:   time.Now,
		log:   log,
	}
}

// SetAccessAuthorizer wires route-level channel membership checks.
func (s *Service) SetAccessAuthorizer(a channelaccess.Authorizer) {
	s.accessMu.Lock()
	s.access = a
	s.accessMu.Unlock()
}

func (s *Service) accessAuthorizer() channelaccess.Authorizer {
	s.accessMu.RLock()
	defer s.accessMu.RUnlock()
	return s.access
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

// SetReclaimHandler wires the server composition root callback that
// can push control.daemon_reclaim to a candidate daemon. placements
// owns the SQL state machine; the gateway owns daemonbus routing.
func (s *Service) SetReclaimHandler(h func(context.Context, placement.Placement) error) {
	s.reclaimMu.Lock()
	s.reclaimHandler = h
	s.reclaimMu.Unlock()
}

func (s *Service) triggerReclaim(ctx context.Context, p placement.Placement) error {
	s.reclaimMu.RLock()
	h := s.reclaimHandler
	s.reclaimMu.RUnlock()
	if h == nil {
		return nil
	}
	return h(ctx, p)
}

// Reserve performs proto-foundation §3.3.3 Phase 1 + impl-layer2 §3.2.1
// — INSERT a placement row in 'creating' with owner_epoch=0 and an
// empty fencing_token. The fencing trust root is the daemon: the
// daemon-side bootstrap saga (Phase 2) generates fencing_token + sets
// owner_epoch=1 and returns them in the ack; the server writes those
// values back via the Phase 3 CAS (Activate).
//
// Returns the (Placement, CreateChannelRequest) pair so the gateway
// can immediately ship the control.create_channel frame to daemon.
// Note the CreateChannelRequest deliberately does NOT carry
// fencing_token / owner_epoch — those are daemon outputs, not inputs.
//
// This is the launch demo-friendly entry point — the federation /
// tenancy reservation columns introduced by launch-ticket notes §T10 are
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
// launch demo flows can keep calling Reserve; this entry point is for
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

	// proto-foundation §3.3.3 Phase 1: state='creating', owner_epoch=0,
	// fencing_token empty until daemon-side Phase 2 generates them.
	p := placement.Placement{
		ChannelID:             channelID,
		DaemonID:              daemonID,
		State:                 placement.StateCreating,
		OwnerEpoch:            0,
		FencingToken:          "",
		CreateRequestID:       placement.CreateRequestID(uuid.NewString()),
		DaemonConnectionEpoch: connectionEpoch,
		CreatedAt:             now,
		EnteredStateAt:        now,
		// Federation / tenancy reservation per launch-ticket notes §T10.
		// Zero values land as NULL in sqlite via nullableString.
		HostActorID:     opts.HostActorID,
		FederatedOrigin: opts.FederatedOrigin,
		TenantID:        tenantOrDefault(opts.TenantID),
	}
	out, _, err := s.store.ReserveWithSaga(ctx, p, StartSagaInput{
		ChannelID:             p.ChannelID,
		CreateRequestID:       p.CreateRequestID,
		OwnerEpoch:            p.OwnerEpoch,
		DaemonID:              p.DaemonID,
		DaemonConnectionEpoch: p.DaemonConnectionEpoch,
		SagaKind:              SagaKindBootstrapReserve,
		Phase:                 SagaPhaseSent,
		SentAt:                now,
		ExpectedAckFrameKind:  string(kerneldaemonbus.FrameTypeControlCreateChannelAck),
		NowMs:                 now,
	})
	if err != nil {
		return placement.Placement{}, placement.CreateChannelRequest{}, err
	}

	req := placement.CreateChannelRequest{
		ChannelID:       out.ChannelID,
		CreateRequestID: out.CreateRequestID,
		InitialMembers:  initialMembers,
		// L4 channel-template key (catalog.Channel.Type) — daemon side
		// resolves the template from this value during bootstrap saga
		// (M1.6-T5 phase-2).
		ChannelType: opts.ChannelType,
	}
	return out, req, nil
}

// Activate runs L2 §1.4.11.3 step 5 on a daemon ACK. It validates
// the ACK's result before invoking the SQL CAS (ack.Match is also
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
	if ack.Result != placement.CreateChannelAccepted {
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
	now := s.now().UnixMilli()
	ok, err = s.store.CASActivate(ctx, ack, newConnectionEpoch, now)
	if err != nil {
		return false, err
	}
	if ok {
		if saga, found, err := s.store.SagaForCreateRequest(ctx, SagaKindBootstrapReserve, ack.ChannelID, ack.CreateRequestID); err != nil {
			return false, err
		} else if found {
			if err := s.store.CompleteSaga(ctx, saga.SagaID, "accepted", now); err != nil {
				return false, err
			}
		}
	}
	return ok, nil
}

// RejectCreate marks a create-channel saga failed/orphan after the daemon
// emits control.reject_channel. Reconcile owns final cleanup; this explicit
// transition prevents a rejected saga from lingering until timeout.
func (s *Service) RejectCreate(ctx context.Context, rej placement.RejectChannel) (bool, error) {
	if rej.ChannelID == "" || rej.CreateRequestID == "" {
		return false, nil
	}
	return s.OrphanCreating(ctx, rej.ChannelID, rej.CreateRequestID)
}

// OrphanCreating actively rolls back a specific creating saga. It is used
// when Phase 3 CAS loses after the daemon may already hold the channel, so
// callers do not have to wait for reconcile's create timeout.
func (s *Service) OrphanCreating(
	ctx context.Context,
	channelID channel.ID,
	createRequestID placement.CreateRequestID,
) (bool, error) {
	if channelID == "" || createRequestID == "" {
		return false, nil
	}
	now := s.now().UnixMilli()
	ok, err := s.store.CASOrphanCreating(ctx, channelID, createRequestID, now)
	if err != nil {
		return false, err
	}
	for _, kind := range []SagaKind{SagaKindBootstrapReserve, SagaKindReclaimReserve} {
		saga, found, err := s.store.SagaForCreateRequest(ctx, kind, channelID, createRequestID)
		if err != nil {
			return false, err
		}
		if found {
			if err := s.store.AbandonSaga(ctx, saga.SagaID, "orphaned", now); err != nil {
				return false, err
			}
		}
	}
	return ok, nil
}

// OrphanCreatingPlacementTx is the transaction-scoped placement-state CAS
// used by gateway rollback intents. It intentionally does not touch placement
// sagas; the caller owns the companion rollback saga in the same transaction.
func (s *Service) OrphanCreatingPlacementTx(
	ctx context.Context,
	tx *sql.Tx,
	channelID channel.ID,
	createRequestID placement.CreateRequestID,
	nowMs int64,
) (bool, error) {
	if tx == nil {
		return false, errors.New("placements: transaction required")
	}
	if channelID == "" || createRequestID == "" {
		return false, nil
	}
	return s.store.CASOrphanCreatingTx(ctx, tx, channelID, createRequestID, nowMs)
}

// ActiveOrCreatingCountsByDaemon returns load counts owned by each daemon.
func (s *Service) ActiveOrCreatingCountsByDaemon(ctx context.Context) (map[placement.DaemonID]int, error) {
	counts := map[placement.DaemonID]int{}
	for _, state := range []placement.State{placement.StateActive, placement.StateCreating} {
		placements, err := s.store.ListByState(ctx, state)
		if err != nil {
			return nil, err
		}
		for _, p := range placements {
			counts[p.DaemonID]++
		}
	}
	return counts, nil
}

// Heartbeat refreshes last_heartbeat_at on a control.heartbeat
// frame. Used by daemonbus dispatch path.
func (s *Service) Heartbeat(ctx context.Context, channelID channel.ID, daemonID placement.DaemonID) error {
	return s.store.Heartbeat(ctx, channelID, daemonID, s.now().UnixMilli())
}

// ObserveHeartbeat compares the daemon-held channel fencing tuples
// against the placement table and returns the spec placement_diff closed
// set. Only exact active-owner matches refresh last_heartbeat_at: a stale
// owner, wrong epoch, orphan, or missing directory must not extend
// liveness while the server is trying to reclaim or unload it.
func (s *Service) ObserveHeartbeat(
	ctx context.Context,
	daemonID placement.DaemonID,
	held []placement.HeartbeatHeldChannel,
) ([]placement.PlacementDiff, error) {
	out := make([]placement.PlacementDiff, 0, len(held))
	for _, h := range held {
		diff, err := s.placementDiffForHeld(ctx, daemonID, h)
		if err != nil {
			return nil, err
		}
		if diff.Action == placement.PlacementDiffActionOK {
			if err := s.Heartbeat(ctx, h.ChannelID, daemonID); err != nil {
				return nil, err
			}
		}
		out = append(out, diff)
	}
	return out, nil
}

func (s *Service) placementDiffForHeld(
	ctx context.Context,
	daemonID placement.DaemonID,
	held placement.HeartbeatHeldChannel,
) (placement.PlacementDiff, error) {
	p, ok, err := s.store.Get(ctx, held.ChannelID)
	if err != nil {
		return placement.PlacementDiff{}, err
	}
	if !ok {
		return placement.PlacementDiff{
			ChannelID:   held.ChannelID,
			ServerState: placement.PlacementDiffStateUnknown,
			Action:      placement.PlacementDiffActionDirectoryMissing,
		}, nil
	}
	owner := p.DaemonID
	diff := placement.PlacementDiff{
		ChannelID:         held.ChannelID,
		ServerState:       heartbeatState(p.State),
		ServerOwnerEpoch:  p.OwnerEpoch,
		ServerOwnerDaemon: &owner,
	}
	switch {
	case p.State == placement.StateActive &&
		p.DaemonID == daemonID &&
		p.OwnerEpoch == held.OwnerEpoch &&
		p.FencingToken == held.FencingToken:
		diff.Action = placement.PlacementDiffActionOK
	case p.State == placement.StateActive && p.DaemonID != daemonID:
		diff.Action = placement.PlacementDiffActionReclaimPending
	case p.State == placement.StateOrphan || p.State == placement.StateStale:
		diff.Action = placement.PlacementDiffActionReclaimPending
	default:
		diff.Action = placement.PlacementDiffActionReclaimPending
	}
	return diff, nil
}

func heartbeatState(state placement.State) placement.PlacementDiffState {
	switch state {
	case placement.StateActive:
		return placement.PlacementDiffStateActive
	case placement.StateOrphan:
		return placement.PlacementDiffStateOrphan
	case placement.StateStale:
		return placement.PlacementDiffStateStale
	default:
		return placement.PlacementDiffStateUnknown
	}
}

// ValidatePushFencing checks a daemon-sent viewsync.push before the
// server applies it to viewcache. Any mismatch maps to the single
// mux_owner_epoch_stale reject reason at the daemonbus layer.
func (s *Service) ValidatePushFencing(
	ctx context.Context,
	channelID channel.ID,
	daemonID placement.DaemonID,
	ownerEpoch placement.OwnerEpoch,
	fencingToken placement.FencingToken,
) (bool, error) {
	p, ok, err := s.store.Get(ctx, channelID)
	if err != nil {
		return false, err
	}
	if !ok || p.DaemonID != daemonID {
		return false, nil
	}
	switch p.State {
	case placement.StateActive:
		return p.OwnerEpoch == ownerEpoch && p.FencingToken == fencingToken, nil
	case placement.StateCreating:
		if p.OwnerEpoch != ownerEpoch {
			return false, nil
		}
		if p.FencingToken == "" {
			return fencingToken != "", nil
		}
		return p.FencingToken == fencingToken, nil
	default:
		return false, nil
	}
}

// AcceptHeldChannel runs the cold-start held-channel report CAS. daemonID
// MUST be the WS-authenticated owner identifier (Connection.DaemonID)
// — the SQL CAS pins it into the WHERE so a different daemon presenting
// the same epoch/token cannot hijack ownership (FIX-T4 invariant).
func (s *Service) AcceptHeldChannel(
	ctx context.Context,
	channelID channel.ID,
	daemonID placement.DaemonID,
	req placement.HeldChannel,
	newConnectionEpoch placement.ConnectionEpoch,
) (bool, string, error) {
	return s.store.AcceptHeldChannel(ctx, channelID, daemonID, req, newConnectionEpoch, s.now().UnixMilli())
}

// ReserveReclaim starts server-initiated reclaim Phase 1.
func (s *Service) ReserveReclaim(
	ctx context.Context,
	channelID channel.ID,
	candidate placement.DaemonID,
	connectionEpoch placement.ConnectionEpoch,
) (placement.Placement, placement.DaemonReclaimRequest, bool, error) {
	prev, ok, err := s.store.Get(ctx, channelID)
	if err != nil {
		return placement.Placement{}, placement.DaemonReclaimRequest{}, false, err
	}
	if !ok {
		return placement.Placement{}, placement.DaemonReclaimRequest{}, false, nil
	}
	createReqID := placement.CreateRequestID(uuid.NewString())
	now := s.now().UnixMilli()
	out, ok, err := s.store.ReserveReclaimWithSaga(ctx, channelID, candidate, connectionEpoch, createReqID, now, StartSagaInput{
		ChannelID:             channelID,
		CreateRequestID:       createReqID,
		OwnerEpoch:            prev.OwnerEpoch + 1,
		DaemonID:              candidate,
		DaemonConnectionEpoch: connectionEpoch,
		SagaKind:              SagaKindReclaimReserve,
		Phase:                 SagaPhaseSent,
		SentAt:                now,
		ExpectedAckFrameKind:  string(kerneldaemonbus.FrameTypeControlReclaimAccepted),
		NowMs:                 now,
	})
	if err != nil || !ok {
		return placement.Placement{}, placement.DaemonReclaimRequest{}, ok, err
	}
	prevOwner := prev.DaemonID
	req := placement.DaemonReclaimRequest{
		ChannelID:           out.ChannelID,
		CreateRequestID:     out.CreateRequestID,
		NewOwnerEpoch:       out.OwnerEpoch,
		PreviousOwnerDaemon: &prevOwner,
		PreviousState:       reclaimOrigin(prev.State),
	}
	return out, req, true, nil
}

// ActivateReclaim completes server-initiated reclaim Phase 3.
func (s *Service) ActivateReclaim(
	ctx context.Context,
	ack placement.ReclaimAccepted,
	daemonID placement.DaemonID,
	newConnectionEpoch placement.ConnectionEpoch,
) (bool, error) {
	cur, ok, err := s.store.Get(ctx, ack.ChannelID)
	if err != nil {
		return false, err
	}
	if !ok {
		return false, fmt.Errorf("placements: activate reclaim unknown channel %q", ack.ChannelID)
	}
	if cur.State != placement.StateCreating ||
		cur.CreateRequestID != ack.CreateRequestID ||
		cur.OwnerEpoch != ack.NewOwnerEpoch {
		return false, nil
	}
	now := s.now().UnixMilli()
	activated, err := s.store.CASActivateReclaim(ctx, ack, daemonID, newConnectionEpoch, now)
	if err != nil {
		return false, err
	}
	if activated {
		if saga, found, err := s.store.SagaForCreateRequest(ctx, SagaKindReclaimReserve, ack.ChannelID, ack.CreateRequestID); err != nil {
			return false, err
		} else if found {
			if err := s.store.CompleteSaga(ctx, saga.SagaID, "accepted", now); err != nil {
				return false, err
			}
		}
	}
	return activated, nil
}

func reclaimOrigin(state placement.State) placement.ReclaimOriginState {
	switch state {
	case placement.StateOrphan:
		return placement.ReclaimOriginOrphan
	case placement.StateStale:
		return placement.ReclaimOriginStale
	default:
		return placement.ReclaimOriginActiveLost
	}
}

// Get is a thin pass-through.
func (s *Service) Get(ctx context.Context, channelID channel.ID) (placement.Placement, bool, error) {
	return s.store.Get(ctx, channelID)
}

// ListByDaemon is a thin pass-through.
func (s *Service) ListByDaemon(ctx context.Context, daemonID placement.DaemonID) ([]placement.Placement, error) {
	return s.store.ListByDaemon(ctx, daemonID)
}

// MarkDaemonStale transitions every active placement owned by daemonID to
// stale immediately and invokes the reclaim hook for each affected channel.
// This is the deterministic handoff path used when a daemon connection
// leaves intentionally or the server is shutting down; heartbeat timeout is
// only a fallback for missed disconnect signals.
func (s *Service) MarkDaemonStale(ctx context.Context, daemonID placement.DaemonID, reason string) ([]placement.Placement, error) {
	if daemonID == "" {
		return nil, nil
	}
	rows, err := s.store.ListByDaemon(ctx, daemonID)
	if err != nil {
		return nil, err
	}
	nowMs := s.now().UnixMilli()
	var changed []placement.Placement
	var errs []error
	for _, p := range rows {
		if p.State != placement.StateActive {
			continue
		}
		if err := s.store.MarkStale(ctx, p.ChannelID, nowMs); err != nil {
			errs = append(errs, err)
			continue
		}
		p.State = placement.StateStale
		p.EnteredStateAt = nowMs
		changed = append(changed, p)
		s.log.Info().
			Str("event", "placement.transition").
			Str("request_id", requestctx.RequestID(ctx)).
			Str("channel_id", string(p.ChannelID)).
			Str("daemon_id", string(daemonID)).
			Str("from_state", string(placement.StateActive)).
			Str("to_state", string(placement.StateStale)).
			Str("reason", reason).
			Msg("placement marked stale after daemon left")
		if err := s.triggerReclaim(ctx, p); err != nil {
			errs = append(errs, fmt.Errorf("placements: trigger reclaim %s: %w", p.ChannelID, err))
		}
	}
	return changed, errors.Join(errs...)
}

// ResolveDaemonForChannel returns the active daemon currently owning
// channelID. ok=false means the channel has no active placement.
func (s *Service) ResolveDaemonForChannel(ctx context.Context, channelID channel.ID) (placement.DaemonID, bool, error) {
	return s.store.ResolveDaemonForChannel(ctx, channelID)
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
// (always) + active→stale (only after row-local grace), then invokes
// the optional reclaim handler for orphan/stale rows that are eligible.
func (s *Service) ReconcileOnce(ctx context.Context) error {
	s.MarkStartedAt()
	now := s.now()
	cutoffCreate := now.Add(-s.cfg.CreateTimeout).UnixMilli()
	cutoffHeart := now.Add(-s.cfg.HeartbeatTimeout).UnixMilli()
	cutoffGrace := now.Add(-s.cfg.GracePeriod).UnixMilli()

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
		s.log.Info().
			Str("event", "placement.transition").
			Str("request_id", requestctx.RequestID(ctx)).
			Str("channel_id", string(p.ChannelID)).
			Str("daemon_id", string(p.DaemonID)).
			Str("from_state", string(placement.StateCreating)).
			Str("to_state", string(placement.StateOrphan)).
			Str("reason", "create_timeout").
			Msg("placement transitioned")
		for _, kind := range []SagaKind{SagaKindBootstrapReserve, SagaKindReclaimReserve} {
			saga, found, err := s.store.SagaForCreateRequest(ctx, kind, p.ChannelID, p.CreateRequestID)
			if err != nil {
				return err
			}
			if found {
				if err := s.store.AbandonSaga(ctx, saga.SagaID, "create_timeout", now.UnixMilli()); err != nil {
					return err
				}
			}
		}
	}
	if err := s.store.AbandonTimedOutSagas(ctx, cutoffCreate, "phase_timeout", now.UnixMilli()); err != nil {
		return err
	}

	// 2) active + last_heartbeat_at < cutoffHeart → stale only after
	//    this placement has spent GracePeriod in its current state.
	active, err := s.store.ListByState(ctx, placement.StateActive)
	if err != nil {
		return err
	}
	for _, p := range active {
		enteredStateAt := p.EnteredStateAt
		if enteredStateAt == 0 {
			enteredStateAt = p.ActivatedAt
		}
		if enteredStateAt > cutoffGrace {
			continue
		}
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
		s.log.Info().
			Str("event", "placement.transition").
			Str("request_id", requestctx.RequestID(ctx)).
			Str("channel_id", string(p.ChannelID)).
			Str("daemon_id", string(p.DaemonID)).
			Str("from_state", string(placement.StateActive)).
			Str("to_state", string(placement.StateStale)).
			Str("reason", "heartbeat_timeout").
			Msg("placement transitioned")
	}

	if err := s.triggerEligibleReclaims(ctx, placement.StateOrphan, cutoffGrace); err != nil {
		return err
	}
	if err := s.triggerEligibleReclaims(ctx, placement.StateStale, now.UnixMilli()+1); err != nil {
		return err
	}
	return nil
}

func (s *Service) triggerEligibleReclaims(ctx context.Context, state placement.State, cutoffMs int64) error {
	rows, err := s.store.ListByState(ctx, state)
	if err != nil {
		return err
	}
	for _, p := range rows {
		if p.EnteredStateAt > cutoffMs {
			continue
		}
		if err := s.triggerReclaim(ctx, p); err != nil {
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
				s.log.Warn().Err(err).
					Str("event", "placements.reconcile_failed").
					Msg("placements reconcile failed")
			}
		}
	}
}
