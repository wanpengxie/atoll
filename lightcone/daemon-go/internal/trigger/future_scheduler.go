package trigger

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/coagent-ai/daemon-go/pkg/v4types"
)

// DefaultFutureSchedulerPeriod is the L1 §5.3 / L2 §1.4.10 baseline
// scan cadence — once per second the scheduler walks every
// not_before<=now row that has not been delivered or expired. Tests
// inject sub-millisecond periods via SchedulerConfig.Period.
const DefaultFutureSchedulerPeriod = 1 * time.Second

// DefaultFutureSchedulerBatch caps how many candidate rows one Tick
// processes. Protocol baseline picks 256 to keep a single Tick's
// transactional footprint bounded while still draining ordinary
// backlog in one wake-up. Tests can shrink/grow via SchedulerConfig.Batch.
const DefaultFutureSchedulerBatch = 256

// FutureSchedulerUpstream is the dispatch-path "upstream" sentinel
// the scheduler hands to Gateway.Dispatch. L1 §5.3 says future-message
// triggers MUST NOT filter the original sender (the scheduler itself
// is the upstream, not the sender). We pass an empty string, which
// the Gateway interprets as "self-trigger filter disabled".
const FutureSchedulerUpstream = ""

// FutureSchedulerExpiredError is the sentinel `last_error` value the
// scheduler stamps on rows whose expires_at < now at scan time. The
// L1 §5.4 envelope-trailer rule pairs it with delivery_failed_at to
// give downstream consumers a single status to filter on. Keeping the
// constant exported lets callers grep for it in audit queries.
const FutureSchedulerExpiredError = "expired"

// GatewayDispatcher is the subset of Gateway the scheduler calls. It's
// a 1-method interface so tests can inject a spy without spinning up
// an ActorLookup. The production wiring passes *Gateway directly —
// Gateway.Dispatch implements this signature exactly.
type GatewayDispatcher interface {
	Dispatch(ctx context.Context, env *v4types.Envelope, upstream string) ([]string, error)
}

// FutureScheduler scans `messages` for future-message rows
// (`not_before <= now AND delivered_at IS NULL`) and injects each
// into the trigger gateway per L1 §5.3.
//
// One scheduler per channel sqlite. The daemon owns N schedulers
// when it hosts N channels. They share the protocol baseline cadence
// (DefaultFutureSchedulerPeriod) unless channel-specific override is
// wired via SchedulerConfig.
//
// Crash recovery is built-in: scheduler state lives entirely in the
// channel sqlite (messages.delivered_at / delivery_failed_at flags).
// A restarted daemon simply re-runs Tick and picks up where it left
// off — there is no in-memory queue to lose.
type FutureScheduler struct {
	db        *sql.DB
	dispatch  GatewayDispatcher
	channelID string
	cfg       SchedulerConfig
}

// SchedulerConfig tunes the scheduler. Zero value yields baseline
// behaviour (1s period, 256 batch, wall-clock now, slog.Default).
type SchedulerConfig struct {
	// Period is the ticker interval for Run; defaults to
	// DefaultFutureSchedulerPeriod (1 second).
	Period time.Duration

	// Batch caps the number of rows one Tick processes. Defaults to
	// DefaultFutureSchedulerBatch (256). Tests use 1-2 to drive
	// deterministic ordering.
	Batch int

	// Now returns the current wall-clock time in **milliseconds**
	// (matches harness.Clock convention; envelope `not_before` /
	// `expires_at` are stored in the same unit by callers writing
	// future messages). Tests inject a fixed-clock pointer.
	Now func() int64

	// Logger receives structured scheduler events. Defaults to
	// slog.Default(); test callers can pass a discard logger.
	Logger *slog.Logger
}

// NewFutureScheduler constructs a scheduler. Returns an error on
// missing inputs so misuse surfaces at startup rather than as a
// silent no-op tick.
func NewFutureScheduler(
	db *sql.DB,
	dispatch GatewayDispatcher,
	channelID string,
	cfg SchedulerConfig,
) (*FutureScheduler, error) {
	if db == nil {
		return nil, errors.New("future_scheduler: db is nil")
	}
	if dispatch == nil {
		return nil, errors.New("future_scheduler: dispatcher is nil")
	}
	if channelID == "" {
		return nil, errors.New("future_scheduler: channel_id is required")
	}
	if cfg.Period <= 0 {
		cfg.Period = DefaultFutureSchedulerPeriod
	}
	if cfg.Batch <= 0 {
		cfg.Batch = DefaultFutureSchedulerBatch
	}
	if cfg.Now == nil {
		cfg.Now = func() int64 { return time.Now().UnixMilli() }
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	return &FutureScheduler{
		db:        db,
		dispatch:  dispatch,
		channelID: channelID,
		cfg:       cfg,
	}, nil
}

// Run drives the scheduler loop until ctx is cancelled. Each
// cfg.Period tick fires exactly one Tick; Tick errors are logged but
// do not stop the loop (a transient sqlite error on one tick should
// not silence the scheduler).
//
// First tick fires immediately so daemon startup doesn't wait
// `Period` seconds before noticing pending backlog (matches the
// supervisor.Run pattern).
func (s *FutureScheduler) Run(ctx context.Context) error {
	ticker := time.NewTicker(s.cfg.Period)
	defer ticker.Stop()

	if err := s.Tick(ctx); err != nil && !errors.Is(err, context.Canceled) {
		s.cfg.Logger.Error("future_scheduler.tick.error",
			"channel_id", s.channelID, "err", err.Error())
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if err := s.Tick(ctx); err != nil && !errors.Is(err, context.Canceled) {
				s.cfg.Logger.Error("future_scheduler.tick.error",
					"channel_id", s.channelID, "err", err.Error())
			}
		}
	}
}

// Tick runs one scan + dispatch cycle. Idempotent — duplicate calls
// produce duplicate Dispatcher invocations only for rows that were
// **newly** eligible since the previous call. Rows already marked
// `delivered_at` / `delivery_failed_at` are silently skipped by the
// scan SQL.
//
// Per-row processing:
//
//  1. expires_at < now → CAS UPDATE delivery_failed_at + last_error;
//     do not dispatch.
//  2. else → Dispatch(env, upstream="" per L1 §5.3); on success CAS
//     UPDATE delivered_at.
//
// Dispatch failure leaves the row pending — next Tick retries. The
// CAS guards (WHERE delivered_at IS NULL / WHERE delivery_failed_at
// IS NULL) make duplicate scheduler invocations safe.
func (s *FutureScheduler) Tick(ctx context.Context) error {
	now := s.cfg.Now()
	rows, err := s.scanReady(ctx, now)
	if err != nil {
		return fmt.Errorf("future_scheduler: scan: %w", err)
	}
	for i := range rows {
		row := &rows[i]
		if err := s.processRow(ctx, row, now); err != nil {
			// Log and continue — one bad row doesn't kill the Tick.
			s.cfg.Logger.Error("future_scheduler.row.error",
				"channel_id", s.channelID, "id", row.ID, "err", err.Error())
		}
	}
	return nil
}

// readyRow is the minimal projection one scheduler row needs. Only
// the columns the gateway / CAS update consume are loaded; payload
// is intentionally NOT pulled so a 1 MB attachment doesn't pollute
// scheduler RAM on each scan.
type readyRow struct {
	Seq           int64
	ID            string
	TS            int64
	ChannelID     string
	SenderKind    string
	SenderID      string
	Kind          string
	Type          string
	ParentID      sql.NullString
	CorrelationID sql.NullString
	Visibility    string
	Audience      string // JSON encoded
	NotBefore     sql.NullInt64
	ExpiresAt     sql.NullInt64
}

// scanReady executes the L1 §5.3 / L2 §1.4.1 scan: every future
// message whose not_before has elapsed and which has not yet been
// delivered or marked expired.
//
// The SQL uses the partial index `ix_messages_not_before` (created in
// L2 §1.4.1 with `WHERE not_before IS NOT NULL`), so the scan is
// cheap even with a large messages table.
func (s *FutureScheduler) scanReady(ctx context.Context, now int64) ([]readyRow, error) {
	rows, err := s.db.QueryContext(ctx, scanReadySQL, now, s.cfg.Batch)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	var out []readyRow
	for rows.Next() {
		var r readyRow
		if err := rows.Scan(
			&r.Seq, &r.ID, &r.TS, &r.ChannelID,
			&r.SenderKind, &r.SenderID,
			&r.Kind, &r.Type,
			&r.ParentID, &r.CorrelationID,
			&r.Visibility, &r.Audience,
			&r.NotBefore, &r.ExpiresAt,
		); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

// scanReadySQL projects exactly the columns readyRow needs and orders
// by seq ASC so the trigger order is deterministic (matches store
// arrival order; same convention as supervisor.BacklogScan).
const scanReadySQL = `
SELECT seq, id, ts, channel_id,
       sender_kind, sender_id,
       kind, type,
       parent_id, correlation_id,
       visibility, audience,
       not_before, expires_at
  FROM messages
 WHERE not_before IS NOT NULL
   AND not_before <= ?
   AND delivered_at IS NULL
   AND delivery_failed_at IS NULL
 ORDER BY seq ASC
 LIMIT ?
`

// processRow either marks the row expired or dispatches it through
// the gateway. expires_at < now ALWAYS wins over dispatch — L1 §5.3
// "scheduler 投递时若 expires_at < now，跳过 trigger".
//
// Concurrent-tick safety (FIX-6 §2 / codex t92):
//
//   - Pre-fix: Dispatch happened first, then markDelivered. Two
//     schedulers ticking the same due row both Dispatched, then raced
//     on the idempotent markDelivered — both side-effects fired.
//   - Post-fix: a CAS UPDATE claims the row by stamping delivered_at
//     BEFORE Dispatch runs. Only the CAS winner proceeds; the loser
//     sees rows_affected == 0 and yields. On Dispatch failure the
//     winner releases the claim (delivered_at → NULL, guarded by the
//     same timestamp) so a later tick retries.
//
// `delivered_at` thus widens semantically from "dispatch finished" to
// "claim acquired AND dispatch tentatively running"; downstream readers
// (scanReadySQL `delivered_at IS NULL` filter, viewsync replay) tolerate
// that window. SQLite serializes writers via WAL + busy_timeout so the
// CAS is atomic against concurrent UPDATEs to the same row.
func (s *FutureScheduler) processRow(ctx context.Context, r *readyRow, now int64) error {
	if r.ExpiresAt.Valid && r.ExpiresAt.Int64 < now {
		if err := s.markExpired(ctx, r.ID, now); err != nil {
			return fmt.Errorf("mark expired: %w", err)
		}
		s.cfg.Logger.Info("future_scheduler.expired",
			"channel_id", s.channelID, "id", r.ID, "expires_at", r.ExpiresAt.Int64, "now", now)
		return nil
	}

	// Claim the row before Dispatch. CAS-fail (0 rows affected) → loser
	// of the race; another scheduler already owns this row.
	claimed, err := s.claim(ctx, r.ID, now)
	if err != nil {
		return fmt.Errorf("claim: %w", err)
	}
	if !claimed {
		s.cfg.Logger.Debug("future_scheduler.claim.lost",
			"channel_id", s.channelID, "id", r.ID, "now", now)
		return nil
	}

	env, err := buildEnvelope(r)
	if err != nil {
		// Release the claim so a later tick can pick the row up again.
		if relErr := s.releaseClaim(ctx, r.ID, now); relErr != nil {
			s.cfg.Logger.Warn("future_scheduler.release_claim.error",
				"channel_id", s.channelID, "id", r.ID, "err", relErr.Error())
		}
		return fmt.Errorf("build envelope: %w", err)
	}

	triggered, derr := s.dispatch.Dispatch(ctx, env, FutureSchedulerUpstream)
	if derr != nil {
		// Dispatch failed — release the claim so the next Tick retries.
		if relErr := s.releaseClaim(ctx, r.ID, now); relErr != nil {
			s.cfg.Logger.Warn("future_scheduler.release_claim.error",
				"channel_id", s.channelID, "id", r.ID, "err", relErr.Error())
		}
		return fmt.Errorf("dispatch: %w", derr)
	}

	s.cfg.Logger.Info("future_scheduler.delivered",
		"channel_id", s.channelID, "id", r.ID,
		"type", r.Type, "triggered_count", len(triggered))
	return nil
}

// claim runs the CAS-UPDATE that turns a pending row into a tentatively
// delivered one. Returns (true, nil) when this scheduler instance wins
// the race and SHOULD Dispatch; (false, nil) when another instance has
// already claimed it.
//
// The CAS is guarded on BOTH delivered_at IS NULL AND delivery_failed_at
// IS NULL so an already-expired row (markExpired path) does not get
// double-counted.
func (s *FutureScheduler) claim(ctx context.Context, id string, now int64) (bool, error) {
	res, err := s.db.ExecContext(ctx,
		`UPDATE messages
		    SET delivered_at = ?,
		        attempts     = attempts + 1
		  WHERE id = ?
		    AND delivered_at IS NULL
		    AND delivery_failed_at IS NULL`,
		now, id,
	)
	if err != nil {
		return false, err
	}
	affected, _ := res.RowsAffected()
	return affected == 1, nil
}

// releaseClaim reverts a claim made by this scheduler so a later tick
// can retry. The WHERE clause matches both `id` and the exact
// `delivered_at` timestamp we wrote — that prevents us from clobbering
// a claim that ANOTHER scheduler has since acquired (e.g. after a long
// dispatch failure path).
func (s *FutureScheduler) releaseClaim(ctx context.Context, id string, claimedAt int64) error {
	_, err := s.db.ExecContext(ctx,
		`UPDATE messages
		    SET delivered_at = NULL
		  WHERE id = ? AND delivered_at = ?`,
		id, claimedAt,
	)
	return err
}

// buildEnvelope materialises a v4types.Envelope from a readyRow. The
// audience JSON column is parsed back into a []string; the rest of
// the fields are straight copies (with NullString/NullInt64 unwrap).
//
// Payload is left empty — the gateway does not inspect payload in the
// protocol baseline, and skipping the column keeps scheduler scan
// memory bounded. If a future SubscriptionFilter wants payload-level
// matching the scan SQL + this builder can be widened in one place.
func buildEnvelope(r *readyRow) (*v4types.Envelope, error) {
	var audience []string
	if err := json.Unmarshal([]byte(r.Audience), &audience); err != nil {
		return nil, fmt.Errorf("audience json: %w", err)
	}
	env := &v4types.Envelope{
		ID:         r.ID,
		TS:         r.TS,
		ChannelID:  r.ChannelID,
		Sender:     v4types.Sender{Kind: v4types.SenderKind(r.SenderKind), ID: r.SenderID},
		Kind:       v4types.Kind(r.Kind),
		Type:       r.Type,
		Visibility: v4types.Visibility(r.Visibility),
		Audience:   audience,
	}
	if r.ParentID.Valid {
		env.ParentID = r.ParentID.String
	}
	if r.CorrelationID.Valid {
		env.CorrelationID = r.CorrelationID.String
	}
	if r.NotBefore.Valid {
		v := r.NotBefore.Int64
		env.NotBefore = &v
	}
	if r.ExpiresAt.Valid {
		v := r.ExpiresAt.Int64
		env.ExpiresAt = &v
	}
	return env, nil
}

// markExpired writes the L0 §2.5 delivery_failed_at + last_error
// pair atomically. Same CAS protection as claim/releaseClaim.
func (s *FutureScheduler) markExpired(ctx context.Context, id string, now int64) error {
	res, err := s.db.ExecContext(ctx,
		`UPDATE messages
		    SET delivery_failed_at = ?,
		        last_error         = ?
		  WHERE id = ? AND delivery_failed_at IS NULL`,
		now, FutureSchedulerExpiredError, id,
	)
	if err != nil {
		return err
	}
	affected, _ := res.RowsAffected()
	if affected == 0 {
		s.cfg.Logger.Debug("future_scheduler.expired.cas_miss",
			"channel_id", s.channelID, "id", id)
	}
	return nil
}
