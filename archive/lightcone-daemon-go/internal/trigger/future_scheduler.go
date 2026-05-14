package trigger

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
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

// FutureSchedulerMaxAttemptsError is the sentinel `last_error` value the
// scheduler stamps on rows that have been claimed more times than the
// configured MaxAttempts. The row is moved to the terminal failed state
// so a poison message stops chewing CPU.
const FutureSchedulerMaxAttemptsError = "max_attempts_exceeded"

// DefaultFutureSchedulerClaimTTL is the staleness threshold after which
// scanReady reclaims a row whose claim_owner never released the row —
// the typical case is a daemon SIGKILL between claim() and Dispatch
// completion. 60s is long enough that an honest in-flight Dispatch
// (e.g. waiting on a downstream HTTP timeout) will not be ripped out
// from under the holder.
const DefaultFutureSchedulerClaimTTL = 60 * time.Second

// DefaultFutureSchedulerMaxAttempts caps how many times the scheduler
// will re-Dispatch a single row before marking it permanently failed.
// 10 matches the spec example in fix-spec.md R2-FIX-3 and gives a
// flapping downstream ~10 retries (≈10 ticks ≈ 10 s with the 1 s
// baseline period) before the row drops out of the scheduler hot loop.
const DefaultFutureSchedulerMaxAttempts = 10

// GatewayDispatcher is the subset of Gateway the scheduler calls. It's
// a 1-method interface so tests can inject a spy without spinning up
// an ActorLookup. The production wiring passes *Gateway directly —
// Gateway.Dispatch implements this signature exactly.
type GatewayDispatcher interface {
	Dispatch(ctx context.Context, env *v4types.Envelope, upstream string) ([]string, error)
}

// FutureScheduler scans `messages` for future-message rows
// (`not_before <= now AND delivered_at IS NULL AND
// delivery_failed_at IS NULL`) and injects each into the trigger
// gateway per L1 §5.3.
//
// One scheduler per channel sqlite. The daemon owns N schedulers
// when it hosts N channels. They share the protocol baseline cadence
// (DefaultFutureSchedulerPeriod) unless channel-specific override is
// wired via SchedulerConfig.
//
// In-flight claim vs. terminal state (R2-FIX-3):
//
//   - claim_owner / claimed_at — the scheduler instance that is
//     currently dispatching this row and when it took the claim. These
//     are NULL whenever the row is dispatchable. A claim is the
//     scheduler's "I am working on this" flag; it does NOT advance the
//     row to a terminal state.
//   - delivered_at — written exactly once, when Dispatch returns
//     successfully. This is the L0 §2.5 terminal "delivered" marker.
//   - delivery_failed_at + last_error — written exactly once, when the
//     row reaches a terminal failure (expires_at < now at scan time
//     OR attempts > MaxAttempts).
//
// Crash-recovery contract: state lives entirely in the channel sqlite,
// so a restarted daemon simply re-runs Tick. The scan SQL admits a row
// when `claim_owner IS NULL` OR `claimed_at < now - claim_ttl_ms`, so
// a ghost claim left behind by a SIGKILL between claim() and Dispatch
// completion is reclaimed once the TTL elapses. delivered_at remains
// strictly terminal — at-least-once delivery, no silent-loss window.
type FutureScheduler struct {
	db           *sql.DB
	dispatch     GatewayDispatcher
	channelID    string
	cfg          SchedulerConfig
	ownerID      string
	claimTTLMs   int64
	maxAttempts  int
}

// SchedulerConfig tunes the scheduler. Zero value yields baseline
// behaviour (1s period, 256 batch, wall-clock now, slog.Default,
// random 16-hex OwnerID, 60s ClaimTTL, 10 MaxAttempts).
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

	// OwnerID identifies this scheduler instance in the messages
	// `claim_owner` column. Multiple schedulers sharing a sqlite (e.g.
	// during a daemon hot-restart or HA failover) MUST have distinct
	// owner ids so claim/release CAS clauses do not clobber each
	// other. Defaults to a random 16-hex string generated at
	// construction; production callers may pass the daemon instance
	// id for easier debugging.
	OwnerID string

	// ClaimTTL is the staleness threshold after which scanReady
	// reclaims a row whose claim_owner is set but claimed_at is older
	// than now - ClaimTTL. Defaults to DefaultFutureSchedulerClaimTTL
	// (60s). Tests use sub-second TTLs to exercise stale-claim paths.
	ClaimTTL time.Duration

	// MaxAttempts caps how many claim+Dispatch cycles one row receives
	// before scheduler stamps delivery_failed_at +
	// FutureSchedulerMaxAttemptsError and stops re-claiming. Defaults
	// to DefaultFutureSchedulerMaxAttempts (10). Tests use 1-3 to
	// exercise the cap quickly.
	MaxAttempts int
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
	if cfg.ClaimTTL <= 0 {
		cfg.ClaimTTL = DefaultFutureSchedulerClaimTTL
	}
	if cfg.MaxAttempts <= 0 {
		cfg.MaxAttempts = DefaultFutureSchedulerMaxAttempts
	}
	ownerID := cfg.OwnerID
	if ownerID == "" {
		var err error
		ownerID, err = generateOwnerID()
		if err != nil {
			return nil, fmt.Errorf("future_scheduler: generate owner id: %w", err)
		}
	}
	return &FutureScheduler{
		db:          db,
		dispatch:    dispatch,
		channelID:   channelID,
		cfg:         cfg,
		ownerID:     ownerID,
		claimTTLMs:  cfg.ClaimTTL.Milliseconds(),
		maxAttempts: cfg.MaxAttempts,
	}, nil
}

// generateOwnerID returns a random 16-hex (8-byte) string used as the
// default claim_owner value when SchedulerConfig.OwnerID is empty.
// crypto/rand is overkill collision-wise but guarantees uniqueness
// across two schedulers in the same process without any coordination.
func generateOwnerID() (string, error) {
	var buf [8]byte
	if _, err := rand.Read(buf[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(buf[:]), nil
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
// scan SQL, and rows still under a fresh claim are skipped via the
// claim_owner / claimed_at columns.
//
// Per-row processing:
//
//  1. expires_at < now → CAS UPDATE delivery_failed_at + last_error;
//     do not dispatch.
//  2. else → claim() CAS sets claim_owner + claimed_at + bumps
//     attempts. CAS-loser yields. Post-claim, if attempts >
//     MaxAttempts the row is moved to terminal failure and skipped.
//  3. Dispatch(env, upstream="" per L1 §5.3). On success, markDelivered
//     CAS stamps delivered_at and clears the claim. On failure,
//     releaseClaim CAS clears the claim (so the next Tick retries) —
//     attempts stays at the bumped value so the cap is enforced.
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
// message whose not_before has elapsed, has not yet been delivered or
// marked expired, AND is either unclaimed or whose claim has gone
// stale past ClaimTTL.
//
// The SQL uses the partial index `ix_messages_not_before` (created in
// L2 §1.4.1 with `WHERE not_before IS NOT NULL`), so the scan is
// cheap even with a large messages table. The trailing claim_owner
// clause is evaluated per-row after the partial index narrows the set.
func (s *FutureScheduler) scanReady(ctx context.Context, now int64) ([]readyRow, error) {
	staleBefore := now - s.claimTTLMs
	rows, err := s.db.QueryContext(ctx, scanReadySQL, now, staleBefore, s.cfg.Batch)
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
//
// The `claim_owner IS NULL OR claimed_at < ?` predicate makes scan
// recover ghost claims left behind by a crashed daemon — without it
// any row that won a CAS-claim before SIGKILL would silently vanish
// from the scheduler's view forever.
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
   AND (claim_owner IS NULL OR claimed_at < ?)
 ORDER BY seq ASC
 LIMIT ?
`

// processRow either marks the row expired or dispatches it through
// the gateway. expires_at < now ALWAYS wins over dispatch — L1 §5.3
// "scheduler 投递时若 expires_at < now，跳过 trigger".
//
// Crash-safety contract (R2-FIX-3):
//
//   - claim() CAS sets claim_owner + claimed_at + bumps attempts. The
//     CAS predicate (delivered_at IS NULL AND delivery_failed_at IS
//     NULL AND (claim_owner IS NULL OR claimed_at < stale_before))
//     means a ghost claim left behind by SIGKILL is reclaimed once
//     ClaimTTL elapses. delivered_at is NEVER touched here, so it
//     stays a true terminal marker.
//   - On Dispatch success, markDelivered CAS sets delivered_at and
//     clears the claim atomically — the row reaches its real terminal
//     state in one statement.
//   - On Dispatch failure, releaseClaim clears the claim (so the next
//     Tick can retry). The release WHERE clause is owner-scoped
//     (claim_owner = s.ownerID) so a concurrent winner's claim cannot
//     be clobbered.
//   - If attempts > MaxAttempts after a claim, markPermanentlyFailed
//     stamps delivery_failed_at + FutureSchedulerMaxAttemptsError so
//     the row drops out of the scan set.
//
// SQLite serializes writers via WAL + busy_timeout so each CAS is
// atomic against concurrent UPDATEs to the same row.
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
	// of the race; another scheduler already owns this row (or it has
	// since reached a terminal state).
	attempts, claimed, err := s.claim(ctx, r.ID, now)
	if err != nil {
		return fmt.Errorf("claim: %w", err)
	}
	if !claimed {
		s.cfg.Logger.Debug("future_scheduler.claim.lost",
			"channel_id", s.channelID, "id", r.ID, "now", now)
		return nil
	}

	// Cap re-claims at MaxAttempts so a poison row drops out of the
	// scheduler hot loop. The check runs AFTER claim so a concurrent
	// winner does not get falsely "failed" by a loser racing here.
	if attempts > int64(s.maxAttempts) {
		if err := s.markPermanentlyFailed(ctx, r.ID, now, FutureSchedulerMaxAttemptsError); err != nil {
			s.cfg.Logger.Warn("future_scheduler.mark_failed.error",
				"channel_id", s.channelID, "id", r.ID, "err", err.Error())
			return fmt.Errorf("mark permanently failed: %w", err)
		}
		s.cfg.Logger.Warn("future_scheduler.max_attempts_exceeded",
			"channel_id", s.channelID, "id", r.ID,
			"attempts", attempts, "max_attempts", s.maxAttempts)
		return nil
	}

	env, err := buildEnvelope(r)
	if err != nil {
		// Release the claim so a later tick can pick the row up again.
		if relErr := s.releaseClaim(ctx, r.ID); relErr != nil {
			s.cfg.Logger.Warn("future_scheduler.release_claim.error",
				"channel_id", s.channelID, "id", r.ID, "err", relErr.Error())
		}
		return fmt.Errorf("build envelope: %w", err)
	}

	triggered, derr := s.dispatch.Dispatch(ctx, env, FutureSchedulerUpstream)
	if derr != nil {
		// Dispatch failed — release the claim so the next Tick retries.
		if relErr := s.releaseClaim(ctx, r.ID); relErr != nil {
			s.cfg.Logger.Warn("future_scheduler.release_claim.error",
				"channel_id", s.channelID, "id", r.ID, "err", relErr.Error())
		}
		return fmt.Errorf("dispatch: %w", derr)
	}

	if err := s.markDelivered(ctx, r.ID, now); err != nil {
		s.cfg.Logger.Warn("future_scheduler.mark_delivered.error",
			"channel_id", s.channelID, "id", r.ID, "err", err.Error())
		return fmt.Errorf("mark delivered: %w", err)
	}

	s.cfg.Logger.Info("future_scheduler.delivered",
		"channel_id", s.channelID, "id", r.ID,
		"type", r.Type, "triggered_count", len(triggered))
	return nil
}

// claim runs the CAS-UPDATE that takes the in-flight slot for one row.
// It sets claim_owner + claimed_at and atomically increments attempts.
// Returns (newAttempts, true, nil) when this scheduler instance wins
// the race; (0, false, nil) when another instance already holds a
// fresh claim or the row has reached a terminal state.
//
// The CAS predicate covers four cases at once:
//
//   - delivered_at IS NULL              — row not yet delivered
//   - delivery_failed_at IS NULL        — row not permanently failed
//   - claim_owner IS NULL               — no current holder
//   - OR claimed_at < now - ClaimTTL    — current claim has gone stale
//     (crashed daemon never released)
//
// delivered_at is NEVER mutated here — that's the whole point of the
// R2-FIX-3 split: the claim is just a "working on it" flag, and only
// markDelivered turns the row terminal.
func (s *FutureScheduler) claim(ctx context.Context, id string, now int64) (int64, bool, error) {
	staleBefore := now - s.claimTTLMs
	res, err := s.db.ExecContext(ctx,
		`UPDATE messages
		    SET claim_owner = ?,
		        claimed_at  = ?,
		        attempts    = attempts + 1
		  WHERE id = ?
		    AND delivered_at IS NULL
		    AND delivery_failed_at IS NULL
		    AND (claim_owner IS NULL OR claimed_at < ?)`,
		s.ownerID, now, id, staleBefore,
	)
	if err != nil {
		return 0, false, err
	}
	affected, _ := res.RowsAffected()
	if affected == 0 {
		return 0, false, nil
	}
	// Read back the new attempts value so processRow can enforce
	// MaxAttempts. The single-conn pool + WAL guarantees the SELECT
	// observes this transaction's UPDATE.
	var attempts int64
	if err := s.db.QueryRowContext(ctx,
		`SELECT attempts FROM messages WHERE id = ?`, id,
	).Scan(&attempts); err != nil {
		return 0, false, fmt.Errorf("read attempts: %w", err)
	}
	return attempts, true, nil
}

// releaseClaim drops the in-flight claim held by this scheduler so a
// later tick (or a different scheduler) can retry. The WHERE clause is
// **owner-scoped** — it only matches rows whose claim_owner equals
// this instance's ownerID. That guarantees a loser whose CAS clock
// happened to fall on the same wall-clock ms as the winner cannot
// accidentally release the winner's claim.
func (s *FutureScheduler) releaseClaim(ctx context.Context, id string) error {
	_, err := s.db.ExecContext(ctx,
		`UPDATE messages
		    SET claim_owner = NULL,
		        claimed_at  = NULL
		  WHERE id = ? AND claim_owner = ?`,
		id, s.ownerID,
	)
	return err
}

// markDelivered stamps the row's terminal "delivered" state and clears
// the claim columns in one atomic CAS. The owner-scoped WHERE clause
// is defensive: only the holder of the in-flight claim may move the
// row to terminal-delivered.
func (s *FutureScheduler) markDelivered(ctx context.Context, id string, now int64) error {
	res, err := s.db.ExecContext(ctx,
		`UPDATE messages
		    SET delivered_at = ?,
		        claim_owner  = NULL,
		        claimed_at   = NULL
		  WHERE id = ? AND claim_owner = ? AND delivered_at IS NULL`,
		now, id, s.ownerID,
	)
	if err != nil {
		return err
	}
	affected, _ := res.RowsAffected()
	if affected == 0 {
		// Either the claim was stolen via stale-TTL, or the row was
		// flipped terminal by another path. Log so audit can correlate
		// with the corresponding releaseClaim/markPermanentlyFailed.
		s.cfg.Logger.Debug("future_scheduler.mark_delivered.cas_miss",
			"channel_id", s.channelID, "id", id, "owner", s.ownerID)
	}
	return nil
}

// markPermanentlyFailed moves a row to terminal failure (max attempts
// exceeded). It clears the claim slot in the same UPDATE so a future
// scan does not bother with the row again. Owner-scoped to mirror
// markDelivered's safety guarantee.
func (s *FutureScheduler) markPermanentlyFailed(ctx context.Context, id string, now int64, reason string) error {
	_, err := s.db.ExecContext(ctx,
		`UPDATE messages
		    SET delivery_failed_at = ?,
		        last_error         = ?,
		        claim_owner        = NULL,
		        claimed_at         = NULL
		  WHERE id = ?
		    AND claim_owner = ?
		    AND delivery_failed_at IS NULL`,
		now, reason, id, s.ownerID,
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
