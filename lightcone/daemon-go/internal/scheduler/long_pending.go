package scheduler

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/coagent-ai/daemon-go/pkg/harness"
	"github.com/coagent-ai/daemon-go/pkg/v4types"
)

// Authoritative spec references:
//
//   - L1 §6.4   long-pending scheduler contract
//   - L2 §3.7   three-step scan SQL + expires_at default table
//   - L2 §3.7.3 fallback terminal emit envelope template
//   - L1 §10.2  harness 9-step Write chain (the scheduler hands every
//               fallback envelope into Write — no shortcut path)
//
// scheduler 周期扫描 channel-local sqlite，找到 receiver 没有按时给出
// terminal response 的 pending request，分三步分支兜底 emit kind=response
// terminal。所有 emit 都走完整 harness 9 步链：
//
//   - Step 1: agent / system receiver 的 expires_at 过期      → unanswered_timeout
//   - Step 2: human receiver 的 expires_at 过期 (channel config
//     启用时 harness normalize 才会填 expires_at；scheduler 只看列) → human_unanswered_timeout
//   - Step 3: receiver 已 deregistered 或 actor_registry 中根本不存在
//     （不等 expires_at；避免永久违反 The One Law）             → receiver_unavailable
//
// fallback envelope id = "fallback:" + request.id + ":" + reason，是
// deterministic 派生——重复扫描时 harness Step 0.5 会按 envelope.id +
// canonical_hash 比较直接 dedupe，不会重复落库。

// ---------------------------------------------------------------------------
// Defaults
// ---------------------------------------------------------------------------

// DefaultPeriod is the L1 §6.4 / ticket §T9 baseline scan cadence — once per
// second the scheduler walks every pending request whose expires_at has
// elapsed (Step 1 / 2) or whose receiver has gone missing (Step 3). Tests
// inject sub-millisecond periods via Config.Period.
const DefaultPeriod = 1 * time.Second

// DefaultBatch caps how many candidate rows one Tick processes **per step**.
// Protocol baseline picks 256 to keep a single Tick's transactional footprint
// bounded while still draining ordinary backlog in one wake-up.
const DefaultBatch = 256

// SystemActorID is the canonical sender.id the harness Step 3 sender-deregistered
// check exempts (L1 §10.2 step 3 "sender.id='system' 例外放行"). The scheduler
// always writes fallback terminals as this actor.
const SystemActorID = "system"

// ---------------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------------

// Config tunes the long-pending scheduler. Zero value yields baseline
// behaviour (1 s period, 256 batch, wall-clock now, slog.Default()).
type Config struct {
	// Period is the ticker interval for Run; defaults to DefaultPeriod (1 s).
	Period time.Duration

	// Batch caps the number of rows one Tick processes **per step**. Defaults
	// to DefaultBatch (256). Tests use 1-2 to drive deterministic ordering.
	Batch int

	// Now returns the current wall-clock time in **milliseconds** — matches
	// the harness Clock + envelope expires_at convention. Tests inject a
	// fixed-clock pointer.
	Now func() int64

	// Logger receives structured scheduler events. Defaults to slog.Default();
	// test callers can pass a discard logger.
	Logger *slog.Logger
}

// ---------------------------------------------------------------------------
// Scheduler
// ---------------------------------------------------------------------------

// HarnessWriter is the 1-method surface the scheduler needs from the
// pkg/harness layer. Production wires this to a `harnessWriteFn` that calls
// pkg/harness.Write(deps, env, callerCtx); tests inject a spy that records
// every call without touching sqlite.
type HarnessWriter interface {
	Write(ctx context.Context, env *v4types.Envelope, callerCtx harness.CallerCtx) (*harness.Result, error)
}

// HarnessWriteFunc is a function adapter for HarnessWriter — production wiring
// passes `HarnessWriteFunc(func(ctx, env, cctx) { return harness.Write(ctx,
// deps, env, cctx) })` so the scheduler stays decoupled from harness Deps
// construction (mirrors trigger.GatewayDispatcher pattern).
type HarnessWriteFunc func(ctx context.Context, env *v4types.Envelope, callerCtx harness.CallerCtx) (*harness.Result, error)

// Write satisfies HarnessWriter.
func (f HarnessWriteFunc) Write(ctx context.Context, env *v4types.Envelope, callerCtx harness.CallerCtx) (*harness.Result, error) {
	return f(ctx, env, callerCtx)
}

// Scheduler implements the L2 §3.7 three-step long-pending fallback emitter.
// One scheduler per channel sqlite. The daemon owns N schedulers when it
// hosts N channels. Schedulers share the protocol baseline cadence unless
// channel-specific overrides are wired via Config.
//
// Crash recovery is built-in: scheduler state lives entirely in the channel
// sqlite (the messages.id deterministic-fallback row + The One Law uniqueness
// index). A restarted daemon simply re-runs Tick and picks up where it left
// off — there is no in-memory queue to lose, and the harness Step 0.5
// envelope-id dedupe makes duplicate emit attempts idempotent.
type Scheduler struct {
	db        *sql.DB
	writer    HarnessWriter
	channelID string
	cfg       Config
}

// NewLongPendingScheduler constructs a Scheduler. Returns an error on missing
// inputs so misuse surfaces at startup rather than as a silent no-op tick.
func NewLongPendingScheduler(
	db *sql.DB,
	writer HarnessWriter,
	channelID string,
	cfg Config,
) (*Scheduler, error) {
	if db == nil {
		return nil, errors.New("long_pending: db is nil")
	}
	if writer == nil {
		return nil, errors.New("long_pending: harness writer is nil")
	}
	if channelID == "" {
		return nil, errors.New("long_pending: channel_id is required")
	}
	if cfg.Period <= 0 {
		cfg.Period = DefaultPeriod
	}
	if cfg.Batch <= 0 {
		cfg.Batch = DefaultBatch
	}
	if cfg.Now == nil {
		cfg.Now = func() int64 { return time.Now().UnixMilli() }
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	return &Scheduler{
		db:        db,
		writer:    writer,
		channelID: channelID,
		cfg:       cfg,
	}, nil
}

// Run drives the scheduler loop until ctx is cancelled. Each cfg.Period tick
// fires exactly one Tick; Tick errors are logged but do not stop the loop (a
// transient sqlite error on one tick should not silence the scheduler).
//
// First tick fires immediately so daemon startup doesn't wait `Period`
// seconds before noticing pending backlog (matches supervisor.Run + future
// scheduler conventions).
func (s *Scheduler) Run(ctx context.Context) error {
	ticker := time.NewTicker(s.cfg.Period)
	defer ticker.Stop()

	if err := s.Tick(ctx); err != nil && !errors.Is(err, context.Canceled) {
		s.cfg.Logger.Error("long_pending.tick.error",
			"channel_id", s.channelID, "err", err.Error())
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if err := s.Tick(ctx); err != nil && !errors.Is(err, context.Canceled) {
				s.cfg.Logger.Error("long_pending.tick.error",
					"channel_id", s.channelID, "err", err.Error())
			}
		}
	}
}

// Tick runs one scan + emit cycle through all three steps. Idempotent —
// duplicate Ticks on the same pending request hit harness Step 0.5 dedupe
// (via deterministic fallback id) so the messages table grows by exactly one
// row per (request, reason) pair regardless of how many times Tick fires.
//
// Per-step error handling:
//
//   - scanX SQL error → return wrapped error; Run logs + continues next tick.
//   - per-row processing error (harness reject other than dedupe) → log +
//     continue to next row; one bad row doesn't kill the Tick.
func (s *Scheduler) Tick(ctx context.Context) error {
	now := s.cfg.Now()

	// Step 1: agent / system receiver expires_at expired.
	rows1, err := s.scanStep1(ctx, now)
	if err != nil {
		return fmt.Errorf("long_pending: scan step 1: %w", err)
	}
	for i := range rows1 {
		s.emit(ctx, &rows1[i], v4types.TerminalUnansweredTimeout, "", now)
	}

	// Step 2: human receiver expires_at expired (channel config enabled).
	rows2, err := s.scanStep2(ctx, now)
	if err != nil {
		return fmt.Errorf("long_pending: scan step 2: %w", err)
	}
	for i := range rows2 {
		s.emit(ctx, &rows2[i], v4types.TerminalHumanUnansweredTimeout, "", now)
	}

	// Step 3: receiver deregistered or missing — does not wait for expires_at.
	rows3, err := s.scanStep3(ctx)
	if err != nil {
		return fmt.Errorf("long_pending: scan step 3: %w", err)
	}
	for i := range rows3 {
		r := &rows3[i]
		missing := r.AudienceFirst
		if missing == "" {
			// Defensive: harness Step 5 already rejects empty audience on
			// request writes, but in case a future code path slips one
			// through, skip rather than emit a malformed fallback.
			s.cfg.Logger.Warn("long_pending.step3.skip_empty_audience",
				"channel_id", s.channelID, "request_id", r.ID)
			continue
		}
		s.emit(ctx, r, v4types.TerminalReceiverUnavailable, missing, now)
	}

	return nil
}

// pendingRow captures the columns the emit path needs from one pending
// request row. Loaded by every scanStep helper.
type pendingRow struct {
	ID            string
	TS            int64
	Type          string
	SenderID      string // original request's sender — becomes audience[0] of fallback
	CorrelationID sql.NullString
	AudienceFirst string // json_extract(audience, '$[0]') — Step 3 'missing_actor_id'
}

// scanStep1 finds pending request rows whose receiver is an active
// agent/system actor and whose expires_at has elapsed.
func (s *Scheduler) scanStep1(ctx context.Context, now int64) ([]pendingRow, error) {
	return s.scanWithReceiverKind(ctx, scanExpiredByReceiverKindSQL, now, "agent", "system")
}

// scanStep2 finds pending request rows whose receiver is an active human
// actor and whose expires_at has elapsed (channel config must have populated
// expires_at; otherwise harness normalize leaves NULL and this SQL skips the
// row via the `expires_at IS NOT NULL` filter).
func (s *Scheduler) scanStep2(ctx context.Context, now int64) ([]pendingRow, error) {
	return s.scanWithReceiverKind(ctx, scanExpiredByReceiverKindSQL, now, "human", "")
}

// scanWithReceiverKind drives Step 1 / Step 2 — same SQL template, different
// actor_kind filter. kind2 may be "" (Step 2 needs only one kind); the SQL
// IN clause silently ignores empty values via parameter binding.
func (s *Scheduler) scanWithReceiverKind(ctx context.Context, query string, now int64, kind1, kind2 string) ([]pendingRow, error) {
	if kind2 == "" {
		kind2 = kind1
	}
	rows, err := s.db.QueryContext(ctx, query, now, kind1, kind2, s.cfg.Batch)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	return scanPendingRows(rows)
}

// scanStep3 finds pending request rows whose receiver is deregistered or
// missing from actor_registry — emit `receiver_unavailable` without waiting
// for expires_at (L2 §3.7.1 "不等 expires_at"). The LEFT JOIN keeps the row
// even when actor_registry has no matching id; the WHERE filters down to
// "missing OR deregistered" rows.
func (s *Scheduler) scanStep3(ctx context.Context) ([]pendingRow, error) {
	rows, err := s.db.QueryContext(ctx, scanReceiverUnavailableSQL, s.cfg.Batch)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	return scanPendingRows(rows)
}

// scanPendingRows decodes the shared SELECT shape used by all three steps.
func scanPendingRows(rows *sql.Rows) ([]pendingRow, error) {
	var out []pendingRow
	for rows.Next() {
		var r pendingRow
		if err := rows.Scan(
			&r.ID, &r.TS, &r.Type, &r.SenderID,
			&r.CorrelationID, &r.AudienceFirst,
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

// scanExpiredByReceiverKindSQL is the Step 1 / Step 2 query template.
// `actor_kind` is parameterised so a single SQL string serves both steps.
//
// Result projection matches pendingRow Scan order.
const scanExpiredByReceiverKindSQL = `
SELECT m.id, m.ts, m.type, m.sender_id,
       m.correlation_id,
       json_extract(m.audience, '$[0]') AS receiver_id
  FROM messages m
  JOIN actor_registry a
    ON a.actor_id = json_extract(m.audience, '$[0]')
   AND a.deregistered_at IS NULL
 WHERE m.kind = 'request'
   AND m.expires_at IS NOT NULL
   AND m.expires_at < ?
   AND a.actor_kind IN (?, ?)
   AND NOT EXISTS (
       SELECT 1 FROM messages r
        WHERE r.parent_id = m.id
          AND r.kind = 'response'
          AND r.is_terminal = 1)
 ORDER BY m.seq ASC
 LIMIT ?
`

// scanReceiverUnavailableSQL is the Step 3 query. LEFT JOIN keeps rows
// whose audience[0] does not exist in actor_registry; the WHERE captures
// both "missing" (a.actor_id IS NULL) and "deregistered" cases.
const scanReceiverUnavailableSQL = `
SELECT m.id, m.ts, m.type, m.sender_id,
       m.correlation_id,
       json_extract(m.audience, '$[0]') AS missing_actor_id
  FROM messages m
  LEFT JOIN actor_registry a
    ON a.actor_id = json_extract(m.audience, '$[0]')
 WHERE m.kind = 'request'
   AND (a.actor_id IS NULL OR a.deregistered_at IS NOT NULL)
   AND NOT EXISTS (
       SELECT 1 FROM messages r
        WHERE r.parent_id = m.id
          AND r.kind = 'response'
          AND r.is_terminal = 1)
 ORDER BY m.seq ASC
 LIMIT ?
`

// ---------------------------------------------------------------------------
// Emit path
// ---------------------------------------------------------------------------

// emit builds the fallback envelope for `row` with the given reason and hands
// it to the harness writer. `missingActorID` is non-empty only for Step 3
// (`receiver_unavailable`).
//
// Result handling:
//
//   - Write success → log emit (with Dedupe flag so observability captures
//     "fresh vs replay").
//   - Write reject → log + return; one bad row doesn't kill the Tick.
//   - Write infrastructure error → log; same.
func (s *Scheduler) emit(
	ctx context.Context,
	row *pendingRow,
	reason v4types.TerminalFailureReason,
	missingActorID string,
	now int64,
) {
	env, err := buildFallbackEnvelope(row, reason, missingActorID, s.channelID, now)
	if err != nil {
		s.cfg.Logger.Error("long_pending.build_envelope.error",
			"channel_id", s.channelID, "request_id", row.ID,
			"reason", string(reason), "err", err.Error())
		return
	}

	result, werr := s.writer.Write(ctx, env, harness.CallerCtx{
		Authenticated: true,
		ActorID:       SystemActorID,
	})
	if werr != nil {
		var rerr *harness.RejectError
		if errors.As(werr, &rerr) {
			s.cfg.Logger.Warn("long_pending.emit.reject",
				"channel_id", s.channelID, "request_id", row.ID,
				"reason", string(reason),
				"harness_reason", string(rerr.Reason),
				"detail", rerr.Detail)
			return
		}
		s.cfg.Logger.Error("long_pending.emit.error",
			"channel_id", s.channelID, "request_id", row.ID,
			"reason", string(reason), "err", werr.Error())
		return
	}

	s.cfg.Logger.Info("long_pending.emit.ok",
		"channel_id", s.channelID, "request_id", row.ID,
		"fallback_id", env.ID, "reason", string(reason),
		"dedupe", result.Dedupe)
}

// fallbackPayload mirrors the L2 §3.7.3 payload template. `missing_actor_id`
// is rendered at the top level (`omitempty` keeps it out of Step 1 / Step 2
// fallbacks where it doesn't apply).
type fallbackPayload struct {
	Status         string `json:"status"`
	Reason         string `json:"reason"`
	MissingActorID string `json:"missing_actor_id,omitempty"`
}

// buildFallbackEnvelope materialises the L2 §3.7.3 envelope template:
//
//	id             = "fallback:" + request.id + ":" + reason
//	type           = request.type
//	kind           = response
//	sender         = {kind: system, id: system}
//	parent_id      = request.id
//	correlation_id = request.correlation_id
//	audience       = [request.sender.id]
//	payload        = {status:'failed', reason, [missing_actor_id]?}
//	visibility     = system
//
// `visibility=system` keeps the fallback off the user-facing UI by default
// (L0 §2.4 — system-emitted audit messages); the original request's response
// chain is closed regardless. `ts` uses the scheduler clock so canonical_hash
// stays stable for the deterministic-id dedupe path.
func buildFallbackEnvelope(
	row *pendingRow,
	reason v4types.TerminalFailureReason,
	missingActorID string,
	channelID string,
	now int64,
) (*v4types.Envelope, error) {
	payloadBytes, err := json.Marshal(fallbackPayload{
		Status:         "failed",
		Reason:         string(reason),
		MissingActorID: missingActorID,
	})
	if err != nil {
		return nil, fmt.Errorf("marshal fallback payload: %w", err)
	}
	env := &v4types.Envelope{
		ID:         FallbackID(row.ID, reason),
		TS:         now,
		ChannelID:  channelID,
		Sender:     v4types.Sender{Kind: v4types.SenderSystem, ID: SystemActorID},
		Kind:       v4types.KindResponse,
		Type:       row.Type,
		Payload:    payloadBytes,
		ParentID:   row.ID,
		Visibility: v4types.VisibilitySystem,
		Audience:   []string{row.SenderID},
	}
	if row.CorrelationID.Valid {
		env.CorrelationID = row.CorrelationID.String
	}
	return env, nil
}

// FallbackID renders the deterministic envelope id used by every fallback
// emit. Same (request_id, reason) pair always produces the same string, so
// harness Step 0.5 / Step 8 dedupe a repeat scan into an idempotent no-op.
//
// Exported so tests + audit tools can re-derive the id without re-importing
// the private template.
func FallbackID(requestID string, reason v4types.TerminalFailureReason) string {
	return "fallback:" + requestID + ":" + string(reason)
}
