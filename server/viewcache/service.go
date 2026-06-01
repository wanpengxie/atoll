// Package viewcache holds the server-side view of the daemon's
// channel log — apply (in-order, out-of-order, duplicate), gap
// detection, the closed-interval Resync protocol, and the cursor +
// messages tables defined by L1 §8 + T1.1 + T1.8.
//
// Authoritative spec: launch-ticket notes §T6 (viewcache) +
// framework/multiuser/viewsync (Pusher / Receiver / Resyncer contracts).
//
// Concurrency model:
//   - Apply is safe for concurrent invocations across different
//     channel_ids (sqlite serializes writers).
//   - For a single channel, callers MUST funnel push frames in
//     daemonbus dispatch order; per-channel ordering is guaranteed
//     by the daemonbus mux (L2 §9.1).
//   - The in-memory buffer (un-applied out-of-order frames) is
//     guarded by a per-channel mutex managed in this package — no
//     global lock to avoid contention.
package viewcache

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"sync"
	"time"

	"github.com/wanpengxie/ActOS/framework/multiuser/viewsync"
	"github.com/wanpengxie/ActOS/kernel/channel"
	"github.com/wanpengxie/ActOS/kernel/message"
	"github.com/wanpengxie/ActOS/pkg/requestctx"
	"github.com/wanpengxie/ActOS/server/channelaccess"
)

// Resyncer is what viewcache calls when it detects a gap.
// runtime/transit on the daemon side implements ServeResync; the
// gateway wires a daemonbus client that satisfies RequestResync to
// hand it here. Tests pass a fake.
type Resyncer interface {
	RequestResync(ctx context.Context, channelID channel.ID, since, until viewsync.Seq) ([]viewsync.ResyncMessage, error)
}

type ResyncCompletionNotifier interface {
	NotifyResyncComplete(ctx context.Context, channelID channel.ID, lastReceivedSeq viewsync.LastReceivedSeq) error
}

// Service is the viewcache facade.
type Service struct {
	db  *sql.DB
	log *slog.Logger

	mu      sync.Mutex
	buffers map[channel.ID]*channelBuffer

	resyncer                 Resyncer
	resyncCompletionNotifier ResyncCompletionNotifier

	accessMu sync.RWMutex
	access   channelaccess.Authorizer

	// fireResyncFn overrides the default fire-and-forget resync
	// goroutine — set by tests via SetFireResyncForTest so they can
	// observe trigger invocations synchronously without spawning real
	// daemon RPC. nil → use the default goroutine path.
	fireResyncFn func(channelID channel.ID, since, until viewsync.Seq)
}

type channelBuffer struct {
	mu             sync.Mutex
	pending        map[viewsync.Seq]viewsync.PushFrame
	pendingBytes   int
	resyncPending  bool
	resyncSince    viewsync.Seq
	resyncUntil    viewsync.Seq
	lastResyncAtMs int64
}

const (
	defaultPendingFrameCap = 1000
	defaultPendingBytesCap = 8 << 20
	gapResyncCooldown      = 5 * time.Second
)

// NewService constructs a Service. Resyncer can be wired later via
// SetResyncer.
func NewService(db *sql.DB) *Service {
	return &Service{
		db:      db,
		log:     slog.Default().With("subsystem", "viewcache"),
		buffers: map[channel.ID]*channelBuffer{},
	}
}

// SetLogger overrides the structured logger. nil restores slog.Default.
func (s *Service) SetLogger(log *slog.Logger) {
	if log == nil {
		log = slog.Default()
	}
	s.log = log.With("subsystem", "viewcache")
}

// SetResyncer plugs in the gap-recovery RPC client.
func (s *Service) SetResyncer(r Resyncer) { s.resyncer = r }

func (s *Service) SetResyncCompletionNotifier(n ResyncCompletionNotifier) {
	s.resyncCompletionNotifier = n
}

// SetAccessAuthorizer wires the route-level channel access check.
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

// SetFireResyncForTest overrides the fire-and-forget gap-resync
// goroutine launched by Apply when it sees a gap. Tests use this to
// capture invocations synchronously without scheduling real goroutines
// or RPC. Pass nil to restore the default. NOT for production callers.
func (s *Service) SetFireResyncForTest(fn func(channelID channel.ID, since, until viewsync.Seq)) {
	s.fireResyncFn = fn
}

// bufferFor returns the per-channel pending map (lazy-init).
func (s *Service) bufferFor(channelID channel.ID) *channelBuffer {
	s.mu.Lock()
	b, ok := s.buffers[channelID]
	if !ok {
		b = &channelBuffer{pending: map[viewsync.Seq]viewsync.PushFrame{}}
		s.buffers[channelID] = b
	}
	s.mu.Unlock()
	return b
}

// Apply runs the L1 §8.4 server apply rule for one push frame:
//
//  1. open a SQLite transaction
//  2. INSERT OR IGNORE the view_cache_messages row (idempotent on
//     (channel_id, seq) PRIMARY KEY and (channel_id, message_id)
//     UNIQUE)
//  3. classify the seq:
//     - seq <= cursor: duplicate (ApplyOutcomeDuplicate)
//     - seq == cursor+1: contiguous; advance cursor (+ drain buffer);
//     buffered frames whose seq just crossed into "contiguous" are
//     returned in ApplyResult.DrainedMessages alongside the current
//     frame so the gateway can fan-out missed pushes (FIX-T5)
//     - seq > cursor+1: gap; row persisted, cursor unchanged; a
//     fire-and-forget TriggerResync goroutine is launched to ask the
//     daemon for the missing closed interval [cursor+1, seq-1]
//     (FIX-T5)
//  4. COMMIT
//
// Returns an ApplyResult carrying the post-commit cursor — the caller
// MUST emit ack(LastReceivedSeq) AFTER the COMMIT (this method
// already commits before returning, so the contract is satisfied).
func (s *Service) Apply(ctx context.Context, frame viewsync.PushFrame) (viewsync.ApplyResult, error) {
	envJSON, err := json.Marshal(frame.Envelope)
	if err != nil {
		return viewsync.ApplyResult{}, fmt.Errorf("viewcache: marshal envelope: %w", err)
	}

	now := nowMs()

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return viewsync.ApplyResult{}, fmt.Errorf("viewcache: begin: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	cursor, err := readCursorTx(ctx, tx, frame.ChannelID)
	if err != nil {
		return viewsync.ApplyResult{}, err
	}

	if _, err := tx.ExecContext(
		ctx,
		`INSERT OR IGNORE INTO view_cache_messages
		 (channel_id, seq, message_id, envelope_json, received_at)
		 VALUES (?, ?, ?, ?, ?)`,
		string(frame.ChannelID), int64(frame.Seq), frame.MessageID, string(envJSON), now,
	); err != nil {
		return viewsync.ApplyResult{}, fmt.Errorf("viewcache: insert message: %w", err)
	}

	var (
		outcome viewsync.ApplyOutcome
		newCur  viewsync.LastReceivedSeq
		extras  []viewsync.PushFrame // buffered frames drained (excludes current)
		// Gap window — captured before commit so the fire-and-forget
		// resync goroutine sees a stable snapshot.
		gapSince viewsync.Seq
		gapUntil viewsync.Seq
	)
	switch {
	case frame.Seq <= cursor:
		// Duplicate / late frame — row is idempotent, cursor unchanged.
		outcome = viewsync.ApplyOutcomeDuplicate
		newCur = cursor
	case frame.Seq == cursor+1:
		// Contiguous — advance cursor, then drain buffer for any
		// already-stored seq cursor+1, +2, … in view_cache_messages.
		next, drainedRows, err := advanceCursorTx(ctx, tx, frame.ChannelID, frame.Seq)
		if err != nil {
			return viewsync.ApplyResult{}, err
		}
		outcome = viewsync.ApplyOutcomeContiguous
		newCur = next
		extras = drainedRows
	default:
		// Gap — row persisted but cursor stays put. Window of missing
		// seqs is [cursor+1, frame.Seq-1] inclusive.
		outcome = viewsync.ApplyOutcomeGap
		newCur = cursor
		gapSince = viewsync.Seq(cursor) + 1
		gapUntil = frame.Seq - 1
	}

	if err := tx.Commit(); err != nil {
		return viewsync.ApplyResult{}, fmt.Errorf("viewcache: commit: %w", err)
	}

	// In-memory buffer of out-of-order frames is updated only after
	// COMMIT — the buffer is a convenience for tests inspecting state;
	// authoritative state lives in sqlite.
	buf := s.bufferFor(frame.ChannelID)
	frameBytes := pendingFrameBytes(frame, envJSON)
	overflow := false
	buf.mu.Lock()
	switch outcome {
	case viewsync.ApplyOutcomeGap:
		if len(buf.pending) >= defaultPendingFrameCap ||
			buf.pendingBytes+frameBytes > defaultPendingBytesCap {
			buf.pending = map[viewsync.Seq]viewsync.PushFrame{}
			buf.pendingBytes = 0
			overflow = true
		} else if _, exists := buf.pending[frame.Seq]; !exists {
			buf.pending[frame.Seq] = frame
			buf.pendingBytes += frameBytes
		}
	case viewsync.ApplyOutcomeContiguous:
		for seq := range buf.pending {
			if seq <= newCur {
				buf.pendingBytes -= pendingFrameBytes(buf.pending[seq], nil)
				delete(buf.pending, seq)
			}
		}
		if buf.pendingBytes < 0 {
			buf.pendingBytes = 0
		}
	}
	buf.mu.Unlock()
	if overflow {
		outcome = viewsync.ApplyOutcomeResyncRequired
	}

	// Build DrainedMessages payload. Convention (FIX-T5): nil unless at
	// least one buffered row drained — then [current frame, ...extras]
	// in seq ASC order. Plain contiguous → nil so the gateway falls
	// back to fan-out of the current frame.
	var drained []viewsync.PushFrame
	if len(extras) > 0 {
		drained = make([]viewsync.PushFrame, 0, 1+len(extras))
		drained = append(drained, frame)
		drained = append(drained, extras...)
	}

	// Gap → fire-and-forget resync request. We detach from caller ctx
	// because the dispatch goroutine that invoked Apply may return
	// before resync completes. Production path: spawn a goroutine and
	// run TriggerResync against the wired daemonbus resyncer. Test
	// path: synchronous invocation of the captured hook so assertions
	// are deterministic (no flaky goroutine scheduling). We skip when
	// neither a hook nor a resyncer is wired so service-level tests
	// without a resyncer don't accumulate noisy goroutines.
	if (outcome == viewsync.ApplyOutcomeGap || outcome == viewsync.ApplyOutcomeResyncRequired) &&
		gapSince <= gapUntil &&
		(s.fireResyncFn != nil || s.resyncer != nil) {
		if since, until, ok := s.beginGapResync(frame.ChannelID, gapSince, gapUntil); ok {
			s.log.Info("viewcache.gap_drain_started",
				"request_id", requestctx.RequestID(ctx),
				"channel_id", string(frame.ChannelID),
				"since_seq", since,
				"until_seq", until,
				"trigger", "apply")
			s.dispatchGapResync(frame.ChannelID, since, until)
		}
	}

	return viewsync.ApplyResult{
		Outcome:         outcome,
		LastReceivedSeq: newCur,
		DrainedMessages: drained,
	}, nil
}

// fireResync wraps TriggerResync for the fire-and-forget gap path. It
// uses a fresh background context so the recovery survives the request
// scope and swallows errors — the next gap or a periodic reconcile
// will retry. Production path only; tests intercept via fireResyncFn.
func (s *Service) fireResync(channelID channel.ID, since, until viewsync.Seq) {
	defer s.finishGapResync(channelID, since, until)
	if _, err := s.TriggerResync(context.Background(), channelID, since, until); err != nil {
		s.log.Warn("viewcache.gap_drain_failed",
			"channel_id", string(channelID),
			"since_seq", since,
			"until_seq", until,
			"error", err.Error())
	}
}

func (s *Service) dispatchGapResync(channelID channel.ID, since, until viewsync.Seq) {
	switch {
	case s.fireResyncFn != nil:
		s.fireResyncFn(channelID, since, until)
		s.finishGapResync(channelID, since, until)
	case s.resyncer != nil:
		go s.fireResync(channelID, since, until)
	}
}

func pendingFrameBytes(frame viewsync.PushFrame, envJSON []byte) int {
	n := len(frame.MessageID) + 32
	if envJSON != nil {
		return n + len(envJSON)
	}
	if raw, err := json.Marshal(frame.Envelope); err == nil {
		return n + len(raw)
	}
	return n
}

func capResyncWindow(since, until viewsync.Seq) (viewsync.Seq, viewsync.Seq) {
	if since > until {
		return since, until
	}
	maxUntil := since + viewsync.Seq(maxResyncRange) - 1
	if until > maxUntil {
		until = maxUntil
	}
	return since, until
}

func (s *Service) beginGapResync(channelID channel.ID, since, until viewsync.Seq) (viewsync.Seq, viewsync.Seq, bool) {
	return s.startGapResync(channelID, since, until, true)
}

func (s *Service) startGapResync(channelID channel.ID, since, until viewsync.Seq, respectCooldown bool) (viewsync.Seq, viewsync.Seq, bool) {
	since, until = capResyncWindow(since, until)
	buf := s.bufferFor(channelID)
	now := nowMs()
	buf.mu.Lock()
	defer buf.mu.Unlock()
	if buf.resyncPending {
		if buf.resyncSince == 0 || since < buf.resyncSince {
			buf.resyncSince = since
		}
		if until > buf.resyncUntil {
			_, buf.resyncUntil = capResyncWindow(buf.resyncSince, until)
		}
		return 0, 0, false
	}
	if respectCooldown && buf.lastResyncAtMs > 0 && now-buf.lastResyncAtMs < gapResyncCooldown.Milliseconds() {
		return 0, 0, false
	}
	buf.resyncPending = true
	buf.resyncSince = since
	buf.resyncUntil = until
	buf.lastResyncAtMs = now
	return since, until, true
}

func (s *Service) finishGapResync(channelID channel.ID, completedSince, completedUntil viewsync.Seq) {
	buf := s.bufferFor(channelID)
	var followSince, followUntil viewsync.Seq
	buf.mu.Lock()
	widenedSince, widenedUntil := buf.resyncSince, buf.resyncUntil
	buf.resyncPending = false
	buf.resyncSince = 0
	buf.resyncUntil = 0
	buf.mu.Unlock()

	if widenedSince == 0 || widenedUntil == 0 ||
		(widenedSince >= completedSince && widenedUntil <= completedUntil) ||
		(s.fireResyncFn == nil && s.resyncer == nil) {
		s.log.Info("viewcache.gap_drain_finished",
			"channel_id", string(channelID),
			"since_seq", completedSince,
			"until_seq", completedUntil)
		return
	}

	switch {
	case widenedSince < completedSince && widenedUntil > completedUntil:
		followSince, followUntil = widenedSince, widenedUntil
	case widenedSince < completedSince:
		followSince, followUntil = widenedSince, minSeq(widenedUntil, completedSince-1)
	default:
		followSince, followUntil = maxSeq(widenedSince, completedUntil+1), widenedUntil
	}
	if followSince > followUntil {
		s.log.Info("viewcache.gap_drain_finished",
			"channel_id", string(channelID),
			"since_seq", completedSince,
			"until_seq", completedUntil)
		return
	}
	if since, until, ok := s.startGapResync(channelID, followSince, followUntil, false); ok {
		s.log.Info("viewcache.gap_drain_started",
			"channel_id", string(channelID),
			"since_seq", since,
			"until_seq", until,
			"trigger", "widened_followup")
		s.dispatchGapResync(channelID, since, until)
	}
}

func minSeq(a, b viewsync.Seq) viewsync.Seq {
	if a < b {
		return a
	}
	return b
}

func maxSeq(a, b viewsync.Seq) viewsync.Seq {
	if a > b {
		return a
	}
	return b
}

// RecoverGaps scans durable viewcache state for committed out-of-order rows
// that sit beyond the cursor and re-triggers the missing closed interval.
// This is the crash-recovery counterpart to Apply's in-memory goroutine:
// if the process dies after committing a gap but before fireResync runs,
// startup / periodic reconcile can still make progress.
func (s *Service) RecoverGaps(ctx context.Context) error {
	rows, err := s.db.QueryContext(ctx, `
		SELECT c.channel_id, c.last_received_seq, MIN(m.seq)
		  FROM view_cache_cursors c
		  JOIN view_cache_messages m
		    ON m.channel_id = c.channel_id
		   AND m.seq > c.last_received_seq + 1
		 GROUP BY c.channel_id, c.last_received_seq`)
	if err != nil {
		return fmt.Errorf("viewcache: recover gaps scan: %w", err)
	}
	rowsClosed := false
	closeRows := func() error {
		if rowsClosed {
			return nil
		}
		rowsClosed = true
		return rows.Close()
	}
	defer func() { _ = closeRows() }()

	type gap struct {
		channelID channel.ID
		since     viewsync.Seq
		until     viewsync.Seq
	}
	var gaps []gap
	for rows.Next() {
		var (
			chID string
			cur  int64
			min  int64
		)
		if err := rows.Scan(&chID, &cur, &min); err != nil {
			return fmt.Errorf("viewcache: recover gaps scan row: %w", err)
		}
		if min <= cur+1 {
			continue
		}
		since := viewsync.Seq(cur + 1)
		until := viewsync.Seq(min - 1)
		gaps = append(gaps, gap{channelID: channel.ID(chID), since: since, until: until})
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("viewcache: recover gaps rows: %w", err)
	}
	if err := closeRows(); err != nil {
		return fmt.Errorf("viewcache: recover gaps close: %w", err)
	}
	for _, g := range gaps {
		if s.fireResyncFn == nil && s.resyncer == nil {
			continue
		}
		since, until, ok := s.beginGapResync(g.channelID, g.since, g.until)
		if !ok {
			continue
		}
		s.log.Info("viewcache.gap_drain_started",
			"request_id", requestctx.RequestID(ctx),
			"channel_id", string(g.channelID),
			"since_seq", since,
			"until_seq", until,
			"trigger", "recover_gaps")
		switch {
		case s.fireResyncFn != nil:
			s.fireResyncFn(g.channelID, since, until)
			s.finishGapResync(g.channelID, since, until)
		case s.resyncer != nil:
			if _, err := s.TriggerResync(ctx, g.channelID, since, until); err != nil {
				s.log.Warn("viewcache.gap_drain_failed",
					"request_id", requestctx.RequestID(ctx),
					"channel_id", string(g.channelID),
					"since_seq", since,
					"until_seq", until,
					"trigger", "recover_gaps",
					"error", err.Error())
				s.finishGapResync(g.channelID, since, until)
				return err
			}
			s.finishGapResync(g.channelID, since, until)
		}
	}
	return nil
}

// Cursor returns the current last_received_seq for channelID.
func (s *Service) Cursor(ctx context.Context, channelID channel.ID) (viewsync.LastReceivedSeq, error) {
	var seq int64
	err := s.db.QueryRowContext(
		ctx,
		`SELECT last_received_seq FROM view_cache_cursors WHERE channel_id = ?`,
		string(channelID),
	).Scan(&seq)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return 0, nil
		}
		return 0, fmt.Errorf("viewcache: cursor: %w", err)
	}
	return viewsync.LastReceivedSeq(seq), nil
}

// Messages returns all stored messages for channelID with seq in
// (afterSeq, +∞], in ascending order. Used by gateway resync /
// frontend initial-load endpoints.
func (s *Service) Messages(ctx context.Context, channelID channel.ID, afterSeq viewsync.Seq, limit int) ([]StoredMessage, error) {
	if limit <= 0 || limit > 1000 {
		limit = 200
	}
	rows, err := s.db.QueryContext(
		ctx,
		`SELECT seq, message_id, envelope_json, received_at
		   FROM view_cache_messages
		  WHERE channel_id = ? AND seq > ?
		  ORDER BY seq ASC
		  LIMIT ?`,
		string(channelID), int64(afterSeq), limit,
	)
	if err != nil {
		return nil, fmt.Errorf("viewcache: messages: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var out []StoredMessage
	for rows.Next() {
		var (
			m       StoredMessage
			seq     int64
			recvAt  int64
			envJSON string
		)
		if err := rows.Scan(&seq, &m.MessageID, &envJSON, &recvAt); err != nil {
			return nil, err
		}
		m.Seq = viewsync.Seq(seq)
		m.ReceivedAt = recvAt
		if err := json.Unmarshal([]byte(envJSON), &m.Envelope); err != nil {
			return nil, fmt.Errorf("viewcache: unmarshal seq=%d: %w", seq, err)
		}
		out = append(out, m)
	}
	return out, rows.Err()
}

// MessageByID returns one cached message by (channel_id, message_id).
func (s *Service) MessageByID(ctx context.Context, channelID channel.ID, messageID string) (StoredMessage, bool, error) {
	var (
		m       StoredMessage
		seq     int64
		recvAt  int64
		envJSON string
	)
	err := s.db.QueryRowContext(
		ctx,
		`SELECT seq, message_id, envelope_json, received_at
		   FROM view_cache_messages
		  WHERE channel_id = ? AND message_id = ?
		  LIMIT 1`,
		string(channelID), messageID,
	).Scan(&seq, &m.MessageID, &envJSON, &recvAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return StoredMessage{}, false, nil
		}
		return StoredMessage{}, false, fmt.Errorf("viewcache: message_by_id: %w", err)
	}
	m.Seq = viewsync.Seq(seq)
	m.ReceivedAt = recvAt
	if err := json.Unmarshal([]byte(envJSON), &m.Envelope); err != nil {
		return StoredMessage{}, false, fmt.Errorf("viewcache: unmarshal message_id=%s: %w", messageID, err)
	}
	return m, true, nil
}

// StoredMessage is the read shape returned by Messages.
type StoredMessage struct {
	Seq        viewsync.Seq
	MessageID  string
	Envelope   message.Envelope
	ReceivedAt int64
}

// TriggerResync pulls the closed interval [since, until] from the
// daemon via the wired Resyncer + applies each message via Apply.
// Returns the final cursor.
func (s *Service) TriggerResync(ctx context.Context, channelID channel.ID, since, until viewsync.Seq) (viewsync.LastReceivedSeq, error) {
	if s.resyncer == nil {
		return 0, fmt.Errorf("viewcache: no resyncer wired")
	}
	if since > until {
		return 0, fmt.Errorf("viewcache: resync range invalid: since=%d > until=%d", since, until)
	}

	for start := since; start <= until; {
		_, chunkUntil := capResyncWindow(start, until)
		msgs, err := s.resyncer.RequestResync(ctx, channelID, start, chunkUntil)
		if err != nil {
			return 0, fmt.Errorf("viewcache: resync rpc: %w", err)
		}
		// Apply in seq ascending order — daemon should return them sorted
		// but enforce locally so a faulty daemon can't break us.
		sort.SliceStable(msgs, func(i, j int) bool { return msgs[i].Seq < msgs[j].Seq })

		for _, m := range msgs {
			frame := viewsync.PushFrame{
				ChannelID: channelID,
				Seq:       m.Seq,
				MessageID: m.MessageID,
				Envelope:  m.Envelope,
			}
			if _, err := s.Apply(ctx, frame); err != nil {
				return 0, err
			}
		}
		if chunkUntil == until {
			break
		}
		start = chunkUntil + 1
	}
	cur, err := s.Cursor(ctx, channelID)
	if err != nil {
		return 0, err
	}
	if s.resyncCompletionNotifier != nil {
		if err := s.resyncCompletionNotifier.NotifyResyncComplete(ctx, channelID, cur); err != nil {
			return 0, fmt.Errorf("viewcache: notify resync complete: %w", err)
		}
	}
	return cur, nil
}

// readCursorTx returns the current cursor inside a transaction;
// creates the row at 0 if missing so the UPDATE in advanceCursorTx
// is single-statement.
func readCursorTx(ctx context.Context, tx *sql.Tx, channelID channel.ID) (viewsync.LastReceivedSeq, error) {
	if _, err := tx.ExecContext(
		ctx,
		`INSERT OR IGNORE INTO view_cache_cursors (channel_id, last_received_seq) VALUES (?, 0)`,
		string(channelID),
	); err != nil {
		return 0, fmt.Errorf("viewcache: cursor upsert: %w", err)
	}
	var seq int64
	if err := tx.QueryRowContext(
		ctx,
		`SELECT last_received_seq FROM view_cache_cursors WHERE channel_id = ?`,
		string(channelID),
	).Scan(&seq); err != nil {
		return 0, fmt.Errorf("viewcache: cursor read: %w", err)
	}
	return viewsync.LastReceivedSeq(seq), nil
}

// advanceCursorTx bumps the cursor to seq, then drains any contiguous
// stored rows beyond seq (i.e. messages that arrived during a gap and
// are now connected). Returns the final cursor and the drained extra
// frames (envelopes loaded from view_cache_messages, in seq ASC order).
// The current frame's own row is NOT included in extras — the caller
// has it in hand.
func advanceCursorTx(
	ctx context.Context,
	tx *sql.Tx,
	channelID channel.ID,
	seq viewsync.Seq,
) (viewsync.LastReceivedSeq, []viewsync.PushFrame, error) {
	cur := seq
	var extras []viewsync.PushFrame

	for {
		// Set cursor = cur and look for cur+1 stored.
		if _, err := tx.ExecContext(
			ctx,
			`UPDATE view_cache_cursors SET last_received_seq = ? WHERE channel_id = ?`,
			int64(cur), string(channelID),
		); err != nil {
			return 0, nil, fmt.Errorf("viewcache: cursor update: %w", err)
		}
		var (
			nextSeq   int64
			messageID string
			envJSON   string
		)
		err := tx.QueryRowContext(
			ctx,
			`SELECT seq, message_id, envelope_json FROM view_cache_messages
			  WHERE channel_id = ? AND seq = ?
			  LIMIT 1`,
			string(channelID), int64(cur)+1,
		).Scan(&nextSeq, &messageID, &envJSON)
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return cur, extras, nil
			}
			return 0, nil, fmt.Errorf("viewcache: drain probe: %w", err)
		}
		var env message.Envelope
		if err := json.Unmarshal([]byte(envJSON), &env); err != nil {
			return 0, nil, fmt.Errorf("viewcache: drain unmarshal seq=%d: %w", nextSeq, err)
		}
		extras = append(extras, viewsync.PushFrame{
			ChannelID: channelID,
			Seq:       viewsync.Seq(nextSeq),
			MessageID: message.ID(messageID),
			Envelope:  env,
		})
		cur = viewsync.Seq(nextSeq)
	}
}

// nowMs returns the current wall-clock as unix ms. Indirected through
// a package var so tests could swap it; default is time.Now.
var nowMs = realNow

func realNow() int64 { return timeNowFn().UnixMilli() }
