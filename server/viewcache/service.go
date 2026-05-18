// Package viewcache holds the server-side view of the daemon's
// channel log — apply (in-order, out-of-order, duplicate), gap
// detection, the closed-interval Resync protocol, and the cursor +
// messages tables defined by L1 §8 + T1.1 + T1.8.
//
// Authoritative spec: .dalek/pm/m1.5-tickets.md §T6 (viewcache) +
// kernel/viewsync (Pusher / Receiver / Resyncer contracts).
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
	"sort"
	"sync"

	"github.com/wanpengxie/ActOS/kernel/channel"
	"github.com/wanpengxie/ActOS/kernel/message"
	"github.com/wanpengxie/ActOS/kernel/viewsync"
)

// Resyncer is what viewcache calls when it detects a gap.
// runtime/transit on the daemon side implements ServeResync; the
// gateway wires a daemonbus client that satisfies RequestResync to
// hand it here. Tests pass a fake.
type Resyncer interface {
	RequestResync(ctx context.Context, channelID channel.ID, since, until viewsync.Seq) ([]viewsync.ResyncMessage, error)
}

// Service is the viewcache facade.
type Service struct {
	db *sql.DB

	mu      sync.Mutex
	buffers map[channel.ID]*channelBuffer

	resyncer Resyncer

	// fireResyncFn overrides the default fire-and-forget resync
	// goroutine — set by tests via SetFireResyncForTest so they can
	// observe trigger invocations synchronously without spawning real
	// daemon RPC. nil → use the default goroutine path.
	fireResyncFn func(channelID channel.ID, since, until viewsync.Seq)
}

type channelBuffer struct {
	mu      sync.Mutex
	pending map[viewsync.Seq]viewsync.PushFrame
}

// NewService constructs a Service. Resyncer can be wired later via
// SetResyncer.
func NewService(db *sql.DB) *Service {
	return &Service{
		db:      db,
		buffers: map[channel.ID]*channelBuffer{},
	}
}

// SetResyncer plugs in the gap-recovery RPC client.
func (s *Service) SetResyncer(r Resyncer) { s.resyncer = r }

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
	buf.mu.Lock()
	switch outcome {
	case viewsync.ApplyOutcomeGap:
		buf.pending[frame.Seq] = frame
	case viewsync.ApplyOutcomeContiguous:
		for seq := range buf.pending {
			if seq <= newCur {
				delete(buf.pending, seq)
			}
		}
	}
	buf.mu.Unlock()

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
	if outcome == viewsync.ApplyOutcomeGap && gapSince <= gapUntil {
		switch {
		case s.fireResyncFn != nil:
			s.fireResyncFn(frame.ChannelID, gapSince, gapUntil)
		case s.resyncer != nil:
			go s.fireResync(frame.ChannelID, gapSince, gapUntil)
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
	_, _ = s.TriggerResync(context.Background(), channelID, since, until)
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
	msgs, err := s.resyncer.RequestResync(ctx, channelID, since, until)
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
	cur, err := s.Cursor(ctx, channelID)
	if err != nil {
		return 0, err
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
			MessageID: messageID,
			Envelope:  env,
		})
		cur = viewsync.Seq(nextSeq)
	}
}

// nowMs returns the current wall-clock as unix ms. Indirected through
// a package var so tests could swap it; default is time.Now.
var nowMs = realNow

func realNow() int64 { return timeNowFn().UnixMilli() }
