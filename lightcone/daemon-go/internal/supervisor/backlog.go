package supervisor

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
)

// BacklogMessage is the subset of `messages` columns the supervisor
// hands to a freshly spawned worker. It's intentionally narrow — the
// worker's harness will re-read full envelope shape on demand, and
// keeping the SELECT lean avoids paying for blob payload reads at
// supervisor startup.
//
// All time-typed fields are Unix seconds (matches the rest of the
// daemon-go convention).
type BacklogMessage struct {
	Seq         int64
	ID          string
	TS          int64
	ChannelID   string
	SenderKind  string
	SenderID    string
	Kind        string
	Type        string
	Visibility  string
	Audience    string // raw JSON value of the column — `["*"]` or `["agent:x", ...]`
	NotBefore   *int64
	ExpiresAt   *int64
	ParentID    string
}

// BacklogScan executes the L2 §1.4.10 normative SQL — every message
// the supervisor MUST replay through the freshly-spawned worker.
//
// The 5 filters L1 §5.1 trigger gateway demands:
//
//  1. m.seq > actor_cursors.last_consumed_seq         (cursor delta)
//  2. m.visibility != 'system'                         (system events don't trigger)
//  3. m.sender_id != self_actor_id                     (no self-trigger)
//  4. audience is `["*"]` OR contains self             (audience filter)
//  5. (not_before IS NULL OR <= now) AND
//     (expires_at IS NULL OR > now)                    (time window)
//
// Returns rows in seq ASC order. An empty slice means "no backlog —
// enter wait state" per the supervisor pseudocode.
//
// Implementation notes:
//
//   - The JOIN target is actor_cursors.last_consumed_seq. The
//     bootstrap saga (registry.Register) always INSERT OR IGNORE
//     seeds last_consumed_seq=0, so the JOIN is non-empty for any
//     legitimate agent. If the seed somehow missed, the JOIN silently
//     returns 0 rows (caller logs "no backlog") rather than crashing.
//
//   - audience is stored as a JSON string. The first arm
//     `json_extract(m.audience, '$') = '["*"]'` matches the canonical
//     broadcast literal; the second arm walks `json_each` for
//     directed audiences containing self_actor_id.
//
//   - `json_each` and `json_extract` are part of the json1 sqlite
//     extension that modernc.org/sqlite ships built-in (no opt-in
//     pragma needed).
func BacklogScan(
	ctx context.Context,
	q Executor,
	agentID string,
	now int64,
) ([]BacklogMessage, error) {
	if strings.TrimSpace(agentID) == "" {
		return nil, fmt.Errorf("%w: agent_id required", ErrInvalidInput)
	}
	if now <= 0 {
		return nil, fmt.Errorf("%w: now must be positive, got %d", ErrInvalidInput, now)
	}
	if q == nil {
		return nil, fmt.Errorf("%w: executor is nil", ErrInvalidInput)
	}

	rows, err := q.QueryContext(ctx, backlogScanSQL, agentID, agentID, agentID, now, now)
	if err != nil {
		return nil, fmt.Errorf("backlog: scan %s: %w", agentID, err)
	}
	defer func() { _ = rows.Close() }()

	var out []BacklogMessage
	for rows.Next() {
		var m BacklogMessage
		var parent sql.NullString
		var notBefore, expiresAt sql.NullInt64
		if err := rows.Scan(
			&m.Seq, &m.ID, &m.TS, &m.ChannelID,
			&m.SenderKind, &m.SenderID,
			&m.Kind, &m.Type,
			&m.Visibility, &m.Audience,
			&notBefore, &expiresAt,
			&parent,
		); err != nil {
			return nil, fmt.Errorf("backlog: scan row: %w", err)
		}
		if parent.Valid {
			m.ParentID = parent.String
		}
		if notBefore.Valid {
			v := notBefore.Int64
			m.NotBefore = &v
		}
		if expiresAt.Valid {
			v := expiresAt.Int64
			m.ExpiresAt = &v
		}
		out = append(out, m)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("backlog: rows: %w", err)
	}
	return out, nil
}

// backlogScanSQL is the normative L2 §1.4.10 trigger-gateway scan,
// adapted to bind variables in placeholder order:
//
//	?1 = self_actor_id (JOIN predicate)
//	?2 = self_actor_id (sender filter — no self-trigger)
//	?3 = self_actor_id (json_each audience match)
//	?4 = now (not_before <= ?)
//	?5 = now (expires_at > ?)
//
// We keep the SELECT column list explicit (no `m.*`) so the Scan
// target column count stays a code-level invariant.
const backlogScanSQL = `
SELECT m.seq, m.id, m.ts, m.channel_id,
       m.sender_kind, m.sender_id,
       m.kind, m.type,
       m.visibility, m.audience,
       m.not_before, m.expires_at,
       m.parent_id
  FROM messages m
  JOIN actor_cursors c ON c.actor_id = ?
 WHERE m.seq > c.last_consumed_seq
   AND m.visibility != 'system'
   AND m.sender_id  != ?
   AND (
         json_extract(m.audience, '$') = '["*"]'
      OR EXISTS (
           SELECT 1 FROM json_each(m.audience) je
            WHERE je.value = ?
         )
       )
   AND (m.not_before IS NULL OR m.not_before <= ?)
   AND (m.expires_at IS NULL OR m.expires_at  >  ?)
 ORDER BY m.seq ASC
`
