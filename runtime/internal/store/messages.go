package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/wanpengxie/ActOS/protocol/actor"
	"github.com/wanpengxie/ActOS/protocol/message"
	"github.com/wanpengxie/ActOS/runtime/storespec"
)

// messages implements runtime/storespec.MessageLog over the messages table.
//
// Per L2 §1.4.5 engine-append ACL, messages is a PURE PERSISTENCE SINK:
// every caller MUST run the L1 §10.2 9-step Message-Write Harness chain
// FIRST (runtime/harness.Chain). The chain is the only legitimate
// principal that may call Append; every write path flows through
// harness → store. Direct Append calls are a debug-only escape hatch that
// bypasses normalize, sender_kind overwrite, type / schema validation, and
// The One Law uniqueness contract.
//
// Append INSERTs the messages row in one transaction (raises *AppendError
// on the messages.id UNIQUE violation or the terminal-duplicate UNIQUE
// INDEX violation per L2 §1.4.1). envelope.id is a caller-generated random
// uuid correlation anchor — uniqueness is a pure integrity guarantee, NOT
// a dedup/idempotency seam (the v1 at-least-once dedupe machinery was
// retired under v2 caller-scoped closure). There are no same-transaction
// side-row observers (the v1 outbox projection is removed — see newMessages).
//
// IsTerminal is NOT computed here: the harness step 8 derives it from the
// response's Layer-1 final status (proto-layer0 §2.5.1) and hands it to
// Append.
//
// **Protocol contract (FIX-T10):** `env.IsTerminal` is NOT a caller-
// settable knob. It must be resolved by the harness chain (step 8) BEFORE
// Append is reached. Store treats the field as a pre-computed harness
// output and persists it verbatim; it neither validates nor recomputes the
// value, keeping the harness the single source of truth for terminal
// classification.
type messages struct {
	db *sql.DB
	// onCommit is the substrate's post-commit signal source: the append
	// chokepoint authoritatively produces "the log advanced" after a durable
	// commit (Postgres WAL / Kafka offset — commit signal belongs to the log
	// append point, not to any one writer). nil = no subscriber wired. The
	// callback must be non-blocking (a lossy fan-out wake), and it fires AFTER
	// tx.Commit so a woken reader sees the row.
	onCommit func()
}

// NewMessages returns a *messages bound to the channel sqlite.
//
// v2: no fencing — the channel has a SINGLE write path by construction
// (proto-v2-physical §4), so the channel-write fence is obsolete. No outbox
// observer — the store is a pure persistence sink; any fan-out to compute is a
// concern above this layer, not a same-tx side-table projection here.
func newMessages(db *sql.DB, onCommit func()) *messages {
	return &messages{db: db, onCommit: onCommit}
}

// Append implements storespec.MessageLog. The harness supplies the
// pre-computed is_terminal (step 8) since the pure envelope no longer
// carries that store-derived column.
func (m *messages) Append(ctx context.Context, env *message.Envelope, isTerminal bool) (storespec.AppendResult, error) {
	if env == nil {
		return storespec.AppendResult{}, errors.New("store: append nil envelope")
	}
	if env.ID == "" {
		return storespec.AppendResult{}, errors.New("store: append empty envelope.id")
	}
	// FIX-T10 protocol defense: Payload is a REQUIRED field per L0 §2.1
	// (every envelope carries a payload object, even if the body is the
	// empty JSON object `{}`). Silently coercing nil to `{}` masks
	// caller bugs that bypass harness Step 4 normalize
	// and lets non-canonical rows enter the store. Reject loudly so the
	// caller (harness chain) is forced to materialize the payload before
	// reaching the persistence sink.
	if env.Payload == nil {
		return storespec.AppendResult{}, errors.New("store: append nil payload (harness step 4 must materialize payload before reaching store)")
	}

	tx, err := m.db.BeginTx(ctx, nil)
	if err != nil {
		return storespec.AppendResult{}, fmt.Errorf("store: append begin: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	res, err := appendTx(ctx, tx, env, isTerminal)
	if err != nil {
		return storespec.AppendResult{}, err
	}
	if err := tx.Commit(); err != nil {
		return storespec.AppendResult{}, fmt.Errorf("store: append commit: %w", err)
	}
	// Truth advanced durably — fire the substrate commit signal so any tap
	// (delivery pump / client tail) wakes and reads forward. Both write paths
	// reach a commit; this is the harness path's chokepoint.
	if m.onCommit != nil {
		m.onCommit()
	}
	return res, nil
}

// appendTx is the raw INSERT of one envelope row within an existing tx. It
// is an UNEXPORTED package func, NOT a method on an exported type: there is
// deliberately no public "append into this tx" primitive. The only callers
// are Append (which wraps it in its own tx) and the membership control-plane
// op in actors.go (which needs the row + its mirror event in one atomic tx).
// No receiver is taken — it touches only tx, so it can never be a capability
// someone obtains by constructing a *messages.
func appendTx(ctx context.Context, tx *sql.Tx, env *message.Envelope, isTerminal bool) (storespec.AppendResult, error) {
	if tx == nil {
		return storespec.AppendResult{}, errors.New("store: append tx nil")
	}
	if env == nil {
		return storespec.AppendResult{}, errors.New("store: append nil envelope")
	}
	if env.ID == "" {
		return storespec.AppendResult{}, errors.New("store: append empty envelope.id")
	}
	if env.Payload == nil {
		return storespec.AppendResult{}, errors.New("store: append nil payload (harness step 4 must materialize payload before reaching store)")
	}

	// terminal-uniqueness, provisional facet (proto-layer1 §2.8). The final
	// facet is geometry — the ux_terminal_response_per_request UNIQUE INDEX
	// rejects a second final at INSERT. The provisional facet (no provisional
	// after a final) had only a harness pre-check (HasFinalResponse) running in
	// a DIFFERENT tx than this INSERT, so a final committing in the TOCTOU window
	// let a zombie provisional land — same conservation law, one facet geometric
	// and one racy. A provisional INSERT collides with no index, so the only
	// atomic guard is an in-tx re-check on the SAME serialized connection (pool=1
	// pins one conn, so this read sees every committed row). Scoped to a
	// provisional response: terminals are caught by the index, events/requests
	// carry no parent terminal to violate.
	if env.Kind == message.KindResponse && !isTerminal && env.ParentID != "" {
		final, err := finalExistsTx(ctx, tx, env.ParentID)
		if err != nil {
			return storespec.AppendResult{}, err
		}
		if final {
			return storespec.AppendResult{}, &storespec.AppendError{
				Reason:           "harness_provisional_after_final",
				Detail:           "provisional response after final is forbidden for parent: " + string(env.ParentID),
				PartialMessageID: env.ID,
			}
		}
	}

	// INSERT row. env.id is a caller-generated random uuid: uniqueness is a
	// pure integrity constraint, so a collision is an error (no dedup path).
	audJSON, _ := json.Marshal(env.Audience)

	const ins = `INSERT INTO messages (
	   id, ts, ts_received, channel_id,
	   sender_kind, sender_id,
	   kind, type, payload,
	   parent_id, correlation_id,
	   visibility, audience, expires_at,
	   is_terminal
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`

	terminalInt := 0
	if isTerminal {
		terminalInt = 1
	}
	res, err := tx.ExecContext(ctx, ins,
		env.ID, env.TS, env.TSReceived, env.ChannelID,
		string(env.Sender.Kind), string(env.Sender.ID),
		string(env.Kind), env.Type, string(env.Payload),
		nullableString(string(env.ParentID)), nullableString(string(env.CorrelationID)),
		env.Visibility, string(audJSON),
		nullableInt(env.ExpiresAt),
		terminalInt,
	)
	if err != nil {
		return storespec.AppendResult{}, classifyAppendErr(err, string(env.ID))
	}
	seq, _ := res.LastInsertId()
	return storespec.AppendResult{Seq: storespec.Seq(seq)}, nil
}

// MaxSeq returns the highest committed seq in this channel's message log
// (0 when empty).
func (m *messages) MaxSeq(ctx context.Context) (int64, error) {
	const q = `SELECT COALESCE(MAX(seq), 0) FROM messages`
	var seq int64
	if err := m.db.QueryRowContext(ctx, q).Scan(&seq); err != nil {
		return 0, fmt.Errorf("store: max seq: %w", err)
	}
	return seq, nil
}

// ReadAfterSeq returns up to `limit` envelopes with seq > afterSeq for the
// channel, in ascending seq order. seq is the monotonic ordering guarantee:
// reading forward from a cursor never skips a committed row.
func (m *messages) ReadAfterSeq(ctx context.Context, afterSeq int64, limit int) ([]storespec.StoredRow, error) {
	if limit <= 0 {
		limit = 256
	}
	const q = `SELECT id, ts, ts_received, channel_id,
	                  sender_kind, sender_id,
	                  kind, type, payload,
	                  COALESCE(parent_id,''), COALESCE(correlation_id,''),
	                  visibility, audience,
	                  expires_at,
	                  is_terminal, seq
	             FROM messages
	             WHERE seq > ?
	             ORDER BY seq ASC LIMIT ?`
	rows, err := m.db.QueryContext(ctx, q, afterSeq, limit)
	if err != nil {
		return nil, fmt.Errorf("store: read after seq: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var out []storespec.StoredRow
	for rows.Next() {
		env, err := scanEnvelopeRows(rows)
		if err != nil {
			return nil, fmt.Errorf("store: read after seq scan: %w", err)
		}
		out = append(out, env)
	}
	return out, rows.Err()
}

// OpenRequestsForActor returns ALL open request rows whose first audience
// member is actorID and that have no terminal response yet — regardless of
// expires_at. It is unbounded by construction: closing a dead actor's
// in-flight requests must drain EVERY one of them (a limit would leave the
// overflow callers hanging — broken closure). The store reports only the open
// set it positively holds; it never guesses "slow" (construction-spec §3.3).
func (m *messages) OpenRequestsForActor(ctx context.Context, actorID actor.ActorID) ([]storespec.StoredRow, error) {
	const q = `SELECT id, ts, ts_received, channel_id,
	                  sender_kind, sender_id,
	                  kind, type, payload,
	                  COALESCE(parent_id,''), COALESCE(correlation_id,''),
	                  visibility, audience,
	                  expires_at,
	                  is_terminal, seq
	             FROM messages m
	             WHERE m.kind = 'request'
	               AND m.is_terminal = 0
	               AND json_extract(m.audience, '$[0]') = ?
	               AND NOT EXISTS (
	                 SELECT 1 FROM messages r
	                  WHERE r.parent_id = m.id
	                    AND r.kind = 'response'
	                    AND r.is_terminal = 1
	               )
	             ORDER BY m.seq ASC`
	rows, err := m.db.QueryContext(ctx, q, string(actorID))
	if err != nil {
		return nil, fmt.Errorf("store: open requests for actor: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var out []storespec.StoredRow
	for rows.Next() {
		env, err := scanEnvelopeRows(rows)
		if err != nil {
			return nil, fmt.Errorf("store: open requests for actor scan: %w", err)
		}
		out = append(out, env)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: open requests for actor rows: %w", err)
	}
	return out, nil
}

// DistinctOpenRequestReceivers returns the DISTINCT first-audience receivers of
// every still-open request — the truth-derived view the closure reconciler
// scans. Same open-set predicate as OpenRequestsForActor (kind=request,
// is_terminal=0, no terminal response yet), grouped to its receiver. Unbounded
// by construction (the reconciler must consider every receiver with an orphan
// candidate; a cap would silently leave some receivers' callers hanging).
func (m *messages) DistinctOpenRequestReceivers(ctx context.Context) ([]actor.ActorID, error) {
	const q = `SELECT DISTINCT json_extract(m.audience, '$[0]') AS receiver
	             FROM messages m
	            WHERE m.kind = 'request'
	              AND m.is_terminal = 0
	              AND receiver IS NOT NULL
	              AND NOT EXISTS (
	                SELECT 1 FROM messages r
	                 WHERE r.parent_id = m.id
	                   AND r.kind = 'response'
	                   AND r.is_terminal = 1
	              )`
	rows, err := m.db.QueryContext(ctx, q)
	if err != nil {
		return nil, fmt.Errorf("store: distinct open request receivers: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var out []actor.ActorID
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("store: distinct open request receivers scan: %w", err)
		}
		out = append(out, actor.ActorID(id))
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: distinct open request receivers rows: %w", err)
	}
	return out, nil
}

// HasFinalResponse implements storespec.MessageLog. Returns true when at
// least one kind=response row exists for parent_id=parentID with the
// row's is_terminal column set — store layer has already materialised
// the (kind==response && payload.status ∈ {completed, failed})
// derivation per proto-layer0 §2.5.1 + L2 §1.4.1, so the bit is the
// canonical "final exists" answer.
//
// Used by harness Step 8 (proto-layer1 §2.8) to distinguish
// final-after-final from provisional-after-final. The
// `ux_terminal_response_per_request` UNIQUE INDEX guards final-after-
// final at INSERT time; this query is the pre-check that lets the
// harness reject provisional-after-final with the correct closed-set
// reason instead of silently appending a zombie row.
func (m *messages) HasFinalResponse(ctx context.Context, parentID message.ID) (bool, error) {
	if parentID == "" {
		return false, nil
	}
	return finalExistsQuery(ctx, m.db, parentID)
}

// finalQuery is the single "a final response exists for parentID" predicate,
// shared by the harness pre-check (HasFinalResponse, off-tx) and the in-tx
// re-check inside appendTx. One SQL string, two run contexts, so the pre-check
// and the authoritative atomic guard can never drift apart.
const finalQuery = `SELECT 1 FROM messages
	            WHERE parent_id = ?
	              AND kind = 'response'
	              AND is_terminal = 1
	            LIMIT 1`

// rowQuerier abstracts *sql.DB / *sql.Tx for the shared final-existence query.
type rowQuerier interface {
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

func finalExistsQuery(ctx context.Context, q rowQuerier, parentID message.ID) (bool, error) {
	var one int
	switch err := q.QueryRowContext(ctx, finalQuery, parentID).Scan(&one); {
	case err == nil:
		return true, nil
	case errors.Is(err, sql.ErrNoRows):
		return false, nil
	default:
		return false, fmt.Errorf("store: has final response: %w", err)
	}
}

// finalExistsTx runs the final-existence predicate on an open tx — the atomic
// guard appendTx uses to close the provisional-after-final TOCTOU window.
func finalExistsTx(ctx context.Context, tx *sql.Tx, parentID message.ID) (bool, error) {
	return finalExistsQuery(ctx, tx, parentID)
}

// FindByID implements storespec.MessageLog.
func (m *messages) FindByID(ctx context.Context, id message.ID) (*storespec.StoredRow, bool, error) {
	const q = `SELECT id, ts, ts_received, channel_id,
	                  sender_kind, sender_id,
	                  kind, type, payload,
	                  COALESCE(parent_id,''), COALESCE(correlation_id,''),
	                  visibility, audience,
	                  expires_at,
	                  is_terminal, seq
	             FROM messages WHERE id=?`
	row := m.db.QueryRowContext(ctx, q, id)
	sr, err := scanEnvelope(row)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	return &sr, true, nil
}

// rowScanner abstracts *sql.Row / *sql.Rows for the Scan call so the
// multi-row read paths can share the materialization code with FindByID.
type rowScanner interface {
	Scan(dest ...any) error
}

// scanEnvelope materializes a row into a StoredRow.
func scanEnvelope(row *sql.Row) (storespec.StoredRow, error) {
	return scanEnvelopeFrom(row)
}

// scanEnvelopeRows materializes the current *sql.Rows position into a
// StoredRow. Caller is responsible for rows.Next() / rows.Close().
func scanEnvelopeRows(rows *sql.Rows) (storespec.StoredRow, error) {
	return scanEnvelopeFrom(rows)
}

// scanEnvelopeFrom is the shared implementation. It returns a StoredRow:
// the pure Envelope plus the store-derived columns (seq / is_terminal)
// that kernel keeps off the envelope.
func scanEnvelopeFrom(s rowScanner) (storespec.StoredRow, error) {
	var sr storespec.StoredRow
	env := &sr.Envelope
	var kind, sKind, senderID, vis string
	var audJSON, payloadStr string
	var expiresAt sql.NullInt64
	var termInt int
	if err := s.Scan(
		&env.ID, &env.TS, &env.TSReceived, &env.ChannelID,
		&sKind, &senderID,
		&kind, &env.Type, &payloadStr,
		&env.ParentID, &env.CorrelationID,
		&vis, &audJSON,
		&expiresAt,
		&termInt, &sr.Seq,
	); err != nil {
		return storespec.StoredRow{}, err
	}
	sk, ok := actor.ParseKind(sKind)
	if !ok {
		return storespec.StoredRow{}, fmt.Errorf("store: scan invalid sender kind %q (out of closed set)", sKind)
	}
	env.Sender.Kind = sk
	env.Sender.ID = actor.ActorID(senderID)
	mk, ok := message.ParseKind(kind)
	if !ok {
		return storespec.StoredRow{}, fmt.Errorf("store: scan invalid message kind %q (out of closed set)", kind)
	}
	env.Kind = mk
	mv, ok := message.ParseVisibility(vis)
	if !ok {
		return storespec.StoredRow{}, fmt.Errorf("store: scan invalid visibility %q (out of closed set)", vis)
	}
	env.Visibility = mv
	env.Payload = json.RawMessage(payloadStr)
	if err := json.Unmarshal([]byte(audJSON), &env.Audience); err != nil {
		return storespec.StoredRow{}, fmt.Errorf("store: scan audience: %w", err)
	}
	if expiresAt.Valid {
		v := expiresAt.Int64
		env.ExpiresAt = &v
	}
	sr.IsTerminal = termInt == 1
	return sr, nil
}

func nullableString(s string) any {
	if s == "" {
		return nil
	}
	return s
}

func nullableInt(p *int64) any {
	if p == nil {
		return nil
	}
	return *p
}

// classifyAppendErr maps sqlite UNIQUE constraint failures to typed
// *AppendError so the harness chain can map them to HarnessRejectReason.
func classifyAppendErr(err error, envID string) error {
	if err == nil {
		return nil
	}
	msg := err.Error()
	switch {
	case strings.Contains(msg, "UNIQUE constraint failed: messages.id"):
		return &storespec.AppendError{
			Reason:           "harness_id_duplicate_conflict",
			Detail:           msg,
			PartialMessageID: message.ID(envID),
		}
	case strings.Contains(msg, "ux_terminal_response_per_request") ||
		strings.Contains(msg, "UNIQUE constraint failed: messages.parent_id") ||
		strings.Contains(msg, "parent_id, kind, is_terminal"):
		return &storespec.AppendError{
			Reason:           "harness_terminal_duplicate",
			Detail:           msg,
			PartialMessageID: message.ID(envID),
		}
	default:
		return fmt.Errorf("store: append insert: %w", err)
	}
}
