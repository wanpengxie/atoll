package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
)

// MigrationReport summarises a `migrate from-node` run. Returned to the
// CLI so operators can confirm row counts + see which legacy types were
// intentionally dropped (vs the typemap rejecting an unknown row).
type MigrationReport struct {
	// SourceRows counts the rows iterated from the Node sqlite.
	SourceRows int
	// InsertedRows counts the rows successfully written to v4 sqlite.
	InsertedRows int
	// DroppedTypes counts each legacy payload_type explicitly removed
	// by §4.1.3 (dispatch.self_check_due, cron.tick).
	DroppedTypes map[string]int
}

// NodeRow mirrors the legacy `messages.sqlite` row shape needed for
// migration. Columns not consulted by the rewrite logic (envelope_json,
// payload_json, origin, created_at, last_attempt_at) are intentionally
// omitted — the new schema does not store them.
type NodeRow struct {
	ID            string
	TS            int64
	TSReceived    int64
	ChannelID     string
	SenderKind    string
	SenderID      string
	SenderName    sql.NullString
	PayloadType   string
	PayloadBody   string
	ParentID      sql.NullString
	CorrelationID sql.NullString
	TaskID        sql.NullString
	Audience      string
	Mentions      sql.NullString
	NotBefore     sql.NullInt64
	ExpiresAt     sql.NullInt64
	DeliveredAt   sql.NullInt64
	DeliveryFail  sql.NullInt64
	Attempts      sql.NullInt64
	LastError     sql.NullString
}

// MigrateFromNode reads every row of the legacy `messages` table in src,
// transforms it per §4.1 rules, and INSERTs into dst's v4 `messages`
// table. dst MUST be a freshly-opened channel sqlite (built by
// OpenChannel) with empty messages.
//
// The two DBs are kept separate so the source file can be opened
// read-only and the destination can enforce v4 CHECK constraints.
//
// Errors from the typemap (unknown legacy types) abort the run — we
// fail loudly rather than silently drop data.
func MigrateFromNode(ctx context.Context, src, dst *sql.DB) (MigrationReport, error) {
	if src == nil || dst == nil {
		return MigrationReport{}, errors.New("store: MigrateFromNode requires non-nil src + dst")
	}

	report := MigrationReport{DroppedTypes: make(map[string]int)}

	rows, err := src.QueryContext(ctx, `
		SELECT id, ts, ts_received, channel_id, sender_kind, sender_id, sender_name,
		       payload_type, payload_body, parent_id, correlation_id, task_id,
		       audience, mentions, not_before, expires_at,
		       delivered_at, delivery_failed_at, delivery_attempts, last_error
		  FROM messages
		 ORDER BY ts_received ASC, id ASC
	`)
	if err != nil {
		return report, fmt.Errorf("store: scan Node messages: %w", err)
	}
	defer func() { _ = rows.Close() }()

	// We want every new row to land with a fresh seq. AUTOINCREMENT on
	// the dst messages PK handles that automatically as long as we
	// INSERT in source order — which is exactly what the SELECT order
	// guarantees.
	insertStmt, err := dst.PrepareContext(ctx, insertV4MessageSQL)
	if err != nil {
		return report, fmt.Errorf("store: prepare INSERT: %w", err)
	}
	defer func() { _ = insertStmt.Close() }()

	for rows.Next() {
		var n NodeRow
		if err := rows.Scan(
			&n.ID, &n.TS, &n.TSReceived, &n.ChannelID, &n.SenderKind, &n.SenderID, &n.SenderName,
			&n.PayloadType, &n.PayloadBody, &n.ParentID, &n.CorrelationID, &n.TaskID,
			&n.Audience, &n.Mentions, &n.NotBefore, &n.ExpiresAt,
			&n.DeliveredAt, &n.DeliveryFail, &n.Attempts, &n.LastError,
		); err != nil {
			return report, fmt.Errorf("store: scan row: %w", err)
		}
		report.SourceRows++

		bodyType, _ := extractBodyType(n.PayloadBody)
		mapping, dropped, err := MapType(n.PayloadType, bodyType)
		if err != nil {
			return report, fmt.Errorf("store: row %q: %w", n.ID, err)
		}
		if dropped {
			report.DroppedTypes[n.PayloadType]++
			continue
		}

		v4, err := buildV4Row(n, mapping)
		if err != nil {
			return report, fmt.Errorf("store: row %q transform: %w", n.ID, err)
		}

		if _, err := insertStmt.ExecContext(ctx,
			v4.ID, v4.TS, v4.TSReceived, v4.ChannelID, v4.SenderKind, v4.SenderID, v4.SenderName,
			v4.Kind, v4.Type, v4.Payload, v4.ParentID, v4.CorrelationID, v4.DocRefs,
			v4.Visibility, v4.Audience, v4.NotBefore, v4.ExpiresAt,
			v4.DeliveredAt, v4.DeliveryFail, v4.LastError, v4.Attempts, v4.IsTerminal,
		); err != nil {
			return report, fmt.Errorf("store: insert %q: %w", v4.ID, err)
		}
		report.InsertedRows++
	}
	if err := rows.Err(); err != nil {
		return report, fmt.Errorf("store: iterate rows: %w", err)
	}
	return report, nil
}

// v4Row is the transformed shape destined for the v4 messages table.
// All `any` fields cooperate with sql.NullX → driver.Value coercion.
type v4Row struct {
	ID            string
	TS            int64
	TSReceived    int64
	ChannelID     string
	SenderKind    string
	SenderID      string
	SenderName    any // sql.NullString or nil
	Kind          string
	Type          string
	Payload       string
	ParentID      any // string or nil
	CorrelationID any // string or nil
	DocRefs       any // JSON string or nil
	Visibility    string
	Audience      string // JSON array
	NotBefore     any
	ExpiresAt     any
	DeliveredAt   any
	DeliveryFail  any
	LastError     any
	Attempts      int64
	IsTerminal    int
}

const insertV4MessageSQL = `
INSERT INTO messages
  (id, ts, ts_received, channel_id, sender_kind, sender_id, sender_name,
   kind, type, payload, parent_id, correlation_id, doc_refs,
   visibility, audience, not_before, expires_at,
   delivered_at, delivery_failed_at, last_error, attempts, is_terminal)
VALUES
  (?, ?, ?, ?, ?, ?, ?,
   ?, ?, ?, ?, ?, ?,
   ?, ?, ?, ?,
   ?, ?, ?, ?, ?)
`

// buildV4Row applies §4.1.2 column-by-column rewrite + the per-type
// payload/audience/visibility/doc_refs overlays from `mapping`.
func buildV4Row(n NodeRow, mapping TypeMapping) (v4Row, error) {
	// --- payload: parse, strip body.type, overlay, re-marshal ---
	payloadObj, err := parsePayload(n.PayloadBody)
	if err != nil {
		return v4Row{}, err
	}
	stripBodyType(payloadObj)
	for k, v := range mapping.PayloadOverlay {
		// Overlays merge at the top level of payload (not into body).
		payloadObj[k] = v
	}
	payloadBytes, err := json.Marshal(payloadObj)
	if err != nil {
		return v4Row{}, fmt.Errorf("re-marshal payload: %w", err)
	}

	// --- audience: derive new JSON array per §4.1.2 ---
	audience := deriveAudience(n, mapping)

	// --- visibility: explicit override > self → private > public ---
	visibility := mapping.OverrideVisibility
	if visibility == "" {
		if n.Audience == "self" {
			visibility = VisibilityPrivate
		} else {
			visibility = VisibilityPublic
		}
	}

	// --- correlation_id: backfill from task_id when v4 corr is empty ---
	correlationID := nullStrToAny(n.CorrelationID)
	if !n.CorrelationID.Valid || n.CorrelationID.String == "" {
		if n.TaskID.Valid && n.TaskID.String != "" {
			correlationID = n.TaskID.String
		}
	}

	// --- doc_refs: lift from payload.body.<field> when mapping requests ---
	// The legacy schema nests business fields under `body`; the field
	// name (`doc_ref`, `path`, ...) is read from there. v4 hoists this
	// value into the envelope-level `doc_refs` JSON array.
	var docRefs any
	if field := mapping.DocRefsFromBodyField; field != "" {
		if body, ok := payloadObj["body"].(map[string]any); ok {
			if v, ok := body[field]; ok {
				if s, ok := v.(string); ok && s != "" {
					bs, err := json.Marshal([]string{s})
					if err != nil {
						return v4Row{}, fmt.Errorf("marshal doc_refs: %w", err)
					}
					docRefs = string(bs)
				}
			}
		}
	}

	out := v4Row{
		ID:            n.ID,
		TS:            n.TS,
		TSReceived:    n.TSReceived,
		ChannelID:     n.ChannelID,
		SenderKind:    coerceSenderKind(n.SenderKind),
		SenderID:      n.SenderID,
		SenderName:    nullStrToAny(n.SenderName),
		Kind:          mapping.Kind,
		Type:          mapping.NewType,
		Payload:       string(payloadBytes),
		ParentID:      nullStrToAny(n.ParentID),
		CorrelationID: correlationID,
		DocRefs:       docRefs,
		Visibility:    visibility,
		Audience:      audience,
		NotBefore:     nullInt64ToAny(n.NotBefore),
		ExpiresAt:     nullInt64ToAny(n.ExpiresAt),
		DeliveredAt:   nullInt64ToAny(n.DeliveredAt),
		DeliveryFail:  nullInt64ToAny(n.DeliveryFail),
		LastError:     nullStrToAny(n.LastError),
		Attempts:      coerceAttempts(n.Attempts),
		IsTerminal:    mapping.IsTerminal,
	}
	return out, nil
}

// extractBodyType pulls payload.body.type from the legacy payload_body
// JSON. The Node daemon nests the business sub-type one level deep:
//
//	{ "body": { "type": "xhs.publish", "title": "..." } }
//
// Returns ("", false) if the field is absent — dispatch.* migrations
// then surface "unknown dispatch.start body.type" upstream.
func extractBodyType(raw string) (string, bool) {
	if raw == "" {
		return "", false
	}
	var obj map[string]any
	if err := json.Unmarshal([]byte(raw), &obj); err != nil {
		return "", false
	}
	body, ok := obj["body"].(map[string]any)
	if !ok {
		return "", false
	}
	t, ok := body["type"].(string)
	if !ok || t == "" {
		return "", false
	}
	return t, true
}

// parsePayload unmarshals the legacy payload_body JSON, returning an
// empty object when the column is empty/null. We don't fail on bad
// JSON because the upstream invariant is "payload_body is valid JSON";
// reaching this with bad JSON indicates a corrupt source.
func parsePayload(raw string) (map[string]any, error) {
	if raw == "" {
		return map[string]any{}, nil
	}
	var obj map[string]any
	if err := json.Unmarshal([]byte(raw), &obj); err != nil {
		return nil, fmt.Errorf("parse payload_body: %w", err)
	}
	if obj == nil {
		return map[string]any{}, nil
	}
	return obj, nil
}

// stripBodyType removes the redundant `body.type` field — in v4 the
// `type` lives on the envelope, not nested inside payload.
func stripBodyType(payload map[string]any) {
	body, ok := payload["body"].(map[string]any)
	if !ok {
		return
	}
	delete(body, "type")
}

// deriveAudience implements the §4.1.2 audience rewrite (single string
// → JSON array) plus the §4.1.2 mentions union rule.
//
// Branches (in evaluation order):
//  1. Mapping forces audience=[<sender.id>]      (self.memo)
//  2. Old audience='self'                         → [<sender.id>]
//     • sender.id empty → fall back to ["*"]
//  3. Old audience='channel'                      → ["*"]
//     • + mentions present                        → ["*"] ∪ mentions
//  4. Old audience starts with 'external:'         → [audience]
//  5. Anything else (bare actor id, defensive)    → [audience]
func deriveAudience(n NodeRow, mapping TypeMapping) string {
	if mapping.OverrideAudienceSelf {
		if n.SenderID != "" {
			return mustJSONArray([]string{n.SenderID})
		}
		return `["*"]`
	}

	switch n.Audience {
	case "self":
		if n.SenderID != "" {
			return mustJSONArray([]string{n.SenderID})
		}
		return `["*"]`
	case "channel":
		mentions := parseMentions(n.Mentions)
		if len(mentions) == 0 {
			return `["*"]`
		}
		all := append([]string{"*"}, mentions...)
		return mustJSONArray(all)
	}
	return mustJSONArray([]string{n.Audience})
}

// parseMentions decodes the legacy `mentions` column (JSON array of
// strings, or NULL).
func parseMentions(nm sql.NullString) []string {
	if !nm.Valid || nm.String == "" {
		return nil
	}
	var arr []string
	if err := json.Unmarshal([]byte(nm.String), &arr); err != nil {
		return nil
	}
	return arr
}

// coerceSenderKind maps the legacy 'external' sender kind onto v4's
// 'tool' (closest semantic — external adapter integrations land in v4
// as tool actors). Other values pass through; an unknown kind survives
// to the v4 CHECK constraint where it surfaces as an INSERT error.
func coerceSenderKind(in string) string {
	if in == "external" {
		return "tool"
	}
	return in
}

// coerceAttempts maps the legacy `delivery_attempts` (nullable int) onto
// the v4 `attempts INTEGER NOT NULL DEFAULT 0` shape.
func coerceAttempts(in sql.NullInt64) int64 {
	if !in.Valid {
		return 0
	}
	return in.Int64
}

func nullStrToAny(n sql.NullString) any {
	if !n.Valid || n.String == "" {
		return nil
	}
	return n.String
}

func nullInt64ToAny(n sql.NullInt64) any {
	if !n.Valid {
		return nil
	}
	return n.Int64
}

func mustJSONArray(xs []string) string {
	b, err := json.Marshal(xs)
	if err != nil {
		// Encoding a []string never errors in practice; surface a
		// safe default rather than panic the migrate tool.
		return `[]`
	}
	return string(b)
}
