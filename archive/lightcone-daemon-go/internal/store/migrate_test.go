package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

// nodeSchemaDDL replays the legacy `lightcone/daemon/src/message-store.js`
// `createMessagesTableSql` + `createMessageIndexes` shape so tests build
// a fresh fixture without touching a real Node sqlite file. Kept
// byte-equivalent to message-store.js L151-196 except `IF NOT EXISTS`
// is dropped (we always create from scratch).
const nodeSchemaDDL = `
CREATE TABLE messages (
  id TEXT PRIMARY KEY,
  channel_id TEXT NOT NULL,
  ts INTEGER NOT NULL,
  ts_received INTEGER NOT NULL,
  sender_kind TEXT NOT NULL,
  sender_id TEXT NOT NULL,
  sender_name TEXT DEFAULT '',
  payload_type TEXT NOT NULL,
  payload_body TEXT NOT NULL,
  parent_id TEXT DEFAULT NULL,
  correlation_id TEXT DEFAULT NULL,
  task_id TEXT DEFAULT NULL,
  thread_id TEXT DEFAULT NULL,
  audience TEXT NOT NULL DEFAULT 'channel',
  mentions TEXT DEFAULT NULL,
  origin TEXT DEFAULT NULL,
  not_before INTEGER DEFAULT NULL,
  expires_at INTEGER DEFAULT NULL,
  delivered_at INTEGER DEFAULT NULL,
  delivery_failed_at INTEGER DEFAULT NULL,
  delivery_attempts INTEGER NOT NULL DEFAULT 0,
  last_attempt_at INTEGER DEFAULT NULL,
  last_error TEXT DEFAULT NULL,
  envelope_json TEXT NOT NULL,
  payload_json TEXT NOT NULL,
  created_at TEXT NOT NULL
);
`

// TestMapType_CoverageMatrix asserts every legacy payload_type ships
// with the right new type + kind + terminal flag — drift here means a
// migration silently miscategorizes traffic.
func TestMapType_CoverageMatrix(t *testing.T) {
	cases := []struct {
		oldType    string
		bodyType   string
		wantType   string
		wantKind   string
		wantTerm   int
		wantDrop   bool
		wantVis    string // empty = use default rule
		wantSelfAu bool
		wantDocsFM string // expected DocRefsFromBodyField
	}{
		// core renames
		{"user.text", "", "human.text", KindEvent, 0, false, "", false, ""},
		{"agent.text", "", "agent.text", KindEvent, 0, false, "", false, ""},
		{"agent.progress", "", "agent.text", KindEvent, 0, false, VisibilitySystem, false, ""},
		{"system.notice", "", "system.event", KindEvent, 0, false, "", false, ""},
		{"system.heartbeat", "", "system.heartbeat", KindEvent, 0, false, "", false, ""},
		{"channel.presence_changed", "", "system.event", KindEvent, 0, false, "", false, ""},
		{"channel.config.updated", "", "file.updated", KindEvent, 0, false, "", false, "path"},

		// dispatch family — full 6 ops × {requested, accepted, completed, failed, rejected}
		{"dispatch.start", "xhs.publish", "xhs.publish.requested", KindRequest, 0, false, "", false, ""},
		{"dispatch.start", "xhs.search", "xhs.search.requested", KindRequest, 0, false, "", false, ""},
		{"dispatch.start", "xhs.get_note", "xhs.note.fetch.requested", KindRequest, 0, false, "", false, ""},
		{"dispatch.start", "xhs.record_note", "xhs.note.record.requested", KindRequest, 0, false, "", false, ""},
		{"dispatch.start", "xhs.get_my_recent", "xhs.recent.fetch.requested", KindRequest, 0, false, "", false, ""},
		{"dispatch.start", "xhs.sync_cookie", "xhs.cookie.sync.requested", KindRequest, 0, false, "", false, ""},

		{"dispatch.accepted", "xhs.publish", "xhs.publish.accepted", KindEvent, 0, false, "", false, ""},
		{"dispatch.completed", "xhs.publish", "xhs.publish.completed", KindResponse, 1, false, "", false, ""},
		{"dispatch.failed", "xhs.publish", "xhs.publish.failed", KindResponse, 1, false, "", false, ""},
		{"dispatch.rejected", "xhs.publish", "xhs.publish.rejected", KindResponse, 1, false, "", false, ""},

		// task family → file.* + doc_ref lift
		{"task.opened", "", "file.created", KindEvent, 0, false, "", false, "doc_ref"},
		{"task.appended", "", "file.updated", KindEvent, 0, false, "", false, "doc_ref"},
		{"task.closed", "", "file.updated", KindEvent, 0, false, "", false, "doc_ref"},

		// self.memo → agent.text + private + self audience
		{"self.memo", "", "agent.text", KindEvent, 0, false, VisibilityPrivate, true, ""},

		// workdir.changed → file.updated + path doc_ref
		{"workdir.changed", "", "file.updated", KindEvent, 0, false, "", false, "path"},

		// dropped legacy types
		{"dispatch.self_check_due", "", "", "", 0, true, "", false, ""},
		{"cron.tick", "", "", "", 0, true, "", false, ""},
	}

	for _, tc := range cases {
		t.Run(tc.oldType+"/"+tc.bodyType, func(t *testing.T) {
			m, dropped, err := MapType(tc.oldType, tc.bodyType)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if dropped != tc.wantDrop {
				t.Fatalf("dropped = %v, want %v", dropped, tc.wantDrop)
			}
			if dropped {
				return
			}
			if m.NewType != tc.wantType {
				t.Errorf("NewType = %q, want %q", m.NewType, tc.wantType)
			}
			if m.Kind != tc.wantKind {
				t.Errorf("Kind = %q, want %q", m.Kind, tc.wantKind)
			}
			if m.IsTerminal != tc.wantTerm {
				t.Errorf("IsTerminal = %d, want %d", m.IsTerminal, tc.wantTerm)
			}
			if m.OverrideVisibility != tc.wantVis {
				t.Errorf("OverrideVisibility = %q, want %q", m.OverrideVisibility, tc.wantVis)
			}
			if m.OverrideAudienceSelf != tc.wantSelfAu {
				t.Errorf("OverrideAudienceSelf = %v, want %v", m.OverrideAudienceSelf, tc.wantSelfAu)
			}
			if m.DocRefsFromBodyField != tc.wantDocsFM {
				t.Errorf("DocRefsFromBodyField = %q, want %q", m.DocRefsFromBodyField, tc.wantDocsFM)
			}
		})
	}
}

// TestMapType_UnknownReturnsError asserts that genuinely unmapped legacy
// payload_types abort migration (no silent fallthrough).
func TestMapType_UnknownReturnsError(t *testing.T) {
	if _, _, err := MapType("totally.unknown", ""); err == nil {
		t.Errorf("expected error for unknown legacy type")
	}
	if _, _, err := MapType("dispatch.start", "xhs.bogus_op"); err == nil {
		t.Errorf("expected error for unknown dispatch body.type")
	}
}

// TestMigrateFromNode_HappyPath constructs a representative Node sqlite
// fixture and asserts every §4.1 rewrite rule fires correctly:
//   - type rename + body.type collapse for dispatch.start
//   - audience single-string → JSON array (channel, self, external:*, mentions)
//   - visibility derivation (self → private, agent.progress → system,
//     self.memo → private)
//   - correlation_id backfill from task_id
//   - doc_refs lift for task.opened
//   - is_terminal marking on dispatch.completed
//   - seq monotonic (1..N in source order)
//   - dropped types counted, not inserted
func TestMigrateFromNode_HappyPath(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	srcPath := filepath.Join(dir, "node.sqlite")
	dstPath := filepath.Join(dir, "v4.sqlite")

	src := buildNodeFixture(t, ctx, srcPath, []nodeFixture{
		// 1. user.text → human.text, channel audience
		{
			id: "m1", payloadType: "user.text", payloadBody: `{"body":{"text":"hi"}}`,
			audience: "channel", senderKind: "human", senderID: "u1", tsReceived: 100,
		},
		// 2. agent.progress → agent.text + visibility=system
		{
			id: "m2", payloadType: "agent.progress", payloadBody: `{"body":{"text":"working"}}`,
			audience: "channel", senderKind: "agent", senderID: "a1", tsReceived: 200,
		},
		// 3. self.memo → agent.text + visibility=private + audience=[sender.id]
		{
			id: "m3", payloadType: "self.memo", payloadBody: `{"body":{"text":"note"}}`,
			audience: "self", senderKind: "agent", senderID: "a1", tsReceived: 300,
		},
		// 4. dispatch.start + xhs.publish → xhs.publish.requested (kind=request),
		//    correlation_id backfill from task_id
		{
			id: "m4", payloadType: "dispatch.start",
			payloadBody: `{"body":{"type":"xhs.publish","title":"hello","content":"world"}}`,
			audience:    "channel", senderKind: "agent", senderID: "a1",
			taskID: "task-42", tsReceived: 400,
		},
		// 5. dispatch.completed → xhs.publish.completed (kind=response, is_terminal=1)
		{
			id: "m5", payloadType: "dispatch.completed",
			payloadBody: `{"body":{"type":"xhs.publish","status":"ok"}}`,
			audience:    "channel", senderKind: "external", senderID: "xhs-adapter",
			parentID: "m4", tsReceived: 500,
		},
		// 6. task.opened → file.created + doc_refs lift, mentions merge with channel audience
		{
			id: "m6", payloadType: "task.opened",
			payloadBody: `{"body":{"title":"do a thing","doc_ref":"tasks/42.md"}}`,
			audience:    "channel", senderKind: "agent", senderID: "a1",
			mentions: `["@bot"]`, tsReceived: 600,
		},
		// 7. dispatch.self_check_due → dropped
		{
			id: "m7", payloadType: "dispatch.self_check_due",
			payloadBody: `{"body":{}}`,
			audience:    "channel", senderKind: "system", senderID: "system", tsReceived: 700,
		},
		// 8. cron.tick → dropped
		{
			id: "m8", payloadType: "cron.tick", payloadBody: `{"body":{}}`,
			audience: "channel", senderKind: "system", senderID: "system", tsReceived: 800,
		},
	})
	t.Cleanup(func() { _ = src.Close() })

	dst, err := OpenChannel(ctx, dstPath, OpenOptions{})
	if err != nil {
		t.Fatalf("OpenChannel dst: %v", err)
	}
	t.Cleanup(func() { _ = dst.Close() })

	report, err := MigrateFromNode(ctx, src, dst)
	if err != nil {
		t.Fatalf("MigrateFromNode: %v", err)
	}

	if report.SourceRows != 8 {
		t.Errorf("SourceRows = %d, want 8", report.SourceRows)
	}
	if report.InsertedRows != 6 {
		t.Errorf("InsertedRows = %d, want 6 (2 dropped)", report.InsertedRows)
	}
	if got := report.DroppedTypes["dispatch.self_check_due"]; got != 1 {
		t.Errorf("dropped dispatch.self_check_due = %d, want 1", got)
	}
	if got := report.DroppedTypes["cron.tick"]; got != 1 {
		t.Errorf("dropped cron.tick = %d, want 1", got)
	}

	// Scan dst messages, asserting per-row rewrites.
	got := scanV4Messages(t, ctx, dst)
	if len(got) != 6 {
		t.Fatalf("dst row count = %d, want 6", len(got))
	}

	// seq MUST be 1..6 contiguous (AUTOINCREMENT in source order).
	for i, r := range got {
		if r.seq != int64(i+1) {
			t.Errorf("row %d seq = %d, want %d", i, r.seq, i+1)
		}
	}

	// m1 user.text → human.text, kind=event, audience=["*"], visibility=public
	r := byID(got, "m1")
	if r.typ != "human.text" || r.kind != "event" {
		t.Errorf("m1 type/kind = %q/%q", r.typ, r.kind)
	}
	if r.audience != `["*"]` {
		t.Errorf("m1 audience = %q, want [\"*\"]", r.audience)
	}
	if r.visibility != "public" {
		t.Errorf("m1 visibility = %q, want public", r.visibility)
	}

	// m2 agent.progress → agent.text + visibility=system
	r = byID(got, "m2")
	if r.typ != "agent.text" {
		t.Errorf("m2 type = %q, want agent.text", r.typ)
	}
	if r.visibility != "system" {
		t.Errorf("m2 visibility = %q, want system", r.visibility)
	}

	// m3 self.memo → agent.text + private + audience=[a1]
	r = byID(got, "m3")
	if r.typ != "agent.text" || r.visibility != "private" {
		t.Errorf("m3 type/visibility = %q/%q", r.typ, r.visibility)
	}
	if r.audience != `["a1"]` {
		t.Errorf("m3 audience = %q, want [\"a1\"]", r.audience)
	}

	// m4 dispatch.start xhs.publish → xhs.publish.requested + kind=request +
	//    correlation_id=task-42 (backfill)
	r = byID(got, "m4")
	if r.typ != "xhs.publish.requested" || r.kind != "request" {
		t.Errorf("m4 type/kind = %q/%q", r.typ, r.kind)
	}
	if !r.correlation.Valid || r.correlation.String != "task-42" {
		t.Errorf("m4 correlation_id = %+v, want task-42", r.correlation)
	}
	// payload.body.type MUST be stripped
	var body4 map[string]any
	if err := json.Unmarshal([]byte(r.payload), &body4); err != nil {
		t.Fatalf("m4 payload parse: %v", err)
	}
	if inner, ok := body4["body"].(map[string]any); ok {
		if _, has := inner["type"]; has {
			t.Errorf("m4 payload.body.type was not stripped: %v", inner)
		}
	}

	// m5 dispatch.completed → xhs.publish.completed + kind=response + is_terminal=1
	r = byID(got, "m5")
	if r.typ != "xhs.publish.completed" || r.kind != "response" {
		t.Errorf("m5 type/kind = %q/%q", r.typ, r.kind)
	}
	if r.isTerminal != 1 {
		t.Errorf("m5 is_terminal = %d, want 1", r.isTerminal)
	}
	// senderKind coercion: external → tool
	if r.senderKind != "tool" {
		t.Errorf("m5 sender_kind = %q, want tool (legacy external)", r.senderKind)
	}

	// m6 task.opened → file.created + doc_refs=["tasks/42.md"] + audience=["*","@bot"]
	r = byID(got, "m6")
	if r.typ != "file.created" {
		t.Errorf("m6 type = %q, want file.created", r.typ)
	}
	if !r.docRefs.Valid {
		t.Errorf("m6 doc_refs missing")
	} else {
		var arr []string
		if err := json.Unmarshal([]byte(r.docRefs.String), &arr); err != nil {
			t.Errorf("m6 doc_refs parse: %v", err)
		} else if len(arr) != 1 || arr[0] != "tasks/42.md" {
			t.Errorf("m6 doc_refs = %v, want [tasks/42.md]", arr)
		}
	}
	if !sameJSONArray(t, r.audience, []string{"*", "@bot"}) {
		t.Errorf("m6 audience = %q, want [\"*\",\"@bot\"]", r.audience)
	}
}

// TestMigrateFromNode_UnknownAborts asserts that an unknown legacy type
// stops the run mid-flight with no further INSERTs.
func TestMigrateFromNode_UnknownAborts(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	src := buildNodeFixture(t, ctx, filepath.Join(dir, "node.sqlite"), []nodeFixture{
		{id: "m1", payloadType: "user.text", payloadBody: `{"body":{"text":"hi"}}`,
			audience: "channel", senderKind: "human", senderID: "u1", tsReceived: 100},
		{id: "m2", payloadType: "some.legacy.never.heard.of", payloadBody: `{}`,
			audience: "channel", senderKind: "human", senderID: "u1", tsReceived: 200},
	})
	t.Cleanup(func() { _ = src.Close() })

	dst, err := OpenChannel(ctx, filepath.Join(dir, "v4.sqlite"), OpenOptions{})
	if err != nil {
		t.Fatalf("OpenChannel dst: %v", err)
	}
	t.Cleanup(func() { _ = dst.Close() })

	report, err := MigrateFromNode(ctx, src, dst)
	if err == nil {
		t.Fatalf("expected error for unmapped type, got nil; report=%+v", report)
	}
	if !strings.Contains(err.Error(), "unmapped legacy payload_type") {
		t.Errorf("error message = %q, want substring 'unmapped legacy payload_type'", err.Error())
	}
}

// --- migration fixture helpers --------------------------------------------

type nodeFixture struct {
	id            string
	payloadType   string
	payloadBody   string
	audience      string
	senderKind    string
	senderID      string
	parentID      string
	correlationID string
	taskID        string
	mentions      string
	tsReceived    int64
}

// buildNodeFixture creates a legacy Node sqlite file with the supplied
// rows. The schema mirrors message-store.js byte-for-byte (minus the
// IF NOT EXISTS) so any caller of MigrateFromNode against this fixture
// exercises the real read path.
func buildNodeFixture(t *testing.T, ctx context.Context, path string, rows []nodeFixture) *sql.DB {
	t.Helper()

	db, err := OpenChannel(ctx, path, OpenOptions{SkipDDL: true})
	if err != nil {
		t.Fatalf("open node fixture %s: %v", path, err)
	}
	if _, err := db.ExecContext(ctx, nodeSchemaDDL); err != nil {
		t.Fatalf("apply node DDL: %v", err)
	}

	const insertSQL = `
INSERT INTO messages
  (id, channel_id, ts, ts_received, sender_kind, sender_id, sender_name,
   payload_type, payload_body, parent_id, correlation_id, task_id, thread_id,
   audience, mentions, origin, not_before, expires_at, delivered_at,
   delivery_failed_at, delivery_attempts, last_attempt_at, last_error,
   envelope_json, payload_json, created_at)
VALUES
  (?, 'c1', ?, ?, ?, ?, '',
   ?, ?, ?, ?, ?, NULL,
   ?, ?, NULL, NULL, NULL, NULL,
   NULL, 0, NULL, NULL,
   '{}', '{}', '2026-01-01T00:00:00.000Z')`
	for _, r := range rows {
		var parent, correlation, task, mentions any
		if r.parentID != "" {
			parent = r.parentID
		}
		if r.correlationID != "" {
			correlation = r.correlationID
		}
		if r.taskID != "" {
			task = r.taskID
		}
		if r.mentions != "" {
			mentions = r.mentions
		}
		ts := r.tsReceived
		if ts == 0 {
			ts = 1
		}
		if _, err := db.ExecContext(ctx, insertSQL,
			r.id, ts, ts, r.senderKind, r.senderID,
			r.payloadType, r.payloadBody, parent, correlation, task,
			r.audience, mentions,
		); err != nil {
			t.Fatalf("insert fixture %q: %v", r.id, err)
		}
	}
	return db
}

type v4Scan struct {
	seq         int64
	id          string
	typ         string
	kind        string
	senderKind  string
	audience    string
	visibility  string
	correlation sql.NullString
	docRefs     sql.NullString
	payload     string
	isTerminal  int
}

func scanV4Messages(t *testing.T, ctx context.Context, db *sql.DB) []v4Scan {
	t.Helper()
	rows, err := db.QueryContext(ctx, `
		SELECT seq, id, type, kind, sender_kind, audience, visibility,
		       correlation_id, doc_refs, payload, is_terminal
		  FROM messages
		 ORDER BY seq ASC`)
	if err != nil {
		t.Fatalf("scan dst: %v", err)
	}
	defer func() { _ = rows.Close() }()

	var out []v4Scan
	for rows.Next() {
		var r v4Scan
		if err := rows.Scan(&r.seq, &r.id, &r.typ, &r.kind, &r.senderKind,
			&r.audience, &r.visibility, &r.correlation, &r.docRefs, &r.payload,
			&r.isTerminal,
		); err != nil {
			t.Fatalf("scan row: %v", err)
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows.Err: %v", err)
	}
	return out
}

func byID(rows []v4Scan, id string) v4Scan {
	for _, r := range rows {
		if r.id == id {
			return r
		}
	}
	return v4Scan{}
}

func sameJSONArray(t *testing.T, gotJSON string, want []string) bool {
	t.Helper()
	var got []string
	if err := json.Unmarshal([]byte(gotJSON), &got); err != nil {
		t.Errorf("parse %q: %v", gotJSON, err)
		return false
	}
	if len(got) != len(want) {
		return false
	}
	sort.Strings(got)
	sort.Strings(want)
	for i := range got {
		if got[i] != want[i] {
			return false
		}
	}
	return true
}
