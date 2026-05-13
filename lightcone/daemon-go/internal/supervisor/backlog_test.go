package supervisor

import (
	"context"
	"database/sql"
	"errors"
	"testing"
)

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

// seedCursor INSERTs the actor_cursors row Required by the JOIN. The
// real bootstrap saga does this via registry.Register; tests skip that
// roundtrip and write the seed row directly to keep the SQL fixture
// minimal.
func seedCursor(t *testing.T, ctx context.Context, db *sql.DB, agentID string, lastSeq int64) {
	t.Helper()
	if _, err := db.ExecContext(ctx,
		`INSERT OR REPLACE INTO actor_cursors (actor_id, last_consumed_seq, last_consumed_id, updated_at)
		 VALUES (?, ?, NULL, ?)`,
		agentID, lastSeq, 1_700_000_000,
	); err != nil {
		t.Fatalf("seed cursor: %v", err)
	}
}

// msgFixture captures every channel-level message column the backlog
// scan filters touch. Tests build a slice and call insertMessages.
type msgFixture struct {
	id         string
	senderID   string
	senderKind string
	kind       string // "event" | "request" | "response"
	typ        string
	visibility string // "public" | "private" | "system"
	audience   string // raw JSON
	notBefore  *int64
	expiresAt  *int64
}

func ptrInt(v int64) *int64 { return &v }

func insertMessages(t *testing.T, ctx context.Context, db *sql.DB, rows []msgFixture) {
	t.Helper()
	for _, r := range rows {
		// payload + parent_id NULL + delivery columns left default.
		_, err := db.ExecContext(ctx,
			`INSERT INTO messages
			   (id, ts, ts_received, channel_id, sender_kind, sender_id,
			    kind, type, payload, parent_id, visibility, audience,
			    not_before, expires_at, is_terminal)
			 VALUES (?, 1, 1, 'ch-test', ?, ?, ?, ?, '{}', NULL, ?, ?, ?, ?, 0)`,
			r.id, r.senderKind, r.senderID,
			r.kind, r.typ, r.visibility, r.audience,
			nullIfNilInt(r.notBefore), nullIfNilInt(r.expiresAt),
		)
		if err != nil {
			t.Fatalf("insert message %q: %v", r.id, err)
		}
	}
}

func nullIfNilInt(p *int64) any {
	if p == nil {
		return nil
	}
	return *p
}

func backlogIDs(rows []BacklogMessage) []string {
	out := make([]string, len(rows))
	for i, r := range rows {
		out[i] = r.ID
	}
	return out
}

// ---------------------------------------------------------------------------
// 1. No cursor row → backlog empty (defensive — JOIN returns 0 rows).
// ---------------------------------------------------------------------------

func TestBacklog_NoCursorRow_ReturnsEmpty(t *testing.T) {
	ctx := context.Background()
	db := openChannel(t)

	// Insert a message but skip cursor seeding.
	insertMessages(t, ctx, db, []msgFixture{
		{id: "m1", senderID: "agent:other", senderKind: "agent",
			kind: "event", typ: "agent.text",
			visibility: "public", audience: `["*"]`},
	})

	got, err := BacklogScan(ctx, db, "agent:writer", 1_700_000_500)
	if err != nil {
		t.Fatalf("BacklogScan: %v", err)
	}
	if len(got) != 0 {
		t.Errorf("len(got) = %d, want 0", len(got))
	}
}

// ---------------------------------------------------------------------------
// 2. Cursor filter — only messages with seq > last_consumed_seq are
//    returned, in seq ASC order.
// ---------------------------------------------------------------------------

func TestBacklog_CursorFilter(t *testing.T) {
	ctx := context.Background()
	db := openChannel(t)

	insertMessages(t, ctx, db, []msgFixture{
		// 3 messages — seq is AUTO_INCREMENT so insertion order = seq.
		{id: "m1", senderID: "agent:other", senderKind: "agent",
			kind: "event", typ: "agent.text",
			visibility: "public", audience: `["*"]`},
		{id: "m2", senderID: "agent:other", senderKind: "agent",
			kind: "event", typ: "agent.text",
			visibility: "public", audience: `["*"]`},
		{id: "m3", senderID: "agent:other", senderKind: "agent",
			kind: "event", typ: "agent.text",
			visibility: "public", audience: `["*"]`},
	})

	// Cursor parked at seq=2 → only m3 should come back.
	seedCursor(t, ctx, db, "agent:writer", 2)

	got, err := BacklogScan(ctx, db, "agent:writer", 1_700_000_500)
	if err != nil {
		t.Fatalf("BacklogScan: %v", err)
	}
	if want := []string{"m3"}; !equalStrings(backlogIDs(got), want) {
		t.Errorf("ids = %v, want %v", backlogIDs(got), want)
	}
	if got[0].Seq != 3 {
		t.Errorf("Seq = %d, want 3", got[0].Seq)
	}
	if got[0].Type != "agent.text" || got[0].Kind != "event" {
		t.Errorf("scanned columns wrong: %+v", got[0])
	}
}

// ---------------------------------------------------------------------------
// 3. visibility != 'system' filter — system events are skipped.
// ---------------------------------------------------------------------------

func TestBacklog_VisibilityFilter_SystemSkipped(t *testing.T) {
	ctx := context.Background()
	db := openChannel(t)
	seedCursor(t, ctx, db, "agent:writer", 0)

	insertMessages(t, ctx, db, []msgFixture{
		{id: "sys", senderID: "system", senderKind: "system",
			kind: "event", typ: "system.event",
			visibility: "system", audience: `["*"]`},
		{id: "pub", senderID: "agent:other", senderKind: "agent",
			kind: "event", typ: "agent.text",
			visibility: "public", audience: `["*"]`},
		{id: "priv", senderID: "agent:other", senderKind: "agent",
			kind: "event", typ: "agent.text",
			visibility: "private", audience: `["*"]`},
	})

	got, _ := BacklogScan(ctx, db, "agent:writer", 1_700_000_500)
	if want := []string{"pub", "priv"}; !equalStrings(backlogIDs(got), want) {
		t.Errorf("ids = %v, want %v", backlogIDs(got), want)
	}
}

// ---------------------------------------------------------------------------
// 4. Self-trigger filter — messages from self are skipped.
// ---------------------------------------------------------------------------

func TestBacklog_SelfTriggerFilter(t *testing.T) {
	ctx := context.Background()
	db := openChannel(t)
	seedCursor(t, ctx, db, "agent:writer", 0)

	insertMessages(t, ctx, db, []msgFixture{
		{id: "self", senderID: "agent:writer", senderKind: "agent",
			kind: "event", typ: "agent.text",
			visibility: "public", audience: `["*"]`},
		{id: "other", senderID: "agent:other", senderKind: "agent",
			kind: "event", typ: "agent.text",
			visibility: "public", audience: `["*"]`},
	})

	got, _ := BacklogScan(ctx, db, "agent:writer", 1_700_000_500)
	if want := []string{"other"}; !equalStrings(backlogIDs(got), want) {
		t.Errorf("ids = %v, want %v", backlogIDs(got), want)
	}
}

// ---------------------------------------------------------------------------
// 5. Audience filter — broadcast vs directed vs not-addressed-to-self.
// ---------------------------------------------------------------------------

func TestBacklog_AudienceFilter(t *testing.T) {
	ctx := context.Background()
	db := openChannel(t)
	seedCursor(t, ctx, db, "agent:writer", 0)

	insertMessages(t, ctx, db, []msgFixture{
		// Broadcast — matches.
		{id: "bcast", senderID: "agent:other", senderKind: "agent",
			kind: "event", typ: "agent.text",
			visibility: "public", audience: `["*"]`},
		// Directed to self — matches.
		{id: "to-self", senderID: "agent:other", senderKind: "agent",
			kind: "event", typ: "agent.text",
			visibility: "public", audience: `["agent:writer"]`},
		// Directed audience list containing self alongside others.
		{id: "multi", senderID: "agent:other", senderKind: "agent",
			kind: "event", typ: "agent.text",
			visibility: "public", audience: `["agent:writer","agent:third"]`},
		// Directed to someone else — skipped.
		{id: "to-other", senderID: "agent:other", senderKind: "agent",
			kind: "event", typ: "agent.text",
			visibility: "public", audience: `["agent:third"]`},
		// Empty audience — skipped (no recipient).
		{id: "empty", senderID: "agent:other", senderKind: "agent",
			kind: "event", typ: "agent.text",
			visibility: "public", audience: `[]`},
	})

	got, _ := BacklogScan(ctx, db, "agent:writer", 1_700_000_500)
	if want := []string{"bcast", "to-self", "multi"}; !equalStrings(backlogIDs(got), want) {
		t.Errorf("ids = %v, want %v", backlogIDs(got), want)
	}
}

// ---------------------------------------------------------------------------
// 6. not_before / expires_at window — future / expired messages skipped.
// ---------------------------------------------------------------------------

func TestBacklog_TimeWindowFilter(t *testing.T) {
	ctx := context.Background()
	db := openChannel(t)
	seedCursor(t, ctx, db, "agent:writer", 0)

	now := int64(1_700_000_500)
	insertMessages(t, ctx, db, []msgFixture{
		// No time window — always matches.
		{id: "noWindow", senderID: "agent:other", senderKind: "agent",
			kind: "event", typ: "agent.text",
			visibility: "public", audience: `["*"]`},
		// not_before in the past — matches.
		{id: "past", senderID: "agent:other", senderKind: "agent",
			kind: "event", typ: "agent.text",
			visibility: "public", audience: `["*"]`,
			notBefore: ptrInt(now - 10)},
		// not_before == now — matches (predicate is <=).
		{id: "equal", senderID: "agent:other", senderKind: "agent",
			kind: "event", typ: "agent.text",
			visibility: "public", audience: `["*"]`,
			notBefore: ptrInt(now)},
		// not_before in the future — skipped.
		{id: "future", senderID: "agent:other", senderKind: "agent",
			kind: "event", typ: "agent.text",
			visibility: "public", audience: `["*"]`,
			notBefore: ptrInt(now + 100)},
		// expires_at after now — matches.
		{id: "freshExpiry", senderID: "agent:other", senderKind: "agent",
			kind: "event", typ: "agent.text",
			visibility: "public", audience: `["*"]`,
			expiresAt: ptrInt(now + 100)},
		// expires_at == now — skipped (predicate is `>`).
		{id: "expiresNow", senderID: "agent:other", senderKind: "agent",
			kind: "event", typ: "agent.text",
			visibility: "public", audience: `["*"]`,
			expiresAt: ptrInt(now)},
		// expires_at in the past — skipped.
		{id: "expired", senderID: "agent:other", senderKind: "agent",
			kind: "event", typ: "agent.text",
			visibility: "public", audience: `["*"]`,
			expiresAt: ptrInt(now - 1)},
	})

	got, _ := BacklogScan(ctx, db, "agent:writer", now)
	want := []string{"noWindow", "past", "equal", "freshExpiry"}
	if !equalStrings(backlogIDs(got), want) {
		t.Errorf("ids = %v, want %v", backlogIDs(got), want)
	}
}

// ---------------------------------------------------------------------------
// 7. End-to-end mix — all 5 filters compose; order is seq ASC and
//    NotBefore / ExpiresAt scan into pointers correctly.
// ---------------------------------------------------------------------------

func TestBacklog_AllFiltersCompose(t *testing.T) {
	ctx := context.Background()
	db := openChannel(t)
	seedCursor(t, ctx, db, "agent:writer", 1) // cursor past seq=1

	now := int64(1_700_000_500)
	insertMessages(t, ctx, db, []msgFixture{
		{id: "below", senderID: "agent:other", senderKind: "agent",
			kind: "event", typ: "agent.text",
			visibility: "public", audience: `["*"]`}, // seq=1 < cursor → skipped
		{id: "self", senderID: "agent:writer", senderKind: "agent",
			kind: "event", typ: "agent.text",
			visibility: "public", audience: `["*"]`}, // self-trigger
		{id: "sys", senderID: "system", senderKind: "system",
			kind: "event", typ: "system.event",
			visibility: "system", audience: `["*"]`}, // system
		{id: "future", senderID: "agent:other", senderKind: "agent",
			kind: "event", typ: "agent.text",
			visibility: "public", audience: `["*"]`,
			notBefore: ptrInt(now + 10)},
		{id: "expired", senderID: "agent:other", senderKind: "agent",
			kind: "event", typ: "agent.text",
			visibility: "public", audience: `["*"]`,
			expiresAt: ptrInt(now - 10)},
		{id: "toOther", senderID: "agent:other", senderKind: "agent",
			kind: "event", typ: "agent.text",
			visibility: "public", audience: `["agent:third"]`},
		// THREE messages that should make it through.
		{id: "okBcast", senderID: "agent:other", senderKind: "agent",
			kind: "event", typ: "agent.text",
			visibility: "public", audience: `["*"]`},
		{id: "okDirect", senderID: "agent:other", senderKind: "agent",
			kind: "event", typ: "agent.text",
			visibility: "public", audience: `["agent:writer"]`},
		{id: "okWithExpiry", senderID: "agent:other", senderKind: "agent",
			kind: "event", typ: "agent.text",
			visibility: "public", audience: `["*"]`,
			notBefore: ptrInt(now - 10), expiresAt: ptrInt(now + 10)},
	})

	got, err := BacklogScan(ctx, db, "agent:writer", now)
	if err != nil {
		t.Fatalf("BacklogScan: %v", err)
	}
	want := []string{"okBcast", "okDirect", "okWithExpiry"}
	if !equalStrings(backlogIDs(got), want) {
		t.Errorf("ids = %v, want %v", backlogIDs(got), want)
	}

	// Pointer fields scanned correctly for the row with both times set.
	for _, r := range got {
		if r.ID == "okWithExpiry" {
			if r.NotBefore == nil || *r.NotBefore != now-10 {
				t.Errorf("okWithExpiry NotBefore = %v, want %d", r.NotBefore, now-10)
			}
			if r.ExpiresAt == nil || *r.ExpiresAt != now+10 {
				t.Errorf("okWithExpiry ExpiresAt = %v, want %d", r.ExpiresAt, now+10)
			}
		}
		if r.ID == "okBcast" && (r.NotBefore != nil || r.ExpiresAt != nil) {
			t.Errorf("okBcast unexpected non-nil time pointers: %+v", r)
		}
	}
}

// ---------------------------------------------------------------------------
// 8. Input validation.
// ---------------------------------------------------------------------------

func TestBacklog_InputValidation(t *testing.T) {
	ctx := context.Background()
	db := openChannel(t)

	cases := []struct {
		name string
		run  func() error
	}{
		{"empty agent", func() error {
			_, err := BacklogScan(ctx, db, "", 1)
			return err
		}},
		{"zero now", func() error {
			_, err := BacklogScan(ctx, db, "agent:x", 0)
			return err
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.run(); !errors.Is(err, ErrInvalidInput) {
				t.Errorf("err = %v, want ErrInvalidInput", err)
			}
		})
	}
}

// equalStrings is a tiny test helper kept private; reflect.DeepEqual
// gives uglier diff output on slice-of-strings.
func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
