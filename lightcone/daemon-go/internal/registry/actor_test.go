package registry

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"path/filepath"
	"testing"

	"github.com/coagent-ai/daemon-go/internal/store"
)

// ---------------------------------------------------------------------------
// Test fixtures
// ---------------------------------------------------------------------------

const testChannelID = "ch-test"

// openChannel creates a fresh channel sqlite under t.TempDir() with the
// full L2 channel DDL applied. Returns the *sql.DB pool; callers do not
// need to close it (t.Cleanup handles it).
func openChannel(t *testing.T) *sql.DB {
	t.Helper()
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "messages.sqlite")
	db, err := store.OpenChannel(ctx, path, store.OpenOptions{})
	if err != nil {
		t.Fatalf("OpenChannel: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

// withConn runs body inside a BEGIN IMMEDIATE tx held on a *sql.Conn —
// matching the bootstrap saga's call pattern. The body's error decides
// whether the tx commits or rolls back so we can exercise the
// "Register fails mid-tx → no rows survive" acceptance.
func withConn(t *testing.T, db *sql.DB, body func(ctx context.Context, conn *sql.Conn) error) error {
	t.Helper()
	return store.WithImmediate(context.Background(), db, body)
}

// countRows is the standard COUNT helper used by sibling test packages.
func countRows(t *testing.T, ctx context.Context, db *sql.DB, sqlText string, args ...any) int {
	t.Helper()
	var n int
	if err := db.QueryRowContext(ctx, sqlText, args...).Scan(&n); err != nil {
		t.Fatalf("count %q: %v", sqlText, err)
	}
	return n
}

// happyMeta returns an ActorMeta that always validates — tests mutate
// individual fields to drive each error path.
func happyMeta() ActorMeta {
	return ActorMeta{
		ActorID:   "agent:writer",
		Kind:      KindAgent,
		Binding:   BindingDaemonRPC,
		CreatedAt: 1_700_000_001,
	}
}

// ---------------------------------------------------------------------------
// 1. Register happy path — writes all 3 rows.
// ---------------------------------------------------------------------------

func TestRegister_Happy(t *testing.T) {
	ctx := context.Background()
	db := openChannel(t)
	meta := happyMeta()

	if err := withConn(t, db, func(ctx context.Context, conn *sql.Conn) error {
		return Register(ctx, conn, testChannelID, meta)
	}); err != nil {
		t.Fatalf("Register: %v", err)
	}

	// actor_registry row exists with the expected metadata.
	if n := countRows(t, ctx, db,
		`SELECT COUNT(*) FROM actor_registry WHERE actor_id=? AND actor_kind=? AND actor_binding=?`,
		meta.ActorID, string(meta.Kind), string(meta.Binding)); n != 1 {
		t.Errorf("actor_registry row missing or wrong: got count=%d", n)
	}

	// actor_cursors row seeded with last_consumed_seq=0.
	var seq int64
	if err := db.QueryRowContext(ctx,
		`SELECT last_consumed_seq FROM actor_cursors WHERE actor_id=?`,
		meta.ActorID,
	).Scan(&seq); err != nil {
		t.Fatalf("read actor_cursors: %v", err)
	}
	if seq != 0 {
		t.Errorf("actor_cursors.last_consumed_seq = %d, want 0", seq)
	}

	// system.event row emitted with deterministic id + audience=['*'].
	eventID := actorRegisteredEventID(testChannelID, meta.ActorID)
	var payload string
	var audience string
	var visibility string
	if err := db.QueryRowContext(ctx,
		`SELECT payload, audience, visibility FROM messages WHERE id=?`,
		eventID,
	).Scan(&payload, &audience, &visibility); err != nil {
		t.Fatalf("read actor_registered event: %v", err)
	}
	if visibility != "system" {
		t.Errorf("visibility = %q, want system", visibility)
	}
	var aud []string
	if err := json.Unmarshal([]byte(audience), &aud); err != nil {
		t.Fatalf("audience JSON: %v", err)
	}
	if len(aud) != 1 || aud[0] != "*" {
		t.Errorf("audience = %v, want ['*']", aud)
	}
	var p map[string]any
	if err := json.Unmarshal([]byte(payload), &p); err != nil {
		t.Fatalf("payload JSON: %v", err)
	}
	if p["kind"] != "actor_registered" || p["actor_id"] != meta.ActorID {
		t.Errorf("payload = %v, want kind/actor_id set", p)
	}
	if p["actor_kind"] != string(meta.Kind) {
		t.Errorf("payload.actor_kind = %v, want %s", p["actor_kind"], meta.Kind)
	}
	if p["actor_binding"] != string(meta.Binding) {
		t.Errorf("payload.actor_binding = %v, want %s", p["actor_binding"], meta.Binding)
	}
}

// Register a human (NULL binding) — payload.actor_binding must be JSON null.
func TestRegister_HumanNullBinding(t *testing.T) {
	ctx := context.Background()
	db := openChannel(t)
	meta := ActorMeta{
		ActorID:   "user-001",
		Kind:      KindHuman,
		Binding:   BindingNone,
		CreatedAt: 1_700_000_010,
	}
	if err := withConn(t, db, func(ctx context.Context, conn *sql.Conn) error {
		return Register(ctx, conn, testChannelID, meta)
	}); err != nil {
		t.Fatalf("Register: %v", err)
	}

	// actor_registry.actor_binding stored as NULL.
	var binding sql.NullString
	if err := db.QueryRowContext(ctx,
		`SELECT actor_binding FROM actor_registry WHERE actor_id=?`,
		meta.ActorID,
	).Scan(&binding); err != nil {
		t.Fatalf("read actor_registry: %v", err)
	}
	if binding.Valid {
		t.Errorf("actor_binding = %q, want NULL", binding.String)
	}

	// payload.actor_binding present and JSON null.
	eventID := actorRegisteredEventID(testChannelID, meta.ActorID)
	var payload string
	if err := db.QueryRowContext(ctx,
		`SELECT payload FROM messages WHERE id=?`, eventID,
	).Scan(&payload); err != nil {
		t.Fatalf("read event: %v", err)
	}
	var p map[string]any
	if err := json.Unmarshal([]byte(payload), &p); err != nil {
		t.Fatalf("payload JSON: %v", err)
	}
	if v, ok := p["actor_binding"]; !ok || v != nil {
		t.Errorf("payload.actor_binding = %v (ok=%v), want JSON null", v, ok)
	}
}

// ---------------------------------------------------------------------------
// 2. Register tx rollback — failure mid-Register must not leave
//    actor_cursors or messages rows behind.
// ---------------------------------------------------------------------------

// TestRegister_TxRollback_NoLeftover: trigger a UNIQUE conflict on the
// SECOND Register call inside the same outer tx, then have the outer tx
// rollback. The acceptance criterion "Register 失败时 actor_cursors row
// 不残留" follows from the outer tx semantics — verify it.
func TestRegister_TxRollback_NoLeftover(t *testing.T) {
	ctx := context.Background()
	db := openChannel(t)

	// First seed an actor outside the test tx so we have a row to
	// conflict against.
	first := happyMeta()
	if err := withConn(t, db, func(ctx context.Context, conn *sql.Conn) error {
		return Register(ctx, conn, testChannelID, first)
	}); err != nil {
		t.Fatalf("seed Register: %v", err)
	}

	// Now drive a tx that registers a brand-new actor (succeeds) and
	// then re-registers `first` (must fail with ErrActorExists). The
	// outer WithImmediate must see the error and ROLLBACK, undoing the
	// successful new-actor inserts too.
	second := ActorMeta{
		ActorID:   "agent:new",
		Kind:      KindAgent,
		Binding:   BindingDaemonRPC,
		CreatedAt: 1_700_000_002,
	}
	err := withConn(t, db, func(ctx context.Context, conn *sql.Conn) error {
		if err := Register(ctx, conn, testChannelID, second); err != nil {
			return err
		}
		// Same actor_id as `first` → PK conflict → ErrActorExists.
		return Register(ctx, conn, testChannelID, first)
	})
	if !errors.Is(err, ErrActorExists) {
		t.Fatalf("err = %v, want ErrActorExists", err)
	}

	// The first seed actor + its cursor + its event survive.
	if n := countRows(t, ctx, db, `SELECT COUNT(*) FROM actor_registry`); n != 1 {
		t.Errorf("actor_registry rows = %d, want 1 (only the seed)", n)
	}
	// `second` should NOT have any leftover row in any table.
	if n := countRows(t, ctx, db,
		`SELECT COUNT(*) FROM actor_registry WHERE actor_id=?`, second.ActorID); n != 0 {
		t.Errorf("actor_registry has %d rows for %s after rollback, want 0", n, second.ActorID)
	}
	if n := countRows(t, ctx, db,
		`SELECT COUNT(*) FROM actor_cursors WHERE actor_id=?`, second.ActorID); n != 0 {
		t.Errorf("actor_cursors has %d rows for %s after rollback, want 0", n, second.ActorID)
	}
	if n := countRows(t, ctx, db,
		`SELECT COUNT(*) FROM messages WHERE id=?`,
		actorRegisteredEventID(testChannelID, second.ActorID)); n != 0 {
		t.Errorf("messages has %d actor_registered rows for %s after rollback, want 0",
			n, second.ActorID)
	}
}

// ---------------------------------------------------------------------------
// 3. Register validation — input rejection table.
// ---------------------------------------------------------------------------

func TestRegister_Validation(t *testing.T) {
	ctx := context.Background()
	cases := []struct {
		name     string
		mut      func(*ActorMeta) string // returns channelID override (or "" to keep default)
		wantErr  error
	}{
		{"empty channel_id", func(_ *ActorMeta) string { return "" }, ErrInvalidChannelID},
		{"empty actor_id", func(m *ActorMeta) string { m.ActorID = "  "; return testChannelID }, ErrInvalidActorID},
		{"bogus kind", func(m *ActorMeta) string { m.Kind = "wizard"; return testChannelID }, ErrInvalidKind},
		{"bogus binding", func(m *ActorMeta) string { m.Binding = "carrier-pigeon"; return testChannelID }, ErrInvalidBinding},
		{"human with binding", func(m *ActorMeta) string {
			m.Kind = KindHuman
			m.Binding = BindingDaemonRPC
			return testChannelID
		}, ErrInvalidBinding},
		{"agent without binding", func(m *ActorMeta) string {
			m.Kind = KindAgent
			m.Binding = BindingNone
			return testChannelID
		}, ErrInvalidBinding},
		{"tool without binding", func(m *ActorMeta) string {
			m.Kind = KindTool
			m.Binding = BindingNone
			return testChannelID
		}, ErrInvalidBinding},
		{"system with binding", func(m *ActorMeta) string {
			m.Kind = KindSystem
			m.Binding = BindingInWorkerBus
			return testChannelID
		}, ErrInvalidBinding},
		{"non-positive created_at", func(m *ActorMeta) string { m.CreatedAt = 0; return testChannelID }, ErrInvalidNow},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			db := openChannel(t)
			m := happyMeta()
			ch := c.mut(&m)
			err := withConn(t, db, func(ctx context.Context, conn *sql.Conn) error {
				return Register(ctx, conn, ch, m)
			})
			if !errors.Is(err, c.wantErr) {
				t.Fatalf("err = %v, want errors.Is(%v)", err, c.wantErr)
			}
			// No row was written on validation failure.
			if n := countRows(t, ctx, db, `SELECT COUNT(*) FROM actor_registry`); n != 0 {
				t.Errorf("actor_registry rows = %d, want 0", n)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// 4. Deregister soft-delete — Get still returns row with deregistered_at.
// ---------------------------------------------------------------------------

func TestDeregister_SoftDelete(t *testing.T) {
	ctx := context.Background()
	db := openChannel(t)
	meta := happyMeta()
	if err := withConn(t, db, func(ctx context.Context, conn *sql.Conn) error {
		return Register(ctx, conn, testChannelID, meta)
	}); err != nil {
		t.Fatalf("Register: %v", err)
	}

	const dregNow = int64(1_700_000_500)
	if err := withConn(t, db, func(ctx context.Context, conn *sql.Conn) error {
		return Deregister(ctx, conn, meta.ActorID, dregNow)
	}); err != nil {
		t.Fatalf("Deregister: %v", err)
	}

	// Get still returns the row (acceptance criterion).
	got, err := Get(ctx, db, meta.ActorID)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.DeregisteredAt == nil || *got.DeregisteredAt != dregNow {
		t.Errorf("DeregisteredAt = %v, want *%d", got.DeregisteredAt, dregNow)
	}
	if got.Kind != meta.Kind {
		t.Errorf("Kind = %s, want %s", got.Kind, meta.Kind)
	}
}

// Deregister a second time on the same id is a no-op (the WHERE clause
// filters out the already-deregistered row) and returns ErrActorNotFound.
func TestDeregister_AlreadyDeregistered(t *testing.T) {
	ctx := context.Background()
	db := openChannel(t)
	meta := happyMeta()
	if err := withConn(t, db, func(ctx context.Context, conn *sql.Conn) error {
		return Register(ctx, conn, testChannelID, meta)
	}); err != nil {
		t.Fatalf("Register: %v", err)
	}
	if err := withConn(t, db, func(ctx context.Context, conn *sql.Conn) error {
		return Deregister(ctx, conn, meta.ActorID, 1_700_000_500)
	}); err != nil {
		t.Fatalf("first Deregister: %v", err)
	}
	err := withConn(t, db, func(ctx context.Context, conn *sql.Conn) error {
		return Deregister(ctx, conn, meta.ActorID, 1_700_000_600)
	})
	if !errors.Is(err, ErrActorNotFound) {
		t.Errorf("err = %v, want ErrActorNotFound", err)
	}

	// And the original timestamp survives — CAS protected it.
	got, err := Get(ctx, db, meta.ActorID)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.DeregisteredAt == nil || *got.DeregisteredAt != 1_700_000_500 {
		t.Errorf("DeregisteredAt = %v, want *1700000500", got.DeregisteredAt)
	}
}

// Deregister a never-registered actor must return ErrActorNotFound.
func TestDeregister_NotFound(t *testing.T) {
	db := openChannel(t)
	err := withConn(t, db, func(ctx context.Context, conn *sql.Conn) error {
		return Deregister(ctx, conn, "agent:missing", 1_700_000_500)
	})
	if !errors.Is(err, ErrActorNotFound) {
		t.Errorf("err = %v, want ErrActorNotFound", err)
	}
}

// ---------------------------------------------------------------------------
// 5. Get on missing actor → ErrActorNotFound.
// ---------------------------------------------------------------------------

func TestGet_NotFound(t *testing.T) {
	db := openChannel(t)
	_, err := Get(context.Background(), db, "agent:ghost")
	if !errors.Is(err, ErrActorNotFound) {
		t.Errorf("err = %v, want ErrActorNotFound", err)
	}
}

// ---------------------------------------------------------------------------
// 6. ListActive excludes deregistered + sorts deterministically.
// ---------------------------------------------------------------------------

func TestListActive_ExcludesDeregistered(t *testing.T) {
	ctx := context.Background()
	db := openChannel(t)
	// Seed three actors of different kinds.
	actors := []ActorMeta{
		{ActorID: "system", Kind: KindSystem, Binding: BindingNone, CreatedAt: 1_700_000_001},
		{ActorID: "user-001", Kind: KindHuman, Binding: BindingNone, CreatedAt: 1_700_000_002},
		{ActorID: "agent:writer", Kind: KindAgent, Binding: BindingDaemonRPC, CreatedAt: 1_700_000_003},
		{ActorID: "tool:xhs", Kind: KindTool, Binding: BindingDaemonRPC, CreatedAt: 1_700_000_004},
	}
	for _, a := range actors {
		a := a
		if err := withConn(t, db, func(ctx context.Context, conn *sql.Conn) error {
			return Register(ctx, conn, testChannelID, a)
		}); err != nil {
			t.Fatalf("Register %s: %v", a.ActorID, err)
		}
	}
	// Deregister the agent.
	if err := withConn(t, db, func(ctx context.Context, conn *sql.Conn) error {
		return Deregister(ctx, conn, "agent:writer", 1_700_000_500)
	}); err != nil {
		t.Fatalf("Deregister: %v", err)
	}

	active, err := ListActive(ctx, db)
	if err != nil {
		t.Fatalf("ListActive: %v", err)
	}
	if len(active) != 3 {
		t.Fatalf("ListActive size = %d, want 3", len(active))
	}
	// Deterministic order: agent < human < system < tool by actor_kind ASC.
	// Actor "agent:writer" is deregistered so it shouldn't appear.
	want := []string{"user-001", "system", "tool:xhs"} // human, system, tool
	for i, a := range active {
		if a.ActorID != want[i] {
			t.Errorf("active[%d].ActorID = %q, want %q", i, a.ActorID, want[i])
		}
		if a.DeregisteredAt != nil {
			t.Errorf("active[%d] = %s has non-nil DeregisteredAt", i, a.ActorID)
		}
	}
	// Sanity check kinds.
	if active[0].Kind != KindHuman || active[1].Kind != KindSystem || active[2].Kind != KindTool {
		t.Errorf("kinds out of order: %s/%s/%s", active[0].Kind, active[1].Kind, active[2].Kind)
	}
}

// ---------------------------------------------------------------------------
// 7. GetKind — active filter; missing OR deregistered → ErrActorNotFound.
// ---------------------------------------------------------------------------

func TestGetKind_ActiveOnly(t *testing.T) {
	ctx := context.Background()
	db := openChannel(t)
	meta := happyMeta()
	if err := withConn(t, db, func(ctx context.Context, conn *sql.Conn) error {
		return Register(ctx, conn, testChannelID, meta)
	}); err != nil {
		t.Fatalf("Register: %v", err)
	}

	kind, err := GetKind(ctx, db, meta.ActorID)
	if err != nil {
		t.Fatalf("GetKind: %v", err)
	}
	if kind != meta.Kind {
		t.Errorf("Kind = %s, want %s", kind, meta.Kind)
	}

	// Missing actor.
	if _, err := GetKind(ctx, db, "agent:ghost"); !errors.Is(err, ErrActorNotFound) {
		t.Errorf("GetKind missing err = %v, want ErrActorNotFound", err)
	}

	// Deregister → GetKind returns ErrActorNotFound even though row exists.
	if err := withConn(t, db, func(ctx context.Context, conn *sql.Conn) error {
		return Deregister(ctx, conn, meta.ActorID, 1_700_000_900)
	}); err != nil {
		t.Fatalf("Deregister: %v", err)
	}
	if _, err := GetKind(ctx, db, meta.ActorID); !errors.Is(err, ErrActorNotFound) {
		t.Errorf("GetKind deregistered err = %v, want ErrActorNotFound", err)
	}
	// But Get still returns the row.
	if _, err := Get(ctx, db, meta.ActorID); err != nil {
		t.Errorf("Get after deregister err = %v, want nil", err)
	}
}

// ---------------------------------------------------------------------------
// 8. Re-registering a deregistered actor_id is REJECTED (PK conflict).
//    L1 §12.4 "不重用 deregistered id" rule.
// ---------------------------------------------------------------------------

func TestRegister_DuplicateAfterDeregister(t *testing.T) {
	db := openChannel(t)
	meta := happyMeta()
	if err := withConn(t, db, func(ctx context.Context, conn *sql.Conn) error {
		return Register(ctx, conn, testChannelID, meta)
	}); err != nil {
		t.Fatalf("Register: %v", err)
	}
	if err := withConn(t, db, func(ctx context.Context, conn *sql.Conn) error {
		return Deregister(ctx, conn, meta.ActorID, 1_700_000_900)
	}); err != nil {
		t.Fatalf("Deregister: %v", err)
	}
	err := withConn(t, db, func(ctx context.Context, conn *sql.Conn) error {
		m := meta
		m.CreatedAt = 1_700_001_000
		return Register(ctx, conn, testChannelID, m)
	})
	if !errors.Is(err, ErrActorExists) {
		t.Errorf("err = %v, want ErrActorExists", err)
	}
}
