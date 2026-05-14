package tools

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"

	internalharness "github.com/coagent-ai/daemon-go/internal/harness"
	"github.com/coagent-ai/daemon-go/internal/registry"
	"github.com/coagent-ai/daemon-go/internal/store"
	pkgharness "github.com/coagent-ai/daemon-go/pkg/harness"
)

// openTestChannel opens a fresh channel sqlite + applies DDL. Tests
// reuse this rather than duplicating boilerplate.
func openTestChannel(t *testing.T) *sql.DB {
	t.Helper()
	db, err := store.OpenChannel(context.Background(),
		filepath.Join(t.TempDir(), "messages.sqlite"), store.OpenOptions{})
	if err != nil {
		t.Fatalf("open channel: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

// openTestChannelRWandRO mirrors the production runtime wiring after
// R2-FIX-7 (t113): one writable + one `mode=ro` `*sql.DB` pointed at
// the same file. Tests that exercise BuildTools / BuildConfig must
// supply both because sqlite.query consumes the ro handle.
func openTestChannelRWandRO(t *testing.T) (rw, ro *sql.DB) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "messages.sqlite")
	ctx := context.Background()
	var err error
	rw, err = store.OpenChannel(ctx, path, store.OpenOptions{})
	if err != nil {
		t.Fatalf("open channel rw: %v", err)
	}
	ro, err = store.OpenChannel(ctx, path, store.OpenOptions{ReadOnly: true, SkipDDL: true})
	if err != nil {
		_ = rw.Close()
		t.Fatalf("open channel ro: %v", err)
	}
	t.Cleanup(func() {
		_ = ro.Close()
		_ = rw.Close()
	})
	return rw, ro
}

// TestEnsureToolActors_HappyPath verifies every Catalog entry lands as
// (a) an active actor_registry row with kind=tool/binding=in_worker_bus
// and (b) a type_registry row with the prescribed max_pending_ms.
func TestEnsureToolActors_HappyPath(t *testing.T) {
	t.Parallel()
	db := openTestChannel(t)
	ctx := context.Background()
	now := int64(1700000000)
	if err := EnsureToolActors(ctx, EnsureConfig{
		DB: db, ChannelID: "ch-1", Now: now,
	}); err != nil {
		t.Fatalf("EnsureToolActors: %v", err)
	}

	descriptors := Catalog()
	for _, d := range descriptors {
		meta, err := registry.Get(ctx, db, d.ActorID())
		if err != nil {
			t.Fatalf("registry.Get %s: %v", d.ActorID(), err)
		}
		if meta.Kind != registry.KindTool {
			t.Fatalf("%s kind = %q, want tool", d.ActorID(), meta.Kind)
		}
		if meta.Binding != registry.BindingInWorkerBus {
			t.Fatalf("%s binding = %q, want in_worker_bus", d.ActorID(), meta.Binding)
		}
		if meta.DeregisteredAt != nil {
			t.Fatalf("%s deregistered_at must be NULL", d.ActorID())
		}
	}

	// type_registry: each row exists + carries max_pending_ms.
	for _, d := range descriptors {
		row := db.QueryRowContext(ctx,
			`SELECT max_pending_ms, handler_actor_id, handler_binding FROM type_registry WHERE type = ?`,
			d.Type)
		var (
			maxPending     sql.NullInt64
			handlerActor   sql.NullString
			handlerBinding string
		)
		if err := row.Scan(&maxPending, &handlerActor, &handlerBinding); err != nil {
			t.Fatalf("query type %s: %v", d.Type, err)
		}
		if !maxPending.Valid || maxPending.Int64 != d.MaxPendingMs {
			t.Fatalf("%s max_pending_ms = %v, want %d", d.Type, maxPending, d.MaxPendingMs)
		}
		if !handlerActor.Valid || handlerActor.String != d.ActorID() {
			t.Fatalf("%s handler_actor_id = %v, want %s", d.Type, handlerActor, d.ActorID())
		}
		if handlerBinding != "in_worker_bus" {
			t.Fatalf("%s handler_binding = %q, want in_worker_bus", d.Type, handlerBinding)
		}
	}
}

// TestEnsureToolActors_Idempotent verifies a second call is a no-op
// (no duplicate actor rows / type rows / audit messages).
func TestEnsureToolActors_Idempotent(t *testing.T) {
	t.Parallel()
	db := openTestChannel(t)
	ctx := context.Background()
	for i := 0; i < 3; i++ {
		if err := EnsureToolActors(ctx, EnsureConfig{
			DB: db, ChannelID: "ch-1", Now: 1700000000,
		}); err != nil {
			t.Fatalf("EnsureToolActors iter %d: %v", i, err)
		}
	}

	// Count actor_registry rows scoped to tool:* — must equal len(Catalog()).
	row := db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM actor_registry WHERE actor_id LIKE 'tool:%'`)
	var got int
	if err := row.Scan(&got); err != nil {
		t.Fatalf("count actors: %v", err)
	}
	if want := len(Catalog()); got != want {
		t.Fatalf("actor_registry tool rows = %d, want %d", got, want)
	}

	// Count type_registry rows.
	row = db.QueryRowContext(ctx, `SELECT COUNT(*) FROM type_registry`)
	if err := row.Scan(&got); err != nil {
		t.Fatalf("count types: %v", err)
	}
	if want := len(Catalog()); got != want {
		t.Fatalf("type_registry tool rows = %d, want %d", got, want)
	}

	// system.event audit row per actor → equals len(Catalog()).
	row = db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM messages WHERE type = 'system.event' AND payload LIKE '%actor_registered%'`)
	if err := row.Scan(&got); err != nil {
		t.Fatalf("count audit msgs: %v", err)
	}
	if want := len(Catalog()); got != want {
		t.Fatalf("actor_registered audit rows = %d, want %d (idempotent)", got, want)
	}
}

// TestEnsureToolActors_InputValidation verifies the helper rejects
// invalid inputs before touching sqlite.
func TestEnsureToolActors_InputValidation(t *testing.T) {
	t.Parallel()
	db := openTestChannel(t)
	cases := []struct {
		name string
		cfg  EnsureConfig
	}{
		{"nil db", EnsureConfig{ChannelID: "ch-1", Now: 1}},
		{"empty channel", EnsureConfig{DB: db, Now: 1}},
		{"non-positive now", EnsureConfig{DB: db, ChannelID: "ch-1"}},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			if err := EnsureToolActors(context.Background(), tc.cfg); err == nil {
				t.Fatalf("expected error for %s", tc.name)
			}
		})
	}
}

// TestBuildTools_HappyPath verifies BuildTools returns one entry per
// catalog descriptor with the right v4 type name.
func TestBuildTools_HappyPath(t *testing.T) {
	t.Parallel()
	db, roDB := openTestChannelRWandRO(t)
	ctx := context.Background()
	if err := EnsureToolActors(ctx, EnsureConfig{
		DB: db, ChannelID: "ch-1", Now: 1700000000,
	}); err != nil {
		t.Fatalf("EnsureToolActors: %v", err)
	}
	// Seed the caller agent so V4ize's harness doesn't reject Step 3
	// for missing sender — register.go does not seed the agent (the
	// agent is registered by the channel bootstrap saga).
	if _, err := db.ExecContext(ctx,
		`INSERT INTO actor_registry (actor_id, actor_kind, actor_binding, created_at, deregistered_at)
		 VALUES ('alice', 'agent', 'in_worker_bus', 1700000000, NULL)`); err != nil {
		t.Fatalf("seed alice: %v", err)
	}

	typeLookup, err := internalharness.LoadTypeLookup(ctx, db)
	if err != nil {
		t.Fatalf("load types: %v", err)
	}
	deps := pkgharness.New(
		internalharness.NewSQLiteStore(db),
		internalharness.NewSQLiteActors(db),
		typeLookup,
		internalharness.NewSQLiteWorkerLocks(db),
		"ch-1",
	)
	deps.Clock = func() int64 { return 1700000001_000 }

	wrapped, err := BuildTools(BuildConfig{
		DB:         db,
		ReadOnlyDB: roDB,
		ChannelID:  "ch-1",
		AgentID:    "alice",
		TurnID:     "turn:alice:t1",
		WorkDir:    t.TempDir(),
		Deps:       deps,
	})
	if err != nil {
		t.Fatalf("BuildTools: %v", err)
	}
	want := len(Catalog())
	if len(wrapped) != want {
		t.Fatalf("BuildTools returned %d tools, want %d", len(wrapped), want)
	}

	// The wrapped tools must expose the v4 type name (not the underlying
	// kimi tool's name) per L2 §3.9.4 design choice.
	want_types := make(map[string]bool, want)
	for _, d := range Catalog() {
		want_types[d.Type] = true
	}
	for _, w := range wrapped {
		if !want_types[w.Name()] {
			t.Fatalf("wrapped tool reports Name() = %q, not in catalog", w.Name())
		}
		delete(want_types, w.Name())
	}
	if len(want_types) > 0 {
		t.Fatalf("missing wrapped tools: %v", want_types)
	}
}

// TestBuildTools_ValidatesConfig verifies the helper rejects missing
// required fields before constructing tools.
func TestBuildTools_ValidatesConfig(t *testing.T) {
	t.Parallel()
	db, roDB := openTestChannelRWandRO(t)
	deps := pkgharness.Deps{
		Store:     internalharness.NewSQLiteStore(db),
		ChannelID: "ch-1",
	}
	cases := []struct {
		name string
		cfg  BuildConfig
	}{
		{"nil db", BuildConfig{ReadOnlyDB: roDB, ChannelID: "ch-1", AgentID: "a", TurnID: "t", WorkDir: "/tmp", Deps: deps}},
		{"nil readonly db", BuildConfig{DB: db, ChannelID: "ch-1", AgentID: "a", TurnID: "t", WorkDir: "/tmp", Deps: deps}},
		{"empty channel", BuildConfig{DB: db, ReadOnlyDB: roDB, AgentID: "a", TurnID: "t", WorkDir: "/tmp", Deps: deps}},
		{"empty agent", BuildConfig{DB: db, ReadOnlyDB: roDB, ChannelID: "ch-1", TurnID: "t", WorkDir: "/tmp", Deps: deps}},
		{"empty turn", BuildConfig{DB: db, ReadOnlyDB: roDB, ChannelID: "ch-1", AgentID: "a", WorkDir: "/tmp", Deps: deps}},
		{"empty workdir", BuildConfig{DB: db, ReadOnlyDB: roDB, ChannelID: "ch-1", AgentID: "a", TurnID: "t", Deps: deps}},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			if _, err := BuildTools(tc.cfg); err == nil {
				t.Fatalf("expected error for %s", tc.name)
			}
		})
	}
}
