package registry

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"path/filepath"
	"strings"
	"testing"

	"github.com/coagent-ai/daemon-go/internal/store"
	"github.com/coagent-ai/daemon-go/pkg/v4types"
)

// ---------------------------------------------------------------------------
// Test fixtures — kept private; this file lives in the same package so
// the helpers do NOT collide with actor_test.go (each file gets its own
// `helper` namespace by name).
// ---------------------------------------------------------------------------

// openChannelForTypes opens a fresh channel sqlite under t.TempDir().
// Mirrors openChannel() in actor_test.go but uses a distinct name to
// avoid duplicate-declaration errors when both files compile together.
func openChannelForTypes(t *testing.T) *sql.DB {
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

// seedActor registers `meta` in actor_registry within its own
// BEGIN IMMEDIATE tx — so subsequent Install calls (run inside a
// different tx) see the actor as already committed.
func seedActor(t *testing.T, db *sql.DB, meta ActorMeta) {
	t.Helper()
	ctx := context.Background()
	err := store.WithImmediate(ctx, db, func(ctx context.Context, conn *sql.Conn) error {
		return Register(ctx, conn, "ch-test", meta)
	})
	if err != nil {
		t.Fatalf("seedActor %s: %v", meta.ActorID, err)
	}
}

// runInstall runs Install inside a BEGIN IMMEDIATE tx — matches the
// real saga's call site and gives every test a clean tx boundary.
func runInstall(t *testing.T, db *sql.DB, rows []TypeRow, now int64) error {
	t.Helper()
	ctx := context.Background()
	return store.WithImmediate(ctx, db, func(ctx context.Context, conn *sql.Conn) error {
		return Install(ctx, conn, rows, now)
	})
}

// countTypeRows reads the type_registry row count outside any tx
// (i.e. on the *sql.DB pool) so it observes only committed state. We
// use this to verify "install failed → no type row survived" without
// leaking the test's own connection.
func countTypeRows(t *testing.T, db *sql.DB) int {
	t.Helper()
	var n int
	if err := db.QueryRowContext(context.Background(),
		`SELECT COUNT(*) FROM type_registry`,
	).Scan(&n); err != nil {
		t.Fatalf("count type_registry: %v", err)
	}
	return n
}

// minRequestResponseSchema is the smallest pair of schemas that passes
// every validator: object request + object response containing
// `status` + `reason` (matching the L2 §1.4.2 fallback contract).
// Tests mutate copies of this for negative cases.
func minRequestResponseSchema() json.RawMessage {
	return mustJSON(map[string]any{
		"request": map[string]any{
			"type": "object",
		},
		"response": map[string]any{
			"type":     "object",
			"required": []string{"status"},
			"properties": map[string]any{
				"status": map[string]any{"type": "string", "enum": []string{"completed", "failed"}},
				"reason": map[string]any{"type": "string"},
			},
		},
	})
}

// baseAdapterRow returns a TypeRow that satisfies every Install
// invariant when the adapter actor is registered ahead of time.
// Individual tests mutate one field at a time to drive each reject
// reason.
func baseAdapterRow() TypeRow {
	max := int64(5000)
	return TypeRow{
		Type:           "xhs.publish",
		AllowedKinds:   []string{"request", "response"},
		SchemasByKind:  minRequestResponseSchema(),
		HandlerBinding: HandlerBindingDaemonRPC,
		MaxPendingMs:   &max,
		HandlerActorID: "tool:xhs-adapter",
		Domain:         "xhs",
	}
}

// seedAdapter registers the canonical xhs-adapter actor used by most
// tests. Centralised so tests stay focused on the validator behaviour.
func seedAdapter(t *testing.T, db *sql.DB) {
	seedActor(t, db, ActorMeta{
		ActorID:   "tool:xhs-adapter",
		Kind:      KindTool,
		Binding:   BindingDaemonRPC,
		CreatedAt: 1_700_000_000,
	})
}

// assertReason asserts err is an InstallError with the expected
// closed-set Reason. Detail-string substring is checked too so we
// know the right call site fired (not a different validator
// accidentally returning the same reason).
func assertReason(t *testing.T, err error, want v4types.InstallReason, detailSubstr string) {
	t.Helper()
	if err == nil {
		t.Fatalf("expected InstallError, got nil")
	}
	var ie *InstallError
	if !errors.As(err, &ie) {
		t.Fatalf("expected *InstallError, got %T: %v", err, err)
	}
	if ie.Reason != want {
		t.Fatalf("Reason = %q, want %q (detail=%q)", ie.Reason, want, ie.Detail)
	}
	if detailSubstr != "" && !strings.Contains(ie.Detail, detailSubstr) {
		t.Errorf("Detail = %q, want substring %q", ie.Detail, detailSubstr)
	}
}

// ---------------------------------------------------------------------------
// Reject reason #1 — type_registry_invalid (structural)
// ---------------------------------------------------------------------------

func TestInstall_TypeRegistryInvalid_EmptyType(t *testing.T) {
	db := openChannelForTypes(t)
	seedAdapter(t, db)
	row := baseAdapterRow()
	row.Type = ""
	err := runInstall(t, db, []TypeRow{row}, 1_700_000_001)
	assertReason(t, err, v4types.InstallTypeRegistryInvalid, "type must be non-empty")
}

func TestInstall_TypeRegistryInvalid_BadAllowedKind(t *testing.T) {
	db := openChannelForTypes(t)
	seedAdapter(t, db)
	row := baseAdapterRow()
	row.AllowedKinds = []string{"request", "boguskind"}
	err := runInstall(t, db, []TypeRow{row}, 1_700_000_001)
	assertReason(t, err, v4types.InstallTypeRegistryInvalid, "boguskind")
}

func TestInstall_TypeRegistryInvalid_DuplicateAllowedKind(t *testing.T) {
	db := openChannelForTypes(t)
	seedAdapter(t, db)
	row := baseAdapterRow()
	row.AllowedKinds = []string{"request", "request"}
	err := runInstall(t, db, []TypeRow{row}, 1_700_000_001)
	assertReason(t, err, v4types.InstallTypeRegistryInvalid, "duplicate")
}

func TestInstall_TypeRegistryInvalid_BadHandlerBinding(t *testing.T) {
	db := openChannelForTypes(t)
	seedAdapter(t, db)
	row := baseAdapterRow()
	row.HandlerBinding = "rest_over_pigeons"
	err := runInstall(t, db, []TypeRow{row}, 1_700_000_001)
	assertReason(t, err, v4types.InstallTypeRegistryInvalid, "handler_binding")
}

func TestInstall_TypeRegistryInvalid_BadTerminalConvention(t *testing.T) {
	db := openChannelForTypes(t)
	seedAdapter(t, db)
	row := baseAdapterRow()
	row.TerminalConvention = "always-fail"
	err := runInstall(t, db, []TypeRow{row}, 1_700_000_001)
	assertReason(t, err, v4types.InstallTypeRegistryInvalid, "terminal_convention")
}

func TestInstall_TypeRegistryInvalid_SchemaKeyNotInAllowed(t *testing.T) {
	db := openChannelForTypes(t)
	seedAdapter(t, db)
	row := baseAdapterRow()
	// event isn't in allowed_kinds → should reject.
	row.SchemasByKind = mustJSON(map[string]any{
		"request":  map[string]any{"type": "object"},
		"response": map[string]any{"type": "object", "properties": map[string]any{"status": map[string]any{"type": "string"}, "reason": map[string]any{"type": "string"}}},
		"event":    map[string]any{"type": "object"},
	})
	err := runInstall(t, db, []TypeRow{row}, 1_700_000_001)
	assertReason(t, err, v4types.InstallTypeRegistryInvalid, "is not in allowed_kinds")
}

func TestInstall_TypeRegistryInvalid_SchemasByKindNotJSON(t *testing.T) {
	db := openChannelForTypes(t)
	seedAdapter(t, db)
	row := baseAdapterRow()
	row.SchemasByKind = json.RawMessage(`{not valid json}`)
	err := runInstall(t, db, []TypeRow{row}, 1_700_000_001)
	assertReason(t, err, v4types.InstallTypeRegistryInvalid, "schemas_by_kind")
}

func TestInstall_TypeRegistryInvalid_SchemaNotCompilable(t *testing.T) {
	db := openChannelForTypes(t)
	seedAdapter(t, db)
	row := baseAdapterRow()
	// `type` must be a string or array; an integer here forces a
	// compile error from the jsonschema/v5 validator.
	row.SchemasByKind = mustJSON(map[string]any{
		"request":  map[string]any{"type": 42},
		"response": map[string]any{"type": "object", "properties": map[string]any{"status": map[string]any{"type": "string"}, "reason": map[string]any{"type": "string"}}},
	})
	err := runInstall(t, db, []TypeRow{row}, 1_700_000_001)
	assertReason(t, err, v4types.InstallTypeRegistryInvalid, "does not compile")
}

// ---------------------------------------------------------------------------
// Reject reason #2 — adapter_timeout_missing
// ---------------------------------------------------------------------------

func TestInstall_AdapterTimeoutMissing_NilMaxPending(t *testing.T) {
	db := openChannelForTypes(t)
	seedAdapter(t, db)
	row := baseAdapterRow()
	row.MaxPendingMs = nil // tool receiver + request in allowed_kinds → required
	err := runInstall(t, db, []TypeRow{row}, 1_700_000_001)
	assertReason(t, err, v4types.InstallAdapterTimeoutMissing, "max_pending_ms")
}

func TestInstall_AdapterTimeoutMissing_ZeroMaxPending(t *testing.T) {
	db := openChannelForTypes(t)
	seedAdapter(t, db)
	row := baseAdapterRow()
	zero := int64(0)
	row.MaxPendingMs = &zero
	err := runInstall(t, db, []TypeRow{row}, 1_700_000_001)
	assertReason(t, err, v4types.InstallAdapterTimeoutMissing, "max_pending_ms")
}

// Counter-example: agent receiver shouldn't trigger the rule even if
// MaxPendingMs is nil. Confirms the spec carve-out that adapter_timeout
// only fires for tool receivers.
func TestInstall_AdapterTimeoutMissing_AgentReceiverOK(t *testing.T) {
	db := openChannelForTypes(t)
	// Seed an agent actor instead of a tool.
	seedActor(t, db, ActorMeta{
		ActorID:   "agent:writer",
		Kind:      KindAgent,
		Binding:   BindingDaemonRPC,
		CreatedAt: 1_700_000_000,
	})
	row := baseAdapterRow()
	row.HandlerActorID = "agent:writer"
	row.MaxPendingMs = nil // OK because handler is an agent, not tool.
	if err := runInstall(t, db, []TypeRow{row}, 1_700_000_001); err != nil {
		t.Fatalf("agent receiver should not require max_pending_ms: %v", err)
	}
}

// ---------------------------------------------------------------------------
// Reject reason #3 — handler_actor_not_registered
// ---------------------------------------------------------------------------

func TestInstall_HandlerActorNotRegistered_Missing(t *testing.T) {
	db := openChannelForTypes(t)
	// Deliberately skip seedAdapter — Install should reject.
	row := baseAdapterRow()
	err := runInstall(t, db, []TypeRow{row}, 1_700_000_001)
	assertReason(t, err, v4types.InstallHandlerActorNotRegistered, "tool:xhs-adapter")
}

func TestInstall_HandlerActorNotRegistered_Deregistered(t *testing.T) {
	db := openChannelForTypes(t)
	seedAdapter(t, db)
	// Soft-deregister the adapter — Install should treat that as
	// "not registered" per the active-only lookup contract.
	ctx := context.Background()
	if err := store.WithImmediate(ctx, db, func(ctx context.Context, conn *sql.Conn) error {
		return Deregister(ctx, conn, "tool:xhs-adapter", 1_700_000_100)
	}); err != nil {
		t.Fatalf("Deregister: %v", err)
	}
	row := baseAdapterRow()
	err := runInstall(t, db, []TypeRow{row}, 1_700_000_200)
	assertReason(t, err, v4types.InstallHandlerActorNotRegistered, "deregistered")
}

// ---------------------------------------------------------------------------
// Reject reason #4 — handler_actor_binding_mismatch
// ---------------------------------------------------------------------------

func TestInstall_HandlerActorBindingMismatch(t *testing.T) {
	db := openChannelForTypes(t)
	// Seed an adapter actor with daemon_rpc binding, then point a row
	// at it but declare handler_binding=in_worker_bus.
	seedAdapter(t, db)
	row := baseAdapterRow()
	row.HandlerBinding = HandlerBindingInWorkerBus
	// Bypass adapter_timeout check (max_pending_ms must still be set
	// because handler is a tool actor and request is in allowed_kinds).
	err := runInstall(t, db, []TypeRow{row}, 1_700_000_001)
	assertReason(t, err, v4types.InstallHandlerActorBindingMismatch, "in_worker_bus")
}

// ---------------------------------------------------------------------------
// Reject reason #5 — fallback_response_schema_invalid
// ---------------------------------------------------------------------------

// fallbackCases drives one test per L2 §1.4.2 fallback sample. Each
// case crafts a response schema that REJECTS the matching sample but
// accepts the other two — proving the validator iterates the full
// sample list rather than stopping at the first one.
//
// L2 §1.4.2 normative samples (in spec order):
//
//	{status: failed, reason: unanswered_timeout}
//	{status: failed, reason: adapter_default_timeout}
//	{status: failed, reason: receiver_unavailable}
//
// We use `reason: {enum: [...]}` to narrow the schema so each
// case rejects exactly one sample. The enum carve-out is also the
// canonical way the spec says the rule fails ("reason 字段必须 type:
// string，不能 enum 收窄").
func TestInstall_FallbackResponseSchemaInvalid(t *testing.T) {
	cases := []struct {
		name           string
		excludedReason string
	}{
		{"rejects unanswered_timeout", string(v4types.TerminalUnansweredTimeout)},
		{"rejects adapter_default_timeout", string(v4types.TerminalAdapterDefaultTimeout)},
		{"rejects receiver_unavailable", string(v4types.TerminalReceiverUnavailable)},
		// FIX-6 §3 / codex t89: long-pending scheduler Step 2 emits
		// `human_unanswered_timeout`; install MUST fail-fast when the
		// type's response schema enum-narrows reason to exclude it.
		{"rejects human_unanswered_timeout", string(v4types.TerminalHumanUnansweredTimeout)},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			db := openChannelForTypes(t)
			seedAdapter(t, db)
			// Build an enum-narrowed response schema that accepts every
			// reason except the one under test.
			allReasons := []string{
				string(v4types.TerminalUnansweredTimeout),
				string(v4types.TerminalAdapterDefaultTimeout),
				string(v4types.TerminalReceiverUnavailable),
				string(v4types.TerminalHumanUnansweredTimeout),
			}
			kept := make([]string, 0, len(allReasons)-1)
			for _, r := range allReasons {
				if r != c.excludedReason {
					kept = append(kept, r)
				}
			}
			row := baseAdapterRow()
			row.SchemasByKind = mustJSON(map[string]any{
				"request": map[string]any{"type": "object"},
				"response": map[string]any{
					"type":     "object",
					"required": []string{"status"},
					"properties": map[string]any{
						"status": map[string]any{"type": "string", "enum": []string{"completed", "failed"}},
						"reason": map[string]any{"type": "string", "enum": kept},
					},
				},
			})
			err := runInstall(t, db, []TypeRow{row}, 1_700_000_001)
			assertReason(t, err, v4types.InstallFallbackResponseSchemaInvalid, c.excludedReason)
		})
	}
}

func TestInstall_FallbackResponseSchemaInvalid_MissingResponseSchema(t *testing.T) {
	db := openChannelForTypes(t)
	seedAdapter(t, db)
	row := baseAdapterRow()
	// schemas_by_kind has only request — response missing entirely.
	// Structural validator accepts (keys ⊆ allowed_kinds), so the
	// fallback validator must catch it.
	row.SchemasByKind = mustJSON(map[string]any{
		"request": map[string]any{"type": "object"},
	})
	err := runInstall(t, db, []TypeRow{row}, 1_700_000_001)
	assertReason(t, err, v4types.InstallFallbackResponseSchemaInvalid, "response is required")
}

// ---------------------------------------------------------------------------
// Acceptance: install 失败时 actor_registry 不被污染
// ---------------------------------------------------------------------------

// TestInstall_ActorRegistryNotPolluted mirrors the saga's
// "actor first, type second, single tx" install contract from L2 §3.5.
// When Install rejects a row, the surrounding tx's rollback MUST take
// the actor_registry INSERT down with it — otherwise the channel ends
// up with an actor that has no types, contradicting the install
// ordering invariant.
func TestInstall_ActorRegistryNotPolluted(t *testing.T) {
	db := openChannelForTypes(t)
	ctx := context.Background()

	row := baseAdapterRow()
	row.MaxPendingMs = nil // forces adapter_timeout_missing

	// Drive the exact saga pattern: same tx for actor seed + Install.
	err := store.WithImmediate(ctx, db, func(ctx context.Context, conn *sql.Conn) error {
		if err := Register(ctx, conn, "ch-test", ActorMeta{
			ActorID:   "tool:xhs-adapter",
			Kind:      KindTool,
			Binding:   BindingDaemonRPC,
			CreatedAt: 1_700_000_000,
		}); err != nil {
			return err
		}
		return Install(ctx, conn, []TypeRow{row}, 1_700_000_001)
	})
	if err == nil {
		t.Fatalf("expected Install to fail and roll back the tx")
	}
	var ie *InstallError
	if !errors.As(err, &ie) || ie.Reason != v4types.InstallAdapterTimeoutMissing {
		t.Fatalf("expected adapter_timeout_missing, got %v", err)
	}

	// After rollback, neither table should hold any row.
	if n := countTypeRows(t, db); n != 0 {
		t.Errorf("type_registry rows = %d, want 0 (rollback failed)", n)
	}
	var actors int
	if err := db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM actor_registry WHERE actor_id = ?`,
		"tool:xhs-adapter",
	).Scan(&actors); err != nil {
		t.Fatalf("count actor_registry: %v", err)
	}
	if actors != 0 {
		t.Errorf("actor_registry rows = %d, want 0 (install pollution)", actors)
	}
}

// ---------------------------------------------------------------------------
// Happy path — Install commits and rows land in type_registry
// ---------------------------------------------------------------------------

func TestInstall_HappyPath_AdapterRow(t *testing.T) {
	db := openChannelForTypes(t)
	seedAdapter(t, db)
	if err := runInstall(t, db, []TypeRow{baseAdapterRow()}, 1_700_000_001); err != nil {
		t.Fatalf("Install: %v", err)
	}
	if n := countTypeRows(t, db); n != 1 {
		t.Errorf("type_registry rows = %d, want 1", n)
	}
	// Sanity: created_at column populated.
	var createdAt int64
	if err := db.QueryRowContext(context.Background(),
		`SELECT created_at FROM type_registry WHERE type=?`, "xhs.publish",
	).Scan(&createdAt); err != nil {
		t.Fatalf("read created_at: %v", err)
	}
	if createdAt != 1_700_000_001 {
		t.Errorf("created_at = %d, want 1700000001", createdAt)
	}
}

// TestInstall_HappyPath_XHSCreatorTemplate exercises the full L4 §2.2
// xhs-creator template against Install. Acts as the integration check
// that XHSCreatorTypes() produces rows the validator accepts and that
// the schemas accept the 3 fallback samples (proving the helper builds
// reason: type=string correctly).
func TestInstall_HappyPath_XHSCreatorTemplate(t *testing.T) {
	db := openChannelForTypes(t)
	seedActor(t, db, ActorMeta{
		ActorID:   XHSCreatorAdapterActorID,
		Kind:      KindTool,
		Binding:   BindingDaemonRPC,
		CreatedAt: 1_700_000_000,
	})
	rows := XHSCreatorTypes()
	if got, want := len(rows), 6; got != want {
		t.Fatalf("XHSCreatorTypes returned %d rows, want %d", got, want)
	}
	if err := runInstall(t, db, rows, 1_700_000_001); err != nil {
		t.Fatalf("Install xhs-creator template: %v", err)
	}
	if n := countTypeRows(t, db); n != 6 {
		t.Errorf("type_registry rows = %d, want 6", n)
	}
	// Spot-check a row's column wiring (max_pending_ms + handler_actor).
	var maxPending sql.NullInt64
	var handlerActor sql.NullString
	if err := db.QueryRowContext(context.Background(),
		`SELECT max_pending_ms, handler_actor_id FROM type_registry WHERE type=?`,
		"xhs.publish",
	).Scan(&maxPending, &handlerActor); err != nil {
		t.Fatalf("read xhs.publish row: %v", err)
	}
	if !maxPending.Valid || maxPending.Int64 != xhsAdapterTimeoutMs {
		t.Errorf("max_pending_ms = %v, want %d", maxPending, xhsAdapterTimeoutMs)
	}
	if !handlerActor.Valid || handlerActor.String != XHSCreatorAdapterActorID {
		t.Errorf("handler_actor_id = %v, want %q", handlerActor, XHSCreatorAdapterActorID)
	}

	// Event-only row should have NULL max_pending_ms + NULL handler_actor_id.
	if err := db.QueryRowContext(context.Background(),
		`SELECT max_pending_ms, handler_actor_id FROM type_registry WHERE type=?`,
		"xhs.note.archived",
	).Scan(&maxPending, &handlerActor); err != nil {
		t.Fatalf("read xhs.note.archived row: %v", err)
	}
	if maxPending.Valid {
		t.Errorf("xhs.note.archived max_pending_ms = %v, want NULL", maxPending)
	}
	if handlerActor.Valid {
		t.Errorf("xhs.note.archived handler_actor_id = %v, want NULL", handlerActor)
	}
}

// TestInstall_NoOpOnEmptyRows guards the documented "rows is empty →
// no-op" contract — useful for callers that build templates conditionally.
func TestInstall_NoOpOnEmptyRows(t *testing.T) {
	db := openChannelForTypes(t)
	if err := runInstall(t, db, nil, 1); err != nil {
		t.Fatalf("Install with nil rows: %v", err)
	}
	if n := countTypeRows(t, db); n != 0 {
		t.Errorf("type_registry rows = %d, want 0", n)
	}
}
