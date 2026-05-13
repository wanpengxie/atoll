package bootstrap

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/coagent-ai/daemon-go/internal/store"
)

// ---------------------------------------------------------------------------
// Test fixtures + helpers
// ---------------------------------------------------------------------------

// fixtureClock returns a deterministic now() that increments by 1 on each
// call so completed_at > started_at without flakiness.
func fixtureClock(start int64) func() int64 {
	cur := start
	return func() int64 {
		cur++
		return cur
	}
}

// happyParams builds a CreateParams that exercises every step of the
// 9-step saga (system + 2 humans + agent + 1 tool adapter with 1 type +
// 1 business type pointing at the adapter actor).
func happyParams(t *testing.T, workdirRoot, requestID, channelID string) CreateParams {
	t.Helper()
	schema, _ := json.Marshal(map[string]any{
		"request": map[string]any{"type": "object"},
	})
	maxPending := int64(30000)
	return CreateParams{
		CreateRequestID: requestID,
		ChannelID:       channelID,
		WorkdirPath:     filepath.Join(workdirRoot, channelID),
		HumanMembers: []HumanMember{
			{ActorID: "user-001"},
			{ActorID: "user-002"},
		},
		ChannelAgent: ChannelAgentSpec{}, // empty → default <channel_id>:channel-agent
		ToolAdapters: []ToolAdapterSpec{
			{
				ActorID: "tool:xhs-publisher",
				Binding: "daemon_rpc",
				TypeRows: []TypeRegistryRow{
					{
						Type:           "xhs.post.publish",
						AllowedKinds:   []string{"request", "response"},
						SchemasByKind:  schema,
						HandlerBinding: "daemon_rpc",
						MaxPendingMs:   &maxPending,
						HandlerActorID: "tool:xhs-publisher",
					},
				},
			},
		},
		BusinessTypes: []TypeRegistryRow{
			{
				Type:           "xhs.draft.created",
				AllowedKinds:   []string{"event"},
				SchemasByKind:  schema,
				HandlerBinding: "in_worker_bus",
				Domain:         "xhs",
				HandlerActorID: "tool:xhs-publisher", // resolves to step-6 actor
			},
		},
	}
}

// newSaga opens a fresh daemon sqlite + returns a Saga wired to it.
// Workdir root is t.TempDir() based so the test is hermetic.
func newSaga(t *testing.T, opts ...Option) (*Saga, *sql.DB, string) {
	t.Helper()
	ctx := context.Background()
	dir := t.TempDir()
	daemonPath := filepath.Join(dir, "daemon.sqlite")
	daemonDB, err := store.OpenDaemon(ctx, daemonPath, store.OpenOptions{})
	if err != nil {
		t.Fatalf("OpenDaemon: %v", err)
	}
	t.Cleanup(func() { _ = daemonDB.Close() })

	allOpts := append([]Option{WithNow(fixtureClock(1_700_000_000))}, opts...)
	return New(daemonDB, allOpts...), daemonDB, dir
}

// countRows is a tiny QueryRow + Scan(&n) helper used to assert row
// presence/absence after rollbacks.
func countRows(t *testing.T, ctx context.Context, db *sql.DB, sqlText string, args ...any) int {
	t.Helper()
	var n int
	if err := db.QueryRowContext(ctx, sqlText, args...).Scan(&n); err != nil {
		t.Fatalf("count %q: %v", sqlText, err)
	}
	return n
}

func mustStatus(t *testing.T, ctx context.Context, db *sql.DB, requestID string) string {
	t.Helper()
	var status string
	if err := db.QueryRowContext(ctx,
		`SELECT status FROM bootstrap_registry WHERE create_request_id = ?`,
		requestID,
	).Scan(&status); err != nil {
		t.Fatalf("read status: %v", err)
	}
	return status
}

// ---------------------------------------------------------------------------
// 1. Happy path — every step lands; bootstrap_registry → completed.
// ---------------------------------------------------------------------------

func TestChannelCreate_HappyPath(t *testing.T) {
	ctx := context.Background()
	saga, daemonDB, workRoot := newSaga(t)

	p := happyParams(t, workRoot, "req-1", "ch-1")
	res, err := saga.ChannelCreate(ctx, p)
	if err != nil {
		t.Fatalf("ChannelCreate happy: %v", err)
	}
	if res.ChannelID != "ch-1" || res.Status != StatusCompleted {
		t.Errorf("Result = %+v, want ch-1 + completed", res)
	}

	// bootstrap_registry row exists and is completed.
	if s := mustStatus(t, ctx, daemonDB, "req-1"); s != StatusCompleted {
		t.Errorf("bootstrap_registry status = %q, want completed", s)
	}

	// Workdir + channel sqlite exist.
	channelDBPath := filepath.Join(p.WorkdirPath, channelDBFilename)
	if _, err := os.Stat(channelDBPath); err != nil {
		t.Errorf("channel sqlite missing: %v", err)
	}

	// Open the channel db and assert the seeded rows.
	channelDB, err := store.OpenChannel(ctx, channelDBPath, store.OpenOptions{})
	if err != nil {
		t.Fatalf("OpenChannel: %v", err)
	}
	t.Cleanup(func() { _ = channelDB.Close() })

	// Actor registry: 1 system + 2 humans + 1 agent + 1 tool = 5 rows.
	if n := countRows(t, ctx, channelDB, `SELECT COUNT(*) FROM actor_registry`); n != 5 {
		t.Errorf("actor_registry rows = %d, want 5", n)
	}
	// Channel agent uses default name.
	wantAgent := "ch-1:" + DefaultChannelAgentName
	if n := countRows(t, ctx, channelDB,
		`SELECT COUNT(*) FROM actor_registry WHERE actor_id = ? AND actor_kind = 'agent'`,
		wantAgent); n != 1 {
		t.Errorf("default channel agent missing for actor_id=%q", wantAgent)
	}

	// Type registry: 1 adapter type + 1 business type = 2.
	if n := countRows(t, ctx, channelDB, `SELECT COUNT(*) FROM type_registry`); n != 2 {
		t.Errorf("type_registry rows = %d, want 2", n)
	}

	// Step 8a channel_created event.
	eventID := channelCreatedEventID("req-1")
	if n := countRows(t, ctx, channelDB,
		`SELECT COUNT(*) FROM messages WHERE id = ? AND type = 'system.event' AND visibility = 'system'`,
		eventID); n != 1 {
		t.Errorf("channel_created event missing (id=%s)", eventID)
	}
}

// ---------------------------------------------------------------------------
// 2. Idempotency — same create_request_id three times → same result.
// ---------------------------------------------------------------------------

func TestChannelCreate_Idempotent_Completed(t *testing.T) {
	ctx := context.Background()
	saga, daemonDB, workRoot := newSaga(t)
	p := happyParams(t, workRoot, "req-id", "ch-id")

	first, err := saga.ChannelCreate(ctx, p)
	if err != nil {
		t.Fatalf("first call: %v", err)
	}
	for i := 0; i < 2; i++ {
		again, err := saga.ChannelCreate(ctx, p)
		if err != nil {
			t.Fatalf("retry %d: %v", i, err)
		}
		if again != first {
			t.Errorf("retry %d Result = %+v, want %+v", i, again, first)
		}
	}
	// Should still be exactly one row.
	if n := countRows(t, ctx, daemonDB,
		`SELECT COUNT(*) FROM bootstrap_registry WHERE create_request_id = ?`,
		"req-id"); n != 1 {
		t.Errorf("expected exactly 1 registry row, got %d", n)
	}
}

func TestChannelCreate_Idempotent_InProgress(t *testing.T) {
	ctx := context.Background()
	saga, _, _ := newSaga(t)
	// Hand-craft an in_progress row, then retry — should return ErrBootstrapInProgress.
	if _, err := saga.daemonDB.ExecContext(ctx,
		`INSERT INTO bootstrap_registry (create_request_id, channel_id, status, workdir_path, started_at)
		 VALUES ('req-ip', 'ch-ip', 'in_progress', '/tmp/wd', 1)`,
	); err != nil {
		t.Fatalf("seed in_progress row: %v", err)
	}
	res, err := saga.ChannelCreate(ctx, CreateParams{
		CreateRequestID: "req-ip",
		ChannelID:       "ch-ip",
		WorkdirPath:     "/tmp/wd",
	})
	if !errors.Is(err, ErrBootstrapInProgress) {
		t.Fatalf("err = %v, want ErrBootstrapInProgress", err)
	}
	if res.ChannelID != "ch-ip" || res.Status != StatusInProgress {
		t.Errorf("Result = %+v, want ch-ip + in_progress", res)
	}
}

func TestChannelCreate_Idempotent_RolledBack(t *testing.T) {
	ctx := context.Background()
	saga, _, _ := newSaga(t)
	if _, err := saga.daemonDB.ExecContext(ctx,
		`INSERT INTO bootstrap_registry (create_request_id, channel_id, status, workdir_path, started_at, rollback_reason)
		 VALUES ('req-rb', 'ch-rb', 'rolled_back', '/tmp/wd', 1, 'mock')`,
	); err != nil {
		t.Fatalf("seed rolled_back row: %v", err)
	}
	res, err := saga.ChannelCreate(ctx, CreateParams{
		CreateRequestID: "req-rb",
		ChannelID:       "ch-rb",
		WorkdirPath:     "/tmp/wd",
	})
	if !errors.Is(err, ErrBootstrapRolledBack) {
		t.Fatalf("err = %v, want ErrBootstrapRolledBack", err)
	}
	if res.Status != StatusRolledBack {
		t.Errorf("Result.Status = %q, want rolled_back", res.Status)
	}
}

// ---------------------------------------------------------------------------
// 3. Param validation — minimal happy + error rejects.
// ---------------------------------------------------------------------------

func TestChannelCreate_ParamsInvalid(t *testing.T) {
	ctx := context.Background()
	saga, _, workRoot := newSaga(t)

	cases := []struct {
		name string
		mut  func(*CreateParams)
		want string
	}{
		{"empty create_request_id", func(p *CreateParams) { p.CreateRequestID = "" }, "create_request_id"},
		{"empty channel_id", func(p *CreateParams) { p.ChannelID = "" }, "channel_id"},
		{"empty workdir_path", func(p *CreateParams) { p.WorkdirPath = "" }, "workdir_path"},
		{"relative workdir_path", func(p *CreateParams) { p.WorkdirPath = "relative/path" }, "absolute"},
		{"bad tool binding", func(p *CreateParams) {
			p.ToolAdapters = []ToolAdapterSpec{{ActorID: "tool:x", Binding: "bogus"}}
		}, "binding"},
		{"missing type", func(p *CreateParams) {
			schema, _ := json.Marshal(map[string]any{})
			p.BusinessTypes = []TypeRegistryRow{{
				AllowedKinds:   []string{"event"},
				SchemasByKind:  schema,
				HandlerBinding: "in_worker_bus",
			}}
		}, "type is required"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			p := happyParams(t, workRoot, "req-"+c.name, "ch-"+c.name)
			c.mut(&p)
			_, err := saga.ChannelCreate(ctx, p)
			if !errors.Is(err, ErrParamsInvalid) {
				t.Fatalf("err = %v, want ErrParamsInvalid", err)
			}
			if !strings.Contains(err.Error(), c.want) {
				t.Errorf("err = %q, want substring %q", err.Error(), c.want)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// 4. Step-by-step failure injection — rollback compensation table.
// ---------------------------------------------------------------------------

func TestChannelCreate_StepFailures_TableDriven(t *testing.T) {
	ctx := context.Background()
	failBoom := errors.New("boom")

	type expect struct {
		status              string // bootstrap_registry status after the call
		workdirExists       bool
		channelDBExists     bool
		errSubstr           string
		needsExternalReason bool // skip the rollback_reason check when true
	}

	cases := []struct {
		name     string
		fp       string
		expected expect
	}{
		{"step1_insert", fpStep1Insert, expect{
			status:              "",
			workdirExists:       false,
			errSubstr:           "step1 insert",
			needsExternalReason: true,
		}},
		{"step2_mkdir", fpStep2Mkdir, expect{
			status:        StatusRolledBack,
			workdirExists: false,
			errSubstr:     "step2 mkdir",
		}},
		{"step2_open_channel", fpStep2OpenCh, expect{
			status:        StatusRolledBack,
			workdirExists: false, // compensate rm's the workdir
			errSubstr:     "step2 open channel",
		}},
		{"step3_system_actor", fpStep3System, expect{
			status:        StatusRolledBack,
			workdirExists: false,
			errSubstr:     "step3 system actor",
		}},
		{"step4_human_member", fpStep4Human, expect{
			status:        StatusRolledBack,
			workdirExists: false,
			errSubstr:     "step4 human member",
		}},
		{"step5_channel_agent", fpStep5Agent, expect{
			status:        StatusRolledBack,
			workdirExists: false,
			errSubstr:     "step5 channel agent",
		}},
		{"step6_adapter", fpStep6Adapter, expect{
			status:        StatusRolledBack,
			workdirExists: false,
			errSubstr:     "step6 adapter",
		}},
		{"step7_business_type", fpStep7Type, expect{
			status:        StatusRolledBack,
			workdirExists: false,
			errSubstr:     "step7 business type",
		}},
		{"step8a_emit", fpStep8aEmit, expect{
			status:        StatusRolledBack,
			workdirExists: false, // compensate path runs on step 8a failure
			errSubstr:     "step8a emit",
		}},
		{"step8b_complete", fpStep8bComplete, expect{
			// 8b failure intentionally leaves status=in_progress + workdir/sqlite intact.
			status:          StatusInProgress,
			workdirExists:   true,
			channelDBExists: true,
			errSubstr:       "step8b complete",
		}},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			fp := map[string]error{c.fp: failBoom}
			saga, daemonDB, workRoot := newSaga(t, withFailpoints(fp))

			p := happyParams(t, workRoot, "req-"+c.name, "ch-"+c.name)
			_, err := saga.ChannelCreate(ctx, p)
			if err == nil {
				t.Fatalf("expected error, got nil")
			}
			if !strings.Contains(err.Error(), c.expected.errSubstr) {
				t.Fatalf("err = %q, want substring %q", err.Error(), c.expected.errSubstr)
			}

			// Step 1 failure → no row inserted at all.
			if c.expected.status == "" {
				if n := countRows(t, ctx, daemonDB,
					`SELECT COUNT(*) FROM bootstrap_registry WHERE create_request_id = ?`,
					"req-"+c.name); n != 0 {
					t.Errorf("expected no registry row, got %d", n)
				}
				return
			}

			// Otherwise the row should be at the expected terminal status.
			if got := mustStatus(t, ctx, daemonDB, "req-"+c.name); got != c.expected.status {
				t.Errorf("status = %q, want %q", got, c.expected.status)
			}

			// workdir / channel sqlite presence.
			workdirInfo, _ := os.Stat(p.WorkdirPath)
			if c.expected.workdirExists && workdirInfo == nil {
				t.Errorf("workdir %q expected to exist", p.WorkdirPath)
			}
			if !c.expected.workdirExists && workdirInfo != nil {
				t.Errorf("workdir %q expected to be removed", p.WorkdirPath)
			}
			if c.expected.channelDBExists {
				if _, err := os.Stat(filepath.Join(p.WorkdirPath, channelDBFilename)); err != nil {
					t.Errorf("channel sqlite expected to exist: %v", err)
				}
			}

			// rollback_reason populated on rolled_back paths.
			if c.expected.status == StatusRolledBack && !c.expected.needsExternalReason {
				var reason sql.NullString
				if err := daemonDB.QueryRowContext(ctx,
					`SELECT rollback_reason FROM bootstrap_registry WHERE create_request_id = ?`,
					"req-"+c.name,
				).Scan(&reason); err != nil {
					t.Fatalf("read rollback_reason: %v", err)
				}
				if !reason.Valid || reason.String == "" {
					t.Errorf("rollback_reason empty on %s failure", c.name)
				}
			}
		})
	}
}

// ---------------------------------------------------------------------------
// 5. handler_actor_id integrity check inside the seed tx.
// ---------------------------------------------------------------------------

func TestChannelCreate_HandlerActorIDMissing(t *testing.T) {
	ctx := context.Background()
	saga, daemonDB, workRoot := newSaga(t)

	p := happyParams(t, workRoot, "req-h", "ch-h")
	// Point a business type at a non-existent actor.
	p.BusinessTypes[0].HandlerActorID = "tool:does-not-exist"

	_, err := saga.ChannelCreate(ctx, p)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !strings.Contains(err.Error(), "handler_actor_id") {
		t.Errorf("err = %q, want substring 'handler_actor_id'", err.Error())
	}
	if got := mustStatus(t, ctx, daemonDB, "req-h"); got != StatusRolledBack {
		t.Errorf("status = %q, want rolled_back", got)
	}
}

// ---------------------------------------------------------------------------
// 6. Concurrent CAS — a second call with a different create_request_id but
//    the same channel_id should fail (UNIQUE constraint on channel_id).
// ---------------------------------------------------------------------------

func TestChannelCreate_ChannelIDUnique(t *testing.T) {
	ctx := context.Background()
	saga, _, workRoot := newSaga(t)

	p := happyParams(t, workRoot, "req-a", "ch-shared")
	if _, err := saga.ChannelCreate(ctx, p); err != nil {
		t.Fatalf("first call: %v", err)
	}

	// Second attempt — different request_id, same channel_id.
	p2 := happyParams(t, workRoot, "req-b", "ch-shared")
	p2.WorkdirPath = filepath.Join(workRoot, "ch-shared-alt")
	_, err := saga.ChannelCreate(ctx, p2)
	if err == nil {
		t.Fatal("expected UNIQUE violation, got nil")
	}
	if !strings.Contains(strings.ToLower(err.Error()), "unique") &&
		!strings.Contains(err.Error(), "step1 insert") {
		t.Errorf("err = %q, want UNIQUE-related", err.Error())
	}
}

// ---------------------------------------------------------------------------
// 7. Filesystem injection sanity — mkdir failure produces a rolled_back row
//    even when stat doesn't see the workdir created.
// ---------------------------------------------------------------------------

func TestChannelCreate_FilesystemInjection_Mkdir(t *testing.T) {
	ctx := context.Background()

	mkdirCalls := 0
	rmCalls := 0
	statCalls := 0
	mkdir := func(path string, perm os.FileMode) error {
		mkdirCalls++
		return fmt.Errorf("simulated mkdir EIO at %s", path)
	}
	rmAll := func(path string) error {
		rmCalls++
		return os.RemoveAll(path)
	}
	stat := func(path string) (fs.FileInfo, error) {
		statCalls++
		return os.Stat(path)
	}

	saga, daemonDB, workRoot := newSaga(t, WithFilesystem(mkdir, rmAll, stat))
	p := happyParams(t, workRoot, "req-fs", "ch-fs")
	_, err := saga.ChannelCreate(ctx, p)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if mkdirCalls != 1 {
		t.Errorf("mkdir calls = %d, want 1", mkdirCalls)
	}
	if rmCalls != 1 {
		t.Errorf("rmAll calls = %d, want 1", rmCalls)
	}
	if got := mustStatus(t, ctx, daemonDB, "req-fs"); got != StatusRolledBack {
		t.Errorf("status = %q, want rolled_back", got)
	}
}
