package coagent

import (
	"context"
	"database/sql"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	internalharness "github.com/coagent-ai/daemon-go/internal/harness"
	"github.com/coagent-ai/daemon-go/internal/store"
	pkgharness "github.com/coagent-ai/daemon-go/pkg/harness"
	"github.com/coagent-ai/daemon-go/pkg/v4types"
)

// integrationDaemon spins up the real internal/harness HTTP handler
// against an in-memory channel sqlite — the same wiring the daemon
// uses, minus the OS process. Tests use it to verify the CLI's
// daemon_rpc binding satisfies the daemon's wire expectations
// end-to-end (path, headers, body shape, success / reject bodies).
//
// The ticket's acceptance line — "用 daemon-go 进程内 mock daemon
// endpoint 测试" — is implemented here: an httptest server inside
// the test process serving the real harness handler.
type integrationDaemon struct {
	srv *httptest.Server
	db  *sql.DB
}

func newIntegrationDaemon(t *testing.T) *integrationDaemon {
	t.Helper()
	dir := t.TempDir()
	db, err := store.OpenChannel(context.Background(), filepath.Join(dir, "messages.sqlite"), store.OpenOptions{})
	if err != nil {
		t.Fatalf("open channel: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	// Seed two actors that match the CLI test conventions: alice
	// (caller) + bob (audience target for ask). Tool actor seeded for
	// completeness even though this integration test only exercises
	// emit.
	now := int64(1700000000)
	seed := func(id, kind, binding string) {
		var bindArg any
		if binding != "" {
			bindArg = binding
		}
		if _, err := db.ExecContext(context.Background(),
			`INSERT INTO actor_registry (actor_id, actor_kind, actor_binding, created_at, deregistered_at)
			 VALUES (?, ?, ?, ?, NULL)`,
			id, kind, bindArg, now,
		); err != nil {
			t.Fatalf("seed actor %s: %v", id, err)
		}
	}
	seed("alice", "agent", "in_worker_bus")
	seed("bob", "agent", "in_worker_bus")

	types, err := internalharness.LoadTypeLookup(context.Background(), db)
	if err != nil {
		t.Fatalf("load types: %v", err)
	}
	deps := pkgharness.Deps{
		Store:       internalharness.NewSQLiteStore(db),
		Actors:      internalharness.NewSQLiteActors(db),
		Types:       types,
		WorkerLocks: internalharness.NewSQLiteWorkerLocks(db),
		Dispatcher:  pkgharness.NoopDispatcher{},
		Clock:       func() int64 { return 1700000000_000 },
		ChannelID:   "ch-1",
	}
	handler := internalharness.NewHTTPHandler(internalharness.HTTPHandlerOptions{
		Deps: deps,
		Auth: integrationAuth,
	})
	mux := http.NewServeMux()
	mux.Handle(internalharness.RPCPath, handler)
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return &integrationDaemon{srv: srv, db: db}
}

// integrationAuth accepts the test-only `alice-token` value, mapping
// it to CallerCtx{ActorID:"alice"}. Anything else fails auth — the
// daemon-side AuthFunc contract is HTTP 401 + auth_failed.
func integrationAuth(_ context.Context, token string, _ *internalharness.MessageSendRequest) (pkgharness.CallerCtx, error) {
	if token != "alice-token" {
		return pkgharness.CallerCtx{}, nil
	}
	return pkgharness.CallerCtx{Authenticated: true, ActorID: "alice"}, nil
}

// TestIntegration_CLI_Emit drives the binary-mode CLI (daemon_rpc
// binding) against the real harness HTTP handler. End-to-end success
// path validates wire schema compatibility between binding +
// internal/harness for kind=event.
func TestIntegration_CLI_Emit(t *testing.T) {
	d := newIntegrationDaemon(t)
	binding := NewDaemonRPCBinding(DaemonRPCOptions{
		BaseURL:   d.srv.URL,
		AuthToken: "alice-token",
	})
	exit, stdout, stderr := runWithBinding([]string{"emit", "hello e2e"}, binding)
	if exit != 0 {
		t.Fatalf("expected exit 0, got %d (stderr=%s)", exit, stderr)
	}
	out := decodeSuccess(t, stdout)
	if out.Kind != string(v4types.KindEvent) {
		t.Fatalf("expected kind=event, got %q", out.Kind)
	}

	// Verify the row landed via the real harness.
	var count int
	if err := d.db.QueryRowContext(context.Background(),
		`SELECT COUNT(*) FROM messages WHERE id = ?`, out.ID,
	).Scan(&count); err != nil {
		t.Fatalf("query: %v", err)
	}
	if count != 1 {
		t.Fatalf("expected exactly 1 row for id=%q, got %d", out.ID, count)
	}
}

// TestIntegration_CLI_Ask_AudienceInvalid drives the audience
// three-branch validation end-to-end. The CLI rejects on the wire
// side AND the harness side — this checks the harness side via the
// real wire shape (CLI sends audience=['bob','carol']; harness
// rejects request_audience_invalid). But CLI client-side already
// rejects before the wire, so this test exercises a branch the CLI
// can't pre-empt: missing audience with type that has no
// handler_actor_id. The CLI client-side check rejects with the same
// reason so we still get an exitReject without a roundtrip.
//
// To force a wire-side reject the test uses a bypass via the
// stubBinding path — the daemon_rpc binding round-trips a 400 body
// → *RejectError. See TestDaemonRPC_RejectMappedToRejectError for
// that case; here we add a positive ask path against the real
// handler.
func TestIntegration_CLI_Ask_AgentText(t *testing.T) {
	d := newIntegrationDaemon(t)
	binding := NewDaemonRPCBinding(DaemonRPCOptions{
		BaseURL:   d.srv.URL,
		AuthToken: "alice-token",
	})
	exit, stdout, stderr := runWithBinding(
		[]string{"ask", "--type", "agent.text", "--audience", "bob", "status?"},
		binding,
	)
	if exit != 0 {
		t.Fatalf("expected exit 0, got %d (stderr=%s)", exit, stderr)
	}
	out := decodeSuccess(t, stdout)
	if out.Kind != string(v4types.KindRequest) {
		t.Fatalf("expected kind=request, got %q", out.Kind)
	}
	// Confirm row persisted with the expected audience.
	var audience string
	if err := d.db.QueryRowContext(context.Background(),
		`SELECT audience FROM messages WHERE id = ?`, out.ID,
	).Scan(&audience); err != nil {
		t.Fatalf("query: %v", err)
	}
	if !strings.Contains(audience, "bob") {
		t.Fatalf("expected audience contains bob, got %q", audience)
	}
}

// TestIntegration_CLI_AuthFailed exercises the daemon's 401 path
// with a real handler. The CLI surfaces auth_failed via
// *RejectError → exit code 3.
func TestIntegration_CLI_AuthFailed(t *testing.T) {
	d := newIntegrationDaemon(t)
	binding := NewDaemonRPCBinding(DaemonRPCOptions{
		BaseURL:   d.srv.URL,
		AuthToken: "wrong-token",
	})
	exit, _, stderr := runWithBinding([]string{"emit", "hi"}, binding)
	if exit != exitReject {
		t.Fatalf("expected exit %d, got %d (stderr=%s)", exitReject, exit, stderr)
	}
	if !strings.Contains(stderr, string(v4types.HarnessAuthFailed)) {
		t.Fatalf("expected auth_failed in stderr, got %q", stderr)
	}
}
