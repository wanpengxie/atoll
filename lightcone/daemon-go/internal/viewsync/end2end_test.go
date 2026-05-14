package viewsync

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	internalharness "github.com/coagent-ai/daemon-go/internal/harness"
	"github.com/coagent-ai/daemon-go/internal/store"
	pkgharness "github.com/coagent-ai/daemon-go/pkg/harness"
)

// openChannelWithSystemActor opens a fresh channel-local sqlite db and
// seeds the `system` actor + the `agent.text` / `system.event` core
// type rows so a harness Write call accepts a system.event emit. We
// bypass registry.Install for the actor seeding because we only need
// the rows, not the audit-event side effects.
func openChannelWithSystemActor(t *testing.T) *sql.DB {
	t.Helper()
	dir := t.TempDir()
	db, err := store.OpenChannel(context.Background(), filepath.Join(dir, "messages.sqlite"), store.OpenOptions{})
	if err != nil {
		t.Fatalf("open channel: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	now := int64(1700000000)
	if _, err := db.ExecContext(context.Background(),
		`INSERT INTO actor_registry (actor_id, actor_kind, actor_binding, created_at, deregistered_at)
		 VALUES ('system', 'system', NULL, ?, NULL)`, now); err != nil {
		t.Fatalf("seed system: %v", err)
	}
	return db
}

// buildE2EDeps wires a harness Deps bundle bound to the supplied DB.
// Mirrors internal/harness's buildSqliteDeps test helper but lives in
// viewsync so we don't reach into another package's *_test.go.
func buildE2EDeps(t *testing.T, db *sql.DB) pkgharness.Deps {
	t.Helper()
	types, err := internalharness.LoadTypeLookup(context.Background(), db)
	if err != nil {
		t.Fatalf("load types: %v", err)
	}
	return pkgharness.Deps{
		Store:       internalharness.NewSQLiteStore(db),
		Actors:      internalharness.NewSQLiteActors(db),
		Types:       types,
		WorkerLocks: internalharness.NewSQLiteWorkerLocks(db),
		Dispatcher:  pkgharness.NoopDispatcher{},
		Clock:       func() int64 { return 1700000010000 },
		ChannelID:   "ch-1",
	}
}

// TestEnd2End_PushFails_EmitsViewSyncFailed_ResyncReturnsIt is the M1.3
// gate that wires every T15 piece together against real sqlite truth:
//
//  1. HTTPPusher posts to a server stub that returns 500.
//  2. The configured HarnessFailureSink lands a system.event payload.kind
//     =view_sync_failed row in the channel-local store.
//  3. ResyncClient pulls that row back through the daemon resync handler,
//     proving the operator-monitoring loop closes round-trip.
//
// This is the smallest fixture that anchors all three T15 deliverables
// (push interface + view_sync_failed emit + resync RPC) without
// touching a real Node server.
func TestEnd2End_PushFails_EmitsViewSyncFailed_ResyncReturnsIt(t *testing.T) {
	t.Parallel()

	db := openChannelWithSystemActor(t)
	deps := buildE2EDeps(t, db)

	// Failing upstream server — 5xx to exercise the http_status branch.
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(`{"error":"server down"}`))
	}))
	t.Cleanup(upstream.Close)

	sink := &HarnessFailureSink{
		Writer:        DefaultHarnessWriter(deps),
		SystemActorID: "system",
	}
	pusher, err := NewHTTPPusher(HTTPPusherOptions{
		BaseURL:       upstream.URL,
		HTTPClient:    upstream.Client(),
		Failure:       sink,
		Clock:         func() int64 { return 1700000010000 },
		RetryAttempts: 1, // single attempt — this test pins the failure-emit shape, not retry behavior
	})
	if err != nil {
		t.Fatalf("new pusher: %v", err)
	}

	failed := newTestEnvelope("ev-real")
	failed.ChannelID = "ch-1"
	if _, err := pusher.PushToServer(context.Background(), failed); err == nil {
		t.Fatalf("expected push error")
	}

	// Stand up the resync handler against the same db so the server
	// stand-in pulls the truth back the same way a real server would.
	resyncSrv := httptest.NewServer(func() http.Handler {
		mux := http.NewServeMux()
		mux.Handle(ResyncRPCPath, NewResyncHandler(ResyncHandlerOptions{
			Resolver: StaticResolver("ch-1", NewSQLiteResyncStore(db)),
			Auth: func(_ context.Context, token string, _ *ResyncRequest) error {
				if token != "server-token" {
					return errors.New("token invalid")
				}
				return nil
			},
		}))
		return mux
	}())
	t.Cleanup(resyncSrv.Close)

	client, err := NewResyncClient(ResyncClientOptions{
		BaseURL:    resyncSrv.URL,
		AuthToken:  "server-token",
		HTTPClient: resyncSrv.Client(),
	})
	if err != nil {
		t.Fatalf("client: %v", err)
	}
	out, err := client.Resync(context.Background(), ResyncRequest{ChannelID: "ch-1"})
	if err != nil {
		t.Fatalf("resync: %v", err)
	}
	if len(out.Envelopes) != 1 {
		t.Fatalf("len(envs) = %d, want exactly 1 (the view_sync_failed event)", len(out.Envelopes))
	}
	got := out.Envelopes[0]
	if got.Type != "system.event" {
		t.Fatalf("Type = %q, want system.event", got.Type)
	}
	if got.Sender.ID != "system" {
		t.Fatalf("Sender.ID = %q, want system", got.Sender.ID)
	}
	var payload map[string]any
	if err := json.Unmarshal(got.Payload, &payload); err != nil {
		t.Fatalf("decode payload: %v", err)
	}
	if payload["kind"] != "view_sync_failed" {
		t.Fatalf("payload.kind = %v", payload["kind"])
	}
	if payload["severity"] != "warn" {
		t.Fatalf("payload.severity = %v", payload["severity"])
	}
	if payload["message_id"] != "ev-real" {
		t.Fatalf("payload.message_id = %v", payload["message_id"])
	}
	if payload["failure"] != "http_status" {
		t.Fatalf("payload.failure = %v", payload["failure"])
	}
	if int(payload["http_status"].(float64)) != 500 {
		t.Fatalf("payload.http_status = %v", payload["http_status"])
	}
	// L1 §8.1.2 contract: daemon truth survives the failed push — i.e.
	// the local store has exactly the view_sync_failed event and NOT
	// the failed envelope itself (the Pusher never writes truth).
	if got.ID == failed.ID {
		t.Fatalf("daemon truth contaminated — failed envelope got persisted")
	}
}

// TestEnd2End_DuplicatePushFailures_DedupeAtHarness verifies the
// deterministic envelope id keeps repeat push failures from spamming
// the local channel — second emit dedupes via harness Step 0.5, so
// resync still returns exactly one row.
func TestEnd2End_DuplicatePushFailures_DedupeAtHarness(t *testing.T) {
	t.Parallel()

	db := openChannelWithSystemActor(t)
	deps := buildE2EDeps(t, db)

	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusBadGateway)
		_, _ = w.Write([]byte(`{"error":"upstream timeout"}`))
	}))
	t.Cleanup(upstream.Close)

	sink := &HarnessFailureSink{
		Writer:        DefaultHarnessWriter(deps),
		SystemActorID: "system",
	}
	pusher, err := NewHTTPPusher(HTTPPusherOptions{
		BaseURL:       upstream.URL,
		HTTPClient:    upstream.Client(),
		Failure:       sink,
		Clock:         func() int64 { return 1700000020000 },
		RetryAttempts: 1, // single attempt per push — test pins dedupe across emits, not retry behavior
	})
	if err != nil {
		t.Fatalf("new pusher: %v", err)
	}
	env := newTestEnvelope("ev-dupe")
	env.ChannelID = "ch-1"

	// Push the same envelope 3 times; each push fails the same way.
	for i := 0; i < 3; i++ {
		if _, err := pusher.PushToServer(context.Background(), env); err == nil {
			t.Fatalf("expected push error on attempt %d", i)
		}
	}

	// Resync — only one view_sync_failed event survives thanks to the
	// deterministic envelope id collapsing at harness Step 0.5.
	srv := httptest.NewServer(func() http.Handler {
		mux := http.NewServeMux()
		mux.Handle(ResyncRPCPath, NewResyncHandler(ResyncHandlerOptions{
			Resolver: StaticResolver("ch-1", NewSQLiteResyncStore(db)),
			Auth:     func(_ context.Context, _ string, _ *ResyncRequest) error { return nil },
		}))
		return mux
	}())
	t.Cleanup(srv.Close)
	client, _ := NewResyncClient(ResyncClientOptions{
		BaseURL:    srv.URL,
		AuthToken:  "any-token",
		HTTPClient: srv.Client(),
	})
	out, err := client.Resync(context.Background(), ResyncRequest{ChannelID: "ch-1"})
	if err != nil {
		t.Fatalf("resync: %v", err)
	}
	if len(out.Envelopes) != 1 {
		t.Fatalf("len(envs) = %d, want 1 after dedupe", len(out.Envelopes))
	}
}
