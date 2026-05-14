package main

// smoke_test.go is the cmd/daemon-level smoke that the M1.3 ticket
// FIX-1 acceptance gate demands:
//
//	go test ./cmd/daemon/... smoke 跑通真 daemon 进程一次完整 ask round-trip
//
// "real daemon process" is satisfied by invoking the same Run() body the
// production binary calls — the wire path (HTTP server, harness handler,
// per-channel sqlite, scheduler goroutines) is identical to the prod
// binary. We avoid exec'ing a child binary because that adds zero
// composition-root coverage and trades the test for an extra `go build`
// step in CI.
//
// Tests:
//
//	TestSmoke_DaemonBoot       — bare daemon with zero channels;
//	                             /api/healthz and /api/channel/list reply.
//	TestSmoke_AskRoundTrip     — bootstrap one channel via saga.ChannelCreate;
//	                             start the daemon; send an `ask` via the
//	                             coagent daemon_rpc binding; observe the
//	                             long-pending scheduler emit the
//	                             unanswered_timeout terminal response.

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/coagent-ai/daemon-go/internal/bootstrap"
	"github.com/coagent-ai/daemon-go/internal/store"
	"github.com/coagent-ai/daemon-go/pkg/coagent"
	"github.com/coagent-ai/daemon-go/pkg/v4types"
)

const smokeAuthToken = "smoke-token"

// silentLogger keeps go test -v output legible. Set to a discard handler
// at Error level so the daemon's normal Info events stay quiet, but real
// failures still surface.
func silentLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError}))
}

// startDaemon launches Run in a goroutine + waits for the Ready signal.
// Returns the bound address + a cancel func + a wait func the caller
// invokes during cleanup so Run's shutdown ordering runs to completion.
func startDaemon(t *testing.T, cfg Config) (addr string, cancel func(), wait func()) {
	t.Helper()
	ready := make(chan ReadyInfo, 1)
	cfg.Ready = ready
	cfg.Logger = silentLogger()

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	var runErr error
	go func() {
		runErr = Run(ctx, cfg)
		close(done)
	}()

	select {
	case info := <-ready:
		wait = func() {
			cancel()
			select {
			case <-done:
			case <-time.After(15 * time.Second):
				t.Fatalf("daemon Run did not return within 15s")
			}
			if runErr != nil {
				t.Fatalf("daemon Run returned error: %v", runErr)
			}
		}
		return info.HTTPAddr, cancel, wait
	case <-done:
		t.Fatalf("daemon Run exited before Ready: %v", runErr)
	case <-time.After(10 * time.Second):
		cancel()
		t.Fatalf("daemon Ready timeout")
	}
	return "", nil, nil
}

// TestSmoke_DaemonBoot exercises the minimal boot path: no channels in
// bootstrap_registry, daemon serves /api/healthz and an empty
// /api/channel/list. Verifies the composition root tolerates an empty
// daemon — the most common production-on-first-boot state.
func TestSmoke_DaemonBoot(t *testing.T) {
	dir := t.TempDir()
	cfg := Config{
		DaemonDBPath: filepath.Join(dir, "daemon.sqlite"),
		ChannelRoot:  filepath.Join(dir, "channels"),
		HTTPListen:   "127.0.0.1:0",
		AuthToken:    smokeAuthToken,
	}
	addr, _, wait := startDaemon(t, cfg)
	defer wait()

	resp, err := http.Get("http://" + addr + "/api/healthz")
	if err != nil {
		t.Fatalf("/api/healthz GET: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("/api/healthz status = %d, want 200", resp.StatusCode)
	}

	resp2, err := http.Get("http://" + addr + bootstrap.ListChannelsPath)
	if err != nil {
		t.Fatalf("/api/channel/list GET: %v", err)
	}
	defer func() { _ = resp2.Body.Close() }()
	if resp2.StatusCode != http.StatusOK {
		t.Fatalf("/api/channel/list status = %d, want 200", resp2.StatusCode)
	}
	body, _ := io.ReadAll(resp2.Body)
	if string(bytes.TrimSpace(body)) != "[]" {
		t.Fatalf("/api/channel/list body = %q, want []", string(body))
	}
}

// TestSmoke_AskRoundTrip drives the full ask → terminal-response cycle
// through the production composition root.
//
// Flow:
//
//  1. seed a channel under daemon.sqlite (saga.ChannelCreate) with
//     system / alice / bob actors + biz.foo type;
//  2. start the daemon (SchedulerPeriod=50ms so the fallback fires
//     quickly; WorkerBinaryPath="" so supervisor loops are skipped —
//     they would only be needed if alice were an in_worker_bus agent
//     we expect to spawn);
//  3. send a kind=request envelope from alice → bob via the daemon_rpc
//     binding, with expires_at in the past so the long-pending Step 1
//     scanner fires on the next tick;
//  4. poll the channel sqlite for the deterministic fallback row
//     (`fallback:<req-id>:unanswered_timeout`).
//
// This proves the entire wire chain — HTTP listener + harness binding +
// per-channel Deps + scheduler goroutine + fallback emit — under
// production code paths, not in-process composition.
func TestSmoke_AskRoundTrip(t *testing.T) {
	dir := t.TempDir()
	daemonDBPath := filepath.Join(dir, "daemon.sqlite")
	channelID := "smoke-ch-1"
	workdir := filepath.Join(dir, "channels", channelID)

	ctx := context.Background()

	// ---- pre-bootstrap channel via saga ---------------------------------
	{
		daemonDB, err := store.OpenDaemon(ctx, daemonDBPath, store.OpenOptions{})
		if err != nil {
			t.Fatalf("open daemon sqlite: %v", err)
		}
		saga := bootstrap.New(daemonDB)
		maxPending := int64(60_000)
		schema := mustRaw(map[string]any{
			"request": map[string]any{"type": "object"},
			"response": map[string]any{
				"type": "object",
				"properties": map[string]any{
					"status":           map[string]any{"type": "string"},
					"reason":           map[string]any{"type": "string"},
					"missing_actor_id": map[string]any{"type": "string"},
				},
				"additionalProperties": true,
			},
		})
		_, err = saga.ChannelCreate(ctx, bootstrap.CreateParams{
			CreateRequestID: "smoke-create-1",
			ChannelID:       channelID,
			WorkdirPath:     workdir,
			ChannelAgent:    bootstrap.ChannelAgentSpec{ActorID: "alice"},
			BusinessTypes: []bootstrap.TypeRegistryRow{
				{
					Type:           "biz.foo",
					AllowedKinds:   []string{"request", "response"},
					SchemasByKind:  schema,
					HandlerBinding: "in_worker_bus",
					MaxPendingMs:   &maxPending,
				},
			},
		})
		if err != nil {
			_ = daemonDB.Close()
			t.Fatalf("saga.ChannelCreate: %v", err)
		}
		// Seed bob (an additional in_worker_bus agent) so the request's
		// audience resolves to an active actor and Step 5 / Step 3 path
		// is exactly "agent receiver, expires_at expired" — i.e. the
		// scheduler's Step 1 unanswered_timeout case.
		channelDB, err := store.OpenChannel(ctx, filepath.Join(workdir, "messages.sqlite"), store.OpenOptions{})
		if err != nil {
			_ = daemonDB.Close()
			t.Fatalf("open channel sqlite: %v", err)
		}
		if _, err := channelDB.ExecContext(ctx,
			`INSERT INTO actor_registry (actor_id, actor_kind, actor_binding, created_at, deregistered_at)
			 VALUES (?, 'agent', 'in_worker_bus', ?, NULL)`,
			"bob", time.Now().Unix(),
		); err != nil {
			_ = channelDB.Close()
			_ = daemonDB.Close()
			t.Fatalf("seed bob: %v", err)
		}
		// Seed bob's actor_cursors row as the registry.Register helper
		// would — the M1.3 supervisor backlog scan JOINs on it, and the
		// scheduler emit's Step 8 doesn't read cursors but other code
		// paths might.
		if _, err := channelDB.ExecContext(ctx,
			`INSERT OR IGNORE INTO actor_cursors (actor_id, last_consumed_seq) VALUES (?, 0)`, "bob"); err != nil {
			_ = channelDB.Close()
			_ = daemonDB.Close()
			t.Fatalf("seed bob cursor: %v", err)
		}
		_ = channelDB.Close()
		_ = daemonDB.Close()
	}

	// ---- start daemon ---------------------------------------------------
	cfg := Config{
		DaemonDBPath:    daemonDBPath,
		ChannelRoot:     filepath.Join(dir, "channels"),
		HTTPListen:      "127.0.0.1:0",
		AuthToken:       smokeAuthToken,
		SchedulerPeriod: 50 * time.Millisecond,
		// WorkerBinaryPath left empty — supervisor disabled. The smoke
		// only needs the harness write + scheduler fallback to succeed.
	}
	addr, _, wait := startDaemon(t, cfg)
	defer wait()

	// ---- send a request via the daemon_rpc binding ---------------------
	binding := coagent.NewDaemonRPCBinding(coagent.DaemonRPCOptions{
		BaseURL:   "http://" + addr,
		AuthToken: smokeAuthToken,
	})

	now := time.Now().UnixMilli()
	past := now - 1_000 // 1s in the past — scheduler.Tick fires immediately
	requestID := "smoke-req-1"
	env := &v4types.Envelope{
		ID:         requestID,
		TS:         now,
		ChannelID:  channelID,
		Sender:     v4types.Sender{Kind: v4types.SenderAgent, ID: "alice"},
		Kind:       v4types.KindRequest,
		Type:       "biz.foo",
		Payload:    json.RawMessage(`{}`),
		Visibility: v4types.VisibilityPublic,
		Audience:   []string{"bob"},
		ExpiresAt:  &past,
	}
	res, err := binding.Send(ctx, env, coagent.SendOptions{
		DeclaredSenderKind: v4types.SenderAgent,
	})
	if err != nil {
		t.Fatalf("binding.Send: %v", err)
	}
	if res.ID != requestID {
		t.Fatalf("response id = %q, want %q", res.ID, requestID)
	}
	if res.Kind != v4types.KindRequest {
		t.Fatalf("response kind = %q, want request", res.Kind)
	}

	// ---- poll for the long-pending fallback terminal response ----------
	channelDB, err := sql.Open("sqlite", filepath.Join(workdir, "messages.sqlite"))
	if err != nil {
		t.Fatalf("open channel sqlite read: %v", err)
	}
	defer func() { _ = channelDB.Close() }()

	deadline := time.Now().Add(5 * time.Second)
	var fallbackPayload string
	var fallbackSender string
	var attempt int
	for time.Now().Before(deadline) {
		attempt++
		var payload sql.NullString
		var senderID sql.NullString
		err := channelDB.QueryRowContext(ctx,
			`SELECT payload, sender_id FROM messages
			  WHERE parent_id = ? AND kind = 'response' AND is_terminal = 1
			  LIMIT 1`,
			requestID,
		).Scan(&payload, &senderID)
		if err == nil && payload.Valid {
			fallbackPayload = payload.String
			fallbackSender = senderID.String
			break
		}
		if err != nil && err != sql.ErrNoRows {
			t.Fatalf("query terminal response: %v (attempt %d)", err, attempt)
		}
		time.Sleep(50 * time.Millisecond)
	}
	if fallbackPayload == "" {
		// dump the messages table for forensics so a failing CI run is
		// debuggable without re-running locally.
		dumpMessagesTable(t, channelDB, channelID)
		t.Fatalf("no terminal response landed within 5s (request_id=%s)", requestID)
	}
	if fallbackSender != "system" {
		t.Fatalf("fallback sender_id = %q, want system", fallbackSender)
	}
	var p struct {
		Status string `json:"status"`
		Reason string `json:"reason"`
	}
	if err := json.Unmarshal([]byte(fallbackPayload), &p); err != nil {
		t.Fatalf("decode fallback payload: %v (raw=%s)", err, fallbackPayload)
	}
	if p.Status != "failed" {
		t.Fatalf("fallback status = %q, want failed", p.Status)
	}
	if p.Reason != string(v4types.TerminalUnansweredTimeout) {
		t.Fatalf("fallback reason = %q, want %s",
			p.Reason, v4types.TerminalUnansweredTimeout)
	}
}

// dumpMessagesTable prints the channel's messages rows + actor_registry
// rows to the test log. Only invoked on a smoke failure — keeps clean
// runs quiet.
func dumpMessagesTable(t *testing.T, db *sql.DB, channelID string) {
	t.Helper()
	t.Logf("--- messages dump for %s ---", channelID)
	rows, err := db.Query(`SELECT seq, id, kind, type, sender_id, expires_at, is_terminal, parent_id FROM messages ORDER BY seq`)
	if err != nil {
		t.Logf("dump query failed: %v", err)
		return
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var seq int64
		var id, kind, typ, sender string
		var exp sql.NullInt64
		var term int
		var parent sql.NullString
		if err := rows.Scan(&seq, &id, &kind, &typ, &sender, &exp, &term, &parent); err != nil {
			t.Logf("scan: %v", err)
			continue
		}
		t.Logf("  seq=%d id=%s kind=%s type=%s sender=%s exp=%v term=%d parent=%v",
			seq, id, kind, typ, sender, exp, term, parent)
	}
}

// mustRaw marshals a Go value to json.RawMessage or fails the test —
// used so the schema literal stays readable.
func mustRaw(v any) json.RawMessage {
	b, err := json.Marshal(v)
	if err != nil {
		panic(fmt.Sprintf("mustRaw: %v", err))
	}
	return b
}

// silence unused-import warnings when only a subset of the smoke runs.
var _ = sync.WaitGroup{}
