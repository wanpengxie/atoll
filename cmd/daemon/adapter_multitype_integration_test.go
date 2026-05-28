package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"
	"time"

	devicexhs "github.com/wanpengxie/ActOS/adapters/device/xhs"
	"github.com/wanpengxie/ActOS/kernel/actor"
	"github.com/wanpengxie/ActOS/kernel/actorreg"
	"github.com/wanpengxie/ActOS/kernel/message"
	"github.com/wanpengxie/ActOS/kernel/placement"
	"github.com/wanpengxie/ActOS/runtime"
	"github.com/wanpengxie/ActOS/runtime/transit"
)

// Response multitype refactor cross-package integration tests.
//
// These cases wire the production daemon assembly (runtime + framework
// Manager + harness Chain + sqlite store) over the mock bus and exercise
// the full provisional + final response stream through real harness Step
// 8 + the ux_terminal_response_per_request UNIQUE INDEX. They complement
// the existing in-package unit tests (runtime/harness step_response_*
// covers Step 8 in isolation; adapters/framework provisional_test covers
// the buildProvisional builder) by proving the seams compose: an adapter
// that calls ctx.Provisional inside Handle ends up writing a kind=response
// envelope to the daemon-local sqlite, harness Step 8 classifies it per
// proto-layer0 §2.5, and the channel log carries the correct is_terminal
// flag.

// TestIntegration_MultitypeProvisionalStream covers the happy-path
// provisional → provisional → final stream:
//
//   - Layer 2 core provisional `received`    → is_terminal=0
//   - Layer 2 core provisional `processing`  → is_terminal=0
//   - Layer 3 namespace provisional `xhs.login_queued`
//     (sender local-name === "xhs", per proto-layer0 §2.5.3) → is_terminal=0
//   - Layer 1 final `completed`              → is_terminal=1
//
// Acceptance criteria mapped:
//   - §6.1 protocol-level: harness 9-step chain accepts the provisional
//     payload.status values and persists is_terminal correctly.
//   - §6.2 substrate-level: ctx.Provisional() composes with chain.Write
//     to land rows in the channel log; final Respond closes the
//     correlation cleanly afterwards.
func TestIntegration_MultitypeProvisionalStream(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	cfg := testXHSConfig{
		Provisionals: []testXHSProvisional{
			{Status: "received", Payload: json.RawMessage(`{"detail":"got it"}`)},
			{Status: "processing", Payload: json.RawMessage(`{"progress_percent":0.4}`)},
			{Status: "xhs.login_queued", Payload: json.RawMessage(`{"queue_position":3}`)},
		},
	}
	d, srv, channelsDir := startIntegrationDaemon(t, ctx, integDaemonOpts{XHSConfig: cfg})
	defer func() { _ = d.Close() }()

	const channelID = "ch-integ-multitype-stream"
	createChannel(t, ctx, d, srv, channelID, []placement.InitialMember{
		{MemberActorID: "user:alice", Kind: "human", DisplayName: "Alice"},
	})

	const requestID = "req-mtype-stream-1"
	ack := writeRequest(t, ctx, d, srv, channelID, requestID, "user:alice",
		devicexhs.TypePublish, []byte(`{"title":"hello"}`))
	if !ack.Accepted {
		t.Fatalf("write_message rejected: reason=%s detail=%s", ack.RejectReason, ack.RejectDetail)
	}

	_, db := openChannelMessages(t, channelsDir, channelID)
	defer func() { _ = db.Close() }()

	// Wait for the final response to land; provisional rows must exist
	// before it. The pollResponse helper only returns the first matching
	// kind=response row, so we use a longer wait via our own poller.
	finalEnv := pollFinalResponse(t, db, ack.MessageID, 3*time.Second)
	if !finalEnv.IsTerminal {
		t.Errorf("final response is_terminal=false; want true")
	}

	rows := listResponseRows(t, db, ack.MessageID)
	// Four rows expected in order: 3 provisional + 1 final.
	if len(rows) != 4 {
		t.Fatalf("response rows=%d want 4; rows=%v", len(rows), rows)
	}
	expected := []struct {
		status   string
		terminal bool
	}{
		{"received", false},
		{"processing", false},
		{"xhs.login_queued", false},
		{"completed", true},
	}
	for i, want := range expected {
		got := rows[i]
		if got.status != want.status {
			t.Errorf("row[%d].status=%q want %q (full payload=%s)", i, got.status, want.status, got.payload)
		}
		if got.isTerminal != want.terminal {
			t.Errorf("row[%d].is_terminal=%v want %v (status=%s)", i, got.isTerminal, want.terminal, got.status)
		}
		if got.senderID != string(devicexhs.DefaultAdapterActorID) {
			t.Errorf("row[%d].sender_id=%q want %q", i, got.senderID, devicexhs.DefaultAdapterActorID)
		}
		if got.senderKind != string(actor.KindTool) {
			t.Errorf("row[%d].sender_kind=%q want tool", i, got.senderKind)
		}
		if got.parentID != ack.MessageID.String() {
			t.Errorf("row[%d].parent_id=%q want %s", i, got.parentID, ack.MessageID)
		}
	}
}

// TestIntegration_MultitypeLayer3NamespaceMismatchRejected proves that
// Step 8 enforces sender local-name ownership over the Layer 3
// namespace. The test xhs adapter (sender local-name = "xhs") emits a
// provisional with namespace = "planner" — harness rejects with
// harness_response_status_namespace_mismatch and the row never lands.
func TestIntegration_MultitypeLayer3NamespaceMismatchRejected(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	var mod *testXHSModule
	cfg := testXHSConfig{
		Provisionals: []testXHSProvisional{
			{Status: "planner.waiting_on_x", Payload: json.RawMessage(`{}`)},
		},
	}
	d, srv, channelsDir := startIntegrationDaemonCapture(t, ctx, integDaemonOpts{XHSConfig: cfg}, &mod)
	defer func() { _ = d.Close() }()

	const channelID = "ch-integ-mtype-ns"
	createChannel(t, ctx, d, srv, channelID, []placement.InitialMember{
		{MemberActorID: "user:alice", Kind: "human", DisplayName: "Alice"},
	})

	const requestID = "req-mtype-ns-1"
	ack := writeRequest(t, ctx, d, srv, channelID, requestID, "user:alice",
		devicexhs.TypePublish, []byte(`{"title":"hi"}`))
	if !ack.Accepted {
		t.Fatalf("write rejected: %s %s", ack.RejectReason, ack.RejectDetail)
	}

	_, db := openChannelMessages(t, channelsDir, channelID)
	defer func() { _ = db.Close() }()

	// Final still arrives because Handle continues past Provisional
	// error in our test adapter.
	pollFinalResponse(t, db, ack.MessageID, 3*time.Second)

	// The spoofed-namespace provisional must NOT be in the channel log.
	rows := listResponseRows(t, db, ack.MessageID)
	for _, r := range rows {
		if r.status == "planner.waiting_on_x" {
			t.Fatalf("namespace-mismatch provisional landed: %+v", r)
		}
	}

	// And Provisional() surfaced the rejection up to the caller.
	if mod == nil {
		t.Fatalf("test module not captured")
	}
	if err := mod.lastProvisionalErr; err == nil ||
		!strings.Contains(err.Error(), "namespace_mismatch") {
		t.Fatalf("expected namespace_mismatch error, got %v", err)
	}
}

// TestIntegration_MultitypeProvisionalAfterFinalRejected proves the
// zombie chain defence per proto-layer1 §2.8 #8: once Step 8 has
// recorded a final response, any subsequent provisional for the same
// parent_id is rejected with harness_provisional_after_final and the
// row does not land.
func TestIntegration_MultitypeProvisionalAfterFinalRejected(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	var mod *testXHSModule
	cfg := testXHSConfig{
		EmitTrailingProvisional: true,
	}
	d, srv, channelsDir := startIntegrationDaemonCapture(t, ctx, integDaemonOpts{XHSConfig: cfg}, &mod)
	defer func() { _ = d.Close() }()

	const channelID = "ch-integ-mtype-zombie"
	createChannel(t, ctx, d, srv, channelID, []placement.InitialMember{
		{MemberActorID: "user:alice", Kind: "human", DisplayName: "Alice"},
	})

	const requestID = "req-mtype-zombie-1"
	ack := writeRequest(t, ctx, d, srv, channelID, requestID, "user:alice",
		devicexhs.TypePublish, []byte(`{"title":"zombie"}`))
	if !ack.Accepted {
		t.Fatalf("write rejected: %s %s", ack.RejectReason, ack.RejectDetail)
	}

	_, db := openChannelMessages(t, channelsDir, channelID)
	defer func() { _ = db.Close() }()

	pollFinalResponse(t, db, ack.MessageID, 3*time.Second)

	// Only the final row must remain; no zombie provisional.
	rows := listResponseRows(t, db, ack.MessageID)
	if len(rows) != 1 {
		t.Fatalf("response rows=%d want 1; rows=%+v", len(rows), rows)
	}
	if !rows[0].isTerminal || rows[0].status != "completed" {
		t.Fatalf("only row is not final: %+v", rows[0])
	}

	// And Provisional() (called after the final) surfaced the
	// rejection. The expected reject reason is one of:
	//   - provisional_after_final  (Step 8 zombie defence)
	//   - correlation_owner_mismatch / correlation_done
	//     (framework respond closed the entry before Provisional ran)
	// Either signals the framework correctly refused the late
	// provisional emit — we accept both.
	if mod == nil {
		t.Fatalf("test module not captured")
	}
	if err := mod.lastTrailingErr; err == nil {
		t.Fatalf("trailing provisional after final should have errored")
	}
}

// TestIntegration_MultitypeFinalDuplicateRejected proves the
// ux_terminal_response_per_request UNIQUE INDEX in store.schema combined
// with Step 8 harness_terminal_duplicate guard.
//
// Once the first final is written, the second Respond either rejects
// inside Step 8 (single-writer pre-check) or hits the UNIQUE INDEX
// (concurrent racer path). Both surface as a non-nil error returned by
// mctx.Respond, and the channel log contains exactly one final row.
func TestIntegration_MultitypeFinalDuplicateRejected(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	var mod *testXHSModule
	cfg := testXHSConfig{
		EmitDuplicateFinal: true,
	}
	d, srv, channelsDir := startIntegrationDaemonCapture(t, ctx, integDaemonOpts{XHSConfig: cfg}, &mod)
	defer func() { _ = d.Close() }()

	const channelID = "ch-integ-mtype-dup"
	createChannel(t, ctx, d, srv, channelID, []placement.InitialMember{
		{MemberActorID: "user:alice", Kind: "human", DisplayName: "Alice"},
	})

	const requestID = "req-mtype-dup-1"
	ack := writeRequest(t, ctx, d, srv, channelID, requestID, "user:alice",
		devicexhs.TypePublish, []byte(`{"title":"dup"}`))
	if !ack.Accepted {
		t.Fatalf("write rejected: %s %s", ack.RejectReason, ack.RejectDetail)
	}

	_, db := openChannelMessages(t, channelsDir, channelID)
	defer func() { _ = db.Close() }()

	pollFinalResponse(t, db, ack.MessageID, 3*time.Second)

	rows := listResponseRows(t, db, ack.MessageID)
	if len(rows) != 1 {
		t.Fatalf("response rows=%d want 1; rows=%+v", len(rows), rows)
	}
	if !rows[0].isTerminal {
		t.Fatalf("only row not terminal: %+v", rows[0])
	}

	if mod == nil {
		t.Fatalf("test module not captured")
	}
	if err := mod.lastDuplicateErr; err == nil {
		t.Fatalf("duplicate final Respond should have errored")
	}
}

// TestIntegration_MultitypeLayer1FailedSetsTerminalReason proves that
// Layer 1 final `failed` carries the reason on the response payload and
// is_terminal=1.
func TestIntegration_MultitypeLayer1FailedSetsTerminalReason(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	cfg := testXHSConfig{
		FinalStatus:  "failed",
		FailedReason: string(message.TerminalReceiverInternalError),
		ResponsePayload: json.RawMessage(`{"error":"boom"}`),
	}
	d, srv, channelsDir := startIntegrationDaemon(t, ctx, integDaemonOpts{XHSConfig: cfg})
	defer func() { _ = d.Close() }()

	const channelID = "ch-integ-mtype-failed"
	createChannel(t, ctx, d, srv, channelID, []placement.InitialMember{
		{MemberActorID: "user:alice", Kind: "human", DisplayName: "Alice"},
	})

	const requestID = "req-mtype-failed-1"
	ack := writeRequest(t, ctx, d, srv, channelID, requestID, "user:alice",
		devicexhs.TypePublish, []byte(`{"title":"fail"}`))
	if !ack.Accepted {
		t.Fatalf("write rejected: %s %s", ack.RejectReason, ack.RejectDetail)
	}

	_, db := openChannelMessages(t, channelsDir, channelID)
	defer func() { _ = db.Close() }()

	final := pollFinalResponse(t, db, ack.MessageID, 3*time.Second)
	if !final.IsTerminal {
		t.Errorf("failed terminal not flagged is_terminal=true")
	}

	var payload map[string]any
	if err := json.Unmarshal(final.Payload, &payload); err != nil {
		t.Fatalf("payload unmarshal: %v", err)
	}
	if payload["status"] != "failed" {
		t.Errorf("payload.status=%v want failed", payload["status"])
	}
	if payload["reason"] != string(message.TerminalReceiverInternalError) {
		t.Errorf("payload.reason=%v want %s", payload["reason"], message.TerminalReceiverInternalError)
	}
}

// ----------------------------------------------------------------------
// helpers
// ----------------------------------------------------------------------

// startIntegrationDaemonCapture is startIntegrationDaemon plus a
// pointer-out parameter that captures the test xhs module instance so
// the case can inspect lastProvisionalErr / lastTrailingErr etc.
//
// This is a thin wrapper rather than a flag on integDaemonOpts because
// passing the **testXHSModule through the opts struct would couple the
// integration helper to a test-only type.
func startIntegrationDaemonCapture(
	t *testing.T,
	ctx context.Context,
	opts integDaemonOpts,
	out **testXHSModule,
) (*runtime.Daemon, *transit.MockServer, string) {
	t.Helper()
	dataDir := t.TempDir()
	channelsDir := filepath.Join(dataDir, "channels")

	period := opts.SchedulerPeriod
	if period == 0 {
		period = 50 * time.Millisecond
	}

	channelTemplates := map[string]runtime.ChannelTemplate{
		XHSCreatorChannelType: {
			AdapterActorSeeds: []actorreg.Record{testXHSActorSeed()},
			WorkdirSubdirs:    devicexhs.WorkdirSubdirs(),
			DomainPrompt:      devicexhs.DomainPrompt(),
		},
	}
	onChannelBoot := wireAdapterFramework(testXHSFactoryCapture(opts.XHSConfig, out))

	cfg := runtime.DaemonConfig{
		DataDir:           dataDir,
		ChannelsDir:       channelsDir,
		DaemonID:          "daemon-integ-capture",
		DaemonEpoch:       43,
		UseMockBus:        true,
		NowFn:             nowMs,
		HumanCallerSecret: []byte(integSecret),
		ReplayWindow:      time.Minute,
		SchedulerPeriod:   period,
		ChannelTemplates:  channelTemplates,
		OnChannelBoot:     onChannelBoot,
	}

	d, err := runtime.AssembleDaemon(ctx, cfg)
	if err != nil {
		t.Fatalf("AssembleDaemon: %v", err)
	}
	if err := d.RunPhases(ctx); err != nil {
		t.Fatalf("RunPhases: %v", err)
	}
	return d, d.Bus().ServerSide(), channelsDir
}

// pollFinalResponse waits for a final (is_terminal=1) response row
// pointing at requestID. Unlike pollResponse it skips over provisional
// rows.
func pollFinalResponse(t *testing.T, db *sql.DB, requestID message.ID, timeout time.Duration) message.Envelope {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		const q = `SELECT id, payload, COALESCE(parent_id,''), kind, sender_kind, sender_id
		             FROM messages
		             WHERE parent_id=? AND kind='response' AND is_terminal=1
		             ORDER BY seq DESC LIMIT 1`
		var (
			id, payload, parent, kind, senderKind, senderID string
		)
		err := db.QueryRowContext(context.Background(), q, requestID.String()).Scan(
			&id, &payload, &parent, &kind, &senderKind, &senderID)
		if err == sql.ErrNoRows {
			time.Sleep(20 * time.Millisecond)
			continue
		}
		if err != nil {
			t.Fatalf("query final response: %v", err)
		}
		return message.Envelope{
			ID:         message.ID(id),
			Payload:    json.RawMessage(payload),
			Kind:       message.Kind(kind),
			ParentID:   message.ID(parent),
			Sender:     message.Sender{Kind: actor.Kind(senderKind), ID: actor.ActorID(senderID)},
			IsTerminal: true,
		}
	}
	t.Fatalf("final response for %s never arrived within %s", requestID, timeout)
	return message.Envelope{}
}

// responseRow projects the columns we assert across the multitype
// integration suite.
type responseRow struct {
	seq        int64
	id         string
	status     string
	isTerminal bool
	senderID   string
	senderKind string
	parentID   string
	payload    string
}

// listResponseRows returns every kind=response row pointing at
// requestID in seq ASC order.
func listResponseRows(t *testing.T, db *sql.DB, requestID message.ID) []responseRow {
	t.Helper()
	rows, err := db.QueryContext(context.Background(), `
		SELECT seq, id, payload, COALESCE(parent_id,''), sender_id, sender_kind, is_terminal
		FROM messages
		WHERE kind='response' AND parent_id=?
		ORDER BY seq ASC`, requestID.String())
	if err != nil {
		t.Fatalf("list response rows: %v", err)
	}
	defer func() { _ = rows.Close() }()
	var out []responseRow
	for rows.Next() {
		var r responseRow
		var terminal int
		if err := rows.Scan(&r.seq, &r.id, &r.payload, &r.parentID, &r.senderID, &r.senderKind, &terminal); err != nil {
			t.Fatalf("scan: %v", err)
		}
		r.isTerminal = terminal == 1
		r.status = extractPayloadStatus(r.payload)
		out = append(out, r)
	}
	return out
}

// extractPayloadStatus pulls payload.status out of a JSON object,
// returning "" when absent or malformed.
func extractPayloadStatus(payload string) string {
	var doc map[string]any
	if err := json.Unmarshal([]byte(payload), &doc); err != nil {
		return ""
	}
	if v, ok := doc["status"].(string); ok {
		return v
	}
	return ""
}
