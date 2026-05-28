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
	"github.com/wanpengxie/ActOS/kernel/channel"
	"github.com/wanpengxie/ActOS/kernel/daemonbus"
	"github.com/wanpengxie/ActOS/kernel/message"
	"github.com/wanpengxie/ActOS/kernel/placement"
	"github.com/wanpengxie/ActOS/pkg/metrics"
	"github.com/wanpengxie/ActOS/runtime"
	"github.com/wanpengxie/ActOS/runtime/store"
	"github.com/wanpengxie/ActOS/runtime/transit"
)

const integSecret = "integ-secret"

func nowMs() int64 { return time.Now().UnixMilli() }

// integDaemonOpts overrides selected DaemonConfig fields for individual
// sub-tests. These tests use a package-local fake xhs module so the
// production daemon remains proxy-facade only.
type integDaemonOpts struct {
	XHSConfig       testXHSConfig
	SchedulerPeriod time.Duration
	NoTestXHS       bool
}

func startIntegrationDaemon(t *testing.T, ctx context.Context, opts integDaemonOpts) (*runtime.Daemon, *transit.MockServer, string) {
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
	onChannelBoot := wireAdapterFramework(testXHSFactory(opts.XHSConfig))
	if opts.NoTestXHS {
		channelTemplates = buildChannelTemplates()
		onChannelBoot = wireAdapterFramework()
	}

	cfg := runtime.DaemonConfig{
		DataDir:           dataDir,
		ChannelsDir:       channelsDir,
		DaemonID:          "daemon-integ",
		DaemonEpoch:       42,
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

// createChannel drives a CreateChannel frame through the mock bus and
// waits for CreateChannelAccepted. It also drains any unrelated frames between.
func createChannel(t *testing.T, ctx context.Context, d *runtime.Daemon, srv *transit.MockServer, channelID string, members []placement.InitialMember) {
	t.Helper()
	req := placement.CreateChannelRequest{
		ChannelID:       channel.ID(channelID),
		CreateRequestID: placement.CreateRequestID("req-" + channelID),
		InitialMembers:  members,
		// M1.6-T5 phase-2 — tag the channel with the xhs-creator
		// template so the daemon resolves the matching test seeds /
		// workdir subdirs / domain prompt.
		ChannelType: XHSCreatorChannelType,
	}
	frame, err := transit.Encode("frame-create-"+channelID,
		daemonbus.FrameTypeControlCreateChannel,
		"server", d.Transit().Epoch(), nowMs(), req)
	if err != nil {
		t.Fatalf("encode create_channel: %v", err)
	}
	if err := srv.SendToDaemon(ctx, frame); err != nil {
		t.Fatalf("SendToDaemon create_channel: %v", err)
	}
	deadline := time.After(5 * time.Second)
	for {
		select {
		case <-deadline:
			t.Fatal("create_channel ack never arrived")
		default:
		}
		recvCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		f, err := srv.RecvFromDaemon(recvCtx)
		cancel()
		if err != nil {
			t.Fatalf("RecvFromDaemon: %v", err)
		}
		if f.FrameKind != daemonbus.FrameTypeControlCreateChannelAck {
			continue
		}
		var ack placement.CreateChannelAck
		if err := transit.DecodePayload(f, &ack); err != nil {
			t.Fatalf("decode ack: %v", err)
		}
		if ack.Result != placement.CreateChannelAccepted {
			t.Fatalf("ack reject: %s", ack.Result)
		}
		return
	}
}

// writeRequest drives a control.write_message frame for a kind=request
// envelope. Returns the write_message_ack body.
func writeRequest(t *testing.T, ctx context.Context, d *runtime.Daemon, srv *transit.MockServer,
	channelID, requestID, callerActor, envType string, payload []byte,
) transit.WriteMessageAckBody {
	return writeRequestWithExpiry(t, ctx, d, srv, channelID, requestID, callerActor, envType, payload, nil)
}

// writeRequestWithExpiry is the same as writeRequest but lets the caller
// set EnvelopePartial.ExpiresAt. Used by T147 acceptance C
// (TestIntegration_LongPending_ToolReceiverSkippedByScheduler_F3Wins) to
// seed a request whose expires_at lapses long before the adapter F3 timer
// fires — proving the scheduler skip + F3-wins contract.
func writeRequestWithExpiry(t *testing.T, ctx context.Context, d *runtime.Daemon, srv *transit.MockServer,
	channelID, requestID, callerActor, envType string, payload []byte, expiresAt *int64,
) transit.WriteMessageAckBody {
	t.Helper()
	ts := nowMs()
	hc := transit.HumanCaller{
		UserID:        "u1",
		MemberActorID: actor.ActorID(callerActor),
		TS:            ts,
		Nonce:         "nonce-" + requestID,
	}
	hc.ServerToken = transit.SignHumanCaller(
		[]byte(integSecret), channelID, hc.UserID, hc.MemberActorID, hc.TS, hc.Nonce,
	)
	body := transit.WriteMessageBody{
		FrameID:     daemonbus.FrameID("frame-write-" + requestID),
		ChannelID:   channel.ID(channelID),
		HumanCaller: hc,
		EnvelopePartial: message.Envelope{
			ID:       message.ID(requestID),
			Type:     envType,
			Kind:     message.KindRequest,
			Payload:  json.RawMessage(payload),
			Audience: message.Audience{devicexhs.DefaultAdapterActorID},
			// Visibility omitted — writemsg.go defaults to Public so the
			// trigger.Gateway audience-expand sees the explicit list (Private
			// would short-circuit Resolve to nil per L1 §5.1 visibility
			// filter and the adapter would never be dispatched).
			TS:        ts,
			Sender:    message.Sender{ID: actor.ActorID(callerActor)},
			ExpiresAt: expiresAt,
		},
	}
	reqFrame, err := transit.Encode("frame-srv-write-"+requestID,
		daemonbus.FrameTypeControlWriteMessage,
		"server", d.Transit().Epoch(), ts, body)
	if err != nil {
		t.Fatalf("encode write_message: %v", err)
	}
	if err := srv.SendToDaemon(ctx, reqFrame); err != nil {
		t.Fatalf("SendToDaemon write_message: %v", err)
	}
	deadline := time.After(5 * time.Second)
	for {
		select {
		case <-deadline:
			t.Fatalf("write_message_ack for %s never arrived", requestID)
		default:
		}
		recvCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		f, err := srv.RecvFromDaemon(recvCtx)
		cancel()
		if err != nil {
			t.Fatalf("RecvFromDaemon: %v", err)
		}
		if f.FrameKind != daemonbus.FrameTypeControlWriteMessageAck {
			continue
		}
		var ack transit.WriteMessageAckBody
		if err := transit.DecodePayload(f, &ack); err != nil {
			t.Fatalf("decode ack: %v", err)
		}
		return ack
	}
}

// openChannelMessages re-opens the channel sqlite (read-only path) and
// returns a *store.Messages handle for assertions. Caller is responsible
// for closing the underlying db.
func openChannelMessages(t *testing.T, channelsDir, channelID string) (*store.Messages, *sql.DB) {
	t.Helper()
	dbPath := filepath.Join(channelsDir, channelID, "channel.sqlite")
	db, err := store.OpenChannel(context.Background(), dbPath, store.OpenOptions{SkipDDL: true})
	if err != nil {
		t.Fatalf("OpenChannel: %v", err)
	}
	return store.NewMessages(db), db
}

// pollResponse polls messages until a response row with parent_id ==
// requestID appears (or deadline). Returns the response envelope.
func pollResponse(t *testing.T, db *sql.DB, requestID message.ID, timeout time.Duration) message.Envelope {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		const q = `SELECT id, payload, COALESCE(parent_id,''), kind, sender_kind, sender_id, is_terminal
		             FROM messages WHERE parent_id=? AND kind='response' LIMIT 1`
		var (
			id, payload, parent, kind, senderKind, senderID string
			term                                            int
		)
		err := db.QueryRowContext(context.Background(), q, requestID.String()).Scan(&id, &payload, &parent, &kind, &senderKind, &senderID, &term)
		if err == sql.ErrNoRows {
			time.Sleep(20 * time.Millisecond)
			continue
		}
		if err != nil {
			t.Fatalf("query response: %v", err)
		}
		var env message.Envelope
		env.ID = message.ID(id)
		env.Payload = json.RawMessage(payload)
		env.Kind = message.Kind(kind)
		env.ParentID = message.ID(parent)
		env.Sender = message.Sender{Kind: actor.Kind(senderKind), ID: actor.ActorID(senderID)}
		if term == 1 {
			env.IsTerminal = true
		}
		return env
	}
	// On timeout dump the messages table so failure context is useful.
	rows, err := db.QueryContext(context.Background(),
		`SELECT id, kind, type, COALESCE(parent_id,''), payload FROM messages ORDER BY seq`)
	if err == nil {
		defer func() { _ = rows.Close() }()
		t.Logf("messages table dump (looking for parent_id=%s):", requestID)
		for rows.Next() {
			var id, kind, typ, parent, payload string
			_ = rows.Scan(&id, &kind, &typ, &parent, &payload)
			t.Logf("  id=%s kind=%s type=%s parent_id=%s payload=%s", id, kind, typ, parent, payload)
		}
	}
	t.Fatalf("response for %s never appeared within %s", requestID, timeout)
	return message.Envelope{}
}

// TestIntegration_XhsPublish_HappyPath covers acceptance #1, #2 and the
// regression sweep:
//
//	create channel via CreateChannel frame → saga seeds tool:xhs
//	+ user:alice → daemon mounts channel → OnChannelBoot installs xhs
//	scaffold via framework.Manager → write_message kind=request for
//	xhs.publish → trigger gateway dispatches to scaffold via deliverer →
//	scaffold synchronously emits success terminal → terminal response
//	row lands in messages table with payload.status=completed +
//	is_terminal=1.
func TestIntegration_XhsPublish_HappyPath(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	d, srv, channelsDir := startIntegrationDaemon(t, ctx, integDaemonOpts{})
	defer func() { _ = d.Close() }()

	const channelID = "ch-integ-happy"
	createChannel(t, ctx, d, srv, channelID, []placement.InitialMember{
		{MemberActorID: "user:alice", Kind: "human", DisplayName: "Alice"},
	})

	if !d.HasChannel(channel.ID(channelID)) {
		t.Fatal("daemon never mounted channel")
	}

	const requestID = "req-publish-1"
	ack := writeRequest(t, ctx, d, srv, channelID, requestID, "user:alice",
		devicexhs.TypePublish, []byte(`{"title":"hello"}`))
	if !ack.Accepted {
		t.Fatalf("write_message rejected: reason=%s detail=%s", ack.RejectReason, ack.RejectDetail)
	}

	_, db := openChannelMessages(t, channelsDir, channelID)
	defer func() { _ = db.Close() }()

	// Daemon canonicalises envelope.id before chain.Write (writemsg.go:321),
	// so the framework Respond uses the canonical id as parent_id. The
	// ack carries that canonical id.
	resp := pollResponse(t, db, ack.MessageID, 3*time.Second)
	if resp.Sender.ID != devicexhs.DefaultAdapterActorID || resp.Sender.Kind != actor.KindTool {
		t.Fatalf("voluntary response sender=(%s,%s) want tool adapter", resp.Sender.Kind, resp.Sender.ID)
	}
	var payload map[string]any
	if err := json.Unmarshal(resp.Payload, &payload); err != nil {
		t.Fatalf("response payload unmarshal: %v", err)
	}
	if payload["status"] != "completed" {
		t.Errorf("payload.status=%v want completed; payload=%v", payload["status"], payload)
	}
	if _, ok := payload["note_id"]; !ok {
		t.Errorf("payload missing note_id: %v", payload)
	}
	if !resp.IsTerminal {
		t.Error("response not marked terminal")
	}
}

func TestAdapterFrameworkMetricsWired(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	d, srv, _ := startIntegrationDaemon(t, ctx, integDaemonOpts{})
	defer func() { _ = d.Close() }()

	createChannel(t, ctx, d, srv, "ch-integ-metrics", []placement.InitialMember{
		{MemberActorID: "user:alice", Kind: "human", DisplayName: "Alice"},
	})

	out := metrics.Default().RenderPrometheus()
	if !strings.Contains(out, "adapter_install_ok") || !strings.Contains(out, `adapter="xhs"`) {
		t.Fatalf("adapter framework metrics were not exported:\n%s", out)
	}
}

// TestIntegration_XhsPublish_Concurrent covers acceptance #3 — two
// concurrent xhs.publish requests get independent correlation entries
// and produce two distinct terminal responses.
func TestIntegration_XhsPublish_Concurrent(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	d, srv, channelsDir := startIntegrationDaemon(t, ctx, integDaemonOpts{})
	defer func() { _ = d.Close() }()

	const channelID = "ch-integ-conc"
	createChannel(t, ctx, d, srv, channelID, []placement.InitialMember{
		{MemberActorID: "user:alice", Kind: "human"},
	})

	requestIDs := []string{"req-conc-A", "req-conc-B"}
	canonical := make([]message.ID, 0, len(requestIDs))
	for i, rid := range requestIDs {
		ack := writeRequest(t, ctx, d, srv, channelID, rid, "user:alice",
			devicexhs.TypePublish, []byte(`{"title":"t-`+rid+`"}`))
		if !ack.Accepted {
			t.Fatalf("req=%s rejected: %s", rid, ack.RejectReason)
		}
		_ = i
		canonical = append(canonical, ack.MessageID)
	}

	_, db := openChannelMessages(t, channelsDir, channelID)
	defer func() { _ = db.Close() }()

	seen := map[string]string{}
	for i, rid := range requestIDs {
		resp := pollResponse(t, db, canonical[i], 3*time.Second)
		var pl map[string]any
		_ = json.Unmarshal(resp.Payload, &pl)
		if pl["status"] != "completed" {
			t.Errorf("req=%s status=%v", rid, pl["status"])
		}
		noteID, _ := pl["note_id"].(string)
		if noteID == "" {
			t.Errorf("req=%s missing note_id", rid)
			continue
		}
		if prev, dup := seen[noteID]; dup {
			t.Errorf("note_id %q collided across requests %s and %s", noteID, prev, rid)
		}
		seen[noteID] = rid
	}
}

// TestIntegration_XhsPublish_PanicEmitsFailedTerminal covers acceptance
// #4 — scaffold configured with PanicOnHandle panics inside Handle,
// framework recovers and emits failed terminal payload.reason=
// receiver_internal_error with detail containing the panic stack.
func TestIntegration_XhsPublish_PanicEmitsFailedTerminal(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	d, srv, channelsDir := startIntegrationDaemon(t, ctx, integDaemonOpts{
		XHSConfig: testXHSConfig{PanicOnHandle: true},
	})
	defer func() { _ = d.Close() }()

	const channelID = "ch-integ-panic"
	createChannel(t, ctx, d, srv, channelID, []placement.InitialMember{
		{MemberActorID: "user:alice", Kind: "human"},
	})

	const requestID = "req-panic-1"
	ack := writeRequest(t, ctx, d, srv, channelID, requestID, "user:alice",
		devicexhs.TypePublish, []byte(`{"title":"boom"}`))
	if !ack.Accepted {
		// Even on panic the write_message ack should still report Accepted
		// because the harness chain finished writing the request row; the
		// panic happens DURING the post-harness gateway dispatch.
		t.Fatalf("write_message rejected: %s", ack.RejectReason)
	}

	_, db := openChannelMessages(t, channelsDir, channelID)
	defer func() { _ = db.Close() }()

	resp := pollResponse(t, db, ack.MessageID, 3*time.Second)
	var payload map[string]any
	if err := json.Unmarshal(resp.Payload, &payload); err != nil {
		t.Fatalf("response payload unmarshal: %v", err)
	}
	if payload["status"] != "failed" {
		t.Errorf("payload.status=%v want failed", payload["status"])
	}
	if payload["reason"] != string(message.TerminalReceiverInternalError) {
		t.Errorf("payload.reason=%v want %s", payload["reason"], message.TerminalReceiverInternalError)
	}
	detail, _ := payload["detail"].(string)
	if detail == "" {
		t.Errorf("payload.detail empty — expected panic stack")
	}
}

// TestIntegration_XhsPublish_TimerEmitsUnansweredTimeout covers
// acceptance #5 — scaffold configured with SkipRespond never replies,
// the framework F3 timer (max_pending_ms) fires and emits a failed
// terminal reason=unanswered_timeout.
func TestIntegration_XhsPublish_TimerEmitsUnansweredTimeout(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	d, srv, channelsDir := startIntegrationDaemon(t, ctx, integDaemonOpts{
		XHSConfig: testXHSConfig{
			SkipRespond:  true,
			MaxPendingMs: 250, // short timeout so the test runs quickly
		},
	})
	defer func() { _ = d.Close() }()

	const channelID = "ch-integ-timer"
	createChannel(t, ctx, d, srv, channelID, []placement.InitialMember{
		{MemberActorID: "user:alice", Kind: "human"},
	})

	const requestID = "req-timer-1"
	ack := writeRequest(t, ctx, d, srv, channelID, requestID, "user:alice",
		devicexhs.TypePublish, []byte(`{"title":"slow"}`))
	if !ack.Accepted {
		t.Fatalf("write_message rejected: %s", ack.RejectReason)
	}

	_, db := openChannelMessages(t, channelsDir, channelID)
	defer func() { _ = db.Close() }()

	resp := pollResponse(t, db, ack.MessageID, 5*time.Second)
	if resp.Sender.ID != actor.SystemActorID || resp.Sender.Kind != actor.KindSystem {
		t.Fatalf("timer fallback sender=(%s,%s) want system actor", resp.Sender.Kind, resp.Sender.ID)
	}
	var payload map[string]any
	_ = json.Unmarshal(resp.Payload, &payload)
	if payload["status"] != "failed" {
		t.Errorf("payload.status=%v want failed", payload["status"])
	}
	if payload["reason"] != string(message.TerminalUnansweredTimeout) {
		t.Errorf("payload.reason=%v want %s", payload["reason"], message.TerminalUnansweredTimeout)
	}
}

// countResponses returns the number of `kind='response' AND parent_id=requestID`
// rows in the channel sqlite. Used by the acceptance C test below to assert
// the long-pending scheduler did NOT double-emit alongside the adapter F3
// timer.
func countResponses(t *testing.T, db *sql.DB, requestID message.ID) int {
	t.Helper()
	var n int
	err := db.QueryRowContext(context.Background(),
		`SELECT COUNT(*) FROM messages WHERE parent_id=? AND kind='response'`, requestID.String()).Scan(&n)
	if err != nil {
		t.Fatalf("count responses: %v", err)
	}
	return n
}

// TestIntegration_LongPending_ToolReceiverSkippedByScheduler_F3Wins covers
// T147 acceptance C — the合并 ticket 价值: 当 xhs.publish 请求 envelope
// expires_at 早于 adapter framework F3 deadline 时，scheduler 扫到
// audience=tool:xhs MUST skip（receiver-kind=tool 分支），由
// framework F3 timer 兜底 emit reason=unanswered_timeout，确保 The
// One Law 单一终态 + 没有 terminal_duplicate.
//
// Wall-clock budget:
//
//	t=0      seed request with expires_at = now+200ms; adapter MaxPendingMs=1500ms
//	t=600ms  poll: must be ZERO response rows (scheduler 已扫多次，tool skip)
//	t≈1.5s   framework F3 emits failed reason=unanswered_timeout
//	t<4s     final poll asserts exactly ONE terminal response with the
//	         unanswered timeout reason.
func TestIntegration_LongPending_ToolReceiverSkippedByScheduler_F3Wins(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	d, srv, channelsDir := startIntegrationDaemon(t, ctx, integDaemonOpts{
		XHSConfig: testXHSConfig{
			SkipRespond:  true,
			MaxPendingMs: 1500, // F3 deadline ~1.5s after Handle
		},
		SchedulerPeriod: 30 * time.Millisecond, // aggressive scan loop
	})
	defer func() { _ = d.Close() }()

	const channelID = "ch-integ-join"
	createChannel(t, ctx, d, srv, channelID, []placement.InitialMember{
		{MemberActorID: "user:alice", Kind: "human"},
	})

	const requestID = "req-join-1"
	expiresAt := nowMs() + 200 // expires well before MaxPendingMs deadline
	ack := writeRequestWithExpiry(t, ctx, d, srv, channelID, requestID,
		"user:alice", devicexhs.TypePublish, []byte(`{"title":"slow"}`), &expiresAt)
	if !ack.Accepted {
		t.Fatalf("write_message rejected: %s", ack.RejectReason)
	}

	_, db := openChannelMessages(t, channelsDir, channelID)
	defer func() { _ = db.Close() }()

	// 1) Mid-flight check at 600ms: expires_at lapsed long ago, scheduler
	//    has scanned ~20 times (period=30ms), MaxPendingMs not yet hit.
	//    There MUST be no response row — tool-skip branch holds.
	time.Sleep(600 * time.Millisecond)
	if c := countResponses(t, db, ack.MessageID); c != 0 {
		t.Fatalf("scheduler emitted %d terminal(s) before F3 fired — tool-skip branch broken", c)
	}

	// 2) Adapter F3 timer eventually fires. Poll up to 5s.
	resp := pollResponse(t, db, ack.MessageID, 5*time.Second)
	if resp.Sender.ID != actor.SystemActorID || resp.Sender.Kind != actor.KindSystem {
		t.Fatalf("timer fallback sender=(%s,%s) want system actor", resp.Sender.Kind, resp.Sender.ID)
	}
	var payload map[string]any
	_ = json.Unmarshal(resp.Payload, &payload)
	if payload["status"] != "failed" {
		t.Errorf("payload.status=%v want failed", payload["status"])
	}
	if payload["reason"] != string(message.TerminalUnansweredTimeout) {
		t.Errorf("payload.reason=%v want %s", payload["reason"], message.TerminalUnansweredTimeout)
	}

	// 3) After F3 fires, give the scheduler a few more ticks to confirm it
	//    does NOT race in a second emit on top of the framework's terminal.
	//    The One Law unique index guards this at the SQL layer, but a stray
	//    emit would surface as a count > 1 OR a noisy terminal_duplicate
	//    error in the daemon log — either way the test fails.
	time.Sleep(200 * time.Millisecond)
	if c := countResponses(t, db, ack.MessageID); c != 1 {
		t.Errorf("terminal response count=%d want 1 — possible double-emit", c)
	}
}
