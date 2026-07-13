package home

import (
	"context"
	"encoding/json"
	"path/filepath"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/lib/behavior"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/message"
)

// expiry_reaper_test.go — the substrate expiry reaper's obligation (义务归位
// D3, DoD-8): a declared deadline outlives the home process (system-authored
// terminal on reopen past the deadline) and an answered request is never
// touched. (The HumanHandle door self-verify tests that once shared this file
// retired with the door in the gateway 期 S5; their cross-incarnation /
// resource-face coverage now lives in the cell's identity-verb tests
// (lib/actorbase.sys_identity_test) and the humancell interpret tests.)

const humanFixtureID = actor.ActorID("user:alice")

// doorHome opens a whitebox activation home (hour-long ticker — tests drive
// the ring synchronously) and admits+embodies the human subject.
func doorHome(t *testing.T) (*Home, actor.ActorID) {
	t.Helper()
	h := openActivationHome(t, &testDesired{}, newTestBuilder())
	humanID := admit(t, h, humanFixtureID, actor.KindHuman)
	h.reconcileActivation(context.Background())
	if !live(h, humanID) {
		t.Fatal("human cell not embodied by the ring")
	}
	return h, humanID
}

// writeDoorRequest authors one request through a welded pen (whitebox mint —
// the test IS the trusted assembler here) and returns its id.
func writeDoorRequest(t *testing.T, h *Home, sender actor.ActorID, kind actor.Kind, target actor.ActorID, msgType string, expiresAt *int64) message.ID {
	t.Helper()
	pen := h.minter.Mint(sender, kind, h.channelID)
	env, err := behavior.BuildRequest(func() time.Time { return time.UnixMilli(h.nowMs()) }, behavior.RequestSpec{
		Type:      msgType,
		Audience:  message.Audience{target},
		ExpiresAt: expiresAt,
	})
	if err != nil {
		t.Fatalf("BuildRequest: %v", err)
	}
	res, err := pen.Write(context.Background(), env)
	if err != nil || !res.Accepted() {
		t.Fatalf("request write = (%+v, %v)", res, err)
	}
	return res.MessageID
}

// findTerminal scans the log for reqID's terminal response.
func findTerminal(t *testing.T, h *Home, reqID message.ID) (*message.Envelope, bool) {
	t.Helper()
	rows, err := h.cs.Query.ReadAfterSeq(context.Background(), 0, 10_000)
	if err != nil {
		t.Fatalf("ReadAfterSeq: %v", err)
	}
	for i := range rows {
		e := rows[i].Envelope
		if e.Kind == message.KindResponse && e.ParentID == reqID && rows[i].IsTerminal {
			return &e, true
		}
	}
	return nil, false
}

// TestExpiryReaper_SystemAuthoredAcrossRestart is DoD-8's core: a declared
// deadline outlives the home process — reopening past the deadline, the
// startup sweep (or any later tick) materialises the terminal, SYSTEM-
// authored with reason=unanswered_timeout + closed_by:system, WITHOUT
// reviving the caller (closure on the message axis only).
func TestExpiryReaper_SystemAuthoredAcrossRestart(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "reaper.sqlite")
	// Real-time deadlines: Home's nowMs (the reaper's clock) is wall time —
	// cfg.Clock only feeds the schedule engine.
	builder1 := newTestBuilder()
	h1, err := Open(Config{
		ChannelID: activationTestChannelID, DBPath: dbPath,
		ReconcileInterval: time.Hour, Desired: &testDesired{}, Builder: builder1,
	})
	if err != nil {
		t.Fatalf("Open #1: %v", err)
	}
	// Receiver = the human subject: its cell is ALIVE on reopen (user-domain
	// supply revives it) and human.approve is default-deferred — so the
	// CLOSURE reconciler (receiver-absent → receiver_unavailable) never
	// touches this request, and the only lawful closer is the expiry reaper.
	callerID := admit(t, h1, "agent:caller", actor.KindAgent)
	humanID := admit(t, h1, humanFixtureID, actor.KindHuman)
	exp := time.Now().UnixMilli() + 50
	// "human.approve" is a LITERAL COPY of humancell's own unexported
	// typeHumanApprove (platform-topology 批 T5b 裁决9②: not part of humancell's
	// five-name wiring seam) — platform/home cannot reach an unexported
	// constant across the package boundary, and the value is a wire-stable
	// domain vocabulary word; drift would be caught by humancell's own tests
	// (循 platform/internal/presence/presence_test.go 先例).
	reqID := writeDoorRequest(t, h1, callerID, actor.KindAgent, humanID, "human.approve", &exp)
	if err := h1.Close(); err != nil {
		t.Fatalf("Close #1: %v", err)
	}
	time.Sleep(80 * time.Millisecond) // cross the declared deadline while the home is DOWN

	// Reopen PAST the deadline with a fresh builder that records any build.
	builder2 := newTestBuilder()
	h2, err := Open(Config{
		ChannelID: activationTestChannelID, DBPath: dbPath,
		ReconcileInterval: time.Hour, Desired: &testDesired{}, Builder: builder2,
	})
	if err != nil {
		t.Fatalf("Open #2: %v", err)
	}
	t.Cleanup(func() { _ = h2.Close() })
	h2.sweepExpired(ctx) // startup already swept; explicit call keeps the assert deterministic

	term, ok := findTerminal(t, h2, reqID)
	if !ok {
		t.Fatal("no terminal after deadline sweep (reaper 义务未兑现)")
	}
	if term.Sender.ID != actor.SystemActorID {
		t.Fatalf("terminal sender = %s, want system (义务归位: system-authored)", term.Sender.ID)
	}
	var p struct {
		Status   string `json:"status"`
		Reason   string `json:"reason"`
		ClosedBy string `json:"closed_by"`
	}
	_ = json.Unmarshal(term.Payload, &p)
	if p.Status != string(message.StatusFailed) || p.Reason != string(message.TerminalUnansweredTimeout) || p.ClosedBy != "system" {
		t.Fatalf("terminal payload = %s, want failed/unanswered_timeout/closed_by:system", string(term.Payload))
	}
	// Liveness-decoupled: closing the account revived NOBODY (the builder's
	// seen map records every caps weld = every build).
	builder2.mu.Lock()
	builds := len(builder2.seen)
	builder2.mu.Unlock()
	if builds != 0 {
		t.Fatalf("reaper triggered %d builder builds — closure touched the actor axis (revive-to-close 回潮)", builds)
	}
	if live(h2, "agent:caller") {
		t.Fatal("reaper revived the gone caller (liveness-decoupled 破)")
	}

	// Idempotent: a second sweep writes nothing new (terminal uniqueness).
	h2.sweepExpired(ctx)
	rows, _ := h2.cs.Query.ReadAfterSeq(ctx, 0, 10_000)
	terms := 0
	for i := range rows {
		if rows[i].Envelope.ParentID == reqID && rows[i].IsTerminal {
			terms++
		}
	}
	if terms != 1 {
		t.Fatalf("terminals for request = %d, want exactly 1", terms)
	}
}

// TestExpiryReaper_AnsweredRequestUntouched: a request answered in time never
// meets the reaper.
func TestExpiryReaper_AnsweredRequestUntouched(t *testing.T) {
	ctx := context.Background()
	h, humanID := doorHome(t)
	callerID := admit(t, h, "agent:caller", actor.KindAgent)

	exp := h.nowMs() + 60_000
	// "human.message" is a LITERAL COPY of humancell's own unexported
	// typeHumanMessage (same drift argument as TestExpiryReaper's
	// human.approve use above).
	reqID := writeDoorRequest(t, h, callerID, actor.KindAgent, humanID, "human.message", &exp)
	// The human cell answers human.message immediately (three-choice table);
	// wait for its receipt.
	deadline := time.Now().Add(5 * time.Second)
	for {
		if _, ok := findTerminal(t, h, reqID); ok {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("human.message never answered")
		}
		time.Sleep(5 * time.Millisecond)
	}
	h.sweepExpired(ctx)
	rows, _ := h.cs.Query.ReadAfterSeq(ctx, 0, 10_000)
	terms := 0
	for i := range rows {
		if rows[i].Envelope.ParentID == reqID && rows[i].IsTerminal {
			terms++
		}
	}
	if terms != 1 {
		t.Fatalf("terminals = %d, want 1 (reaper touched an answered request)", terms)
	}
}
