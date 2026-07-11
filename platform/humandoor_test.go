package platform

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/lib/behavior"
	"github.com/wanpengxie/atoll/protocol/access"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/runtime/accessdoor"
)

// humandoor_test.go — 期12 S4 的门自验族: 跨 incarnation from-log 权威 /
// 门不构建(冻结环) / presence token straddle / expiry reaper (DoD 3/5/7/8/13).

const doorUser = actor.ActorID("user:alice")

// doorHome opens a whitebox activation home (hour-long ticker — tests drive
// the ring synchronously) and admits+embodies the human subject.
func doorHome(t *testing.T) *Home {
	t.Helper()
	h := openActivationHome(t, &testDesired{}, newTestBuilder())
	admit(t, h, doorUser, actor.KindHuman)
	h.reconcileActivation(context.Background())
	if !live(h, doorUser) {
		t.Fatal("human cell not embodied by the ring")
	}
	return h
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

// TestHumanDoor_CrossIncarnationResolveCancel is DoD-3: from-log authority
// means a request left open across a cell kill+re-mint is still resolvable
// (approve) and a subject's own pre-crash request still cancellable — the
// engine's per-incarnation serve account is NOT the authority.
func TestHumanDoor_CrossIncarnationResolveCancel(t *testing.T) {
	ctx := context.Background()
	h := doorHome(t)
	admit(t, h, "agent:asker", actor.KindAgent)

	// An open approve addressed to the subject, and a subject-authored
	// outbound request — both BEFORE the crash.
	approveID := writeDoorRequest(t, h, "agent:asker", actor.KindAgent, doorUser, TypeHumanApprove, nil)
	handle, err := h.Human(ctx, doorUser)
	if err != nil {
		t.Fatalf("Human: %v", err)
	}
	var outID message.ID
	subDeadline := time.Now().Add(5 * time.Second)
	for {
		var serr error
		outID, _, serr = handle.Submit(ctx, SubmitSpec{Type: "agent.ask", Audience: []actor.ActorID{"agent:asker"}})
		if serr == nil {
			break
		}
		if !errors.Is(serr, ErrCellUnavailable) || time.Now().After(subDeadline) {
			t.Fatalf("Submit: %v", serr)
		}
		time.Sleep(5 * time.Millisecond)
	}

	// Kill the cell and re-mint a FRESH incarnation (the crash).
	h.channel.Cells().DespawnID(doorUser)
	h.reconcileActivation(ctx)
	if !live(h, doorUser) {
		t.Fatal("cell not re-minted")
	}

	// Resolve the pre-crash approve on the NEW incarnation (retry through
	// the fresh engine's async Start window).
	deadline := time.Now().Add(5 * time.Second)
	for {
		rerr := handle.Resolve(ctx, approveID, "approved", json.RawMessage(`{"note":"ok"}`))
		if rerr == nil {
			break
		}
		if !errors.Is(rerr, ErrCellUnavailable) {
			t.Fatalf("cross-incarnation Resolve = %v (账本失忆病 — from-log 权威破)", rerr)
		}
		if time.Now().After(deadline) {
			t.Fatal("Resolve never recovered after re-mint")
		}
		time.Sleep(5 * time.Millisecond)
	}
	term, ok := findTerminal(t, h, approveID)
	if !ok {
		t.Fatal("no terminal for the approve")
	}
	if term.Sender.ID != doorUser {
		t.Fatalf("approve terminal sender = %s, want the subject", term.Sender.ID)
	}
	var p struct {
		Decision string `json:"decision"`
		Status   string `json:"status"`
	}
	_ = json.Unmarshal(term.Payload, &p)
	if p.Decision != "approved" || p.Status != string(message.StatusCompleted) {
		t.Fatalf("approve terminal payload = %s", string(term.Payload))
	}

	// Cancel the pre-crash outbound request on the NEW incarnation.
	if err := handle.Cancel(ctx, outID); err != nil {
		t.Fatalf("cross-incarnation Cancel = %v", err)
	}
	term, ok = findTerminal(t, h, outID)
	if !ok {
		t.Fatal("no terminal for the cancelled request")
	}
	var cp struct {
		Cancelled bool   `json:"cancelled"`
		Reason    string `json:"reason"`
	}
	_ = json.Unmarshal(term.Payload, &cp)
	if !cp.Cancelled || cp.Reason != string(message.TerminalUnansweredTimeout) {
		t.Fatalf("cancel terminal payload = %s", string(term.Payload))
	}
}

// TestHumanDoor_NoConstructionFrozenRing is DoD-5: with the ring frozen, a
// write landing in the cell-absent window answers ErrCellUnavailable, spawns
// NOTHING (live set unchanged), and the SAME handle recovers once the ring
// re-mints — no reconnect, no second supply entry.
func TestHumanDoor_NoConstructionFrozenRing(t *testing.T) {
	ctx := context.Background()
	h := doorHome(t)

	handle, err := h.Human(ctx, doorUser)
	if err != nil {
		t.Fatalf("Human: %v", err)
	}
	// Kill the cell; the ring is frozen (hour ticker, no poke).
	h.channel.Cells().DespawnID(doorUser)
	before := len(h.channel.Cells().LiveIDs())

	_, _, werr := handle.Submit(ctx, SubmitSpec{Type: "human.note", Audience: []actor.ActorID{"agent:x"}})
	if !errors.Is(werr, ErrCellUnavailable) {
		t.Fatalf("absent-window Submit = %v, want ErrCellUnavailable", werr)
	}
	if got := len(h.channel.Cells().LiveIDs()); got != before {
		t.Fatalf("live set changed %d→%d — a write constructed an embodiment (门不构建破)", before, got)
	}

	// Ring catches up; the SAME handle writes through (the engine's Start
	// runs async on the cell goroutine — the occupant gate answers
	// unavailable for a beat, which is itself the honest transient contract).
	h.reconcileActivation(ctx)
	deadline := time.Now().Add(5 * time.Second)
	for {
		_, _, err := handle.Submit(ctx, SubmitSpec{Type: "human.note", Audience: []actor.ActorID{"agent:x"}})
		if err == nil {
			break
		}
		if !errors.Is(err, ErrCellUnavailable) {
			t.Fatalf("post-re-mint Submit on same handle = %v", err)
		}
		if time.Now().After(deadline) {
			t.Fatal("Submit never recovered after the ring re-minted")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// TestHumanDoor_PresenceTokenStraddle is DoD-7/13: multi-tab token set;
// Remove clears the account and Forgets the fold (honest unknown); a stale
// pre-Remove handle can neither feed a removed id online (straddle gate) nor
// extinguish a re-admitted sibling's fresh session (token form).
func TestHumanDoor_PresenceTokenStraddle(t *testing.T) {
	ctx := context.Background()
	h := doorHome(t)

	oldHandle, err := h.Human(ctx, doorUser)
	if err != nil {
		t.Fatalf("Human: %v", err)
	}
	tok1 := oldHandle.PresenceConnect()
	tok2 := oldHandle.PresenceConnect()
	if tok1 == "" || tok2 == "" || tok1 == tok2 {
		t.Fatalf("tokens = (%q, %q)", tok1, tok2)
	}
	if _, known := h.View().DevicePresence(doorUser); !known {
		t.Fatal("device presence unknown after connect")
	}

	// Remove: presence account cleared, fold forgotten.
	if err := h.Remove(ctx, doorUser); err != nil {
		t.Fatalf("Remove: %v", err)
	}
	if _, known := h.View().DevicePresence(doorUser); known {
		t.Fatal("device presence still known after Remove (Forget 清账破)")
	}
	// Stale handle cannot feed a removed id online (straddle gate).
	if tok := oldHandle.PresenceConnect(); tok != "" {
		t.Fatalf("removed subject PresenceConnect = %q, want no-op token", tok)
	}

	// Re-admit; a fresh session comes online; the OLD tab's late disconnect
	// only removes itself — the new session stays online.
	newID, err := h.Admit(ctx, actor.KindHuman, "alice")
	if err != nil {
		t.Fatalf("re-Admit: %v", err)
	}
	if newID == doorUser {
		t.Fatal("re-Admit reused removed human id")
	}
	h.reconcileActivation(ctx)
	newHandle, err := h.Human(ctx, newID)
	if err != nil {
		t.Fatalf("Human #2: %v", err)
	}
	tok3 := newHandle.PresenceConnect()
	if tok3 == "" {
		t.Fatal("fresh session got no token")
	}
	oldHandle.PresenceDisconnect(tok1) // stale token: account was cleared — no-op
	oldHandle.PresenceDisconnect(tok2)
	if _, known := h.View().DevicePresence(newID); !known {
		t.Fatal("late stale disconnect extinguished the fresh session (straddle 回归)")
	}
	newHandle.PresenceDisconnect(tok3)
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
	h1, err := Open(HomeConfig{
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
	admit(t, h1, "agent:caller", actor.KindAgent)
	admit(t, h1, doorUser, actor.KindHuman)
	exp := time.Now().UnixMilli() + 50
	reqID := writeDoorRequest(t, h1, "agent:caller", actor.KindAgent, doorUser, TypeHumanApprove, &exp)
	if err := h1.Close(); err != nil {
		t.Fatalf("Close #1: %v", err)
	}
	time.Sleep(80 * time.Millisecond) // cross the declared deadline while the home is DOWN

	// Reopen PAST the deadline with a fresh builder that records any build.
	builder2 := newTestBuilder()
	h2, err := Open(HomeConfig{
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
	h := doorHome(t)
	admit(t, h, "agent:caller", actor.KindAgent)

	exp := h.nowMs() + 60_000
	reqID := writeDoorRequest(t, h, "agent:caller", actor.KindAgent, doorUser, TypeHumanMessage, &exp)
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

// TestHumanDoor_ResourceFaceKVAndShare is DoD-16 (D10): the subject's
// resource face — KV six + Share two — is a TRUE integration (real
// Open→ring→buildCaps→engine→accessdoor, no fakes), literally the cell's own
// Caps.Access. Enforcement model: a live member WITHOUT a grant is refused
// by the door's R (not by any membership special-case); Share opens it; a
// removed member's stale handle never reaches the door (driver classify); a
// dead cell answers the honest transient.
func TestHumanDoor_ResourceFaceKVAndShare(t *testing.T) {
	ctx := context.Background()
	h := doorHome(t)
	const bob = actor.ActorID("user:bob")
	admit(t, h, bob, actor.KindHuman)
	h.reconcileActivation(ctx)

	alice, err := h.Human(ctx, doorUser)
	if err != nil {
		t.Fatalf("Human(alice): %v", err)
	}
	bobH, err := h.Human(ctx, bob)
	if err != nil {
		t.Fatalf("Human(bob): %v", err)
	}

	// Engines start async — retry the first resource verb through the
	// occupant window like the write tests do.
	var out accessdoor.Outcome
	deadline := time.Now().Add(5 * time.Second)
	for {
		out, err = alice.ResourceCreate(ctx, "cfg.tone", []byte(`"formal"`))
		if err == nil {
			break
		}
		if !errors.Is(err, ErrCellUnavailable) || time.Now().After(deadline) {
			t.Fatalf("ResourceCreate: %v", err)
		}
		time.Sleep(5 * time.Millisecond)
	}
	if !out.Accepted() {
		t.Fatalf("create rejected: %s", out.RejectReason)
	}

	// Creator round-trip: read / stat / list / write.
	if out, err = alice.ResourceRead(ctx, "cfg.tone"); err != nil || !out.Accepted() || string(out.Value) != `"formal"` {
		t.Fatalf("read = (%+v, %v)", out, err)
	}
	if _, err := alice.ResourceStat(ctx, "cfg.tone"); err != nil {
		t.Fatalf("stat: %v", err)
	}
	page, err := alice.ResourceList(ctx, accessdoor.ListQuery{})
	if err != nil || len(page.Entries) == 0 {
		t.Fatalf("list = (%+v, %v)", page, err)
	}
	if out, err = alice.ResourceWrite(ctx, "cfg.tone", []byte(`"casual"`)); err != nil || !out.Accepted() {
		t.Fatalf("write = (%+v, %v)", out, err)
	}

	// Live member, NO grant → the door's R refuses (access enforcement, not
	// a membership special-case).
	deadline = time.Now().Add(5 * time.Second)
	for {
		out, err = bobH.ResourceRead(ctx, "cfg.tone")
		if err == nil {
			break
		}
		if !errors.Is(err, ErrCellUnavailable) || time.Now().After(deadline) {
			t.Fatalf("bob read: %v", err)
		}
		time.Sleep(5 * time.Millisecond)
	}
	if out.Accepted() {
		t.Fatal("no-grant member read a foreign resource — door R broken")
	}

	// ShareActor(read) → bob reads ("给 bob 设变量" 的机制半).
	if out, err = alice.ResourceShareActor(ctx, "cfg.tone", bob, []access.Operation{access.OpRead}); err != nil || !out.Accepted() {
		t.Fatalf("share = (%+v, %v)", out, err)
	}
	if out, err = bobH.ResourceRead(ctx, "cfg.tone"); err != nil || !out.Accepted() || string(out.Value) != `"casual"` {
		t.Fatalf("bob post-share read = (%+v, %v)", out, err)
	}

	// ShareMembers on a second resource → any member reads.
	if out, err = alice.ResourceCreate(ctx, "cfg.shared", []byte(`1`)); err != nil || !out.Accepted() {
		t.Fatalf("create shared = (%+v, %v)", out, err)
	}
	if out, err = alice.ResourceShareMembers(ctx, "cfg.shared", []access.Operation{access.OpRead}); err != nil || !out.Accepted() {
		t.Fatalf("share members = (%+v, %v)", out, err)
	}
	if out, err = bobH.ResourceRead(ctx, "cfg.shared"); err != nil || !out.Accepted() {
		t.Fatalf("bob members read = (%+v, %v)", out, err)
	}

	// Dead cell → honest transient (liveAccess/occupant, not a door verdict).
	h.channel.Cells().DespawnID(doorUser)
	if _, err = alice.ResourceRead(ctx, "cfg.tone"); !errors.Is(err, ErrCellUnavailable) {
		t.Fatalf("dead-cell read = %v, want ErrCellUnavailable", err)
	}

	// Removed member's stale handle → classified at the driver seam, the
	// door is never consulted.
	if err := h.Remove(ctx, bob); err != nil {
		t.Fatalf("remove bob: %v", err)
	}
	if _, err = bobH.ResourceRead(ctx, "cfg.tone"); !errors.Is(err, ErrNotMember) {
		t.Fatalf("removed-member read = %v, want ErrNotMember", err)
	}

	// Delete closes the loop (creator holds full rights).
	h.reconcileActivation(ctx) // re-mint alice
	deadline = time.Now().Add(5 * time.Second)
	for {
		out, err = alice.ResourceDelete(ctx, "cfg.tone")
		if err == nil {
			break
		}
		if !errors.Is(err, ErrCellUnavailable) || time.Now().After(deadline) {
			t.Fatalf("delete: %v", err)
		}
		time.Sleep(5 * time.Millisecond)
	}
	if !out.Accepted() {
		t.Fatalf("delete rejected: %s", out.RejectReason)
	}
	if out, err = alice.ResourceRead(ctx, "cfg.tone"); err != nil || out.Accepted() {
		t.Fatalf("post-delete read = (%+v, %v), want reject", out, err)
	}
}

// TestHumanDoor_ResolveNullPayload (修复批 P1-1 regression): JSON `null` is a
// legal payload frame value that unmarshals the merge map to nil — Resolve
// must treat it as "no payload", never panic on the decision write.
func TestHumanDoor_ResolveNullPayload(t *testing.T) {
	ctx := context.Background()
	h := doorHome(t)
	admit(t, h, "agent:asker", actor.KindAgent)
	reqID := writeDoorRequest(t, h, "agent:asker", actor.KindAgent, doorUser, TypeHumanApprove, nil)

	handle, err := h.Human(ctx, doorUser)
	if err != nil {
		t.Fatalf("Human: %v", err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		rerr := handle.Resolve(ctx, reqID, "approved", json.RawMessage(`null`))
		if rerr == nil {
			break
		}
		if !errors.Is(rerr, ErrCellUnavailable) || time.Now().After(deadline) {
			t.Fatalf("Resolve(payload:null) = %v", rerr)
		}
		time.Sleep(5 * time.Millisecond)
	}
	term, ok := findTerminal(t, h, reqID)
	if !ok {
		t.Fatal("no terminal")
	}
	var p struct {
		Decision string `json:"decision"`
	}
	_ = json.Unmarshal(term.Payload, &p)
	if p.Decision != "approved" {
		t.Fatalf("terminal payload = %s", string(term.Payload))
	}
}

// TestHumanDoor_DriveDuringChurn (修复批 P2-6): hammer the door's write verb
// from a foreign goroutine WHILE the cell is repeatedly killed and re-minted —
// the real go-live→Start window and stale-driver replacement races the
// occupant gate + live membranes exist for. Run under -race; the assertion is
// "no panic, and every error is one of the honest family".
func TestHumanDoor_DriveDuringChurn(t *testing.T) {
	ctx := context.Background()
	h := doorHome(t)
	handle, err := h.Human(ctx, doorUser)
	if err != nil {
		t.Fatalf("Human: %v", err)
	}

	stop := make(chan struct{})
	errs := make(chan error, 1)
	go func() {
		defer close(errs)
		for {
			select {
			case <-stop:
				return
			default:
			}
			_, _, serr := handle.Submit(ctx, SubmitSpec{Type: "human.note", Audience: []actor.ActorID{"agent:x"}})
			if serr != nil && !errors.Is(serr, ErrCellUnavailable) {
				var wr *WriteRejectedError
				if !errors.As(serr, &wr) {
					errs <- serr
					return
				}
			}
		}
	}()
	for i := 0; i < 40; i++ {
		h.channel.Cells().DespawnID(doorUser)
		h.reconcileActivation(ctx)
	}
	close(stop)
	if serr, ok := <-errs; ok && serr != nil {
		t.Fatalf("drive-during-churn surfaced a non-honest error: %v", serr)
	}
}
