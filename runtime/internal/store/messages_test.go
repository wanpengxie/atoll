package store_test

import (
	"context"
	"errors"
	"testing"

	"github.com/wanpengxie/ActOS/protocol/actor"
	"github.com/wanpengxie/ActOS/protocol/message"
	"github.com/wanpengxie/ActOS/runtime/storespec"
)

// --- Append: seq allocation + faithful round-trip (A3) -----------------------

// Append must allocate a strictly increasing seq per write and report it
// truthfully in AppendResult. The store does not lie about the position it
// gave a row.
func TestAppend_AllocatesMonotonicSeq(t *testing.T) {
	ctx := context.Background()
	cs := openTestChannel(t)

	var last storespec.Seq
	for i, id := range []string{"m1", "m2", "m3"} {
		res, err := cs.Log.Append(ctx, newEnv(id, message.KindEvent, message.Audience{"bob"}), false)
		if err != nil {
			t.Fatalf("Append %s: %v", id, err)
		}
		if res.Seq <= last {
			t.Fatalf("seq not monotonic at #%d: got %d after %d", i, res.Seq, last)
		}
		last = res.Seq
	}
}

// A row written through Append must read back through FindByID with every
// envelope field preserved exactly (A3: store does not silently mutate truth).
func TestAppend_FindByID_RoundTrip(t *testing.T) {
	ctx := context.Background()
	cs := openTestChannel(t)

	exp := int64(9999)
	in := newEnv("req-1", message.KindRequest, message.Audience{"tool:xhs", "audit"},
		withSender(actor.KindAgent, "planner"),
		withType("xhs.publish"),
		withPayload(`{"note":"hi"}`),
		withVisibility(message.VisibilityPrivate),
		withCorrelation("corr-7"),
	)
	in.ExpiresAt = &exp

	res, err := cs.Log.Append(ctx, in, false)
	if err != nil {
		t.Fatalf("Append: %v", err)
	}

	got, ok, err := cs.Log.FindByID(ctx, "req-1")
	if err != nil || !ok {
		t.Fatalf("FindByID ok=%v err=%v", ok, err)
	}
	if got.Seq != int64(res.Seq) {
		t.Errorf("seq=%d want %d", got.Seq, res.Seq)
	}
	if got.IsTerminal {
		t.Errorf("is_terminal=true want false")
	}
	e := got.Envelope
	if e.ID != "req-1" || e.Kind != message.KindRequest || e.Type != "xhs.publish" {
		t.Errorf("id/kind/type mismatch: %+v", e)
	}
	if e.Sender.Kind != actor.KindAgent || e.Sender.ID != "planner" {
		t.Errorf("sender=%+v", e.Sender)
	}
	if e.Visibility != message.VisibilityPrivate {
		t.Errorf("visibility=%q", e.Visibility)
	}
	if e.CorrelationID != "corr-7" {
		t.Errorf("correlation=%q", e.CorrelationID)
	}
	if string(e.Payload) != `{"note":"hi"}` {
		t.Errorf("payload=%s", e.Payload)
	}
	if e.ExpiresAt == nil || *e.ExpiresAt != exp {
		t.Errorf("expires_at=%v want %d", e.ExpiresAt, exp)
	}
	// A1 addressing: the full audience list is preserved in order.
	if len(e.Audience) != 2 || e.Audience[0] != "tool:xhs" || e.Audience[1] != "audit" {
		t.Errorf("audience=%v want [tool:xhs audit]", e.Audience)
	}
}

// is_terminal is a harness-supplied input that the store persists verbatim —
// it neither recomputes nor second-guesses it (FIX-T10).
func TestAppend_PersistsTerminalVerbatim(t *testing.T) {
	ctx := context.Background()
	cs := openTestChannel(t)

	// A request row passed isTerminal=true: store persists the bit as given
	// even though "request" is semantically never terminal — proving the
	// store does not derive the value itself.
	if _, err := cs.Log.Append(ctx, newEnv("p1", message.KindRequest, message.Audience{"x"}), true); err != nil {
		t.Fatalf("Append: %v", err)
	}
	got, ok, err := cs.Log.FindByID(ctx, "p1")
	if err != nil || !ok {
		t.Fatalf("FindByID ok=%v err=%v", ok, err)
	}
	if !got.IsTerminal {
		t.Errorf("is_terminal=false; store must persist harness bit verbatim")
	}
}

// --- Append: protocol defenses (loud rejection, no silent coercion) ----------

func TestAppend_RejectsProtocolViolations(t *testing.T) {
	ctx := context.Background()
	cs := openTestChannel(t)

	cases := []struct {
		name string
		env  *message.Envelope
	}{
		{"nil envelope", nil},
		{"empty id", newEnv("", message.KindEvent, message.Audience{"x"})},
		{"nil payload", func() *message.Envelope {
			e := newEnv("np", message.KindEvent, message.Audience{"x"})
			e.Payload = nil
			return e
		}()},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := cs.Log.Append(ctx, tc.env, false); err == nil {
				t.Fatalf("Append must reject %s, got nil error", tc.name)
			}
		})
	}
}

// --- Append: id uniqueness is integrity, not dedup (A3) -----------------------

// A second Append of the same envelope.id is a hard integrity violation
// surfaced as a typed AppendError(harness_id_duplicate_conflict) — NOT a
// silent dedup no-op. (The v1 at-least-once dedupe seam was retired.)
func TestAppend_DuplicateID_TypedConflict(t *testing.T) {
	ctx := context.Background()
	cs := openTestChannel(t)

	if _, err := cs.Log.Append(ctx, newEnv("dup", message.KindEvent, message.Audience{"x"}), false); err != nil {
		t.Fatalf("first Append: %v", err)
	}
	_, err := cs.Log.Append(ctx, newEnv("dup", message.KindEvent, message.Audience{"x"}), false)
	if err == nil {
		t.Fatal("duplicate id Append must error, not dedup-noop")
	}
	var ae *storespec.AppendError
	if !errors.As(err, &ae) {
		t.Fatalf("want *storespec.AppendError, got %T: %v", err, err)
	}
	if ae.Reason != "harness_id_duplicate_conflict" {
		t.Errorf("reason=%q want harness_id_duplicate_conflict", ae.Reason)
	}
	if ae.PartialMessageID != "dup" {
		t.Errorf("partial id=%q want dup", ae.PartialMessageID)
	}
}

// --- The One Law: terminal-response uniqueness per request -------------------

// ux_terminal_response_per_request guarantees at most one final response per
// parent. A second terminal response to the same parent is rejected with the
// typed harness_terminal_duplicate reason — even though it has a distinct
// envelope.id. Provisional (is_terminal=false) responses to the same parent
// are NOT constrained.
func TestAppend_TerminalResponseUniquePerRequest(t *testing.T) {
	ctx := context.Background()
	cs := openTestChannel(t)

	mkResp := func(id string, terminal bool) (storespec.AppendResult, error) {
		env := newEnv(id, message.KindResponse, message.Audience{"alice"},
			withSender(actor.KindAgent, "planner"),
			withParent("the-request"),
			withType("agent.text"),
		)
		return cs.Log.Append(ctx, env, terminal)
	}

	// Two provisional responses to the same parent are both allowed.
	if _, err := mkResp("prov-1", false); err != nil {
		t.Fatalf("provisional 1: %v", err)
	}
	if _, err := mkResp("prov-2", false); err != nil {
		t.Fatalf("provisional 2: %v", err)
	}
	// First terminal is allowed.
	if _, err := mkResp("final-1", true); err != nil {
		t.Fatalf("first terminal: %v", err)
	}
	// Second terminal to the same parent is rejected by the UNIQUE INDEX.
	_, err := mkResp("final-2", true)
	if err == nil {
		t.Fatal("second terminal response to same parent must be rejected")
	}
	var ae *storespec.AppendError
	if !errors.As(err, &ae) {
		t.Fatalf("want *storespec.AppendError, got %T: %v", err, err)
	}
	if ae.Reason != "harness_terminal_duplicate" {
		t.Errorf("reason=%q want harness_terminal_duplicate", ae.Reason)
	}
}

// A terminal response to a DIFFERENT parent is unaffected by another parent's
// existing terminal — the uniqueness is scoped per parent_id.
func TestAppend_TerminalUniquenessScopedPerParent(t *testing.T) {
	ctx := context.Background()
	cs := openTestChannel(t)

	mk := func(id, parent string) error {
		env := newEnv(id, message.KindResponse, message.Audience{"alice"},
			withSender(actor.KindAgent, "p"), withParent(message.ID(parent)), withType("agent.text"))
		_, err := cs.Log.Append(ctx, env, true)
		return err
	}
	if err := mk("f-a", "req-a"); err != nil {
		t.Fatalf("terminal for req-a: %v", err)
	}
	if err := mk("f-b", "req-b"); err != nil {
		t.Fatalf("terminal for req-b must be allowed (different parent): %v", err)
	}
}

// --- HasFinalResponse ---------------------------------------------------------

func TestHasFinalResponse(t *testing.T) {
	ctx := context.Background()
	cs := openTestChannel(t)

	// Empty parent id is a definitional false (no parent → no final).
	if has, err := cs.Log.HasFinalResponse(ctx, ""); err != nil || has {
		t.Fatalf("empty parent has=%v err=%v", has, err)
	}
	// No response yet → false.
	if has, err := cs.Log.HasFinalResponse(ctx, "req-x"); err != nil || has {
		t.Fatalf("no-response has=%v err=%v", has, err)
	}
	// A provisional response does NOT count as final.
	prov := newEnv("prov", message.KindResponse, message.Audience{"a"},
		withSender(actor.KindAgent, "p"), withParent("req-x"), withType("agent.text"))
	if _, err := cs.Log.Append(ctx, prov, false); err != nil {
		t.Fatalf("Append provisional: %v", err)
	}
	if has, err := cs.Log.HasFinalResponse(ctx, "req-x"); err != nil || has {
		t.Fatalf("after provisional has=%v err=%v want false", has, err)
	}
	// A terminal response flips it to true.
	fin := newEnv("fin", message.KindResponse, message.Audience{"a"},
		withSender(actor.KindAgent, "p"), withParent("req-x"), withType("agent.text"))
	if _, err := cs.Log.Append(ctx, fin, true); err != nil {
		t.Fatalf("Append terminal: %v", err)
	}
	if has, err := cs.Log.HasFinalResponse(ctx, "req-x"); err != nil || !has {
		t.Fatalf("after terminal has=%v err=%v want true", has, err)
	}
}

// --- FindByID miss ------------------------------------------------------------

func TestFindByID_Missing(t *testing.T) {
	ctx := context.Background()
	cs := openTestChannel(t)
	row, ok, err := cs.Log.FindByID(ctx, "nope")
	if err != nil {
		t.Fatalf("FindByID: %v", err)
	}
	if ok || row != nil {
		t.Fatalf("missing row ok=%v row=%v", ok, row)
	}
}

// --- MaxSeq + ReadAfterSeq: the client tail (A3 ordering) --------------------

func TestMaxSeq(t *testing.T) {
	ctx := context.Background()
	cs := openTestChannel(t)

	if got, err := cs.Query.MaxSeq(ctx); err != nil || got != 0 {
		t.Fatalf("empty MaxSeq=%d err=%v want 0", got, err)
	}
	var lastSeq storespec.Seq
	for _, id := range []string{"a", "b", "c"} {
		res, err := cs.Log.Append(ctx, newEnv(id, message.KindEvent, message.Audience{"x"}), false)
		if err != nil {
			t.Fatalf("Append %s: %v", id, err)
		}
		lastSeq = res.Seq
	}
	got, err := cs.Query.MaxSeq(ctx)
	if err != nil {
		t.Fatalf("MaxSeq: %v", err)
	}
	if got != int64(lastSeq) {
		t.Errorf("MaxSeq=%d want %d (the last allocated seq)", got, lastSeq)
	}
}

// Channel scope is the sqlite file itself (one channel per file), so MaxSeq /
// ReadAfterSeq take no channel id and there is no foreign-channel query to test:
// the former TestMaxSeq_ChannelScoped exercised the now-removed WHERE channel_id
// filter (a constant within a per-channel file).

func TestReadAfterSeq_ForwardOrderedTail(t *testing.T) {
	ctx := context.Background()
	cs := openTestChannel(t)

	var seqs []storespec.Seq
	for _, id := range []string{"a", "b", "c", "d"} {
		res, err := cs.Log.Append(ctx, newEnv(id, message.KindEvent, message.Audience{"x"}), false)
		if err != nil {
			t.Fatalf("Append %s: %v", id, err)
		}
		seqs = append(seqs, res.Seq)
	}

	// Read everything after the first row's seq → exactly b,c,d in seq order.
	rows, err := cs.Query.ReadAfterSeq(ctx, int64(seqs[0]), 100)
	if err != nil {
		t.Fatalf("ReadAfterSeq: %v", err)
	}
	if len(rows) != 3 {
		t.Fatalf("got %d rows want 3", len(rows))
	}
	wantIDs := []string{"b", "c", "d"}
	prev := int64(0)
	for i, r := range rows {
		if r.Envelope.ID != message.ID(wantIDs[i]) {
			t.Errorf("rows[%d].id=%q want %q", i, r.Envelope.ID, wantIDs[i])
		}
		if r.Seq <= prev {
			t.Errorf("rows not seq-ascending: %d after %d", r.Seq, prev)
		}
		prev = r.Seq
	}
}

func TestReadAfterSeq_LimitHonored(t *testing.T) {
	ctx := context.Background()
	cs := openTestChannel(t)
	for _, id := range []string{"a", "b", "c", "d", "e"} {
		if _, err := cs.Log.Append(ctx, newEnv(id, message.KindEvent, message.Audience{"x"}), false); err != nil {
			t.Fatalf("Append %s: %v", id, err)
		}
	}
	rows, err := cs.Query.ReadAfterSeq(ctx, 0, 2)
	if err != nil {
		t.Fatalf("ReadAfterSeq: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("limit not honored: got %d want 2", len(rows))
	}
}

// --- OpenRequestsForActor: A1 audience addressing + A3 liveness reporting -----

// OpenRequestsForActor returns request rows whose FIRST audience member is the
// actor and which have no terminal response yet. This is the death-signal
// supervisor's view of in-flight work owed to an actor.
func TestOpenRequestsForActor(t *testing.T) {
	ctx := context.Background()
	cs := openTestChannel(t)

	mkReq := func(id string, audience message.Audience) {
		t.Helper()
		env := newEnv(id, message.KindRequest, audience,
			withSender(actor.KindAgent, "planner"), withType("xhs.publish"))
		if _, err := cs.Log.Append(ctx, env, false); err != nil {
			t.Fatalf("Append request %s: %v", id, err)
		}
	}
	mkTerminalResponse := func(id, parent string) {
		t.Helper()
		env := newEnv(id, message.KindResponse, message.Audience{"planner"},
			withSender(actor.KindTool, "tool:xhs"), withParent(message.ID(parent)), withType("agent.text"))
		if _, err := cs.Log.Append(ctx, env, true); err != nil {
			t.Fatalf("Append response %s: %v", id, err)
		}
	}

	// r1: addressed to tool:xhs (first member), open.
	mkReq("r1", message.Audience{"tool:xhs"})
	// r2: addressed to tool:xhs, but already has a terminal response → not open.
	mkReq("r2", message.Audience{"tool:xhs"})
	mkTerminalResponse("r2-final", "r2")
	// r3: addressed to someone else → not for tool:xhs.
	mkReq("r3", message.Audience{"other-actor"})
	// r4: tool:xhs is NOT first member → A1 says it's not the addressee.
	mkReq("r4", message.Audience{"other-actor", "tool:xhs"})

	rows, err := cs.Query.OpenRequestsForActor(ctx, "tool:xhs")
	if err != nil {
		t.Fatalf("OpenRequestsForActor: %v", err)
	}
	if len(rows) != 1 {
		var ids []message.ID
		for _, r := range rows {
			ids = append(ids, r.Envelope.ID)
		}
		t.Fatalf("open rows=%v want exactly [r1]", ids)
	}
	if rows[0].Envelope.ID != "r1" {
		t.Errorf("open request id=%q want r1", rows[0].Envelope.ID)
	}
}

// A provisional (non-terminal) response does NOT close an open request — only
// a terminal response does.
func TestOpenRequestsForActor_ProvisionalDoesNotClose(t *testing.T) {
	ctx := context.Background()
	cs := openTestChannel(t)

	req := newEnv("r1", message.KindRequest, message.Audience{"tool:xhs"},
		withSender(actor.KindAgent, "planner"), withType("xhs.publish"))
	if _, err := cs.Log.Append(ctx, req, false); err != nil {
		t.Fatalf("Append request: %v", err)
	}
	prov := newEnv("r1-prov", message.KindResponse, message.Audience{"planner"},
		withSender(actor.KindTool, "tool:xhs"), withParent("r1"), withType("agent.text"))
	if _, err := cs.Log.Append(ctx, prov, false); err != nil {
		t.Fatalf("Append provisional: %v", err)
	}
	rows, err := cs.Query.OpenRequestsForActor(ctx, "tool:xhs")
	if err != nil {
		t.Fatalf("OpenRequestsForActor: %v", err)
	}
	if len(rows) != 1 || rows[0].Envelope.ID != "r1" {
		t.Fatalf("request still open after provisional response; rows=%v", rows)
	}
}
