package store_test

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"testing"

	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/runtime/internal/store"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

// --- Append: seq allocation + faithful round-trip (A3) -----------------------

// Append must allocate a strictly increasing seq per write and report it
// truthfully in AppendResult. The store does not lie about the position it
// gave a row.
func TestAppend_AllocatesMonotonicSeq(t *testing.T) {
	ctx := context.Background()
	cs := openTestChannel(t)

	var last int64
	for i, id := range []string{"m1", "m2", "m3"} {
		res, err := cs.Log.Append(ctx, newEnv(id, message.KindEvent, message.Audience{"bob"}), false, storespec.AppendMetadata{})
		if err != nil {
			t.Fatalf("Append %s: %v", id, err)
		}
		if res.Seq <= last {
			t.Fatalf("seq not monotonic at #%d: got %d after %d", i, res.Seq, last)
		}
		last = res.Seq
	}
}

func TestLatestBySenderAndTypeUsesStoreSeqAndExactSender(t *testing.T) {
	ctx := context.Background()
	cs := openTestChannel(t)
	appendRow := func(id, sender, typ, payload string) {
		t.Helper()
		env := newEnv(id, message.KindEvent, message.Audience{actor.SystemActorID},
			withSender(actor.KindSystem, actor.ActorID(sender)),
			withType(typ),
			withPayload(payload),
			withVisibility(message.VisibilitySystem),
		)
		if _, err := cs.Log.Append(ctx, env, false, storespec.AppendMetadata{}); err != nil {
			t.Fatal(err)
		}
	}
	appendRow("old", string(actor.SystemActorID), "channel.routing.default_set", `{"n":1}`)
	appendRow("other-type", string(actor.SystemActorID), "other", `{"n":2}`)
	appendRow("forged", "agent:forger", "channel.routing.default_set", `{"n":3}`)
	appendRow("new", string(actor.SystemActorID), "channel.routing.default_set", `{"n":4}`)

	row, found, err := cs.Query.LatestBySenderAndType(
		ctx, actor.SystemActorID, "channel.routing.default_set")
	if err != nil || !found || row.Envelope.ID != "new" {
		t.Fatalf("latest=%+v found=%v err=%v", row, found, err)
	}
	if _, found, err := cs.Query.LatestBySenderAndType(ctx, actor.SystemActorID, "missing"); err != nil || found {
		t.Fatalf("missing found=%v err=%v", found, err)
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
		withVisibility(message.VisibilityPublic),
		withCorrelation("corr-7"),
	)
	in.ExpiresAt = &exp

	res, err := cs.Log.Append(ctx, in, false, storespec.AppendMetadata{})
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
	if e.Visibility != message.VisibilityPublic {
		t.Errorf("visibility=%q", e.Visibility)
	}
	if e.CorrelationID != "corr-7" {
		t.Errorf("correlation=%q", e.CorrelationID)
	}
	if string(e.Payload) != `{"body":{"note":"hi"}}` {
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
	if _, err := cs.Log.Append(ctx, newEnv("p1", message.KindRequest, message.Audience{"x"}), true, storespec.AppendMetadata{}); err != nil {
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
			if _, err := cs.Log.Append(ctx, tc.env, false, storespec.AppendMetadata{}); err == nil {
				t.Fatalf("Append must reject %s, got nil error", tc.name)
			}
		})
	}
}

// --- Append: strict internal ids + fingerprinted shell idempotency ------------

// A second Append of the same envelope.id is a hard integrity violation
// surfaced as a typed AppendError(harness_id_duplicate_conflict) — NOT a
// silent dedup no-op. (The v1 at-least-once dedupe seam was retired.)
func TestAppend_DuplicateID_TypedConflict(t *testing.T) {
	ctx := context.Background()
	cs := openTestChannel(t)

	if _, err := cs.Log.Append(ctx, newEnv("dup", message.KindEvent, message.Audience{"x"}), false, storespec.AppendMetadata{}); err != nil {
		t.Fatalf("first Append: %v", err)
	}
	_, err := cs.Log.Append(ctx, newEnv("dup", message.KindEvent, message.Audience{"x"}), false, storespec.AppendMetadata{})
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

func TestAppend_ClientFingerprintReplayAndConflict(t *testing.T) {
	ctx := context.Background()
	cs := openTestChannel(t)
	meta := storespec.AppendMetadata{ClientFingerprint: "v1:same"}

	first, err := cs.Log.Append(ctx, newEnv("client-id", message.KindEvent, message.Audience{"x"}), false, meta)
	if err != nil || first.Replayed {
		t.Fatalf("first append=(%+v,%v)", first, err)
	}
	replay, err := cs.Log.Append(ctx, newEnv("client-id", message.KindEvent, message.Audience{"x"}), false, meta)
	if err != nil || !replay.Replayed || replay.Seq != first.Seq {
		t.Fatalf("same-fingerprint replay=(%+v,%v), first=%+v", replay, err, first)
	}
	if max, err := cs.Query.MaxSeq(ctx); err != nil || max != first.Seq {
		t.Fatalf("replay advanced truth: max=%d err=%v first=%d", max, err, first.Seq)
	}

	_, err = cs.Log.Append(ctx, newEnv("client-id", message.KindEvent, message.Audience{"x"}), false,
		storespec.AppendMetadata{ClientFingerprint: "v1:different"})
	var conflict *storespec.AppendError
	if !errors.As(err, &conflict) || conflict.Reason != storespec.AppendRejectIDDuplicate {
		t.Fatalf("different fingerprint err=%T %v", err, err)
	}
}

func TestAppend_ClientFingerprintConcurrentReplay(t *testing.T) {
	ctx := context.Background()
	cs := openTestChannel(t)
	meta := storespec.AppendMetadata{ClientFingerprint: "v1:concurrent"}
	const writers = 12
	results := make(chan storespec.AppendResult, writers)
	errs := make(chan error, writers)
	var wg sync.WaitGroup
	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			res, err := cs.Log.Append(ctx, newEnv("same-key", message.KindEvent, message.Audience{"x"}), false, meta)
			results <- res
			errs <- err
		}()
	}
	wg.Wait()
	close(results)
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("concurrent replay: %v", err)
		}
	}
	var seq int64
	var inserted int
	for result := range results {
		if seq == 0 {
			seq = result.Seq
		}
		if result.Seq != seq {
			t.Fatalf("replays returned different seq: %d vs %d", result.Seq, seq)
		}
		if !result.Replayed {
			inserted++
		}
	}
	if inserted != 1 {
		t.Fatalf("physical inserts=%d want 1", inserted)
	}
}

func TestAppend_ClientFingerprintSurvivesReopen(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "messages.sqlite")
	open := func() *store.ChannelStores {
		cs, err := store.OpenChannel(ctx, channel.ID("C-restart"), path, store.OpenOptions{}, nil)
		if err != nil {
			t.Fatalf("OpenChannel: %v", err)
		}
		return cs
	}
	meta := storespec.AppendMetadata{ClientFingerprint: "v1:persisted"}
	firstStore := open()
	first, err := firstStore.Log.Append(ctx, newEnv("restart-key", message.KindEvent, message.Audience{"x"}), false, meta)
	if err != nil {
		t.Fatal(err)
	}
	if err := firstStore.Close(); err != nil {
		t.Fatal(err)
	}
	secondStore := open()
	defer secondStore.Close()
	replay, err := secondStore.Log.Append(ctx, newEnv("restart-key", message.KindEvent, message.Audience{"x"}), false, meta)
	if err != nil || !replay.Replayed || replay.Seq != first.Seq {
		t.Fatalf("restart replay=(%+v,%v), first=%+v", replay, err, first)
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
		return cs.Log.Append(ctx, env, terminal, storespec.AppendMetadata{})
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
		_, err := cs.Log.Append(ctx, env, true, storespec.AppendMetadata{})
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
	if _, err := cs.Log.Append(ctx, prov, false, storespec.AppendMetadata{}); err != nil {
		t.Fatalf("Append provisional: %v", err)
	}
	if has, err := cs.Log.HasFinalResponse(ctx, "req-x"); err != nil || has {
		t.Fatalf("after provisional has=%v err=%v want false", has, err)
	}
	// A terminal response flips it to true.
	fin := newEnv("fin", message.KindResponse, message.Audience{"a"},
		withSender(actor.KindAgent, "p"), withParent("req-x"), withType("agent.text"))
	if _, err := cs.Log.Append(ctx, fin, true, storespec.AppendMetadata{}); err != nil {
		t.Fatalf("Append terminal: %v", err)
	}
	if has, err := cs.Log.HasFinalResponse(ctx, "req-x"); err != nil || !has {
		t.Fatalf("after terminal has=%v err=%v want true", has, err)
	}
}

// --- F2: provisional-after-final is rejected geometrically in-tx -------------

// A provisional response whose parent already has a final MUST be rejected at
// Append (in-tx re-check), not silently appended as a zombie. This is the
// atomic guard that closes the harness pre-check TOCTOU window: even if the
// caller's HasFinalResponse pre-check raced and saw no final, the in-tx
// re-check on the same serialized connection catches the final that committed
// in the window.
func TestAppend_ProvisionalAfterFinal_Rejected(t *testing.T) {
	ctx := context.Background()
	cs := openTestChannel(t)

	// A final terminal lands first.
	fin := newEnv("fin", message.KindResponse, message.Audience{"caller"},
		withSender(actor.KindAgent, "p"), withParent("req-1"), withType("agent.text"))
	if _, err := cs.Log.Append(ctx, fin, true, storespec.AppendMetadata{}); err != nil {
		t.Fatalf("Append final: %v", err)
	}

	// A provisional for the same parent must be rejected — and leave no row.
	prov := newEnv("zombie", message.KindResponse, message.Audience{"caller"},
		withSender(actor.KindAgent, "p"), withParent("req-1"), withType("agent.text"))
	_, err := cs.Log.Append(ctx, prov, false, storespec.AppendMetadata{})
	var appErr *storespec.AppendError
	if !errors.As(err, &appErr) {
		t.Fatalf("Append provisional-after-final err=%v, want *AppendError", err)
	}
	if appErr.Reason != "harness_provisional_after_final" {
		t.Fatalf("reject reason=%q want harness_provisional_after_final", appErr.Reason)
	}
	// The zombie must NOT have landed (tx rolled back).
	if row, ok, err := cs.Log.FindByID(ctx, "zombie"); err != nil || ok || row != nil {
		t.Fatalf("zombie provisional landed: ok=%v row=%v err=%v", ok, row, err)
	}
}

// A provisional response with NO final yet is appended normally — the in-tx
// guard only fires once a final exists.
func TestAppend_ProvisionalBeforeFinal_Allowed(t *testing.T) {
	ctx := context.Background()
	cs := openTestChannel(t)

	prov := newEnv("prov", message.KindResponse, message.Audience{"caller"},
		withSender(actor.KindAgent, "p"), withParent("req-1"), withType("agent.text"))
	if _, err := cs.Log.Append(ctx, prov, false, storespec.AppendMetadata{}); err != nil {
		t.Fatalf("Append provisional-before-final: %v", err)
	}
	if _, ok, err := cs.Log.FindByID(ctx, "prov"); err != nil || !ok {
		t.Fatalf("provisional did not land: ok=%v err=%v", ok, err)
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
	var lastSeq int64
	for _, id := range []string{"a", "b", "c"} {
		res, err := cs.Log.Append(ctx, newEnv(id, message.KindEvent, message.Audience{"x"}), false, storespec.AppendMetadata{})
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

	var seqs []int64
	for _, id := range []string{"a", "b", "c", "d"} {
		res, err := cs.Log.Append(ctx, newEnv(id, message.KindEvent, message.Audience{"x"}), false, storespec.AppendMetadata{})
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
		if _, err := cs.Log.Append(ctx, newEnv(id, message.KindEvent, message.Audience{"x"}), false, storespec.AppendMetadata{}); err != nil {
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
		if _, err := cs.Log.Append(ctx, env, false, storespec.AppendMetadata{}); err != nil {
			t.Fatalf("Append request %s: %v", id, err)
		}
	}
	mkTerminalResponse := func(id, parent string) {
		t.Helper()
		env := newEnv(id, message.KindResponse, message.Audience{"planner"},
			withSender(actor.KindTool, "tool:xhs"), withParent(message.ID(parent)), withType("agent.text"))
		if _, err := cs.Log.Append(ctx, env, true, storespec.AppendMetadata{}); err != nil {
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
	if _, err := cs.Log.Append(ctx, req, false, storespec.AppendMetadata{}); err != nil {
		t.Fatalf("Append request: %v", err)
	}
	prov := newEnv("r1-prov", message.KindResponse, message.Audience{"planner"},
		withSender(actor.KindTool, "tool:xhs"), withParent("r1"), withType("agent.text"))
	if _, err := cs.Log.Append(ctx, prov, false, storespec.AppendMetadata{}); err != nil {
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

// DistinctOpenRequestReceivers is the closure reconciler's truth-derived scan:
// the DISTINCT first-audience receivers that still hold an open request. Same
// open-set predicate as OpenRequestsForActor (open = kind=request, no terminal
// response), grouped to receiver and de-duplicated.
func TestDistinctOpenRequestReceivers(t *testing.T) {
	ctx := context.Background()
	cs := openTestChannel(t)

	mkReq := func(id string, audience message.Audience) {
		t.Helper()
		env := newEnv(id, message.KindRequest, audience,
			withSender(actor.KindAgent, "planner"), withType("xhs.publish"))
		if _, err := cs.Log.Append(ctx, env, false, storespec.AppendMetadata{}); err != nil {
			t.Fatalf("Append request %s: %v", id, err)
		}
	}
	mkTerminalResponse := func(id, parent string, audience message.Audience) {
		t.Helper()
		env := newEnv(id, message.KindResponse, audience,
			withSender(actor.KindTool, "tool:xhs"), withParent(message.ID(parent)), withType("agent.text"))
		if _, err := cs.Log.Append(ctx, env, true, storespec.AppendMetadata{}); err != nil {
			t.Fatalf("Append response %s: %v", id, err)
		}
	}

	// alpha: two open requests → counted ONCE (distinct).
	mkReq("a1", message.Audience{"alpha"})
	mkReq("a2", message.Audience{"alpha"})
	// beta: one open request.
	mkReq("b1", message.Audience{"beta"})
	// gamma: only a CLOSED request (terminal response) → NOT a receiver of an
	// open request, excluded.
	mkReq("g1", message.Audience{"gamma"})
	mkTerminalResponse("g1-final", "g1", message.Audience{"planner"})

	recvs, err := cs.Query.DistinctOpenRequestReceivers(ctx)
	if err != nil {
		t.Fatalf("DistinctOpenRequestReceivers: %v", err)
	}
	got := map[actor.ActorID]bool{}
	for _, id := range recvs {
		got[id] = true
	}
	if len(got) != 2 || !got["alpha"] || !got["beta"] {
		t.Fatalf("distinct receivers=%v, want exactly {alpha, beta}", recvs)
	}
	if got["gamma"] {
		t.Fatalf("gamma has only a CLOSED request and must be excluded; got %v", recvs)
	}
}

// ExpiredOpenRequests (期12 S3): only declared-deadline-passed, still-open
// requests ride back, ordered (expires_at, seq); keyset cursor pages without
// re-reading; an answered request never appears (anti-join, not the request
// row's own is_terminal flag).
func TestExpiredOpenRequests(t *testing.T) {
	ctx := context.Background()
	cs := openTestChannel(t)

	mkReq := func(id string, expiresAt int64) {
		t.Helper()
		env := newEnv(id, message.KindRequest, message.Audience{"tool:xhs"},
			withSender(actor.KindAgent, "planner"), withType("xhs.publish"), withExpiresAt(expiresAt))
		if _, err := cs.Log.Append(ctx, env, false, storespec.AppendMetadata{}); err != nil {
			t.Fatalf("Append %s: %v", id, err)
		}
	}

	mkReq("e1", 100)
	mkReq("e2", 200)
	mkReq("e3", 300)  // answered below → never expired-open
	mkReq("e4", 9000) // future deadline → not expired at now=1000
	resp := newEnv("e3-final", message.KindResponse, message.Audience{"planner"},
		withSender(actor.KindTool, "tool:xhs"), withParent("e3"), withType("agent.text"))
	if _, err := cs.Log.Append(ctx, resp, true, storespec.AppendMetadata{}); err != nil {
		t.Fatalf("Append e3-final: %v", err)
	}

	// Page 1: limit 1 → e1 only, cursor advances.
	rows, cur, err := cs.Expiry.ExpiredOpenRequests(ctx, 1000, storespec.ExpiryCursor{}, 1)
	if err != nil || len(rows) != 1 || rows[0].Err != nil || rows[0].Row.Envelope.ID != "e1" {
		t.Fatalf("page1 = (%v, %+v, %v), want [e1]", rows, cur, err)
	}
	// Page 2 from cursor: e2; the exhausted scan reports the ZERO cursor
	// (the contract's own end-of-scan word — the caller never re-derives it
	// from batch length).
	rows, cur2, err := cs.Expiry.ExpiredOpenRequests(ctx, 1000, cur, 10)
	if err != nil || len(rows) != 1 || rows[0].Row.Envelope.ID != "e2" {
		t.Fatalf("page2 = (%v, %v), want [e2]", rows, err)
	}
	if cur2 != (storespec.ExpiryCursor{}) {
		t.Fatalf("exhausted scan cursor = %+v, want zero", cur2)
	}
	// Zero cursor, wide window: e1+e2 only (e3 answered, e4 not yet due).
	rows, _, err = cs.Expiry.ExpiredOpenRequests(ctx, 1000, storespec.ExpiryCursor{}, 10)
	if err != nil || len(rows) != 2 {
		t.Fatalf("full scan = %d rows (%v), want 2", len(rows), err)
	}
}

// TestExpiredOpenRequests_ManyRowsPaginate (DoD-14 half): a backlog well past
// one batch pages fairly across sweeps — every row is visited exactly once
// per full scan and the final page reports the zero cursor.
func TestExpiredOpenRequests_ManyRowsPaginate(t *testing.T) {
	ctx := context.Background()
	cs := openTestChannel(t)
	const n = 300
	for i := 0; i < n; i++ {
		env := newEnv(fmt.Sprintf("m%03d", i), message.KindRequest, message.Audience{"tool:xhs"},
			withSender(actor.KindAgent, "planner"), withType("xhs.publish"), withExpiresAt(int64(100+i)))
		if _, err := cs.Log.Append(ctx, env, false, storespec.AppendMetadata{}); err != nil {
			t.Fatalf("Append %d: %v", i, err)
		}
	}
	seen := map[message.ID]int{}
	cur := storespec.ExpiryCursor{}
	pages := 0
	for {
		rows, next, err := cs.Expiry.ExpiredOpenRequests(ctx, 10_000, cur, 128)
		if err != nil {
			t.Fatalf("page %d: %v", pages, err)
		}
		for _, r := range rows {
			if r.Err != nil {
				t.Fatalf("unexpected poison: %v", r.Err)
			}
			seen[r.Row.Envelope.ID]++
		}
		pages++
		if next == (storespec.ExpiryCursor{}) {
			break
		}
		cur = next
		if pages > 10 {
			t.Fatal("cursor never reached the end")
		}
	}
	if len(seen) != n {
		t.Fatalf("visited %d distinct rows, want %d", len(seen), n)
	}
	for id, c := range seen {
		if c != 1 {
			t.Fatalf("row %s visited %d times in one full scan, want 1", id, c)
		}
	}
	if pages != 3 { // 128+128+44
		t.Fatalf("pages = %d, want 3", pages)
	}
}

// The reaper evaluates the SLIDING deadline from truth: a provisional
// response restarts the window (expires_at − ts) from its own ts, so a request
// past its declared expires_at is not due until that span of silence has
// elapsed since its latest progress.
func TestExpiredOpenRequests_ProgressSlidesDeadline(t *testing.T) {
	ctx := context.Background()
	cs := openTestChannel(t)
	// ts=1000, expires_at=1500 → span 500.
	req := newEnv("s1", message.KindRequest, message.Audience{"tool:xhs"},
		withSender(actor.KindAgent, "planner"), withType("xhs.publish"), withExpiresAt(1500))
	if _, err := cs.Log.Append(ctx, req, false, storespec.AppendMetadata{}); err != nil {
		t.Fatalf("Append s1: %v", err)
	}
	progress := newEnv("s1-p1", message.KindResponse, message.Audience{"planner"},
		withSender(actor.KindTool, "tool:xhs"), withParent("s1"), withType("xhs.publish"),
		func(e *message.Envelope) { e.TS = 1400; e.TSReceived = 1400 })
	if _, err := cs.Log.Append(ctx, progress, false, storespec.AppendMetadata{}); err != nil {
		t.Fatalf("Append s1-p1: %v", err)
	}
	// Declared expires_at (1500) has passed at 1800, but the window restarted
	// at 1400 → due at 1900.
	rows, _, err := cs.Expiry.ExpiredOpenRequests(ctx, 1800, storespec.ExpiryCursor{}, 10)
	if err != nil || len(rows) != 0 {
		t.Fatalf("at 1800: rows=%d err=%v, want none (progress at 1400 slid the deadline to 1900)", len(rows), err)
	}
	rows, _, err = cs.Expiry.ExpiredOpenRequests(ctx, 1900, storespec.ExpiryCursor{}, 10)
	if err != nil || len(rows) != 1 || rows[0].Row.Envelope.ID != "s1" {
		t.Fatalf("at 1900: rows=%v err=%v, want [s1]", rows, err)
	}
}
