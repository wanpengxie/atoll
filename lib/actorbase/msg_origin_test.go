package actorbase

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/runtime/harness"
)

// msg_origin_test.go covers the ONE distinction the retired Identity-suffixed
// verbs used to carry in their names: which ledger authorises a write. It now
// rides on the Msg, so these tests are about Msg construction and about the
// gate the three response verbs run first.

// logMsg builds the from-log write handle the frame interpreter holds: a
// request recovered by id, with no delivery and no ledger entry behind it.
func logMsg(t *testing.T, env *message.Envelope) Msg {
	t.Helper()
	return NewMsg(OriginLog, context.Background(), *env)
}

// A Msg with the zero origin never comes into existence through NewMsg: the
// enum's zero value is illegal and construction is where that is enforced.
// Go cannot express "no zero value" in the type, and a required parameter only
// catches a FORGOTTEN argument, not a wrongly-passed one — so the check is a
// runtime one, in the same fail-loud register as the serve ledger's capacity
// assertion: this can only ever be a wiring bug.
func TestNewMsgRejectsTheZeroOrigin(t *testing.T) {
	t.Parallel()
	defer func() {
		if recover() == nil {
			t.Fatal("NewMsg accepted the zero origin; it must fail loud")
		}
	}()
	NewMsg(OriginUnset, context.Background(), message.Envelope{ID: "r-1"})
}

// A Msg that never went through NewMsg (a zero-value discard that escaped into
// a live path) is refused by every write verb rather than defaulted onto an
// arm. Defaulting to the mailbox would be the worst of the two: the serve
// ledger has never heard of the id, so isClosed says "not closed" and the
// write sails through the gate precisely BECAUSE nothing is known about it.
func TestZeroOriginMsgIsRefusedByEveryWriteVerb(t *testing.T) {
	t.Parallel()
	pen := &fakePen{self: "actor:test"}
	e := newTestEngine(t, pen, Hooks{}, 8, 8)
	e.lifeCtx = context.Background()

	var zero Msg
	zero.ID = "req-never-constructed"

	// The payloads are OBJECTS on purpose: a response payload that the builder
	// would refuse anyway (a bare string, say) would make this test pass even
	// with the gate deleted, and for a reason that has nothing to do with the
	// gate. Everything here must be write-legal except the origin.
	ok := map[string]string{"decision": "approve"}
	if _, err := e.Reply(zero, ok); !errors.Is(err, ErrMsgOriginUnset) {
		t.Fatalf("Reply(zero-origin) = %v, want ErrMsgOriginUnset", err)
	}
	if _, err := e.Fail(zero, "boom", "d"); !errors.Is(err, ErrMsgOriginUnset) {
		t.Fatalf("Fail(zero-origin) = %v, want ErrMsgOriginUnset", err)
	}
	if _, err := e.Progress(zero, message.StatusProcessing, ok); !errors.Is(err, ErrMsgOriginUnset) {
		t.Fatalf("Progress(zero-origin) = %v, want ErrMsgOriginUnset", err)
	}
	if pen.count() != 0 {
		t.Fatalf("a zero-origin Msg produced %d writes, want none", pen.count())
	}
}

// §3.2/§3.3: a log-origin Reply does NOT consult the serve ledger, so it works
// on an incarnation that holds no entry for the request at all. That is not
// only the cross-incarnation case — it is the ordinary OVERLOAD case: a
// request the ledger had no room to admit, or one the reject lane closed
// locally, is still OPEN in truth and must stay answerable.
func TestLogOriginReplyWritesWithNoLedgerEntry(t *testing.T) {
	t.Parallel()
	pen := &fakePen{self: "user:alice"}
	e := newTestEngine(t, pen, Hooks{}, 8, 8)
	e.lifeCtx = context.Background()

	req := newRequestEnv("r-from-log", -1)
	req.Sender.ID = "agent:worker" // the response must address the original sender

	if e.serve.len() != 0 {
		t.Fatalf("serve ledger len = %d, want 0 before the test", e.serve.len())
	}

	id, err := e.Reply(logMsg(t, req), map[string]string{"decision": "approve"})
	if err != nil {
		t.Fatalf("Reply(log origin) = %v, want nil", err)
	}
	if id == "" {
		t.Fatal("Reply returned an empty receipt id")
	}
	resp := pen.last()
	if resp == nil {
		t.Fatal("Reply wrote no envelope")
	}
	if resp.Kind != message.KindResponse {
		t.Fatalf("written kind = %q, want response", resp.Kind)
	}
	if resp.ParentID != req.ID {
		t.Fatalf("response parent_id = %q, want %q", resp.ParentID, req.ID)
	}
	// No entry existed, so closing is a no-op — the projection stays empty and
	// the log carried the authority the whole way.
	if e.serve.len() != 0 {
		t.Fatalf("serve ledger len after a log-origin reply = %d, want 0", e.serve.len())
	}
}

// A log holder cannot see that someone already answered (it does not query
// truth), so racing an existing terminal is expected, not exceptional. The
// harness's terminal-uniqueness index is the real arbiter and behavior.Respond
// folds its duplicate verdict into a success — the request IS closed, which is
// all this caller wanted.
func TestLogOriginReplyAbsorbsAnExistingTerminal(t *testing.T) {
	t.Parallel()
	pen := &fakePen{self: "user:alice", reject: harness.HarnessTerminalDuplicate}
	e := newTestEngine(t, pen, Hooks{}, 8, 8)
	e.lifeCtx = context.Background()

	req := newRequestEnv("r-already-answered", -1)
	req.Sender.ID = "agent:worker"

	if _, err := e.Reply(logMsg(t, req), map[string]string{"decision": "approve"}); err != nil {
		t.Fatalf("Reply against an already-terminal request = %v, want nil (absorbed)", err)
	}
}

// §3.2a-1: the close is unconditional, and this is a NORMAL-path requirement,
// not a cross-incarnation curiosity. A deferred approval reaches the human
// cell through the mailbox (so the local entry exists) and is answered later
// from the log. Skipping the close would pin serve capacity until the
// deadline, leave the original delivery's Ctx() live, and keep this
// incarnation believing a request is open that truth has already closed.
func TestTerminalWriteClosesTheLedgerEntryWhateverTheOrigin(t *testing.T) {
	t.Parallel()
	for _, verb := range terminalVerbs() {
		t.Run(verb.name, func(t *testing.T) {
			t.Parallel()
			pen := &fakePen{self: "user:alice"}
			e := newTestEngine(t, pen, Hooks{}, 8, 8)
			e.lifeCtx = context.Background()

			// The request arrived through the mailbox first: an entry exists and
			// a delivery handle was already handed to the occupant.
			env := newRequestEnv(message.ID("r-deferred-"+verb.name), -1)
			env.Sender.ID = "agent:worker"
			if !e.serve.admit(env) {
				t.Fatal("expected admit to succeed")
			}
			ctx, ok := e.serve.ctxFor(env.ID)
			if !ok {
				t.Fatal("expected ctxFor to resolve the admitted entry")
			}
			delivered := NewMsg(OriginMailbox, ctx, *env)
			if e.serve.len() != 1 {
				t.Fatalf("serve ledger len = %d, want 1", e.serve.len())
			}

			// The answer comes back later through the log, not the delivery.
			if _, err := verb.write(e, logMsg(t, env)); err != nil {
				t.Fatalf("%s(log origin) = %v, want nil", verb.name, err)
			}

			if e.serve.len() != 0 {
				t.Fatalf("serve ledger len after the terminal = %d, want 0", e.serve.len())
			}
			select {
			case <-delivered.Ctx().Done():
			default:
				t.Fatal("the original mailbox delivery's Ctx() was never cancelled")
			}
		})
	}
}

// terminalVerbs is the closing pair. Reply and Fail BOTH end a request, so every
// "a terminal write closes the account" assertion has to run against both — a
// table that exercised only Reply let a missing close in Fail sit green.
// Progress is deliberately absent: a provisional never closes.
func terminalVerbs() []struct {
	name  string
	write func(*engine, Msg) (message.ID, error)
} {
	return []struct {
		name  string
		write func(*engine, Msg) (message.ID, error)
	}{
		{"Reply", func(e *engine, m Msg) (message.ID, error) {
			return e.Reply(m, map[string]string{"decision": "approve"})
		}},
		{"Fail", func(e *engine, m Msg) (message.ID, error) {
			return e.Fail(m, "refused", "not today")
		}},
	}
}

// §3.2a-1 again, on the path that is easiest to lose: the write did not
// produce a new row — the harness absorbed it as a duplicate — but the request
// is closed all the same, so the local account must close too.
func TestTerminalWriteClosesTheLedgerEntryOnAnAbsorbedDuplicate(t *testing.T) {
	t.Parallel()
	for _, verb := range terminalVerbs() {
		t.Run(verb.name, func(t *testing.T) {
			t.Parallel()
			pen := &fakePen{self: "user:alice", reject: harness.HarnessTerminalDuplicate}
			e := newTestEngine(t, pen, Hooks{}, 8, 8)
			e.lifeCtx = context.Background()

			env := newRequestEnv(message.ID("r-dup-close-"+verb.name), -1)
			env.Sender.ID = "agent:worker"
			if !e.serve.admit(env) {
				t.Fatal("expected admit to succeed")
			}

			if _, err := verb.write(e, logMsg(t, env)); err != nil {
				t.Fatalf("%s against a duplicate = %v, want nil", verb.name, err)
			}
			if e.serve.len() != 0 {
				t.Fatalf("serve ledger len after an absorbed duplicate = %d, want 0", e.serve.len())
			}
		})
	}
}

// Reply marshals its value ONCE, and that is the whole contract a caller has
// to hold up its end of. A caller that has already marshalled its result must
// hand back either the original value or a json.RawMessage — never a plain
// []byte, which json.Marshal encodes as a base64 STRING, silently replacing
// the answer with gibberish (and losing every field the reader expects).
//
// The two legal forms are asserted to produce the SAME bytes, because that
// equality is what makes "pick either" true advice.
func TestReplyMarshalsThePayloadExactlyOnce(t *testing.T) {
	t.Parallel()

	answer := map[string]any{"decision": "approve", "note": "looks fine"}
	raw, err := json.Marshal(answer)
	if err != nil {
		t.Fatalf("fixture marshal: %v", err)
	}

	write := func(t *testing.T, v any) []byte {
		t.Helper()
		pen := &fakePen{self: "user:alice"}
		e := newTestEngine(t, pen, Hooks{}, 8, 8)
		e.lifeCtx = context.Background()
		env := newRequestEnv("r-resolve", -1)
		env.Sender.ID = "agent:worker"
		if _, err := e.Reply(logMsg(t, env), v); err != nil {
			t.Fatalf("Reply = %v", err)
		}
		return pen.last().Payload
	}

	fromValue := write(t, answer)
	fromRawMessage := write(t, json.RawMessage(raw))

	var got map[string]any
	if err := json.Unmarshal(fromValue, &got); err != nil {
		t.Fatalf("the written payload is not a JSON object (base64?): %v — %s", err, fromValue)
	}
	if got["decision"] != "approve" {
		t.Fatalf("payload = %s, want the answer's own fields", fromValue)
	}
	if string(fromValue) != string(fromRawMessage) {
		t.Fatalf("value form wrote %s but RawMessage form wrote %s; the two legal forms must agree",
			fromValue, fromRawMessage)
	}
}

// §3.2a-2: a provisional never closes anything. The asymmetry with Reply/Fail
// IS Progress's meaning, and it is the one thing likeliest to be flattened
// when all three gates are rewritten together.
func TestProgressNeverClosesTheLedgerEntry(t *testing.T) {
	t.Parallel()
	pen := &fakePen{self: "actor:test"}
	e := newTestEngine(t, pen, Hooks{}, 8, 8)
	e.lifeCtx = context.Background()

	env := newRequestEnv("r-progress", -1)
	if !e.serve.admit(env) {
		t.Fatal("expected admit to succeed")
	}
	ctx, _ := e.serve.ctxFor(env.ID)

	if _, err := e.Progress(NewMsg(OriginMailbox, ctx, *env), message.StatusProcessing, map[string]string{"step": "1"}); err != nil {
		t.Fatalf("Progress = %v, want nil", err)
	}
	if e.serve.len() != 1 {
		t.Fatalf("serve ledger len after Progress = %d, want 1 (a provisional closes nothing)", e.serve.len())
	}
	if pen.count() != 1 {
		t.Fatalf("pen writes = %d, want 1", pen.count())
	}
}

// §3.1a-4: the log handle is terminal-only. Refusal happens at the entry —
// nothing written, nothing closed — because a log holder calling Progress is
// not caught in a race it could not have seen (behavior.Progress's tolerance
// is for exactly that race); it is holding the wrong kind of handle, and
// answering with a fake success would hide the mistake.
func TestProgressRefusesALogOriginHandle(t *testing.T) {
	t.Parallel()
	pen := &fakePen{self: "user:alice"}
	e := newTestEngine(t, pen, Hooks{}, 8, 8)
	e.lifeCtx = context.Background()

	env := newRequestEnv("r-no-progress", -1)
	if !e.serve.admit(env) {
		t.Fatal("expected admit to succeed")
	}

	_, err := e.Progress(logMsg(t, env), message.StatusProcessing, map[string]string{"step": "1"})
	if !errors.Is(err, ErrLogOriginTerminalOnly) {
		t.Fatalf("Progress(log origin) = %v, want ErrLogOriginTerminalOnly", err)
	}
	if pen.count() != 0 {
		t.Fatalf("a refused Progress wrote %d envelopes, want none", pen.count())
	}
	if e.serve.len() != 1 {
		t.Fatalf("a refused Progress closed the account (len=%d, want 1)", e.serve.len())
	}
}

// §3.1a-2/§9-15: the delivery path only ever mints mailbox-origin handles.
// This is the structural half of "a log-origin Msg never enters a mailbox" —
// the other half (nobody else constructs one) is an archtest's job, since it
// is a statement about call sites, not about behaviour.
func TestRecvOnlyEverProducesMailboxOrigin(t *testing.T) {
	t.Parallel()
	e := newTestEngine(t, &fakePen{self: "actor:test"}, Hooks{}, 8, 8)
	e.lifeCtx = context.Background()
	e.occupant.Store(int32(occupantRunning))

	if err := e.Receive(context.Background(), newRequestEnv("r-delivered", -1)); err != nil {
		t.Fatalf("Receive: %v", err)
	}
	msg, err := e.Recv()
	if err != nil {
		t.Fatalf("Recv: %v", err)
	}
	if msg.origin != OriginMailbox {
		t.Fatalf("delivered request origin = %d, want OriginMailbox", msg.origin)
	}

	// The same holds for the no-closure-obligation kinds, which take the other
	// arm of projectWork.
	if err := e.Receive(context.Background(), &message.Envelope{ID: "e-1", Kind: message.KindEvent, Type: "tick"}); err != nil {
		t.Fatalf("Receive(event): %v", err)
	}
	msg, err = e.Recv()
	if err != nil {
		t.Fatalf("Recv(event): %v", err)
	}
	if msg.origin != OriginMailbox {
		t.Fatalf("delivered event origin = %d, want OriginMailbox", msg.origin)
	}
}

// §3.4: the terminal reason is DERIVED from who is writing, because that is
// what the harness authorises on. A receiver holds only its own exit reason; a
// caller closing the account it opened holds the deadline fact. The two arms
// also differ in payload shape, and deliberately so: cancelled:true is the
// structured bit that tells a deliberate close from a deadline that merely
// passed — the same bit callLedger.cancel stamps for the in-process twin of
// this act, and the one asserted end-to-end at the WS frame layer.
func TestFailDerivesTheTerminalReasonFromWhoIsWriting(t *testing.T) {
	t.Parallel()

	// status and reason are merged INTO the response payload by the response
	// builder — that is where the harness's authorization step reads them from.
	type failurePayload struct {
		Status    string `json:"status"`
		Reason    string `json:"reason"`
		ErrorCode string `json:"error_code"`
		Detail    string `json:"detail"`
		Cancelled bool   `json:"cancelled"`
	}

	t.Run("answering someone else's request is a receiver failure", func(t *testing.T) {
		pen := &fakePen{self: "agent:worker"}
		e := newTestEngine(t, pen, Hooks{}, 8, 8)
		e.lifeCtx = context.Background()
		e.actorCtx = &fakeActorContext{self: "agent:worker"}

		env := newRequestEnv("r-receiver-fail", -1)
		env.Sender.ID = "user:alice" // someone else asked
		if !e.serve.admit(env) {
			t.Fatal("expected admit to succeed")
		}
		ctx, _ := e.serve.ctxFor(env.ID)

		if _, err := e.Fail(NewMsg(OriginMailbox, ctx, *env), "tool_unavailable", "the door is shut"); err != nil {
			t.Fatalf("Fail = %v", err)
		}
		var got failurePayload
		if err := json.Unmarshal(pen.last().Payload, &got); err != nil {
			t.Fatalf("payload unmarshal: %v", err)
		}
		if got.Status != string(message.StatusFailed) {
			t.Fatalf("status = %q, want failed", got.Status)
		}
		if got.Reason != string(message.TerminalReceiverInternalError) {
			t.Fatalf("reason = %q, want receiver_internal_error", got.Reason)
		}
		if got.ErrorCode != "tool_unavailable" || got.Detail != "the door is shut" {
			t.Fatalf("payload = %+v, want the code/detail as given", got)
		}
		// ABSENT, not false. Decoding into a bool cannot tell the two apart, and
		// they are not the same statement on the wire: a reader that checks for
		// the key would see `cancelled:false` as "this close was examined and
		// judged not a cancellation", when the truth is that cancellation is not
		// a concept on this arm at all. behavior.Fail's payload is the
		// {error_code, detail} shape and must stay exactly that.
		var keys map[string]json.RawMessage
		if err := json.Unmarshal(pen.last().Payload, &keys); err != nil {
			t.Fatalf("payload key unmarshal: %v", err)
		}
		if _, present := keys["cancelled"]; present {
			t.Fatalf("a receiver failure must not carry the cancelled key at all, got %s", pen.last().Payload)
		}
	})

	t.Run("failing a request I sent is closing my own account", func(t *testing.T) {
		pen := &fakePen{self: "user:alice"}
		e := newTestEngine(t, pen, Hooks{}, 8, 8)
		e.lifeCtx = context.Background()
		e.actorCtx = &fakeActorContext{self: "user:alice"}

		env := newRequestEnv("r-self-close", -1)
		env.Sender.ID = "user:alice" // I asked; I am now taking it back

		if _, err := e.Fail(logMsg(t, env),
			string(message.TerminalUnansweredTimeout), "cancelled by caller"); err != nil {
			t.Fatalf("Fail = %v", err)
		}
		var got failurePayload
		if err := json.Unmarshal(pen.last().Payload, &got); err != nil {
			t.Fatalf("payload unmarshal: %v", err)
		}
		if got.Status != string(message.StatusFailed) {
			t.Fatalf("status = %q, want failed", got.Status)
		}
		if got.Reason != string(message.TerminalUnansweredTimeout) {
			t.Fatalf("reason = %q, want unanswered_timeout", got.Reason)
		}
		if !got.Cancelled {
			t.Fatalf("payload = %+v, want cancelled:true (the WS frame layer asserts this)", got)
		}
		if got.ErrorCode != string(message.TerminalUnansweredTimeout) || got.Detail != "cancelled by caller" {
			t.Fatalf("payload = %+v, want the code/detail as given alongside cancelled", got)
		}
	})
}
