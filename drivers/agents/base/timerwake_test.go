package base

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/drivers/agents/base/internal/book"
	"github.com/wanpengxie/atoll/drivers/agents/runtimeproto"
	"github.com/wanpengxie/atoll/lib/actorbase"
	"github.com/wanpengxie/atoll/lib/behavior"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/runtime/harness"
)

// postingSys captures the self-commission the loop writes when an alarm rings.
type postingSys struct {
	*v7Sys
	posts []behavior.RequestSpec
}

func (p *postingSys) Post(spec behavior.RequestSpec) (message.ID, error) {
	p.posts = append(p.posts, spec)
	return "posted", nil
}

func newWakeLoop(t *testing.T) (*agentLoop, *postingSys) {
	t.Helper()
	l, sys, _ := newV7Loop(t, nil)
	posting := &postingSys{v7Sys: sys}
	l.sys = posting
	l.exec.sys = posting
	return l, posting
}

func fireEvent(id message.ID, typ string, sender actor.ActorID, payload string) actorbase.Msg {
	return actorbase.NewMsg(actorbase.OriginMailbox, context.Background(), message.Envelope{
		ID: id, Kind: message.KindEvent, Type: typ, TS: 1_700_000,
		Sender: message.Sender{Kind: actor.KindAgent, ID: sender}, Payload: json.RawMessage(payload),
	})
}

// An alarm is not a wake-up, it is a commission the agent gave itself: the fire
// event becomes a self-addressed request so the turn it starts has an owner and
// can therefore write progress and a terminal at all.
func TestTimerFireBecomesASelfCommission(t *testing.T) {
	l, sys := newWakeLoop(t)
	now := time.UnixMilli(5_000_000)
	l.nowFn = func() time.Time { return now }

	l.handleIntake(fireEvent("timer:abc123", "standup", "agent:test:1", `{"note":"hi"}`))

	if len(sys.posts) != 1 {
		t.Fatalf("posts=%d, want exactly one self-commission", len(sys.posts))
	}
	spec := sys.posts[0]
	if spec.Type != TypeTimerWake {
		t.Fatalf("type=%q, want %q", spec.Type, TypeTimerWake)
	}
	if len(spec.Audience) != 1 || spec.Audience[0] != "agent:test:1" {
		t.Fatalf("audience=%v, want the agent itself", spec.Audience)
	}
	// Post takes on no caller obligation, so an absent deadline would inherit
	// the substrate's 24h TTL and leave a crashed alarm turn open for a day.
	if spec.ExpiresAt == nil {
		t.Fatal("the self-commission must carry its own deadline")
	}
	if want := now.Add(timerWakeTTL).UnixMilli(); *spec.ExpiresAt != want {
		t.Fatalf("expires_at=%d, want %d", *spec.ExpiresAt, want)
	}

	var body timerWakePayload
	if err := json.Unmarshal(spec.Payload, &body); err != nil {
		t.Fatal(err)
	}
	if body.TimerID != "abc123" || body.MsgType != "standup" || body.FiredAt != 1_700_000 {
		t.Fatalf("body=%+v", body)
	}
	if string(body.Payload) != `{"note":"hi"}` {
		t.Fatalf("payload=%s, want the author's own bytes", body.Payload)
	}
	// The model must be able to tell "my own earlier intent came back" from
	// "somebody is talking to me"; the text is the only place that can say so.
	if body.Text == "" {
		t.Fatal("the commission must say where it came from")
	}
}

// Fires are routed by WHICH timer rang, not by what it is called: a caller can
// name any msg_type through system.timer.set, so dispatching by type would let
// an alarm named agent.hold_expired walk into the loop's private hold branch
// and be swallowed there instead of waking anyone.
func TestAnAlarmNamedLikeTheHoldTimerStillWakesTheAgent(t *testing.T) {
	l, sys := newWakeLoop(t)
	l.holdTimer = "hold-99"

	// The loop's own hold timer, recognised by its id.
	l.handleIntake(fireEvent("timer:hold-99", typeHoldExpired, "agent:test:1", `{"hold_id":"nobody"}`))
	if len(sys.posts) != 0 {
		t.Fatal("the loop's own hold timer must not become an alarm commission")
	}

	// A caller-armed alarm that merely borrows the name.
	l.handleIntake(fireEvent("timer:user-1", typeHoldExpired, "agent:test:1", `{}`))
	if len(sys.posts) != 1 {
		t.Fatalf("posts=%d, want the borrowed-name alarm to still wake its owner", len(sys.posts))
	}
}

// Only the schedule engine's own fire qualifies. The deterministic `timer:` id
// alone is not enough (any actor could address a look-alike at this agent), and
// self-authorship alone is not enough either (ordinary self-emitted events must
// keep being ignored).
func TestOnlyASelfAuthoredTimerFireCommissionsATurn(t *testing.T) {
	for _, tc := range []struct {
		name   string
		msg    actorbase.Msg
		commit bool
	}{
		{"self-authored fire", fireEvent("timer:x", "standup", "agent:test:1", `{}`), true},
		{"look-alike id from elsewhere", fireEvent("timer:x", "standup", "agent:other:1", `{}`), false},
		{"ordinary self event", fireEvent("evt-1", "standup", "agent:test:1", `{}`), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			l, sys := newWakeLoop(t)
			l.handleIntake(tc.msg)
			if got := len(sys.posts) > 0; got != tc.commit {
				t.Fatalf("commissioned=%v, want %v", got, tc.commit)
			}
		})
	}
}

// The commission is self-addressed BY CONSTRUCTION — that is what gives the
// alarm turn an owner — so the loop's "ignore what I sent myself" guard has to
// name the exception rather than swallow the one request an agent is meant to
// send itself. Every other self-sent request stays ignored.
func TestSelfAddressedRequestsAreIgnoredExceptTheCommission(t *testing.T) {
	selfRequest := func(id message.ID, typ, payload string) actorbase.Msg {
		return actorbase.NewMsg(actorbase.OriginMailbox, context.Background(), message.Envelope{
			ID: id, Kind: message.KindRequest, Type: typ,
			Sender:   message.Sender{Kind: actor.KindAgent, ID: "agent:test:1"},
			Audience: message.Audience{"agent:test:1"},
			Payload:  json.RawMessage(payload),
		})
	}

	t.Run("commission starts a turn", func(t *testing.T) {
		l, _ := newWakeLoop(t)
		l.handleIntake(selfRequest("w1", TypeTimerWake, `{"body":{"text":"alarm","timer_id":"a","msg_type":"standup","fired_at":1}}`))
		if l.state.Requests["w1"] == nil {
			t.Fatal("the commission was dropped instead of admitted")
		}
		if l.state.Turn == nil {
			t.Fatal("the commission did not start a turn")
		}
	})

	t.Run("any other self-sent request stays ignored", func(t *testing.T) {
		l, _ := newWakeLoop(t)
		l.handleIntake(selfRequest("w2", TypeAsk, `{"body":{"text":"hello"}}`))
		if l.state.Requests["w2"] != nil {
			t.Fatal("an ordinary self-sent request must stay ignored")
		}
	})

	// A commission is by definition from yourself. One arriving from somebody
	// else is not an alarm, and accepting it would let any caller fake "your
	// own earlier intent came back" — the one thing the wake text promises. It
	// is refused OUT LOUD: silently dropping it would leave the sender waiting
	// out its whole deadline for an answer that was never coming.
	t.Run("a commission from somebody else is refused out loud", func(t *testing.T) {
		l, sys := newWakeLoop(t)
		foreign := actorbase.NewMsg(actorbase.OriginMailbox, context.Background(), message.Envelope{
			ID: "w3", Kind: message.KindRequest, Type: TypeTimerWake,
			Sender:   message.Sender{Kind: actor.KindAgent, ID: "agent:other:1"},
			Audience: message.Audience{"agent:test:1"},
			Payload:  json.RawMessage(`{"body":{"text":"pretend"}}`),
		})
		l.handleIntake(foreign)
		if l.state.Requests["w3"] != nil {
			t.Fatal("a commission from another actor must be refused")
		}
		got := sys.terminal("w3")
		if len(got) != 1 || got[0].code != "type_unsupported" {
			t.Fatalf("terminal=%+v, want a type_unsupported refusal", got)
		}
	})
}

// Controlling a queued request does not require having sent it. Relayed work
// is the case that forces this: when an agent forwards somebody's errand, the
// queued row's sender is the relaying AGENT, so an owner-only rule left the
// person whose errand it is watching their own work sit in the queue with
// nothing they could do to it. A channel is one permission boundary, so every
// member that may write here may steer or replace what is waiting.
func TestQueuedWorkIsControllableByAnyMemberNotOnlyItsSender(t *testing.T) {
	t.Run("replace", func(t *testing.T) {
		l, sys, _ := newV7Loop(t, nil)
		l.state.Turn = &book.Turn{Phase: book.TurnActive, ID: "busy"}
		relayed := &book.Request{
			ID: "relayed", Sender: "agent:relay:1", Location: book.Buffered,
			Input: runtimeproto.Input{Text: "old"}, Bytes: 3,
			Scope: l.vault.Mint("relayed", "relayed"),
		}
		l.state.Requests[relayed.ID] = relayed
		l.state.Buffer, l.state.BufferBytes = []book.RequestID{relayed.ID}, relayed.Bytes

		l.handleIntake(v7Request("edit", TypeReplace, "human:root:1",
			`{"target":"relayed","old_text":"old","new_text":"new"}`))

		if got := sys.terminal("edit"); len(got) != 0 && got[0].fail {
			t.Fatalf("terminal=%v, want the replace accepted", got)
		}
		if l.state.Requests["relayed"] != nil {
			t.Fatal("the replaced row is still in the table")
		}
		row := l.state.Requests["edit"]
		if row == nil || row.Input.Text != "new" {
			t.Fatalf("replacement row=%+v, want it carrying the new text", row)
		}
		// The ledger still says who actually changed it: the replacement is
		// authored by whoever sent the replace, not by the original relay.
		if row.Sender != "human:root:1" {
			t.Fatalf("replacement sender=%q, want the actor that sent the replace", row.Sender)
		}
	})

	t.Run("steer", func(t *testing.T) {
		l, sys, _ := newV7Loop(t, map[string]bool{runtimeproto.CapabilitySteer: true})
		// Distinct callers so the two do not merge into one batch — the point
		// being asserted is the ORDER change, not what a batch happens to own.
		first := &book.Request{ID: "first", Sender: "human:root:1", Location: book.Buffered,
			Input: runtimeproto.Input{Caller: harness.Caller{Actor: "human:root:1"}}}
		relayed := &book.Request{ID: "relayed", Sender: "agent:relay:1", Location: book.Buffered,
			Input: runtimeproto.Input{Caller: harness.Caller{Actor: "agent:relay:1"}}}
		l.state.Requests[first.ID], l.state.Requests[relayed.ID] = first, relayed
		l.state.Buffer = []book.RequestID{first.ID, relayed.ID}

		l.handleIntake(v7Request("insert", TypeSteer, "human:root:1", `{"target":"relayed"}`))

		got := sys.terminal("insert")
		if len(got) != 1 || got[0].fail {
			t.Fatalf("terminal=%v, want the steer accepted", got)
		}
		// Nothing was running, so reaching the front means being started: the
		// relayed row jumps ahead of the one that was queued before it.
		if l.state.Turn == nil || l.state.Turn.Owner != "relayed" {
			t.Fatalf("turn=%+v, want the relayed row to have started", l.state.Turn)
		}
	})
}
