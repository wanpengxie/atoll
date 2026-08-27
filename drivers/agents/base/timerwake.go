package base

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/wanpengxie/atoll/lib/actorbase"
	"github.com/wanpengxie/atoll/lib/behavior"
	"github.com/wanpengxie/atoll/protocol/message"
)

// An alarm reaching an agent has to become a TURN, and a turn needs an owner:
// progress and terminal are both responses, and a response is built from the
// request it answers (its audience IS that request's sender). With no request
// there is literally nothing to write — an ownerless turn could only Emit, so
// the ledger would show the agent suddenly speaking with no trace of why it
// woke or what it did. Worse, the loop treats a turn whose owner is gone as an
// orphan and interrupts it (onTurnStarted → scheduleCleanup → runCleanup).
//
// So the alarm does not "wake" the agent; it hands the agent A COMMISSION FROM
// ITSELF. On the fire event the loop Posts a self-addressed request, which then
// travels the ordinary intake → buffer → turn path with nothing special about
// it: queueing, cancellation, the queued heartbeat, closure and the front end's
// turn rendering all apply unchanged.
//
// Post, not Call: Call(self) is refused before the write (ErrSelfCall — one
// worker cannot wait on itself), and more importantly a Call would REGISTER an
// out-station entry, i.e. someone waiting for the answer. Nobody should wait:
// the terminal is the published copy of what the model already said, and
// feeding it back would put the same sentence in front of the model twice, once
// as its own words and once as an incoming message. Post leaves no waiter, so
// engine.Receive finds no match and drops the terminal.
//
// That cuts the REAL-TIME loop, and only that: agent.timer.wake is not a
// housekeeping word, so a restart's catchup still reads the commission and its
// terminal back out of the ledger as ordinary history. That is history, not an
// echo — but the honest claim is "no real-time feedback loop", never "it can
// never reach the model again".
const (
	// TypeTimerWake is the self-commission an alarm turns into.
	TypeTimerWake = "agent.timer.wake"
	// timerFireIDPrefix marks a message minted by the schedule engine's fire
	// path (the deterministic `timer:<id>` message id).
	timerFireIDPrefix = "timer:"
	// timerWakeTTL bounds the self-commission. Post takes on NO caller
	// obligation — closure belongs to the substrate's expiry reaper — so an
	// absent deadline would inherit the harness's 24h global TTL and leave a
	// crashed alarm turn open on the timeline for a day. It is a SLIDING
	// window: any progress the turn writes restarts it.
	timerWakeTTL = time.Hour
)

// isTimerFire recognises the schedule engine's fire: an event this actor
// authored, carrying the deterministic fire-message id. Both halves matter —
// the prefix alone would let any actor mint a look-alike id, and self-authorship
// alone would catch ordinary self-emitted events.
func (l *agentLoop) isTimerFire(msg actorbase.Msg) bool {
	return msg.Kind == message.KindEvent &&
		msg.Sender.ID == l.sys.Self() &&
		strings.HasPrefix(string(msg.ID), timerFireIDPrefix)
}

// isOwnHoldFire recognises the loop's PRIVATE hold-expiry timer by the id it
// armed, not by the type it chose. Type is caller-nameable through
// system.timer.set; a timer id is not.
func (l *agentLoop) isOwnHoldFire(msg actorbase.Msg) bool {
	return l.holdTimer != "" && l.isTimerFire(msg) &&
		string(msg.ID) == timerFireIDPrefix+string(l.holdTimer)
}

type timerWakePayload struct {
	Text    string          `json:"text"`
	TimerID string          `json:"timer_id"`
	MsgType string          `json:"msg_type"`
	FiredAt int64           `json:"fired_at"`
	Payload json.RawMessage `json:"payload,omitempty"`
}

// postTimerWake converts one fire event into the self-commission described
// above. The text says where the turn came from in plain words: an alarm the
// model reads as an anonymous request would leave it guessing who is talking to
// it, and the one thing it must know is that this is its own earlier intent
// coming back.
func (l *agentLoop) postTimerWake(msg actorbase.Msg) {
	timerID := strings.TrimPrefix(string(msg.ID), timerFireIDPrefix)
	body := timerWakePayload{
		TimerID: timerID,
		MsgType: msg.Type,
		FiredAt: msg.TS,
		Text: fmt.Sprintf("你先前设置的闹钟到点了(timer_id=%s,msg_type=%s)。这是你自己的意图回到你面前,不是别人在跟你说话。",
			timerID, msg.Type),
	}
	if len(msg.Payload) > 0 && string(msg.Payload) != "{}" {
		body.Payload = append(json.RawMessage(nil), msg.Payload...)
	}
	raw, err := json.Marshal(body)
	if err != nil {
		l.logger.Error("agent timer wake payload", "timer", timerID, "error", err)
		return
	}
	expires := l.now().Add(timerWakeTTL).UnixMilli()
	if _, err := l.sys.Post(behavior.RequestSpec{
		Cause:     msg.Cause(),
		Type:      TypeTimerWake,
		Payload:   raw,
		Audience:  message.Audience{l.sys.Self()},
		ExpiresAt: &expires,
	}); err != nil {
		// Nothing here can be retried usefully: the fire is already truth, and
		// a second Post would arm a second turn for the same alarm. Say it
		// loudly and let the alarm be missed rather than doubled.
		l.logger.Error("agent timer wake post", "timer", timerID, "type", msg.Type, "error", err)
	}
}
