package e2e

import (
	"testing"
	"time"
)

// The time axis was always a capability every actor held and no caller could
// reach. This walks the way it became a WORD, against a real server: the door
// arms an alarm for the AUTHENTICATED sender (nothing in the payload says whose
// it is), the alarm is listable while pending — including by another member —
// and the schedule engine fires it as a message that actor authored, after
// which it is gone from the pending set.
//
// The wake half (an agent turning its own fire into a self-addressed
// commission so the alarm becomes an ordinary turn) is unit-tested in
// drivers/agents/base: it needs an agent that can be told to send an arbitrary
// word, and no scripted fixture here can do that — agent.ask is strictly
// decoded to {text, attachments}, deliberately.
func TestTimerWordsArmFireAndVanishFromThePendingSet(t *testing.T) {
	h := newHarness(t)
	api, ws := rootClient(t, h, map[string]int64{c0ChannelID: 0})

	const alarmType = "e2e.standup"
	armed := ws.request(c0ChannelID, "system.timer.set", systemActor, map[string]any{
		"duration_ms": 2000,
		"msg_type":    alarmType,
		"payload":     map[string]any{"note": "stand up"},
	})
	timerID := stringField(t, armed, "timer_id")
	// The subject came off the harness-welded sender, not the payload — which
	// is the whole reason these verbs can live on the system actor at all.
	owner := stringField(t, armed, "subject")
	if owner == "" || owner == systemActor {
		t.Fatalf("subject=%q, want the requesting member's own id", owner)
	}

	// Pending alarms are listable, and a list read carries coordinates only:
	// it answers WHICH alarms exist, never what they will say.
	listed := ws.request(c0ChannelID, "system.timer.list", systemActor, map[string]any{})
	var found map[string]any
	for _, raw := range asSlice(listed["timers"]) {
		if row, _ := raw.(map[string]any); row["timer_id"] == timerID {
			found = row
		}
	}
	if found == nil {
		t.Fatalf("list=%v, want the armed alarm", listed)
	}
	if found["msg_type"] != alarmType {
		t.Fatalf("listed row=%v", found)
	}
	if _, leaked := found["payload"]; leaked {
		t.Fatal("a list read must not carry the alarm's payload")
	}

	// Naming another member on a READ is served — a channel is one permission
	// boundary, so a pending intent is not a secret inside it. (The refusal of
	// the same subject on a WRITE is the test below.)
	if _, _, err := ws.tryRequest(c0ChannelID, "system.timer.list", systemActor, map[string]any{
		"subject": owner,
	}); err != nil {
		t.Fatalf("reading a named member's alarms was refused: %v", err)
	}

	// The fire is a message the OWNER authored: the engine welded that
	// identity, not the scheduler's, so the alarm speaks as the actor that set
	// it and lands in that actor's own mailbox.
	audit := dialWS(t, api.base, api.cookieHeader(), map[string]int64{c0ChannelID: 0})
	deadline := time.Now().Add(45 * time.Second)
	fired := false
	for time.Now().Before(deadline) && !fired {
		select {
		case item := <-audit.feed:
			envelope, _ := item["envelope"].(map[string]any)
			if envelope == nil || envelope["kind"] != "event" || envelope["type"] != alarmType {
				continue
			}
			sender, _ := envelope["sender"].(map[string]any)
			if sender["id"] != owner {
				t.Fatalf("the fire was authored by %v, want the alarm's owner %q", sender["id"], owner)
			}
			audience, _ := envelope["audience"].([]any)
			if len(audience) != 1 || audience[0] != owner {
				t.Fatalf("fire audience=%v, want the owner alone", audience)
			}
			body, _ := envelope["payload"].(map[string]any)
			if body["note"] != "stand up" {
				t.Fatalf("fire payload=%v, want the author's own bytes", body)
			}
			fired = true
		case <-time.After(2 * time.Second):
		}
	}
	if !fired {
		t.Fatal("the alarm never fired as a message its owner authored")
	}

	// A fired alarm is gone: the list answers what is still owed, not what was
	// ever asked for.
	after := ws.request(c0ChannelID, "system.timer.list", systemActor, map[string]any{})
	for _, raw := range asSlice(after["timers"]) {
		if row, _ := raw.(map[string]any); row["timer_id"] == timerID {
			t.Fatalf("the fired alarm is still listed: %v", row)
		}
	}
}

// Cancelling reports whether the alarm was still pending, and reports it the
// same way for "already gone", "never existed" and "not yours" — so no caller
// can probe for another member's alarms by watching the answer change.
func TestTimerCancelStopsAnAlarmAndDoesNotLeak(t *testing.T) {
	h := newHarness(t)
	_, ws := rootClient(t, h, map[string]int64{c0ChannelID: 0})

	armed := ws.request(c0ChannelID, "system.timer.set", systemActor, map[string]any{
		"duration_ms": 3_600_000, "msg_type": "e2e.later",
	})
	id := stringField(t, armed, "timer_id")

	cancelled := ws.request(c0ChannelID, "system.timer.cancel", systemActor, map[string]any{"timer_id": id})
	if cancelled["existed"] != true {
		t.Fatalf("cancel=%v, want existed", cancelled)
	}
	again := ws.request(c0ChannelID, "system.timer.cancel", systemActor, map[string]any{"timer_id": id})
	if again["existed"] != false {
		t.Fatalf("second cancel=%v, want existed=false", again)
	}
	unknown := ws.request(c0ChannelID, "system.timer.cancel", systemActor, map[string]any{"timer_id": "never-existed"})
	if unknown["existed"] != false {
		t.Fatalf("unknown cancel=%v, want existed=false", unknown)
	}
}

// Setting an alarm on somebody else is the power to make them wake and work.
// That grant is not part of "may use timers", so the door refuses it — while
// the same subject on a READ is served (asserted above).
func TestTimerSetForAnotherMemberIsRefused(t *testing.T) {
	h := newHarness(t)
	_, ws := rootClient(t, h, map[string]int64{c0ChannelID: 0})

	// The sender here is the root human's own cell, so naming any other actor
	// is naming somebody else.
	_, failure, err := ws.tryRequest(c0ChannelID, "system.timer.set", systemActor, map[string]any{
		"duration_ms": 60000,
		"msg_type":    "e2e.nope",
		"subject":     systemActor,
	})
	if err == nil {
		t.Fatal("setting an alarm for another member was accepted")
	}
	if failure["error_code"] != "forbidden" {
		t.Fatalf("terminal=%v, want forbidden", failure)
	}
}

func asSlice(v any) []any {
	out, _ := v.([]any)
	return out
}
