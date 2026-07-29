package actorbase

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/runtime/schedule"
)

// recordingSchedule captures the ScheduleReq — the home and the payload bytes
// are exactly what these tests are about, and neither is observable from the
// returned TimerID.
type recordingSchedule struct {
	mu   sync.Mutex
	reqs []schedule.ScheduleReq
}

func (r *recordingSchedule) Schedule(_ context.Context, req schedule.ScheduleReq) (schedule.TimerID, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.reqs = append(r.reqs, req)
	return schedule.TimerID("t-1"), nil
}
func (r *recordingSchedule) Cancel(_ context.Context, _ schedule.TimerID) error { return nil }
func (r *recordingSchedule) Ack(_ context.Context, _ schedule.TimerID) error    { return nil }

func (r *recordingSchedule) only(t *testing.T) schedule.ScheduleReq {
	t.Helper()
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.reqs) != 1 {
		t.Fatalf("scheduler saw %d reqs, want exactly 1", len(r.reqs))
	}
	return r.reqs[0]
}

func schedulingEngine(t *testing.T) (*engine, *recordingSchedule) {
	t.Helper()
	e := newTestEngine(t, &fakePen{self: "user:alice"}, Hooks{}, 8, 8)
	e.lifeCtx = context.Background()
	rec := &recordingSchedule{}
	e.sched = rec
	return e, rec
}

// The storage home is the caller's declaration, carried through untouched.
// There is no default and no sugar for either value: durability is not
// something to inherit — both homes are ActorID-owned and both survive body
// replacement, so the only thing the choice says is how long the reminder must
// outlive the Scheduler itself.
func TestAfterArmsTheHomeTheCallerNamed(t *testing.T) {
	t.Parallel()
	for _, home := range []schedule.TimerHome{schedule.TimerHomeMemory, schedule.TimerHomeDurable} {
		t.Run(string(home), func(t *testing.T) {
			e, rec := schedulingEngine(t)
			id, err := e.After(time.Minute, "reminder.note", map[string]string{"k": "v"}, home)
			if err != nil {
				t.Fatalf("After = %v", err)
			}
			if id != schedule.TimerID("t-1") {
				t.Fatalf("timer id = %q, want t-1", id)
			}
			got := rec.only(t)
			if got.Home != home {
				t.Fatalf("Home = %q, want %q", got.Home, home)
			}
			if got.Type != "reminder.note" {
				t.Fatalf("type = %q, want reminder.note", got.Type)
			}
		})
	}
}

// A payload handed over as json.RawMessage stays JSON: RawMessage's own
// MarshalJSON returns its bytes, so it never suffers the []byte→base64
// encoding that would silently corrupt what the fired timer's recipient
// parses. (Whitespace is compacted on the way through, and invalid JSON is
// caught here rather than at fire time — both strictly better than passing
// bytes through unexamined, and neither changes what the recipient reads.)
func TestAfterCarriesARawMessagePayloadAsJSONNeverAsBase64(t *testing.T) {
	t.Parallel()
	e, rec := schedulingEngine(t)

	payload := json.RawMessage(`{"remind":"call mom","n":42,"nested":{"k":"v"}}`)
	if _, err := e.After(time.Minute, "reminder.note", payload, schedule.TimerHomeDurable); err != nil {
		t.Fatalf("After = %v", err)
	}
	if got := string(rec.only(t).Payload); got != string(payload) {
		t.Fatalf("timer payload = %q, want %q (base64 回潮?)", got, string(payload))
	}
}

// An absent payload must fire as `{}` on BOTH homes. json.Marshal(nil) yields
// the four bytes `null`, which is non-empty, so the fire path's own
// zero-length→`{}` substitution never sees it and the harness — which rejects
// null payloads — kills the timer at FIRE time: the Memory one silently
// dropped, the Durable one parked in a dead row, both long after Schedule
// returned success. Folding it here is what makes "no payload" mean the same
// thing on either home.
func TestAfterFoldsAnAbsentPayloadOnBothHomes(t *testing.T) {
	t.Parallel()
	for _, home := range []schedule.TimerHome{schedule.TimerHomeMemory, schedule.TimerHomeDurable} {
		t.Run(string(home), func(t *testing.T) {
			e, rec := schedulingEngine(t)
			if _, err := e.After(time.Hour, "twin.tick", nil, home); err != nil {
				t.Fatalf("After = %v", err)
			}
			if got := rec.only(t).Payload; len(got) != 0 {
				t.Fatalf("absent payload scheduled as %q, want zero length (the fire path substitutes {})", got)
			}
		})
	}
}

// TestAfterKeepsRawMessagePayloadByteForByte pins the byte-fidelity promise the
// absorbed identity-flavoured timer verb used to make explicitly.
//
// That verb took a json.RawMessage and handed it to the Scheduler untouched, so
// an off-process subject's payload arrived exactly as composed. Routing it
// through json.Marshal is safe from the []byte→base64 trap (RawMessage returns
// its own bytes) but NOT byte-preserving: encoding/json compacts whitespace. A
// subject that composed indented JSON would have found it silently rewritten.
func TestAfterKeepsRawMessagePayloadByteForByte(t *testing.T) {
	spaced := json.RawMessage(`{ "note" : "keep  my  spacing" }`)

	got, err := timerPayloadBytes(spaced)
	if err != nil {
		t.Fatalf("timerPayloadBytes(RawMessage): %v", err)
	}
	if string(got) != string(spaced) {
		t.Fatalf("RawMessage payload was rewritten:\n got %s\nwant %s", got, spaced)
	}
}

// TestAfterMarshalsEverythingElse is the other half: a plain Go value has no
// chosen encoding, so it must still be marshalled (and invalid JSON must fail
// at arm time, not at fire time).
func TestAfterMarshalsEverythingElse(t *testing.T) {
	got, err := timerPayloadBytes(map[string]int{"n": 1})
	if err != nil {
		t.Fatalf("timerPayloadBytes(map): %v", err)
	}
	if string(got) != `{"n":1}` {
		t.Fatalf("map payload = %s, want {\"n\":1}", got)
	}

	if _, err := timerPayloadBytes(make(chan int)); err == nil {
		t.Fatal("timerPayloadBytes: an unmarshalable value must fail at arm time, got nil error")
	}
}
