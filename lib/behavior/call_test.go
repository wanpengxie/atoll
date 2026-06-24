package behavior

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/wanpengxie/ActOS/protocol/actor"
	"github.com/wanpengxie/ActOS/protocol/channel"
	"github.com/wanpengxie/ActOS/protocol/message"
	"github.com/wanpengxie/ActOS/runtime/harness"
)

// recordingWriter is a concurrency-safe harness.Pen test double. It is
// relay-only: it never injects identity, so envelopes it records keep the
// builder's zero Sender/ChannelID (the real boundPen welds those).
type recordingWriter struct {
	mu        sync.Mutex
	writes    []*message.Envelope
	duplicate bool
	err       error
	signal    chan struct{} // closed-once notify on first write (nil = no notify)
	once      sync.Once
}

func (w *recordingWriter) Write(_ context.Context, env *message.Envelope) (harness.WriteResult, error) {
	w.mu.Lock()
	w.writes = append(w.writes, env)
	dup := w.duplicate
	err := w.err
	w.mu.Unlock()
	if w.signal != nil {
		w.once.Do(func() { close(w.signal) })
	}
	if err != nil {
		return harness.WriteResult{}, err
	}
	r := harness.WriteResult{MessageID: env.ID}
	if dup {
		r.RejectReason = harness.HarnessTerminalDuplicate
	}
	return r, nil
}

func (w *recordingWriter) count() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return len(w.writes)
}

func (w *recordingWriter) last() *message.Envelope {
	w.mu.Lock()
	defer w.mu.Unlock()
	if len(w.writes) == 0 {
		return nil
	}
	return w.writes[len(w.writes)-1]
}

func fixedClock(ms int64) func() time.Time {
	return func() time.Time { return time.UnixMilli(ms) }
}

func testSender() message.Sender {
	return message.Sender{Kind: actor.Kind("agent"), ID: actor.ActorID("caller-1")}
}

// newRequest builds a kind=request envelope. expiresAt nil = no deadline.
func newRequest(id message.ID, expiresAt *int64) *message.Envelope {
	return &message.Envelope{
		ID:         id,
		TS:         0,
		ChannelID:  channel.ID("ch-1"),
		Sender:     testSender(),
		Kind:       message.KindRequest,
		Type:       "ask",
		Payload:    json.RawMessage(`{}`),
		Visibility: message.Visibility("channel"),
		Audience:   message.Audience{actor.ActorID("svc")},
		ExpiresAt:  expiresAt,
	}
}

func responseFor(req *message.Envelope, status string) *message.Envelope {
	payload, _ := json.Marshal(map[string]string{"status": status})
	return &message.Envelope{
		ID:        message.ID("resp-" + string(req.ID)),
		ChannelID: req.ChannelID,
		Sender:    message.Sender{Kind: actor.Kind("agent"), ID: actor.ActorID("svc")},
		Kind:      message.KindResponse,
		Type:      req.Type,
		Payload:   payload,
		ParentID:  req.ID,
	}
}

// pendingLen reads the (cell-goroutine-owned) pending map for assertions. The
// test calls this only between synchronous Arm/Match/Stop calls — i.e. on the
// "cell goroutine" — so it observes the same single-threaded contract.
func (c *Caller) pendingLen() int { return len(c.pending) }

func (c *Caller) isPending(id message.ID) bool {
	_, ok := c.pending[id]
	return ok
}

// Arm with no deadline registers the request as in-flight (nil timer) and is
// idempotent on re-Arm.
func TestCaller_ArmNoDeadline(t *testing.T) {
	w := &recordingWriter{}
	c := NewCaller(w, fixedClock(1000), nil)

	req := newRequest("r1", nil)
	c.Arm(req)
	if !c.isPending("r1") {
		t.Fatal("Arm should register an in-flight request")
	}
	if c.pending["r1"] != nil {
		t.Fatal("no-deadline request must have a nil timer")
	}
	// idempotent
	c.Arm(req)
	if c.pendingLen() != 1 {
		t.Fatalf("re-Arm must be a no-op, pending=%d", c.pendingLen())
	}
	// no terminal written from arming
	if w.count() != 0 {
		t.Fatalf("Arm must not write a terminal, writes=%d", w.count())
	}
}

// Arm ignores a nil request or one with an empty id.
func TestCaller_ArmRejectsBadRequest(t *testing.T) {
	c := NewCaller(&recordingWriter{}, fixedClock(1000), nil)
	c.Arm(nil)
	c.Arm(&message.Envelope{ID: ""})
	if c.pendingLen() != 0 {
		t.Fatalf("nil/empty-id Arm must not register, pending=%d", c.pendingLen())
	}
}

// An expired deadline (ExpiresAt <= now) fires the unanswered_timeout terminal
// promptly. The terminal must be a failed response addressed back to the
// caller, carrying the unanswered_timeout reason and parent_id = request id.
func TestCaller_ExpiredDeadlineFires(t *testing.T) {
	sig := make(chan struct{})
	w := &recordingWriter{signal: sig}
	// clock now = 5000; deadline already in the past -> d clamps to 0.
	c := NewCaller(w, fixedClock(5000), nil)

	past := int64(4000)
	req := newRequest("r1", &past)
	c.Arm(req)

	select {
	case <-sig:
	case <-time.After(2 * time.Second):
		t.Fatal("expired deadline did not fire the timeout terminal")
	}

	term := w.last()
	if term == nil {
		t.Fatal("no terminal written")
	}
	if term.Kind != message.KindResponse {
		t.Fatalf("terminal kind = %q, want response", term.Kind)
	}
	if term.ParentID != "r1" {
		t.Fatalf("terminal parent_id = %q, want r1", term.ParentID)
	}
	// The caller's identity is welded onto the pen, not set by the builder. The
	// relay stub does not inject it, so the built terminal keeps a zero Sender
	// (sealed-pen).
	if term.Sender != (message.Sender{}) {
		t.Fatalf("terminal sender = %+v, want zero (pen-injected)", term.Sender)
	}
	var p struct {
		Status string `json:"status"`
		Reason string `json:"reason"`
	}
	if err := json.Unmarshal(term.Payload, &p); err != nil {
		t.Fatalf("terminal payload unmarshal: %v", err)
	}
	if p.Status != "failed" {
		t.Fatalf("terminal status = %q, want failed", p.Status)
	}
	if p.Reason != string(message.TerminalUnansweredTimeout) {
		t.Fatalf("terminal reason = %q, want unanswered_timeout", p.Reason)
	}
}

// fireTimeout runs OFF the cell goroutine and must NEVER touch pending. After a
// fire, the request stays registered in pending until a real terminal flows
// back through Match on the cell goroutine (mailbox is the sole ingress).
func TestCaller_FireTimeoutDoesNotTouchPending(t *testing.T) {
	sig := make(chan struct{})
	w := &recordingWriter{signal: sig}
	c := NewCaller(w, fixedClock(5000), nil)

	past := int64(0)
	req := newRequest("r1", &past)
	c.Arm(req)

	select {
	case <-sig:
	case <-time.After(2 * time.Second):
		t.Fatal("timer never fired")
	}
	// Even after the fire, the cell-goroutine-owned pending is untouched.
	if !c.isPending("r1") {
		t.Fatal("fireTimeout must NOT delete from pending; the back-flowing terminal does, via Match")
	}
}

// A timer-fire write that loses the one-terminal-per-request race
// (HarnessTerminalDuplicate) is benign: fireTimeout ignores the outcome and
// MUST NOT report an onFault — the receiver's real terminal landed, the request
// is closed, there is no liveness break.
func TestCaller_FireTimeoutDuplicateBenign(t *testing.T) {
	sig := make(chan struct{})
	w := &recordingWriter{signal: sig, duplicate: true}
	var faults int32
	c := NewCaller(w, fixedClock(5000), func(message.ID, error) {
		atomic.AddInt32(&faults, 1)
	})

	req := newRequest("r1", ptr(int64(0)))
	c.Arm(req)
	select {
	case <-sig:
	case <-time.After(2 * time.Second):
		t.Fatal("timer never fired")
	}
	// request still pending; no crash from the benign duplicate.
	if !c.isPending("r1") {
		t.Fatal("duplicate write must not change pending")
	}
	// Give the timer goroutine a beat to finish past the write.
	time.Sleep(50 * time.Millisecond)
	if n := atomic.LoadInt32(&faults); n != 0 {
		t.Fatalf("duplicate is benign — onFault must NOT fire, got %d", n)
	}
}

// A timer-fire write that FAILS with a real error is a liveness break (the
// caller-scoped timeout — the last-resort author — could not land, so this
// request may now be closed by no path). The base must report it through
// onFault rather than swallowing it (author#2 fault face, symmetric with
// author#3 in death.go).
func TestCaller_FireTimeoutWriteErrorReportsFault(t *testing.T) {
	sig := make(chan struct{})
	w := &recordingWriter{signal: sig, err: errors.New("store down")}
	faultCh := make(chan message.ID, 1)
	c := NewCaller(w, fixedClock(5000), func(reqID message.ID, _ error) {
		faultCh <- reqID
	})

	req := newRequest("r1", ptr(int64(0)))
	c.Arm(req)
	select {
	case <-sig:
	case <-time.After(2 * time.Second):
		t.Fatal("timer never fired")
	}
	select {
	case got := <-faultCh:
		if got != "r1" {
			t.Fatalf("onFault reqID = %q, want r1", got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("write error did not report an onFault")
	}
}

// Match: a non-response, or a response whose parent is not pending, returns
// false and does not mutate pending.
func TestCaller_MatchIgnoresForeign(t *testing.T) {
	c := NewCaller(&recordingWriter{}, fixedClock(1000), nil)
	c.Arm(newRequest("r1", nil))

	// not a response
	ev := &message.Envelope{Kind: message.KindEvent, ParentID: "r1"}
	if c.Match(ev) {
		t.Fatal("non-response must not Match")
	}
	// response to an unknown parent
	foreign := responseFor(newRequest("other", nil), "completed")
	if c.Match(foreign) {
		t.Fatal("response to a non-pending parent must not Match")
	}
	if !c.isPending("r1") {
		t.Fatal("foreign matches must not touch pending")
	}
}

// Match on a PROVISIONAL response of ours returns true but does NOT close the
// request — it stays in flight (and its timer, if any, keeps running).
func TestCaller_MatchProvisionalDoesNotClose(t *testing.T) {
	c := NewCaller(&recordingWriter{}, fixedClock(1000), nil)
	req := newRequest("r1", nil)
	c.Arm(req)

	prov := responseFor(req, "processing") // provisional status
	if !c.Match(prov) {
		t.Fatal("a provisional response to our request must Match (true)")
	}
	if !c.isPending("r1") {
		t.Fatal("provisional must NOT close the request")
	}
}

// Match on a FINAL response of ours returns true, stops the timer, and deletes
// the pending entry.
func TestCaller_MatchFinalCloses(t *testing.T) {
	c := NewCaller(&recordingWriter{}, fixedClock(1000), nil)
	// deadline far in the future so a real timer is armed (and must be stopped).
	future := int64(1_000_000_000_000)
	req := newRequest("r1", &future)
	c.Arm(req)
	if c.pending["r1"] == nil {
		t.Fatal("a future deadline must arm a real timer")
	}

	final := responseFor(req, "completed")
	if !c.Match(final) {
		t.Fatal("a final response to our request must Match (true)")
	}
	if c.isPending("r1") {
		t.Fatal("final response must close (delete) the pending request")
	}
}

// Stop stops all timers and clears pending.
func TestCaller_StopClears(t *testing.T) {
	future := int64(1_000_000_000_000)
	c := NewCaller(&recordingWriter{}, fixedClock(1000), nil)
	c.Arm(newRequest("r1", nil))
	c.Arm(newRequest("r2", &future))

	if c.pendingLen() != 2 {
		t.Fatalf("pending=%d, want 2", c.pendingLen())
	}
	c.Stop()
	if c.pendingLen() != 0 {
		t.Fatalf("Stop must clear pending, got %d", c.pendingLen())
	}
}

func ptr[T any](v T) *T { return &v }
