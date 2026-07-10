package actorbase

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/runtime/actorrt"
	"github.com/wanpengxie/atoll/runtime/harness"
	"github.com/wanpengxie/atoll/runtime/schedule"
)

// seqPen is fakePen plus a Seq counter — DriveWrite must transparently
// return the harness Seq the engine's own verbs discard.
type seqPen struct {
	fakePen
	nextSeq int64
}

func (p *seqPen) Write(ctx context.Context, env *message.Envelope) (harness.WriteResult, error) {
	res, err := p.fakePen.Write(ctx, env)
	if err == nil && res.RejectReason == "" {
		p.nextSeq++
		res.Seq = p.nextSeq
	}
	return res, nil
}

// recordingSchedule captures the ScheduleReq — the Bind-value assertion
// (DriveAfter=BindIdentity vs Sys.After=BindIncarnation) needs to see it.
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

// runningEngine marks the occupant Running with a live ctx — the whitebox
// stand-in for Start() having completed (its lifeCtx-then-Store order is the
// production happens-before edge; tests reproduce the end state).
func runningEngine(t *testing.T, pen harness.Pen) *engine {
	t.Helper()
	e := &engine{
		pen:      pen,
		access:   fakeAccess{},
		state:    fakeAccess{},
		sched:    fakeSchedule{},
		spawn:    fakeSpawn{},
		clockFn:  time.Now,
		queueCap: 8,
	}
	e.serve = newServeLedger(e.life, 8)
	e.call = newCallLedger(e.life, e.pen, e.clockFn, Hooks{}, e.closureFault)
	e.workQ = newWorkDeque(8)
	e.rejectQ = make(chan *message.Envelope, 8)
	e.rejectStop = make(chan struct{})
	e.lifeCtx = context.Background()
	e.occupant.Store(int32(occupantRunning))
	return e
}

// TestDriveOccupantGate: every Drive* entry refuses with ErrOccupantNotReady
// while the occupant is not Running — the go-live→Start window (lifeCtx nil)
// must never panic (期12 S1 P0 闸).
func TestDriveOccupantGate(t *testing.T) {
	t.Parallel()
	pen := &fakePen{self: "user:alice"}
	e := newTestEngine(t, pen, Hooks{}, 8, 8) // occupant == Starting, lifeCtx nil

	if _, _, err := e.DriveWrite(driveWriteSpec("m1")); !errors.Is(err, ErrOccupantNotReady) {
		t.Fatalf("DriveWrite gate err = %v, want ErrOccupantNotReady", err)
	}
	if _, err := e.DriveRespond(newRequestEnv("r1", -1), driveRespondSpec()); !errors.Is(err, ErrOccupantNotReady) {
		t.Fatalf("DriveRespond gate err = %v, want ErrOccupantNotReady", err)
	}
	if _, err := e.DriveAfter(time.Minute, "reminder.note", nil); !errors.Is(err, ErrOccupantNotReady) {
		t.Fatalf("DriveAfter gate err = %v, want ErrOccupantNotReady", err)
	}
	if err := e.DriveCancelTimer("t"); !errors.Is(err, ErrOccupantNotReady) {
		t.Fatalf("DriveCancelTimer gate err = %v, want ErrOccupantNotReady", err)
	}
	if _, err := e.DriveResourceRead("res"); !errors.Is(err, ErrOccupantNotReady) {
		t.Fatalf("DriveResourceRead gate err = %v, want ErrOccupantNotReady", err)
	}
	if pen.count() != 0 {
		t.Fatalf("gated verbs wrote %d envelopes, want 0", pen.count())
	}

	// Draining/Dead refuse the same way.
	e.lifeCtx = context.Background()
	e.occupant.Store(int32(occupantDraining))
	if _, _, err := e.DriveWrite(driveWriteSpec("m2")); !errors.Is(err, ErrOccupantNotReady) {
		t.Fatalf("DriveWrite while draining err = %v, want ErrOccupantNotReady", err)
	}
}

func driveWriteSpec(id message.ID) actorrt.DriveWrite {
	return actorrt.DriveWrite{
		ID:       id,
		Type:     "human.note",
		Kind:     message.KindRequest,
		Audience: []actor.ActorID{"agent:x"},
	}
}

func driveRespondSpec() actorrt.DriveRespond {
	return actorrt.DriveRespond{Status: message.StatusCompleted}
}

// TestDriveWriteSeqRejectAndGrammar covers DriveWrite's three contracts: the
// Seq passthrough (the engine's own verbs discard it; the subject door needs
// it for the frame ack), the typed WriteRejected carrier, and the
// kind/visibility/audience grammar gates.
func TestDriveWriteSeqRejectAndGrammar(t *testing.T) {
	t.Parallel()
	pen := &seqPen{fakePen: fakePen{self: "user:alice"}}
	e := runningEngine(t, pen)

	// Happy path: Seq rides back, visibility normalises empty→public.
	spec := driveWriteSpec("w1")
	id, seq, err := e.DriveWrite(spec)
	if err != nil || id != "w1" || seq != 1 {
		t.Fatalf("DriveWrite = (%q, %d, %v), want (w1, 1, nil)", id, seq, err)
	}
	if got := pen.last().Visibility; got != message.VisibilityPublic {
		t.Fatalf("empty visibility normalised to %q, want public", got)
	}
	// No call-ledger residue, no author#2 arm (closure is the reaper's).
	if n := len(e.call.list()); n != 0 {
		t.Fatalf("DriveWrite left %d call-ledger entries, want 0", n)
	}

	// Typed reject carrier.
	rejPen := &fakePen{self: "user:alice", reject: harness.HarnessRejectReason("write_denied")}
	e2 := runningEngine(t, rejPen)
	_, _, err = e2.DriveWrite(driveWriteSpec("w2"))
	var wr *WriteRejected
	if !errors.As(err, &wr) || wr.Reason != "write_denied" {
		t.Fatalf("DriveWrite reject err = %v, want *WriteRejected{Reason: write_denied}", err)
	}

	// Grammar gates.
	bad := driveWriteSpec("w3")
	bad.Kind = message.KindResponse
	if _, _, err := e.DriveWrite(bad); err == nil {
		t.Fatal("DriveWrite(kind=response) accepted — a subject could forge closure around DriveRespond")
	}
	badVis := driveWriteSpec("w4")
	badVis.Visibility = message.VisibilitySystem
	if _, _, err := e.DriveWrite(badVis); err == nil {
		t.Fatal("DriveWrite(visibility=system) accepted, want reject (主题A A3)")
	}
	noAud := driveWriteSpec("w5")
	noAud.Audience = nil
	if _, _, err := e.DriveWrite(noAud); err == nil {
		t.Fatal("DriveWrite(request, empty audience) accepted, want reject")
	}
	evNoAud := driveWriteSpec("w6")
	evNoAud.Kind = message.KindEvent
	evNoAud.Audience = nil
	if _, _, err := e.DriveWrite(evNoAud); err != nil {
		t.Fatalf("DriveWrite(event, no audience) = %v, want nil (events may broadcast)", err)
	}
}

// TestDriveRespondCloseAndIdempotent: a serve-ledger entry closes on respond
// (this incarnation's double-answer guard); responding to a request the
// ledger never admitted is zero-action on the account (the log is the
// authority, the account only a projection); a harness terminal-duplicate is
// success (behavior.Respond's contract).
func TestDriveRespondCloseAndIdempotent(t *testing.T) {
	t.Parallel()
	pen := &fakePen{self: "user:alice"}
	e := runningEngine(t, pen)

	req := newRequestEnv("r1", -1)
	if !e.serve.admit(req) {
		t.Fatal("admit failed")
	}
	if _, err := e.DriveRespond(req, driveRespondSpec()); err != nil {
		t.Fatalf("DriveRespond = %v", err)
	}
	if n := e.serve.len(); n != 0 {
		t.Fatalf("serve ledger len after respond = %d, want 0 (closed)", n)
	}

	// No-entry respond: fine, zero account action.
	other := newRequestEnv("r2", -1)
	if _, err := e.DriveRespond(other, driveRespondSpec()); err != nil {
		t.Fatalf("DriveRespond(no ledger entry) = %v, want nil", err)
	}

	// Terminal duplicate degrades to success.
	dupPen := &fakePen{self: "user:alice", reject: harness.HarnessTerminalDuplicate}
	e3 := runningEngine(t, dupPen)
	if _, err := e3.DriveRespond(newRequestEnv("r3", -1), driveRespondSpec()); err != nil {
		t.Fatalf("DriveRespond(terminal duplicate) = %v, want nil", err)
	}
}

// TestDriveAfterBindIdentity: DriveAfter and Sys.After ride the SAME
// schedule engine and differ in exactly the Bind value (D7 — verb semantics,
// not actor category).
func TestDriveAfterBindIdentity(t *testing.T) {
	t.Parallel()
	pen := &fakePen{self: "user:alice"}
	e := runningEngine(t, pen)
	rec := &recordingSchedule{}
	e.sched = rec

	if _, err := e.DriveAfter(time.Minute, "reminder.note", []byte(`{}`)); err != nil {
		t.Fatalf("DriveAfter = %v", err)
	}
	if _, err := e.After(time.Minute, "self.wake", map[string]any{}); err != nil {
		t.Fatalf("After = %v", err)
	}
	if len(rec.reqs) != 2 {
		t.Fatalf("schedule saw %d reqs, want 2", len(rec.reqs))
	}
	if rec.reqs[0].Bind != schedule.BindIdentity {
		t.Fatalf("DriveAfter Bind = %v, want BindIdentity", rec.reqs[0].Bind)
	}
	if rec.reqs[1].Bind != schedule.BindIncarnation {
		t.Fatalf("Sys.After Bind = %v, want BindIncarnation", rec.reqs[1].Bind)
	}
}
