package link_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/platform/internal/link"
	"github.com/wanpengxie/atoll/protocol/access"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/protocol/resource"
	"github.com/wanpengxie/atoll/runtime/accessdoor"
	"github.com/wanpengxie/atoll/runtime/actorrt"
	"github.com/wanpengxie/atoll/runtime/schedule"
)

// The §1.7 capability-parity contract: an out-of-process (port) actor's plane-2
// and time-axis handles must behave IDENTICALLY to a local (cell) actor's — same
// welded caller/author, same Outcome/TimerID, same reject verdict, with the ONE
// documented exception that only the wire path can yield outcome_unknown (an in-
// proc invoke is synchronous and never leaves an outcome unconfirmed). These tests
// run the SAME assertions against the cell handle (the fake minter drawn directly)
// and the port handle (the same fake minter reached across a real link), proving
// the daemon-side arms are not a residual-capability downgrade.

// --- fake plane-2 door ---

type fakeAccessCall struct {
	caller actor.ActorID
	scope  string
	op     access.Operation
	id     resource.ResourceID
	args   []byte
}

// fakeAccessMinter is a test accessdoor.AccessMinter: the handle it welds echoes
// the invocation into a deterministic Outcome (so parity across varied inputs is
// meaningful) and records the welded caller + scope (so a test can assert the home
// welds the connection's authenticated id, never the wire's self-report).
type fakeAccessMinter struct {
	mu    sync.Mutex
	calls []fakeAccessCall
}

func (m *fakeAccessMinter) Mint(caller actor.ActorID) accessdoor.AccessHandle {
	return &fakeAccessHandle{m: m, caller: caller, scope: "channel"}
}

func (m *fakeAccessMinter) MintState(owner actor.ActorID) accessdoor.AccessHandle {
	return &fakeAccessHandle{m: m, caller: owner, scope: "state"}
}

func (m *fakeAccessMinter) record(c fakeAccessCall) {
	m.mu.Lock()
	m.calls = append(m.calls, c)
	m.mu.Unlock()
}

func (m *fakeAccessMinter) last() fakeAccessCall {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.calls[len(m.calls)-1]
}

type fakeAccessHandle struct {
	m      *fakeAccessMinter
	caller actor.ActorID
	scope  string
}

// Invoke echoes deterministically: OpDelete is a reject VERDICT (access_denied,
// nil error — must transit the wire as an Outcome, not a Go error); everything
// else accepts and returns Value=args, Found=len(args)>0.
func (h *fakeAccessHandle) Invoke(_ context.Context, op access.Operation, id resource.ResourceID, args []byte, _ *access.Grant) (accessdoor.Outcome, error) {
	h.m.record(fakeAccessCall{caller: h.caller, scope: h.scope, op: op, id: id, args: args})
	if op == access.OpDelete {
		return accessdoor.Outcome{RejectReason: access.AccessDenied}, nil
	}
	return accessdoor.Outcome{Value: args, Found: len(args) > 0}, nil
}

// --- fake time axis ---

type fakeScheduleCall struct {
	author        actor.ActorID
	correlationID string
	canceled      schedule.TimerID
}

type fakeScheduleMinter struct {
	mu    sync.Mutex
	calls []fakeScheduleCall
}

func (m *fakeScheduleMinter) Mint(author actor.ActorID) schedule.ScheduleHandle {
	return &fakeScheduleHandle{m: m, author: author}
}

func (m *fakeScheduleMinter) record(c fakeScheduleCall) {
	m.mu.Lock()
	m.calls = append(m.calls, c)
	m.mu.Unlock()
}

func (m *fakeScheduleMinter) last() fakeScheduleCall {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.calls[len(m.calls)-1]
}

type fakeScheduleHandle struct {
	m      *fakeScheduleMinter
	author actor.ActorID
}

func (h *fakeScheduleHandle) Schedule(_ context.Context, req schedule.ScheduleReq) (schedule.TimerID, error) {
	h.m.record(fakeScheduleCall{author: h.author, correlationID: req.CorrelationID})
	return schedule.TimerID("timer-for-" + string(h.author)), nil
}

func (h *fakeScheduleHandle) Cancel(_ context.Context, id schedule.TimerID) error {
	h.m.record(fakeScheduleCall{author: h.author, canceled: id})
	return nil
}

// --- capability rig: a home wired with the plane-2 + time-axis minters ---

type capsRig struct {
	access *fakeAccessMinter
	sched  *fakeScheduleMinter
	acc    *link.Acceptor
	srv    *httptest.Server
}

func newCapsRig(t *testing.T) *capsRig {
	t.Helper()
	rt, _ := actorrt.New(actorrt.Config{Parent: context.Background()})
	r := &capsRig{access: &fakeAccessMinter{}, sched: &fakeScheduleMinter{}}
	r.acc = link.NewAcceptor(link.Config{
		Minter:     &stubMinter{},
		Access:     r.access,
		Schedule:   r.sched,
		Runtime:    rt,
		Membership: &stubMembership{},
		ChannelID:  testChannelID,
		LeasePing:  5 * time.Second,
		LeaseTTL:   30 * time.Second,
	})
	r.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		r.acc.Serve(w, req, "daemon-1")
	}))
	t.Cleanup(func() { _ = r.acc.Close(); r.srv.Close(); rt.StopAll() })
	return r
}

func (r *capsRig) wsURL() string { return "ws" + r.srv.URL[4:] }

// dialArms attaches a daemon hosting one actor and returns its port-side arms.
func dialArms(t *testing.T, r *capsRig, id actor.ActorID) (link.CellArms, *link.Dialer) {
	t.Helper()
	d, err := link.Dial(context.Background(), r.wsURL(), "daemon-1",
		[]link.Declaration{{ActorID: id, Kind: actor.KindTool, Binding: actor.BindingEmbedded}}, nil)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	arms, err := d.OpenStream(id, func(*message.Envelope) error { return nil }, nil)
	if err != nil {
		t.Fatalf("OpenStream: %v", err)
	}
	d.Start()
	return arms, d
}

// TestAccessArmCellPortParity runs the same access contract against the cell
// handle (fake minter drawn directly) and the port handle (same fake minter
// reached across the link). Outcomes must be identical, and the home must weld the
// connection's authenticated id as caller.
func TestAccessArmCellPortParity(t *testing.T) {
	const toolID = actor.ActorID("tool:cap")
	r := newCapsRig(t)
	arms, d := dialArms(t, r, toolID)
	defer func() { _ = d.Close() }()

	ctx := context.Background()
	cell := r.access.Mint(toolID) // the in-proc twin: same door, welded directly

	cases := []struct {
		name string
		op   access.Operation
		id   resource.ResourceID
		args []byte
	}{
		{"read", access.OpRead, "res-1", []byte("sel")},
		{"create", access.OpCreate, "res-2", []byte("bytes")},
		{"delete-reject-verdict", access.OpDelete, "res-3", nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cellOut, cellErr := cell.Invoke(ctx, tc.op, tc.id, tc.args, nil)
			portOut, portErr := arms.Access.Invoke(ctx, tc.op, tc.id, tc.args, nil)
			if cellErr != nil || portErr != nil {
				t.Fatalf("errors cell=%v port=%v, want nil (a reject is a verdict, not an error)", cellErr, portErr)
			}
			if string(cellOut.Value) != string(portOut.Value) || cellOut.Found != portOut.Found || cellOut.RejectReason != portOut.RejectReason {
				t.Fatalf("cell/port outcome divergence: cell=%+v port=%+v", cellOut, portOut)
			}
			// The home welded the authenticated bound id — never the wire self-report.
			if got := r.access.last(); got.caller != toolID || got.scope != "channel" {
				t.Fatalf("welded call = %+v, want caller=%q scope=channel", got, toolID)
			}
		})
	}
}

// TestStateArmRoutesToActorScope proves State rides the SAME KindAccess arm but
// routes to MintState (actor-scoped), distinguished only by the scope field.
func TestStateArmRoutesToActorScope(t *testing.T) {
	const toolID = actor.ActorID("tool:state")
	r := newCapsRig(t)
	arms, d := dialArms(t, r, toolID)
	defer func() { _ = d.Close() }()

	if _, err := arms.State.Invoke(context.Background(), access.OpWrite, "own-state", []byte("v"), nil); err != nil {
		t.Fatalf("state invoke: %v", err)
	}
	if got := r.access.last(); got.caller != toolID || got.scope != "state" {
		t.Fatalf("state call = %+v, want caller=%q scope=state (MintState branch)", got, toolID)
	}
}

// TestAccessArmOutcomeUnknownOnlyOnWire: an unconfirmed transport (here a cancelled
// ctx while the verdict is in flight) yields the outcome_unknown VERDICT on the
// port path — the one Outcome state only the wire can produce. The cell path with
// the same cancelled ctx never yields it (it is synchronous — it returns the
// door's real outcome).
func TestAccessArmOutcomeUnknownOnlyOnWire(t *testing.T) {
	const toolID = actor.ActorID("tool:unknown")
	r := newCapsRig(t)
	arms, d := dialArms(t, r, toolID)
	defer func() { _ = d.Close() }()

	cancelled, cancel := context.WithCancel(context.Background())
	cancel()

	// Cell path: synchronous, the cancelled ctx does not manufacture an unknown.
	cellOut, cellErr := r.access.Mint(toolID).Invoke(cancelled, access.OpRead, "r", []byte("x"), nil)
	if cellErr != nil || cellOut.RejectReason == access.OutcomeUnknown {
		t.Fatalf("cell path produced outcome_unknown (err=%v out=%+v) — only the wire may", cellErr, cellOut)
	}

	// Port path: the round-trip's outcome is unconfirmed → outcome_unknown verdict,
	// nil error (a verdict, not a transport error surfaced to the caller).
	portOut, portErr := arms.Access.Invoke(cancelled, access.OpRead, "r", []byte("x"), nil)
	if portErr != nil {
		t.Fatalf("port outcome_unknown must be a verdict, not an error: %v", portErr)
	}
	if portOut.RejectReason != access.OutcomeUnknown {
		t.Fatalf("port unconfirmed outcome = %q, want outcome_unknown", portOut.RejectReason)
	}
}

// TestScheduleArmCellPortParity runs the schedule contract against cell and port
// handles: identical TimerID shape, welded author, and — the施工义务 — the domain
// CorrelationID transits the wire intact.
func TestScheduleArmCellPortParity(t *testing.T) {
	const toolID = actor.ActorID("tool:timer")
	r := newCapsRig(t)
	arms, d := dialArms(t, r, toolID)
	defer func() { _ = d.Close() }()

	ctx := context.Background()
	req := schedule.ScheduleReq{Bind: schedule.BindIdentity, FireAt: 123, Type: "wake", CorrelationID: "corr-xyz"}

	cellTID, cellErr := r.sched.Mint(toolID).Schedule(ctx, req)
	if cellErr != nil {
		t.Fatalf("cell schedule: %v", cellErr)
	}
	portTID, portErr := arms.Schedule.Schedule(ctx, req)
	if portErr != nil {
		t.Fatalf("port schedule: %v", portErr)
	}
	if cellTID != portTID {
		t.Fatalf("cell/port TimerID divergence: cell=%q port=%q", cellTID, portTID)
	}
	got := r.sched.last()
	if got.author != toolID {
		t.Fatalf("welded author = %q, want %q (home welds, wire never self-reports)", got.author, toolID)
	}
	if got.correlationID != "corr-xyz" {
		t.Fatalf("CorrelationID = %q, want corr-xyz (domain causal coordinate must transit the wire)", got.correlationID)
	}

	// Cancel also crosses the wire and welds the same author.
	if err := arms.Schedule.Cancel(ctx, portTID); err != nil {
		t.Fatalf("port cancel: %v", err)
	}
	if got := r.sched.last(); got.author != toolID || got.canceled != portTID {
		t.Fatalf("cancel call = %+v, want author=%q canceled=%q", got, toolID, portTID)
	}
}
