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
	"github.com/wanpengxie/atoll/runtime/resourcespec"
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

func (m *fakeAccessMinter) Mint(caller actor.ActorID) accessdoor.ResourceAccessHandle {
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

// Create/Stat/List complete the ResourceAccessHandle contract for this fake
// (only Invoke parity is exercised by the tests in this file today — these
// exist purely so fakeAccessHandle keeps satisfying the wide interface).
func (h *fakeAccessHandle) Create(_ context.Context, id resource.ResourceID, spec resourcespec.CreateSpec, initial []byte) (accessdoor.Outcome, error) {
	h.m.record(fakeAccessCall{caller: h.caller, scope: h.scope, op: access.OpCreate, id: id, args: initial})
	return accessdoor.Outcome{Value: initial, Found: len(initial) > 0}, nil
}

func (h *fakeAccessHandle) Stat(_ context.Context, id resource.ResourceID) (accessdoor.StatResult, error) {
	h.m.record(fakeAccessCall{caller: h.caller, scope: h.scope, id: id})
	return accessdoor.StatResult{}, nil
}

func (h *fakeAccessHandle) List(_ context.Context, q accessdoor.ListQuery) (accessdoor.ListPage, error) {
	h.m.record(fakeAccessCall{caller: h.caller, scope: h.scope})
	return accessdoor.ListPage{}, nil
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
		[]link.Declaration{{ActorID: id, Kind: actor.KindTool, Binding: actor.BindingEmbedded}}, link.DialConfig{}, nil)
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

// TestAccessArmPreSendCancelIsDefiniteError pins the PRE-send half of the pre/post-
// send split: a ctx ALREADY cancelled before the invoke means the frame never
// leaves the wire, so the op provably did not reach the home — the port arm returns
// a DEFINITE error, NOT outcome_unknown (which is reserved for an op that genuinely
// crossed the wire and whose ack never came, see the transport-death test below).
// The cell path, being synchronous, likewise never manufactures an unknown from a
// cancelled ctx.
func TestAccessArmPreSendCancelIsDefiniteError(t *testing.T) {
	const toolID = actor.ActorID("tool:presend-cancel")
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

	// Port path: pre-send cancel is a definite non-execution → a real error, and
	// specifically NOT the outcome_unknown verdict (the op never reached the home).
	portOut, portErr := arms.Access.Invoke(cancelled, access.OpRead, "r", []byte("x"), nil)
	if portErr == nil {
		t.Fatalf("pre-send cancel port invoke err = nil, want a definite error (op never left the wire)")
	}
	if portOut.RejectReason == access.OutcomeUnknown {
		t.Fatalf("pre-send cancel produced outcome_unknown — reserved for a genuinely in-flight op, not one that never sent")
	}
}

// TestAccessArmPostSendCancelIsUnknown pins the POST-send half: once the frame IS
// on the wire and the home is processing it, a ctx cancellation leaves the op
// GENUINELY in flight — its outcome is unconfirmed, so the port arm yields the
// outcome_unknown VERDICT (nil error), the same class as a transport death. This is
// the legitimate outcome_unknown per §3.5c ("the request may already have executed
// in the home"), and the contrast partner to the pre-send definite-error case.
func TestAccessArmPostSendCancelIsUnknown(t *testing.T) {
	const toolID = actor.ActorID("tool:postsend-cancel")
	m := &blockingAccessMinter{entered: make(chan struct{}), release: make(chan struct{})}
	defer close(m.release)
	arms, d := dialArmsWithMinters(t, m, &fakeScheduleMinter{}, toolID)
	defer func() { _ = d.Close() }()

	ctx, cancel := context.WithCancel(context.Background())
	type result struct {
		out accessdoor.Outcome
		err error
	}
	res := make(chan result, 1)
	go func() {
		o, e := arms.Access.Invoke(ctx, access.OpRead, "r", []byte("x"), nil)
		res <- result{o, e}
	}()
	<-m.entered // the frame crossed the wire and the home is parked on it → in flight
	cancel()    // cancel AFTER the frame was sent

	r := <-res
	if r.err != nil {
		t.Fatalf("post-send cancel must be a verdict, not an error: %v", r.err)
	}
	if r.out.RejectReason != access.OutcomeUnknown {
		t.Fatalf("post-send cancel outcome = %q, want outcome_unknown (genuinely in flight)", r.out.RejectReason)
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

// --- transport-death rig: minters whose handle parks in flight ---

// dialArmsWithMinters stands up a home wired with the given plane-2 / time-axis
// minters and returns one daemon-hosted actor's port arms. It is newCapsRig +
// dialArms fused so a test can inject a blocking minter (the cell-parity rig's
// fake minters answer synchronously and can never leave an op in flight).
func dialArmsWithMinters(t *testing.T, access accessdoor.AccessMinter, sched schedule.Minter, id actor.ActorID) (link.CellArms, *link.Dialer) {
	t.Helper()
	rt, _ := actorrt.New(actorrt.Config{Parent: context.Background()})
	acc := link.NewAcceptor(link.Config{
		Minter:     &stubMinter{},
		Access:     access,
		Schedule:   sched,
		Runtime:    rt,
		Membership: &stubMembership{},
		ChannelID:  testChannelID,
		LeasePing:  5 * time.Second,
		LeaseTTL:   30 * time.Second,
	})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		acc.Serve(w, req, "daemon-1")
	}))
	t.Cleanup(func() { _ = acc.Close(); srv.Close(); rt.StopAll() })

	d, err := link.Dial(context.Background(), "ws"+srv.URL[4:], "daemon-1",
		[]link.Declaration{{ActorID: id, Kind: actor.KindTool, Binding: actor.BindingEmbedded}}, link.DialConfig{}, nil)
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

// blockingAccessMinter's handle signals entry (the invoke has crossed the wire and
// reached the home) then parks until released — so a test can drop the transport
// while an access invoke is genuinely in flight on the arm.
type blockingAccessMinter struct {
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (m *blockingAccessMinter) Mint(actor.ActorID) accessdoor.ResourceAccessHandle {
	return &blockingAccessHandle{m: m}
}
func (m *blockingAccessMinter) MintState(actor.ActorID) accessdoor.AccessHandle {
	return &blockingAccessHandle{m: m}
}

type blockingAccessHandle struct{ m *blockingAccessMinter }

// block signals entry once (the invoke has crossed the wire and reached the
// home) then parks until released or ctx dies — the shared blocking shape
// every method on this handle uses, so any arm (Invoke/Create/Stat/List) can
// exercise the same in-flight-transport-death rig.
func (h *blockingAccessHandle) block(ctx context.Context) {
	h.m.once.Do(func() { close(h.m.entered) })
	select {
	case <-h.m.release:
	case <-ctx.Done():
	}
}

func (h *blockingAccessHandle) Invoke(ctx context.Context, _ access.Operation, _ resource.ResourceID, _ []byte, _ *access.Grant) (accessdoor.Outcome, error) {
	h.block(ctx)
	return accessdoor.Outcome{}, nil
}

func (h *blockingAccessHandle) Create(ctx context.Context, _ resource.ResourceID, _ resourcespec.CreateSpec, _ []byte) (accessdoor.Outcome, error) {
	h.block(ctx)
	return accessdoor.Outcome{}, nil
}

func (h *blockingAccessHandle) Stat(ctx context.Context, _ resource.ResourceID) (accessdoor.StatResult, error) {
	h.block(ctx)
	return accessdoor.StatResult{}, nil
}

func (h *blockingAccessHandle) List(ctx context.Context, _ accessdoor.ListQuery) (accessdoor.ListPage, error) {
	h.block(ctx)
	return accessdoor.ListPage{}, nil
}

type blockingScheduleMinter struct {
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (m *blockingScheduleMinter) Mint(actor.ActorID) schedule.ScheduleHandle {
	return &blockingScheduleHandle{m: m}
}

type blockingScheduleHandle struct{ m *blockingScheduleMinter }

func (h *blockingScheduleHandle) Schedule(ctx context.Context, _ schedule.ScheduleReq) (schedule.TimerID, error) {
	h.m.once.Do(func() { close(h.m.entered) })
	select {
	case <-h.m.release:
	case <-ctx.Done():
	}
	return schedule.TimerID("t"), nil
}

func (h *blockingScheduleHandle) Cancel(context.Context, schedule.TimerID) error { return nil }

// TestAccessArmOutcomeUnknownOnTransportDeath exercises outcome_unknown through the
// PRIMARY real-world producer: the transport dying with an access invoke in flight
// (streamReadLoop teardown → relayClient.close → errRelayClosed → OutcomeUnknown),
// not the cancelled-ctx path. It is the wire-only verdict, so a torn-down
// connection must still surface it as a VERDICT (nil error), never as a transport
// error handed to the caller.
func TestAccessArmOutcomeUnknownOnTransportDeath(t *testing.T) {
	const toolID = actor.ActorID("tool:txdeath-access")
	m := &blockingAccessMinter{entered: make(chan struct{}), release: make(chan struct{})}
	defer close(m.release)
	arms, d := dialArmsWithMinters(t, m, &fakeScheduleMinter{}, toolID)

	type result struct {
		out accessdoor.Outcome
		err error
	}
	res := make(chan result, 1)
	go func() {
		o, e := arms.Access.Invoke(context.Background(), access.OpRead, "r", []byte("x"), nil)
		res <- result{o, e}
	}()
	<-m.entered // the home received the invoke → it is genuinely in flight on the arm

	_ = d.Close() // drop the transport with the verdict unconfirmed

	r := <-res
	if r.err != nil {
		t.Fatalf("transport-death outcome must be a verdict, not an error: %v", r.err)
	}
	if r.out.RejectReason != access.OutcomeUnknown {
		t.Fatalf("unconfirmed outcome = %q, want outcome_unknown", r.out.RejectReason)
	}
}

// TestScheduleArmErrorsOnTransportDeath is the time-axis counterpart: the schedule
// arm has no unknown-verdict slot, so an in-flight schedule interrupted by
// transport death surfaces as a plain error (the timer may or may not exist —
// current at-least-once semantics), never a fabricated verdict.
func TestScheduleArmErrorsOnTransportDeath(t *testing.T) {
	const toolID = actor.ActorID("tool:txdeath-sched")
	m := &blockingScheduleMinter{entered: make(chan struct{}), release: make(chan struct{})}
	defer close(m.release)
	arms, d := dialArmsWithMinters(t, &fakeAccessMinter{}, m, toolID)

	res := make(chan error, 1)
	go func() {
		_, e := arms.Schedule.Schedule(context.Background(),
			schedule.ScheduleReq{Bind: schedule.BindIdentity, FireAt: 1, Type: "wake"})
		res <- e
	}()
	<-m.entered

	_ = d.Close()

	if err := <-res; err == nil {
		t.Fatalf("transport-death schedule err = nil, want non-nil (unconfirmed, no unknown verdict)")
	}
}
