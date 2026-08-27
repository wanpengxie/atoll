package remoteingress_test

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/wanpengxie/atoll/protocol/access"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/protocol/resource"
	"github.com/wanpengxie/atoll/runtime/accessdoor"
	"github.com/wanpengxie/atoll/runtime/actorcaps"
	"github.com/wanpengxie/atoll/runtime/actorctl"
	"github.com/wanpengxie/atoll/runtime/actorhost"
	"github.com/wanpengxie/atoll/runtime/capauth"
	"github.com/wanpengxie/atoll/runtime/harness"
	"github.com/wanpengxie/atoll/runtime/remoteingress"
	"github.com/wanpengxie/atoll/runtime/schedule"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

// ---------------------------------------------------------------------------
// The record port and the ledger face.
// ---------------------------------------------------------------------------

type recordStore struct {
	mu       sync.Mutex
	restored []storespec.ActorRecord
	gone     map[actor.ActorID]bool
}

func newRecordStore(restored ...storespec.ActorRecord) *recordStore {
	return &recordStore{
		restored: restored,
		gone:     make(map[actor.ActorID]bool),
	}
}

func (s *recordStore) RestoreActive(context.Context) ([]storespec.ActorRecord, error) {
	out := make([]storespec.ActorRecord, len(s.restored))
	for i, record := range s.restored {
		out[i] = record.Clone()
	}
	return out, nil
}

func (s *recordStore) Insert(
	context.Context,
	storespec.ActorDraft,
) (storespec.ActorRecord, error) {
	return storespec.ActorRecord{}, errors.New("unused")
}

func (s *recordStore) UpdateDefinition(
	context.Context,
	actor.ActorID,
	storespec.ActorDefinition,
) (storespec.ActorRecord, error) {
	return storespec.ActorRecord{}, errors.New("unused")
}

func (s *recordStore) Deregister(_ context.Context, ids []actor.ActorID) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, id := range ids {
		s.gone[id] = true
	}
	return nil
}

// ledger is the Controller face the ingress consumes: the narrow admission
// questions plus the typed self-command.
type ledger struct{ *actorctl.Controller }

func (l ledger) End(
	ctx context.Context,
	request actorctl.EndRequest,
) (actorctl.EndResult, error) {
	transition, err := l.Controller.End(ctx, request)
	return transition.Result, err
}

// ---------------------------------------------------------------------------
// The four organ doors. Each one does exactly what the real organ's welded
// shell does: run the authority's one complete verdict, then execute.
// ---------------------------------------------------------------------------

type doorLog struct {
	mu       sync.Mutex
	admitted []string
}

func (l *doorLog) record(kind string, authority capauth.Authority) error {
	if err := authority.Admit(); err != nil {
		return err
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	l.admitted = append(l.admitted, kind+":"+string(authority.ActorID()))
	return nil
}

type penDoor struct {
	log  *doorLog
	kind actor.Kind
}

func (d *penDoor) MintAuthority(authority capauth.Authority, kind actor.Kind) harness.Pen {
	return penShell{door: d, authority: authority, kind: kind}
}

type penShell struct {
	door      *penDoor
	authority capauth.Authority
	kind      actor.Kind
}

func (p penShell) Write(
	context.Context,
	*message.Envelope,
) (harness.WriteResult, error) {
	if err := p.door.log.record("pen", p.authority); err != nil {
		return harness.WriteResult{}, err
	}
	p.door.kind = p.kind
	return harness.WriteResult{MessageID: "written"}, nil
}

type resourceDoor struct{ log *doorLog }

func (d *resourceDoor) MintAuthority(authority capauth.Authority) accessdoor.ResourceAccessHandle {
	return resourceShell{log: d.log, authority: authority}
}

type resourceShell struct {
	log       *doorLog
	authority capauth.Authority
}

func (h resourceShell) Invoke(
	context.Context,
	access.Operation,
	resource.ResourceID,
	[]byte,
) (accessdoor.Outcome, error) {
	if err := h.log.record("access", h.authority); err != nil {
		return accessdoor.Outcome{}, err
	}
	return accessdoor.Outcome{Found: true}, nil
}

func (h resourceShell) Create(
	context.Context,
	resource.ResourceID,
	accessdoor.CreateSpec,
	[]byte,
) (accessdoor.Outcome, error) {
	if err := h.log.record("access.create", h.authority); err != nil {
		return accessdoor.Outcome{}, err
	}
	return accessdoor.Outcome{}, nil
}

func (h resourceShell) Stat(
	context.Context,
	resource.ResourceID,
) (accessdoor.StatResult, error) {
	if err := h.log.record("access.stat", h.authority); err != nil {
		return accessdoor.StatResult{}, err
	}
	return accessdoor.StatResult{}, nil
}

func (h resourceShell) List(
	context.Context,
	accessdoor.ListQuery,
) (accessdoor.ListPage, error) {
	if err := h.log.record("access.list", h.authority); err != nil {
		return accessdoor.ListPage{}, err
	}
	return accessdoor.ListPage{}, nil
}

func (h resourceShell) Open(
	context.Context,
	resource.ResourceID,
	access.Operation,
) (accessdoor.FileAccess, accessdoor.Outcome, error) {
	return accessdoor.FileAccess{}, accessdoor.Outcome{}, accessdoor.ErrFileCapabilityUnavailable
}

func (h resourceShell) Redeem(
	context.Context,
	accessdoor.FileRoute,
) (accessdoor.FileAccess, error) {
	return accessdoor.FileAccess{}, accessdoor.ErrFileCapabilityUnavailable
}

// stateDoor is the state organ's per-call entry. The real one routes to a
// backing first; what matters here is that the ingress hands it an IDENTITY
// authority and nothing else.
type stateDoor struct{ log *doorLog }

func (d *stateDoor) StateIngress(
	_ context.Context,
	authority capauth.Authority,
	_ accessdoor.StateOp,
) (accessdoor.Outcome, error) {
	if err := d.log.record("state", authority); err != nil {
		return accessdoor.Outcome{}, err
	}
	return accessdoor.Outcome{Found: true}, nil
}

type scheduleDoor struct{ log *doorLog }

func (d *scheduleDoor) MintAuthority(authority capauth.Authority) schedule.ScheduleHandle {
	return scheduleShell{log: d.log, authority: authority}
}

type scheduleShell struct {
	log       *doorLog
	authority capauth.Authority
}

func (h scheduleShell) Schedule(
	context.Context,
	schedule.ScheduleReq,
) (schedule.TimerID, error) {
	if err := h.log.record("schedule", h.authority); err != nil {
		return "", err
	}
	return "timer:1", nil
}

func (h scheduleShell) Cancel(context.Context, schedule.TimerID) error {
	return h.log.record("schedule.cancel", h.authority)
}

func (h scheduleShell) Ack(context.Context, schedule.TimerID) error {
	return h.log.record("schedule.ack", h.authority)
}
func (h scheduleShell) List(context.Context) ([]schedule.TimerInfo, error) {
	if err := h.log.record("schedule.list", h.authority); err != nil {
		return nil, err
	}
	return []schedule.TimerInfo{{ID: "timer:1", Home: schedule.TimerHomeDurable, FireAt: 42, Type: "standup"}}, nil
}

// ---------------------------------------------------------------------------
// Rig
// ---------------------------------------------------------------------------

const remoteActor = actor.ActorID("agent:remote")

type rig struct {
	controller *actorctl.Controller
	store      *recordStore
	ingress    remoteingress.RemoteIngress
	log        *doorLog
	pen        *penDoor
}

func newRig(t *testing.T) *rig {
	t.Helper()
	placement, err := storespec.NewDaemonPlacement("daemon-1")
	if err != nil {
		t.Fatal(err)
	}
	store := newRecordStore(storespec.ActorRecord{
		ID: remoteActor, Kind: actor.KindAgent, SourceDeclID: "decl:remote",
		Definition: storespec.ActorDefinition{Class: "agent"},
		Placement:  placement,
	})
	controller, err := actorctl.New(store, func() int64 { return 1 })
	if err != nil {
		t.Fatal(err)
	}
	if err := controller.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	log := &doorLog{}
	pen := &penDoor{log: log}
	ingress, err := remoteingress.New(
		controller, ledger{controller}, pen, &resourceDoor{log: log},
		&stateDoor{log: log}, &scheduleDoor{log: log},
	)
	if err != nil {
		t.Fatal(err)
	}
	return &rig{controller: controller, store: store, ingress: ingress, log: log, pen: pen}
}

// keyOf reads the actor's current term the way the daemon plan does. It is
// goroutine-safe to call: it reports absence rather than failing the test.
func keyOf(controller *actorctl.Controller, id actor.ActorID) (actorhost.AttemptKey, bool) {
	desired, err := controller.DesiredFor("daemon-1", "server")
	if err != nil {
		return "", false
	}
	for _, want := range desired {
		if body, ok := want.(actorhost.BodyDesired); ok && body.ActorID == id {
			return body.AttemptKey, true
		}
	}
	return "", false
}

func currentKey(t *testing.T, controller *actorctl.Controller, id actor.ActorID) actorhost.AttemptKey {
	t.Helper()
	key, ok := keyOf(controller, id)
	if !ok {
		t.Fatalf("actor %q has no desired term on the daemon", id)
	}
	return key
}

func envelope() *message.Envelope {
	return &message.Envelope{ID: "m1", Kind: message.KindEvent, Type: "agent.text"}
}

// --- ① a stale term's stream splits two ways ---------------------------------
//
// After a Restart publishes G2, a stream still keyed to G1 is a former term
// acting in the present: its pen and its channel-resource calls are refused
// (A/G — "as the current term"), while state and schedule still go through
// (A — an identity's own belongings outlive the term). Same stream, same
// frame, two different answers, because the two questions differ.
func TestStaleTermSplitsAcrossTheArms(t *testing.T) {
	r := newRig(t)
	ctx := context.Background()
	g1 := currentKey(t, r.controller, remoteActor)

	if _, err := r.controller.Restart(ctx, actorctl.RestartRequest{ActorID: remoteActor}); err != nil {
		t.Fatal(err)
	}
	g2 := currentKey(t, r.controller, remoteActor)
	if g1 == g2 {
		t.Fatal("restart did not mint a new term")
	}

	if _, err := r.ingress.Emit(ctx, remoteActor, g1, envelope()); !errors.Is(err, actorctl.ErrStaleAttempt) {
		t.Fatalf("stale emit err=%v, want stale attempt", err)
	}
	if _, err := r.ingress.Access(ctx, remoteActor, g1, remoteingress.AccessRequest{
		Kind: remoteingress.AccessInvoke, Scope: remoteingress.ScopeChannel,
		Operation: access.OpRead, Resource: "r",
	}); !errors.Is(err, actorctl.ErrStaleAttempt) {
		t.Fatalf("stale channel access err=%v, want stale attempt", err)
	}

	if _, err := r.ingress.Access(ctx, remoteActor, g1, remoteingress.AccessRequest{
		Kind: remoteingress.AccessInvoke, Scope: remoteingress.ScopeState,
		Operation: access.OpRead, Resource: "k",
	}); err != nil {
		t.Fatalf("state across terms err=%v, want accepted", err)
	}
	if _, err := r.ingress.Schedule(ctx, remoteActor, remoteingress.ScheduleRequest{
		Method: remoteingress.ScheduleSet,
	}); err != nil {
		t.Fatalf("schedule across terms err=%v, want accepted", err)
	}

	// The current term's pen works, and it carries the Kind the ledger holds —
	// from the same snapshot that passed the verdict.
	if _, err := r.ingress.Emit(ctx, remoteActor, g2, envelope()); err != nil {
		t.Fatalf("current emit err=%v", err)
	}
	if r.pen.kind != actor.KindAgent {
		t.Fatalf("pen welded kind %q, want the ledger's kind", r.pen.kind)
	}

	// Every arm reached its organ through an authority, never a snapshot.
	if len(r.log.admitted) != 3 {
		t.Fatalf("organ entries = %v, want state, schedule and the current pen", r.log.admitted)
	}
}

// --- ② End is a ledger change, so the next call is simply refused ------------
//
// Nothing has to be dismantled on the server when a remote actor dies: the
// same doors answer differently on the next frame, in the ledger's own words.
func TestEndedActorIsRefusedAtEveryArm(t *testing.T) {
	r := newRig(t)
	ctx := context.Background()
	key := currentKey(t, r.controller, remoteActor)

	if _, err := r.controller.End(ctx, actorctl.EndRequest{
		CallerActorID: remoteActor, CallerAttempt: key, Target: remoteActor,
	}); err != nil {
		t.Fatal(err)
	}

	if _, err := r.ingress.Emit(ctx, remoteActor, key, envelope()); !errors.Is(err, actorctl.ErrInactive) {
		t.Fatalf("emit after end err=%v, want inactive", err)
	}
	if _, err := r.ingress.Access(ctx, remoteActor, key, remoteingress.AccessRequest{
		Kind: remoteingress.AccessInvoke, Operation: access.OpRead, Resource: "r",
	}); !errors.Is(err, actorctl.ErrInactive) {
		t.Fatalf("access after end err=%v, want inactive", err)
	}
	if _, err := r.ingress.Access(ctx, remoteActor, key, remoteingress.AccessRequest{
		Kind: remoteingress.AccessInvoke, Scope: remoteingress.ScopeState,
		Operation: access.OpRead, Resource: "k",
	}); !errors.Is(err, actorctl.ErrInactive) {
		t.Fatalf("state after end err=%v, want inactive", err)
	}
	if _, err := r.ingress.Schedule(ctx, remoteActor, remoteingress.ScheduleRequest{
		Method: remoteingress.ScheduleSet,
	}); !errors.Is(err, actorctl.ErrInactive) {
		t.Fatalf("schedule after end err=%v, want inactive", err)
	}
	if len(r.log.admitted) != 0 {
		t.Fatalf("a dead actor's work reached an organ: %v", r.log.admitted)
	}
}

// --- ④ a stale term cannot attach a body ------------------------------------
func TestStaleTermCannotAttach(t *testing.T) {
	r := newRig(t)
	g1 := currentKey(t, r.controller, remoteActor)
	if err := r.controller.AuthorizeAttach(remoteActor, g1, "daemon-1"); err != nil {
		t.Fatalf("current term attach err=%v", err)
	}
	if _, err := r.controller.Restart(context.Background(), actorctl.RestartRequest{
		ActorID: remoteActor,
	}); err != nil {
		t.Fatal(err)
	}
	if err := r.controller.AuthorizeAttach(remoteActor, g1, "daemon-1"); !errors.Is(err, actorctl.ErrStaleAttempt) {
		t.Fatalf("stale attach err=%v, want stale attempt", err)
	}
	g2 := currentKey(t, r.controller, remoteActor)
	if err := r.controller.AuthorizeAttach(remoteActor, g2, "daemon-1"); err != nil {
		t.Fatalf("new term attach err=%v", err)
	}
	// A body attaching from a daemon the record was never placed on is refused
	// on the same one verdict, not by a second policy.
	if err := r.controller.AuthorizeAttach(remoteActor, g2, "daemon-2"); err == nil {
		t.Fatal("a foreign daemon attached a placed actor")
	}
}

// --- ⑤ two Channels' ingresses are not interchangeable -----------------------
//
// Each ingress holds its own Channel's Controller and its own Channel's doors,
// so an actor of Channel A is simply not a member as far as B's ingress can
// tell. There is no shared registry for a cross-Channel call to slip through.
func TestOneIngressPerChannelCannotBeCrossed(t *testing.T) {
	ctx := context.Background()
	a, b := newRig(t), newRig(t)

	other := actor.ActorID("agent:other-channel")

	keyA := currentKey(t, a.controller, remoteActor)
	if _, err := a.ingress.Emit(ctx, remoteActor, keyA, envelope()); err != nil {
		t.Fatalf("home channel emit err=%v", err)
	}
	// B's ledger holds its own instance of that id under its own term, so A's
	// key is meaningless there.
	if _, err := b.ingress.Emit(ctx, remoteActor, keyA, envelope()); !errors.Is(err, actorctl.ErrStaleAttempt) {
		t.Fatalf("cross-channel emit err=%v, want stale attempt", err)
	}
	if _, err := b.ingress.Emit(ctx, other, keyA, envelope()); !errors.Is(err, actorctl.ErrInactive) {
		t.Fatalf("unknown id emit err=%v, want inactive", err)
	}
	// Work done through A never reached B's organs.
	b.log.mu.Lock()
	defer b.log.mu.Unlock()
	if len(b.log.admitted) != 0 {
		t.Fatalf("the other channel's organs ran: %v", b.log.admitted)
	}
}

// --- ⑥ a restart window is never half-visible --------------------------------
//
// Restart settles inside the ledger lock. The basis read carries no verdict —
// the only A/G gate is the pen's Write — so "whole" here means two things:
// the basis is always complete (Kind welded, coordinates echoing the asked
// key), and the write outcome is always a complete answer against one term
// (accepted under the current key, or refused as stale — never a write that
// half-passed against a term the ledger no longer holds).
func TestRestartWindowIsNeverHalfVisibleAtThePen(t *testing.T) {
	r := newRig(t)
	ctx := context.Background()

	var wg sync.WaitGroup
	stop := make(chan struct{})
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 200; i++ {
			if _, err := r.controller.Restart(ctx, actorctl.RestartRequest{
				ActorID: remoteActor,
			}); err != nil {
				t.Error(err)
				break
			}
		}
		close(stop)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			key, ok := keyOf(r.controller, remoteActor)
			if !ok {
				t.Error("the actor left the ledger during restart")
				return
			}
			basis, err := r.controller.PenBasis(remoteActor, key)
			if err != nil {
				t.Errorf("basis err=%v", err)
				return
			}
			if basis.Kind != actor.KindAgent ||
				basis.Run.ActorID() != remoteActor || basis.Run.AttemptKey() != key {
				t.Errorf("half-read basis: %+v", basis)
				return
			}
			// The single gate: the write either lands under a still-current
			// key or refuses whole as stale. Both are complete answers.
			result, err := r.ingress.Emit(ctx, remoteActor, key, envelope())
			if err != nil {
				if !errors.Is(err, actorctl.ErrStaleAttempt) {
					t.Errorf("emit err=%v", err)
					return
				}
				continue
			}
			if result.MessageID == "" && result.RejectReason == "" {
				t.Errorf("half answer from the pen: %+v", result)
				return
			}
		}
	}()
	wg.Wait()
}

// --- the self-lifecycle arm enters the Controller's own write door ------------
func TestSelfLifecycleEndGoesThroughTheTypedCommand(t *testing.T) {
	r := newRig(t)
	ctx := context.Background()
	key := currentKey(t, r.controller, remoteActor)
	if err := r.ingress.EndSelf(ctx, remoteActor, key, actorcaps.EndSelfRequest{
		Reason: "done",
	}); err != nil {
		t.Fatalf("end self err=%v", err)
	}
	if active, err := r.controller.IsActive(ctx, remoteActor); err != nil || active {
		t.Fatalf("actor still active after EndSelf: active=%v err=%v", active, err)
	}
}

// A remote cell asking "what alarms do I have" is answered for the identity the
// ENDPOINT authenticated, never one the frame names: ScheduleRequest carries no
// author, so the ingress mints the handle against the coordinate it was called
// with. This is the read half of the same weld Schedule/Cancel already had.
func TestRemoteScheduleListAnswersForTheEndpointIdentity(t *testing.T) {
	r := newRig(t)
	ctx := context.Background()

	response, err := r.ingress.Schedule(ctx, remoteActor, remoteingress.ScheduleRequest{
		Method: remoteingress.ScheduleList,
	})
	if err != nil {
		t.Fatalf("list err=%v", err)
	}
	if len(response.Timers) != 1 || response.Timers[0].Type != "standup" {
		t.Fatalf("timers=%+v", response.Timers)
	}
	if response.ID != "" {
		t.Fatalf("list answered a timer id %q; that field belongs to set alone", response.ID)
	}
	want := "schedule.list:" + string(remoteActor)
	r.log.mu.Lock()
	admitted := append([]string(nil), r.log.admitted...)
	r.log.mu.Unlock()
	if len(admitted) != 1 || admitted[0] != want {
		t.Fatalf("admitted=%v, want the list to run as the endpoint's identity %q", admitted, want)
	}
}
