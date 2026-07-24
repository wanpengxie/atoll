package actorctl

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/lib/actorcaps"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/runtime/actorhost"
	"github.com/wanpengxie/atoll/runtime/actorrt"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

type inertActor struct{}

func (inertActor) Receive(context.Context, *message.Envelope) error { return nil }

type fakeEffects struct {
	fatal chan error
}

func (f *fakeEffects) PlanPoke(actorhost.ExecutionDomain) {}
func (f *fakeEffects) ApplyPostCommit(storespec.PostCommitEffects) {
}
func (f *fakeEffects) RunActorBorn(actor.ActorID) error { return nil }
func (f *fakeEffects) RunActorsEnded([]actor.ActorID)   {}
func (f *fakeEffects) Fatal(err error) {
	select {
	case f.fatal <- err:
	default:
	}
}

type fakeStore struct {
	mu                sync.Mutex
	rows              map[actor.ActorID]StoredActor
	system            storespec.ActorControlRow
	forks             map[string]actor.ActorID
	forkEnter         chan struct{}
	forkResume        chan struct{}
	forkCommitted     chan struct{}
	forkPublishResume chan struct{}
	resolveSequence   [][]actor.ActorID
	resolveCalls      int
	committedPlans    [][]actor.ActorID
}

func newFakeStore(ids ...actor.ActorID) *fakeStore {
	f := &fakeStore{
		rows:  make(map[actor.ActorID]StoredActor),
		forks: make(map[string]actor.ActorID),
		system: storespec.ActorControlRow{
			ID: actor.SystemActorID, Kind: actor.KindSystem, Class: "system",
			CurrentDeclVersion: 1, Placement: storespec.NewServerPlacement(),
		},
	}
	for _, id := range ids {
		f.rows[id] = StoredActor{Origin: OriginDurable, Row: storespec.ActorControlRow{
			ID: id, Kind: actor.KindAgent, Class: "test", CurrentDeclVersion: 1,
			Placement: storespec.NewServerPlacement(),
		}}
	}
	return f
}

func (f *fakeStore) ListDeclaredActive(context.Context) ([]storespec.ActorControlRow, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := []storespec.ActorControlRow{f.system}
	for _, row := range f.rows {
		if row.Origin == OriginDurable {
			out = append(out, row.Row)
		}
	}
	return out, nil
}

func (f *fakeStore) LookupActive(
	_ context.Context,
	id actor.ActorID,
) (StoredActor, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	row, ok := f.rows[id]
	return row, ok, nil
}

func (f *fakeStore) Admit(context.Context, AdmitRequest) (ActorCommit[AdmitResult], error) {
	return ActorCommit[AdmitResult]{}, errors.New("unused")
}

func (f *fakeStore) Introduce(context.Context, IntroduceRequest) (ActorCommit[IntroduceResult], error) {
	return ActorCommit[IntroduceResult]{}, errors.New("unused")
}

func forkKey(caller actor.ActorID, request message.ID) string {
	return string(caller) + "\x00" + string(request)
}

func (f *fakeStore) LookupFork(
	_ context.Context,
	caller actor.ActorID,
	request message.ID,
) (actor.ActorID, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	child, ok := f.forks[forkKey(caller, request)]
	return child, ok, nil
}

func (f *fakeStore) CommitFork(
	ctx context.Context,
	request ForkCommitRequest,
) (ForkCommitResult, error) {
	if f.forkEnter != nil {
		select {
		case f.forkEnter <- struct{}{}:
		default:
		}
		select {
		case <-f.forkResume:
		case <-ctx.Done():
			return ForkCommitResult{}, ctx.Err()
		}
	}
	f.mu.Lock()
	key := forkKey(request.CallerActorID, request.RequestID)
	child := f.forks[key]
	if child == "" {
		child = request.ChildActorID
		f.forks[key] = child
		f.rows[child] = StoredActor{Origin: OriginRunWorld, Row: storespec.ActorControlRow{
			ID: child, Kind: request.Spec.Kind, Class: request.Spec.Class,
			Config: append([]byte(nil), request.Spec.Config...), CurrentDeclVersion: 1,
			Placement: request.Placement,
		}}
	}
	result := ForkCommitResult{ChildActorID: child, Actor: f.rows[child]}
	f.mu.Unlock()
	if f.forkCommitted != nil {
		select {
		case f.forkCommitted <- struct{}{}:
		default:
		}
		select {
		case <-f.forkPublishResume:
		case <-ctx.Done():
			return ForkCommitResult{}, ctx.Err()
		}
	}
	return result, nil
}

func (f *fakeStore) Restart(
	_ context.Context,
	request RestartRequest,
) (ActorCommit[struct{}], error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	row, ok := f.rows[request.ActorID]
	if !ok {
		return ActorCommit[struct{}]{}, ErrInactive
	}
	return ActorCommit[struct{}]{Actor: row}, nil
}

func (f *fakeStore) ApplyDeclaration(
	context.Context,
	DeclarationChange,
) (ActorCommit[struct{}], error) {
	return ActorCommit[struct{}]{}, errors.New("unused")
}

func (f *fakeStore) AttachDaemon(
	context.Context,
	AttachDaemonRequest,
) (ValueCommit[AttachDaemonResult], error) {
	return ValueCommit[AttachDaemonResult]{}, errors.New("unused")
}

func (f *fakeStore) ResolveTerminal(
	_ context.Context,
	command TerminalCommand,
	_ []storespec.ActorControlRow,
) (TerminalPlan, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.resolveSequence) != 0 {
		index := f.resolveCalls
		if index >= len(f.resolveSequence) {
			index = len(f.resolveSequence) - 1
		}
		f.resolveCalls++
		return TerminalPlan{IDs: append([]actor.ActorID(nil), f.resolveSequence[index]...)}, nil
	}
	switch command.Kind {
	case TerminalEnd:
		return TerminalPlan{IDs: []actor.ActorID{command.End.Target}}, nil
	case TerminalRemove:
		return TerminalPlan{IDs: []actor.ActorID{command.Remove.Target}}, nil
	default:
		return TerminalPlan{}, nil
	}
}

func (f *fakeStore) CommitTerminal(
	_ context.Context,
	_ TerminalCommand,
	plan TerminalPlan,
) (ValueCommit[TerminalResult], error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	ids := append([]actor.ActorID(nil), plan.IDs...)
	f.committedPlans = append(f.committedPlans, ids)
	for _, id := range ids {
		delete(f.rows, id)
	}
	return ValueCommit[TerminalResult]{
		Result: TerminalResult{Ended: ids},
	}, nil
}

func prepareSystem(t *testing.T) *actorrt.Unit {
	t.Helper()
	return prepareSystemWithActor(t, inertActor{})
}

func prepareSystemWithActor(t *testing.T, impl actorrt.Actor) *actorrt.Unit {
	t.Helper()
	unit, err := actorrt.Prepare(actorrt.UnitConfig{
		ActorID: actor.SystemActorID,
		Kind:    actor.KindSystem,
	}, func(actorrt.Incarnation) actorrt.Actor {
		return impl
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	return unit
}

func newActors(t *testing.T, store *fakeStore, effects Effects) *ChannelActors {
	t.Helper()
	return newActorsWithBuilder(t, store, effects, func(
		ManagedBodyInput,
		actorcaps.Caps,
	) actorrt.Actor {
		return inertActor{}
	})
}

func newActorsWithBuilder(
	t *testing.T,
	store *fakeStore,
	effects Effects,
	build ManagedBodyBuilder,
) *ChannelActors {
	t.Helper()
	actors, err := NewChannelActors(Config{
		Store:        store,
		Effects:      effects,
		ServerDomain: "server",
		ServerHost: actorhost.Config{
			PollInterval: time.Millisecond,
		},
		BuildManagedBody: build,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := actors.Start(context.Background(), prepareSystem(t)); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = actors.Close(ctx)
	})
	return actors
}

func waitUntil(t *testing.T, message string, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for !condition() {
		if time.Now().After(deadline) {
			t.Fatal(message)
		}
		time.Sleep(time.Millisecond)
	}
}

func TestServerDesiredTickerFreshReadsControllerAndHealsStaleLKG(t *testing.T) {
	store := newFakeStore("agent")
	actors, err := NewChannelActors(Config{
		Store:        store,
		ServerDomain: "server",
		ServerHost: actorhost.Config{
			PollInterval: 50 * time.Millisecond,
		},
		BuildManagedBody: func(
			ManagedBodyInput,
			actorcaps.Caps,
		) actorrt.Actor {
			return inertActor{}
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := actors.Start(context.Background(), prepareSystem(t)); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = actors.Close(ctx)
	})

	stale, err := actors.controller.desiredFor("server", "server")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := actors.Remove(context.Background(), RemoveRequest{Target: "agent"}); err != nil {
		t.Fatal(err)
	}
	waitUntil(t, "Server desired did not observe terminal Controller truth", func() bool {
		snapshot, ok := actors.host.Inspect("agent")
		return !ok || snapshot.Desired == nil
	})

	// Simulate an obsolete producer corrupting the Host LKG after terminal.
	// No command wake follows this acceptance, so only the periodic fresh read
	// can repair it.
	if err := actors.host.AcceptFullDesired(stale); err != nil {
		t.Fatal(err)
	}
	snapshot, ok := actors.host.Inspect("agent")
	if !ok || snapshot.Desired == nil {
		t.Fatal("test failed to install the stale Host desired")
	}
	waitUntil(t, "periodic Server read did not remove stale terminal desired", func() bool {
		snapshot, ok := actors.host.Inspect("agent")
		return !ok || snapshot.Desired == nil
	})
}

func TestConcurrentControllerCommandsConvergeServerDesiredToFreshLevel(t *testing.T) {
	const count = 24
	ids := make([]actor.ActorID, count)
	for index := range ids {
		ids[index] = actor.ActorID(fmt.Sprintf("runner-agent-%02d", index))
	}
	actors := newActors(t, newFakeStore(ids...), nil)

	var wg sync.WaitGroup
	for _, id := range ids {
		id := id
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 8 {
				if err := actors.Restart(
					context.Background(), RestartRequest{ActorID: id},
				); err != nil {
					t.Errorf("Restart(%s): %v", id, err)
					return
				}
			}
		}()
	}
	wg.Wait()

	waitUntil(t, "Server Host did not converge to the current Controller level", func() bool {
		for _, id := range ids {
			current, ok, err := actors.controller.lookup(id)
			if err != nil || !ok {
				return false
			}
			snapshot, ok := actors.host.Inspect(id)
			if !ok || snapshot.Desired == nil ||
				snapshot.Desired.Attempt() != current.Desired.AttemptKey {
				return false
			}
		}
		return true
	})
}

func TestControllerSharedContainerDifferentIDRace(t *testing.T) {
	const count = 32
	ids := make([]actor.ActorID, count)
	for i := range ids {
		ids[i] = actor.ActorID(fmt.Sprintf("agent-%02d", i))
	}
	actors := newActors(t, newFakeStore(ids...), nil)
	var wg sync.WaitGroup
	for _, id := range ids {
		_, ok, err := actors.controller.lookup(id)
		if err != nil || !ok {
			t.Fatalf("lookup %s: %v %v", id, ok, err)
		}
		wg.Add(2)
		go func(id actor.ActorID) {
			defer wg.Done()
			_ = actors.Restart(context.Background(), RestartRequest{ActorID: id})
		}(id)
		go func() {
			defer wg.Done()
			_, _ = actors.ListActive(context.Background())
		}()
	}
	wg.Wait()
	if _, err := actors.ListActive(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestLockActorSetUsesOneCanonicalByteOrder(t *testing.T) {
	input := []actor.ActorID{"z", "a", "m", "a"}
	got := canonicalActorIDs(input)
	want := []actor.ActorID{"a", "m", "z"}
	if !slices.Equal(got, want) {
		t.Fatalf("canonical ids = %v, want %v", got, want)
	}
}

func TestLockActorSetOppositeInputsCannotDeadlock(t *testing.T) {
	var gates controlGates
	start := make(chan struct{})
	done := make(chan struct{}, 2)
	for _, ids := range [][]actor.ActorID{{"a", "b"}, {"b", "a"}} {
		ids := ids
		go func() {
			<-start
			unlock := gates.lockActorSet(ids)
			unlock()
			done <- struct{}{}
		}()
	}
	close(start)
	for range 2 {
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatal("opposite multi-gate inputs deadlocked")
		}
	}
}

func TestForkStorePauseDoesNotHoldCallerGateAndQuiesceJoins(t *testing.T) {
	store := newFakeStore("caller")
	store.forkEnter = make(chan struct{}, 1)
	store.forkResume = make(chan struct{})
	actors := newActors(t, store, nil)
	before, _, _ := actors.controller.lookup("caller")
	forkDone := make(chan error, 1)
	go func() {
		_, err := actors.Fork(context.Background(), ForkRequest{
			CallerActorID: "caller",
			CallerAttempt: before.Desired.AttemptKey,
			RequestID:     "fork-1",
			Spec: actorcaps.ForkSpec{
				Kind: actor.KindAgent, Class: "test", NameHint: "child",
				Placement: &channel.Placement{Kind: channel.PlacementServer},
			},
		})
		forkDone <- err
	}()
	<-store.forkEnter

	restarted := make(chan error, 1)
	go func() { restarted <- actors.Restart(context.Background(), RestartRequest{ActorID: "caller"}) }()
	select {
	case err := <-restarted:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("Fork held caller gate across Store")
	}

	quiesced := make(chan error, 1)
	go func() { quiesced <- actors.Quiesce(context.Background()) }()
	select {
	case err := <-quiesced:
		t.Fatalf("Quiesce crossed admitted Fork: %v", err)
	case <-time.After(25 * time.Millisecond):
	}
	close(store.forkResume)
	if err := <-forkDone; err != nil {
		t.Fatal(err)
	}
	if err := <-quiesced; err != nil {
		t.Fatal(err)
	}
}

func TestUnexpectedSystemDoneFailsChannel(t *testing.T) {
	store := newFakeStore()
	effects := &fakeEffects{fatal: make(chan error, 1)}
	actors := newActors(t, store, effects)
	actors.kernel.unit.Stop()
	select {
	case err := <-effects.fatal:
		if err == nil {
			t.Fatal("nil fatal cause")
		}
	case <-time.After(time.Second):
		t.Fatal("unexpected SystemKernel Done did not fail-stop")
	}
}

func TestStaleLifecycleRejectedAfterDirectReplacementButActorAuthorityRemains(t *testing.T) {
	store := newFakeStore("agent")
	actors := newActors(t, store, nil)
	g1, _, _ := actors.controller.lookup("agent")
	if err := actors.Restart(context.Background(), RestartRequest{ActorID: "agent"}); err != nil {
		t.Fatal(err)
	}
	g2, _, _ := actors.controller.lookup("agent")
	if g2.Desired.AttemptKey == g1.Desired.AttemptKey {
		t.Fatal("Restart did not publish a direct successor")
	}
	if _, err := actors.RemoteFork(
		context.Background(), "agent", g1.Desired.AttemptKey, "stale-fork",
		actorcaps.ForkSpec{Kind: actor.KindAgent, Class: "test"},
	); !errors.Is(err, ErrStaleAttempt) {
		t.Fatalf("stale G1 Fork error=%v", err)
	}
	if err := actors.RemoteEndSelf(
		context.Background(), "agent", g1.Desired.AttemptKey,
		actorcaps.EndSelfRequest{Reason: "stale"},
	); !errors.Is(err, ErrStaleAttempt) {
		t.Fatalf("stale G1 EndSelf error=%v", err)
	}
	current, _, _ := actors.controller.lookup("agent")
	if current.Desired.AttemptKey != g2.Desired.AttemptKey {
		t.Fatalf("stale lifecycle changed successor: %#v", current.Desired)
	}
	if err := actors.AuthorActive("agent"); err != nil {
		t.Fatalf("collaboration ActorID authority was incorrectly attempt-fenced: %v", err)
	}
}

func TestConcurrentForkSameRequestIDPublishesOneChild(t *testing.T) {
	store := newFakeStore("caller")
	actors := newActors(t, store, nil)
	caller, _, _ := actors.controller.lookup("caller")
	const callers = 16
	results := make(chan ForkResult, callers)
	errs := make(chan error, callers)
	var wg sync.WaitGroup
	for range callers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			result, err := actors.Fork(context.Background(), ForkRequest{
				CallerActorID: "caller",
				CallerAttempt: caller.Desired.AttemptKey,
				RequestID:     "same-fork-operation",
				Spec: actorcaps.ForkSpec{
					Kind: actor.KindAgent, Class: "test", NameHint: "child",
				},
			})
			results <- result
			errs <- err
		}()
	}
	wg.Wait()
	close(results)
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	var child actor.ActorID
	for result := range results {
		if child == "" {
			child = result.ChildActorID
		}
		if result.ChildActorID != child {
			t.Fatalf("same Fork operation returned %q and %q", child, result.ChildActorID)
		}
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	runWorld := 0
	for _, row := range store.rows {
		if row.Origin == OriginRunWorld {
			runWorld++
		}
	}
	if runWorld != 1 || len(store.forks) != 1 {
		t.Fatalf("run-world children=%d fork anchors=%d, want 1/1", runWorld, len(store.forks))
	}
}

func TestTerminalClosureChangeReleasesAndRetriesCanonicalSet(t *testing.T) {
	tests := []struct {
		name     string
		sequence [][]actor.ActorID
		ended    []actor.ActorID
	}{
		{
			name: "expand",
			sequence: [][]actor.ActorID{
				{"b"}, {"b", "a"}, {"a", "b"}, {"b", "a"},
			},
			ended: []actor.ActorID{"a", "b"},
		},
		{
			name: "shrink",
			sequence: [][]actor.ActorID{
				{"a", "b"}, {"b"}, {"b"}, {"b"},
			},
			ended: []actor.ActorID{"b"},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			store := newFakeStore("a", "b")
			store.resolveSequence = tc.sequence
			actors := newActors(t, store, nil)
			result, err := actors.Remove(context.Background(), RemoveRequest{Target: "b"})
			if err != nil {
				t.Fatal(err)
			}
			_ = result
			store.mu.Lock()
			defer store.mu.Unlock()
			if store.resolveCalls != 4 || len(store.committedPlans) != 1 {
				t.Fatalf("resolve calls=%d committed=%d", store.resolveCalls, len(store.committedPlans))
			}
			if !slices.Equal(store.committedPlans[0], tc.ended) {
				t.Fatalf("committed plan=%v, want canonical %v", store.committedPlans[0], tc.ended)
			}
		})
	}
}

func TestQuiesceWaitsForForkCommittedBeforeControllerPublication(t *testing.T) {
	store := newFakeStore("caller")
	store.forkCommitted = make(chan struct{}, 1)
	store.forkPublishResume = make(chan struct{})
	actors := newActors(t, store, nil)
	caller, _, _ := actors.controller.lookup("caller")
	forkDone := make(chan error, 1)
	go func() {
		_, err := actors.Fork(context.Background(), ForkRequest{
			CallerActorID: "caller",
			CallerAttempt: caller.Desired.AttemptKey,
			RequestID:     "committed-before-publication",
			Spec: actorcaps.ForkSpec{
				Kind: actor.KindAgent, Class: "test", NameHint: "child",
			},
		})
		forkDone <- err
	}()
	<-store.forkCommitted

	quiesced := make(chan error, 1)
	go func() { quiesced <- actors.Quiesce(context.Background()) }()
	select {
	case err := <-quiesced:
		t.Fatalf("Quiesce crossed committed command before publication: %v", err)
	case <-time.After(25 * time.Millisecond):
	}
	close(store.forkPublishResume)
	if err := <-forkDone; err != nil {
		t.Fatal(err)
	}
	if err := <-quiesced; err != nil {
		t.Fatal(err)
	}
	store.mu.Lock()
	child := store.forks[forkKey("caller", "committed-before-publication")]
	store.mu.Unlock()
	if _, ok, err := actors.Lookup(child); err != nil || !ok {
		t.Fatalf("admitted Fork did not publish before Quiesce returned: ok=%v err=%v", ok, err)
	}
}

type countingActor struct{ receives atomic.Int64 }

func (a *countingActor) Receive(context.Context, *message.Envelope) error {
	a.receives.Add(1)
	return nil
}

func TestReplacementDoesNotReplayCollaborationMessage(t *testing.T) {
	store := newFakeStore("agent")
	var builtMu sync.Mutex
	var built []*countingActor
	actors := newActorsWithBuilder(t, store, nil, func(
		ManagedBodyInput,
		actorcaps.Caps,
	) actorrt.Actor {
		body := &countingActor{}
		builtMu.Lock()
		built = append(built, body)
		builtMu.Unlock()
		return body
	})
	deadline := time.Now().Add(time.Second)
	for {
		if _, ok := actors.Stat("agent"); ok {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("initial body did not publish")
		}
		time.Sleep(time.Millisecond)
	}
	if err := actors.Restart(context.Background(), RestartRequest{ActorID: "agent"}); err != nil {
		t.Fatal(err)
	}
	deadline = time.Now().Add(time.Second)
	for {
		builtMu.Lock()
		count := len(built)
		builtMu.Unlock()
		if count >= 2 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("successor body did not build")
		}
		time.Sleep(time.Millisecond)
	}
	builtMu.Lock()
	defer builtMu.Unlock()
	for index, body := range built {
		if got := body.receives.Load(); got != 0 {
			t.Fatalf("body %d received %d lifecycle-triggered replay(s)", index, got)
		}
	}
}

type stopOrderActor struct {
	label string
	mu    *sync.Mutex
	order *[]string
}

func (*stopOrderActor) Receive(context.Context, *message.Envelope) error { return nil }
func (a *stopOrderActor) Stop(context.Context) error {
	a.mu.Lock()
	*a.order = append(*a.order, a.label)
	a.mu.Unlock()
	return nil
}

func TestSystemKernelValidationDoubleStartAndCloseLast(t *testing.T) {
	store := newFakeStore("managed")
	var orderMu sync.Mutex
	var order []string
	actors, err := NewChannelActors(Config{
		Store:        store,
		ServerDomain: "server",
		ServerHost:   actorhost.Config{PollInterval: time.Millisecond},
		BuildManagedBody: func(
			ManagedBodyInput,
			actorcaps.Caps,
		) actorrt.Actor {
			return &stopOrderActor{label: "managed", mu: &orderMu, order: &order}
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	invalid, err := actorrt.Prepare(actorrt.UnitConfig{
		ActorID: "not-system", Kind: actor.KindAgent,
	}, func(actorrt.Incarnation) actorrt.Actor { return inertActor{} }, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := actors.Start(context.Background(), invalid); !errors.Is(err, ErrInvalidKernel) {
		t.Fatalf("invalid kernel error=%v", err)
	}
	invalid.Stop()
	<-invalid.Done()

	kernel := prepareSystemWithActor(t, &stopOrderActor{
		label: "kernel", mu: &orderMu, order: &order,
	})
	if err := actors.Start(context.Background(), kernel); err != nil {
		t.Fatal(err)
	}
	second := prepareSystem(t)
	if err := actors.Start(context.Background(), second); !errors.Is(err, ErrAlreadyStarted) {
		t.Fatalf("double Start error=%v", err)
	}
	second.Stop()
	<-second.Done()
	deadline := time.Now().Add(time.Second)
	for {
		if _, ok := actors.Stat("managed"); ok {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("managed body did not publish")
		}
		time.Sleep(time.Millisecond)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := actors.Close(ctx); err != nil {
		t.Fatal(err)
	}
	orderMu.Lock()
	defer orderMu.Unlock()
	if !slices.Equal(order, []string{"managed", "kernel"}) {
		t.Fatalf("stop order=%v, want managed then kernel", order)
	}
}
