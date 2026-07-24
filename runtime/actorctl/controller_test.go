package actorctl

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
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

func (f *fakeEffects) WakeDomain(actorhost.ExecutionDomain) {}
func (f *fakeEffects) PlanPoke(actorhost.ExecutionDomain)   {}
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
	mu         sync.Mutex
	rows       map[actor.ActorID]StoredActor
	system     storespec.ActorControlRow
	forks      map[string]actor.ActorID
	forkEnter  chan struct{}
	forkResume chan struct{}
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
	defer f.mu.Unlock()
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
	return ForkCommitResult{ChildActorID: child, Actor: f.rows[child]}, nil
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
	context.Context,
	TerminalCommand,
	[]storespec.ActorControlRow,
) (TerminalPlan, error) {
	return TerminalPlan{}, errors.New("unused")
}

func (f *fakeStore) CommitTerminal(
	context.Context,
	TerminalCommand,
	TerminalPlan,
) (ValueCommit[TerminalResult], error) {
	return ValueCommit[TerminalResult]{}, errors.New("unused")
}

func prepareSystem(t *testing.T) *actorrt.Unit {
	t.Helper()
	unit, err := actorrt.Prepare(actorrt.UnitConfig{
		ActorID: actor.SystemActorID,
		Kind:    actor.KindSystem,
	}, func(actorrt.Incarnation) actorrt.Actor {
		return inertActor{}
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	return unit
}

func newActors(t *testing.T, store *fakeStore, effects Effects) *ChannelActors {
	t.Helper()
	actors, err := NewChannelActors(Config{
		Store:        store,
		Effects:      effects,
		ServerDomain: "server",
		ServerHost: actorhost.Config{
			PollInterval: time.Millisecond,
		},
		BuildManagedBody: func(
			actorhost.BodyBuildInput,
			actorcaps.LifecycleHandle,
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
	return actors
}

func TestControllerSharedContainerDifferentIDRace(t *testing.T) {
	const count = 32
	ids := make([]actor.ActorID, count)
	for i := range ids {
		ids[i] = actor.ActorID(fmt.Sprintf("agent-%02d", i))
	}
	actors := newActors(t, newFakeStore(ids...), nil)
	var wg sync.WaitGroup
	for i, id := range ids {
		value, ok, err := actors.controller.lookup(id)
		if err != nil || !ok {
			t.Fatalf("lookup %s: %v %v", id, ok, err)
		}
		wg.Add(3)
		go func(id actor.ActorID, key actorhost.AttemptKey) {
			defer wg.Done()
			_ = actors.requestIdle(context.Background(), id, key)
		}(id, value.Desired.AttemptKey)
		go func(id actor.ActorID) {
			defer wg.Done()
			_, _ = actors.EnsureRun(id)
		}(id)
		go func() {
			defer wg.Done()
			_, _ = actors.ListActive(context.Background())
		}()
		_ = i
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
