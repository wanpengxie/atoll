package actorctl

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/lib/actorcaps"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/runtime/actorhost"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

type fakeStore struct {
	mu     sync.Mutex
	rows   map[actor.ActorID]StoredActor
	system storespec.ActorControlRow
	forks  map[string]actor.ActorID

	restartEntered chan struct{}
	restartResume  chan struct{}
	forkEntered    chan struct{}
	forkResume     chan struct{}
}

func newFakeStore(ids ...actor.ActorID) *fakeStore {
	f := &fakeStore{
		rows:  make(map[actor.ActorID]StoredActor),
		forks: make(map[string]actor.ActorID),
		system: storespec.ActorControlRow{
			ID:                 actor.SystemActorID,
			Kind:               actor.KindSystem,
			Class:              "system",
			CurrentDeclVersion: 1,
			Placement:          storespec.NewServerPlacement(),
		},
	}
	for _, id := range ids {
		f.rows[id] = StoredActor{
			Origin: OriginDurable,
			Row: storespec.ActorControlRow{
				ID:                 id,
				Kind:               actor.KindAgent,
				Class:              "test",
				CurrentDeclVersion: 1,
				Placement:          storespec.NewServerPlacement(),
			},
		}
	}
	return f
}

func (f *fakeStore) ListDeclaredActive(context.Context) ([]storespec.ActorControlRow, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := []storespec.ActorControlRow{f.system}
	for _, value := range f.rows {
		if value.Origin == OriginDurable {
			out = append(out, value.Row)
		}
	}
	return out, nil
}

func (f *fakeStore) LookupActive(_ context.Context, id actor.ActorID) (StoredActor, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	value, ok := f.rows[id]
	return value, ok, nil
}

func (*fakeStore) Admit(context.Context, AdmitRequest) (ActorCommit[AdmitResult], error) {
	return ActorCommit[AdmitResult]{}, errors.New("unused")
}

func (*fakeStore) Introduce(context.Context, IntroduceRequest) (ActorCommit[IntroduceResult], error) {
	return ActorCommit[IntroduceResult]{}, errors.New("unused")
}

func forkKey(caller actor.ActorID, request message.ID) string {
	return string(caller) + "\x00" + string(request)
}

func (f *fakeStore) LookupFork(_ context.Context, caller actor.ActorID, request message.ID) (actor.ActorID, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	child, ok := f.forks[forkKey(caller, request)]
	return child, ok, nil
}

func (f *fakeStore) CommitFork(ctx context.Context, request ForkCommitRequest) (ForkCommitResult, error) {
	if f.forkEntered != nil {
		f.forkEntered <- struct{}{}
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
		f.rows[child] = StoredActor{
			Origin: OriginRunWorld,
			Row: storespec.ActorControlRow{
				ID:                 child,
				Kind:               request.Spec.Kind,
				Class:              request.Spec.Class,
				Config:             append([]byte(nil), request.Spec.Config...),
				CurrentDeclVersion: 1,
				Placement:          request.Placement,
			},
		}
	}
	return ForkCommitResult{ChildActorID: child, Actor: f.rows[child]}, nil
}

func (f *fakeStore) Restart(ctx context.Context, request RestartRequest) (ActorCommit[struct{}], error) {
	f.mu.Lock()
	value, ok := f.rows[request.ActorID]
	f.mu.Unlock()
	if !ok {
		return ActorCommit[struct{}]{}, ErrInactive
	}
	if f.restartEntered != nil {
		f.restartEntered <- struct{}{}
		select {
		case <-f.restartResume:
		case <-ctx.Done():
			return ActorCommit[struct{}]{}, ctx.Err()
		}
	}
	return ActorCommit[struct{}]{Actor: value}, nil
}

func (*fakeStore) ApplyDeclaration(context.Context, DeclarationChange) (ActorCommit[struct{}], error) {
	return ActorCommit[struct{}]{}, errors.New("unused")
}

func (*fakeStore) AttachDaemon(context.Context, AttachDaemonRequest) (ValueCommit[AttachDaemonResult], error) {
	return ValueCommit[AttachDaemonResult]{}, errors.New("unused")
}

func (*fakeStore) ResolveTerminal(_ context.Context, command TerminalCommand, _ []storespec.ActorControlRow) (TerminalPlan, error) {
	if command.Kind == TerminalEnd {
		return TerminalPlan{IDs: []actor.ActorID{command.End.Target}}, nil
	}
	return TerminalPlan{}, nil
}

func (f *fakeStore) CommitTerminal(_ context.Context, _ TerminalCommand, plan TerminalPlan) (ValueCommit[TerminalResult], error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, id := range plan.IDs {
		delete(f.rows, id)
	}
	return ValueCommit[TerminalResult]{
		Result: TerminalResult{Ended: append([]actor.ActorID(nil), plan.IDs...)},
	}, nil
}

func startController(t *testing.T, store *fakeStore) *Controller {
	t.Helper()
	controller, err := New(store)
	if err != nil {
		t.Fatal(err)
	}
	boot, err := controller.Start(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if boot.System.ID != actor.SystemActorID {
		t.Fatalf("system bootstrap = %q", boot.System.ID)
	}
	t.Cleanup(controller.Close)
	return controller
}

func TestControllerDoesNotRetainSystemInManagedSnapshot(t *testing.T) {
	controller := startController(t, newFakeStore("agent:a"))
	rows, err := controller.ActiveRows()
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0].ID != "agent:a" {
		t.Fatalf("managed rows = %+v", rows)
	}
	if _, ok, err := controller.Lookup(actor.SystemActorID); ok || !errors.Is(err, ErrReservedSystem) {
		t.Fatalf("system lookup ok=%v err=%v", ok, err)
	}
}

func TestPreparedRunWeldsIdentityAndRunAuthorityAtDifferentLifetimes(t *testing.T) {
	store := newFakeStore("agent:a")
	controller := startController(t, store)
	current, _, _ := controller.Lookup("agent:a")
	prepared, err := controller.PrepareRun(
		"agent:a",
		current.Desired.AttemptKey,
		current.Definition.Execution,
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := prepared.Identity().Admit(); err != nil {
		t.Fatal(err)
	}
	if err := prepared.Run().Admit(); err != nil {
		t.Fatal(err)
	}
	if _, err := controller.Restart(t.Context(), RestartRequest{ActorID: "agent:a"}); err != nil {
		t.Fatal(err)
	}
	if err := prepared.Identity().Admit(); err != nil {
		t.Fatalf("identity authority died on replacement: %v", err)
	}
	if err := prepared.Run().Admit(); !errors.Is(err, ErrStaleAttempt) {
		t.Fatalf("old run authority err=%v", err)
	}
}

func TestIdentityAdmissionIsOneCoherentActorIDOnlySnapshot(t *testing.T) {
	store := newFakeStore("agent:a")
	controller := startController(t, store)
	before, ok, err := controller.AdmitIdentity(t.Context(), "agent:a")
	if err != nil || !ok || !before.Valid() {
		t.Fatalf("initial admission=(%+v,%v,%v)", before, ok, err)
	}
	if before.Row.ID != "agent:a" || before.World != storespec.WorldDurable {
		t.Fatalf("initial admission=%+v", before)
	}

	if _, err := controller.Restart(t.Context(), RestartRequest{ActorID: "agent:a"}); err != nil {
		t.Fatal(err)
	}
	after, ok, err := controller.AdmitIdentity(t.Context(), "agent:a")
	if err != nil || !ok || !after.Valid() {
		t.Fatalf("post-restart admission=(%+v,%v,%v)", after, ok, err)
	}
	if after.Row.ID != before.Row.ID || after.World != before.World {
		t.Fatalf("ActorID admission changed across G replacement: before=%+v after=%+v", before, after)
	}

	if _, err := controller.End(t.Context(), EndRequest{Target: "agent:a"}); err != nil {
		t.Fatal(err)
	}
	if admission, ok, err := controller.AdmitIdentity(t.Context(), "agent:a"); err != nil || ok || admission.Valid() {
		t.Fatalf("ended admission=(%+v,%v,%v)", admission, ok, err)
	}
}

func TestEndSelfAdmissionLinearizesInsideControllerGate(t *testing.T) {
	store := newFakeStore("agent:a")
	store.restartEntered = make(chan struct{}, 1)
	store.restartResume = make(chan struct{})
	controller := startController(t, store)
	g1, _, _ := controller.Lookup("agent:a")

	restartDone := make(chan error, 1)
	go func() {
		_, err := controller.Restart(context.Background(), RestartRequest{ActorID: "agent:a"})
		restartDone <- err
	}()
	<-store.restartEntered

	endDone := make(chan error, 1)
	go func() {
		_, err := controller.End(context.Background(), EndRequest{
			CallerActorID: "agent:a",
			CallerAttempt: g1.Desired.AttemptKey,
			Target:        "agent:a",
		})
		endDone <- err
	}()
	select {
	case err := <-endDone:
		t.Fatalf("End crossed the Controller gate while Restart was publishing: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	close(store.restartResume)
	if err := <-restartDone; err != nil {
		t.Fatal(err)
	}
	if err := <-endDone; !errors.Is(err, ErrStaleAttempt) {
		t.Fatalf("stale G1 End err=%v", err)
	}
	if _, ok, err := controller.Lookup("agent:a"); err != nil || !ok {
		t.Fatalf("successor was removed ok=%v err=%v", ok, err)
	}
}

func TestForkReleasesCallerGateAfterAdmission(t *testing.T) {
	store := newFakeStore("agent:a")
	store.forkEntered = make(chan struct{}, 1)
	store.forkResume = make(chan struct{})
	controller := startController(t, store)
	g1, _, _ := controller.Lookup("agent:a")

	forkDone := make(chan error, 1)
	go func() {
		_, err := controller.Fork(context.Background(), ForkRequest{
			CallerActorID: "agent:a",
			CallerAttempt: g1.Desired.AttemptKey,
			RequestID:     "fork-1",
			Spec:          actorcaps.ForkSpec{Kind: actor.KindAgent, Class: "child"},
		})
		forkDone <- err
	}()
	<-store.forkEntered

	restartDone := make(chan error, 1)
	go func() {
		_, err := controller.Restart(context.Background(), RestartRequest{ActorID: "agent:a"})
		restartDone <- err
	}()
	select {
	case err := <-restartDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("Restart was blocked by an already-admitted Fork")
	}
	close(store.forkResume)
	if err := <-forkDone; err != nil {
		t.Fatal(err)
	}
}

func TestControllerDifferentActorMutationsAreRaceSafe(t *testing.T) {
	store := newFakeStore("agent:a", "agent:b")
	controller := startController(t, store)
	var wg sync.WaitGroup
	for _, id := range []actor.ActorID{"agent:a", "agent:b"} {
		id := id
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 100 {
				if _, err := controller.Restart(context.Background(), RestartRequest{ActorID: id}); err != nil {
					t.Errorf("restart %s: %v", id, err)
					return
				}
			}
		}()
	}
	wg.Wait()
}

func TestQuiesceJoinsAdmittedCommand(t *testing.T) {
	store := newFakeStore("agent:a")
	store.restartEntered = make(chan struct{}, 1)
	store.restartResume = make(chan struct{})
	controller := startController(t, store)
	go func() {
		_, _ = controller.Restart(context.Background(), RestartRequest{ActorID: "agent:a"})
	}()
	<-store.restartEntered

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := controller.Quiesce(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Quiesce err=%v", err)
	}
	close(store.restartResume)
	if err := controller.Quiesce(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestAttemptKeysAreOpaqueUUIDv7Values(t *testing.T) {
	controller := startController(t, newFakeStore("agent:a"))
	value, _, _ := controller.Lookup("agent:a")
	if _, err := actorhost.ParseAttemptKey(string(value.Desired.AttemptKey)); err != nil {
		t.Fatal(err)
	}
}
