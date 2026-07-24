package home

import (
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/runtime/actorctl"
	"github.com/wanpengxie/atoll/runtime/actorhost"
	"github.com/wanpengxie/atoll/runtime/actorrt"
	"github.com/wanpengxie/atoll/runtime/storespec"
	"github.com/wanpengxie/atoll/runtime/systemkernel"
)

type closeTestActor struct{}

func (closeTestActor) Receive(context.Context, *message.Envelope) error { return nil }

type closeTestStore struct {
	system  storespec.ActorControlRow
	agent   actorctl.StoredActor
	entered chan struct{}
	release chan struct{}
}

func newCloseTestStore() *closeTestStore {
	return &closeTestStore{
		system: storespec.ActorControlRow{
			ID: actor.SystemActorID, Kind: actor.KindSystem, Class: "system",
			CurrentDeclVersion: 1, Placement: storespec.NewServerPlacement(),
		},
		agent: actorctl.StoredActor{
			Origin: actorctl.OriginDurable,
			Row: storespec.ActorControlRow{
				ID: "agent", Kind: actor.KindAgent, Class: "test",
				CurrentDeclVersion: 1, Placement: storespec.NewServerPlacement(),
			},
		},
		entered: make(chan struct{}, 1),
		release: make(chan struct{}),
	}
}

func (s *closeTestStore) ListDeclaredActive(context.Context) ([]storespec.ActorControlRow, error) {
	return []storespec.ActorControlRow{s.system, s.agent.Row}, nil
}

func (s *closeTestStore) LookupActive(
	_ context.Context,
	id actor.ActorID,
) (actorctl.StoredActor, bool, error) {
	return s.agent, id == s.agent.Row.ID, nil
}

func (*closeTestStore) Admit(
	context.Context,
	actorctl.AdmitRequest,
) (actorctl.ActorCommit[actorctl.AdmitResult], error) {
	return actorctl.ActorCommit[actorctl.AdmitResult]{}, errors.New("unused")
}

func (*closeTestStore) Introduce(
	context.Context,
	actorctl.IntroduceRequest,
) (actorctl.ActorCommit[actorctl.IntroduceResult], error) {
	return actorctl.ActorCommit[actorctl.IntroduceResult]{}, errors.New("unused")
}

func (*closeTestStore) LookupFork(
	context.Context,
	actor.ActorID,
	message.ID,
) (actor.ActorID, bool, error) {
	return "", false, errors.New("unused")
}

func (*closeTestStore) CommitFork(
	context.Context,
	actorctl.ForkCommitRequest,
) (actorctl.ForkCommitResult, error) {
	return actorctl.ForkCommitResult{}, errors.New("unused")
}

func (s *closeTestStore) Restart(
	ctx context.Context,
	request actorctl.RestartRequest,
) (actorctl.ActorCommit[struct{}], error) {
	if request.ActorID != s.agent.Row.ID {
		return actorctl.ActorCommit[struct{}]{}, actorctl.ErrInactive
	}
	select {
	case s.entered <- struct{}{}:
	default:
	}
	select {
	case <-s.release:
		return actorctl.ActorCommit[struct{}]{Actor: s.agent}, nil
	case <-ctx.Done():
		return actorctl.ActorCommit[struct{}]{}, ctx.Err()
	}
}

func (*closeTestStore) ApplyDeclaration(
	context.Context,
	actorctl.DeclarationChange,
) (actorctl.ActorCommit[struct{}], error) {
	return actorctl.ActorCommit[struct{}]{}, errors.New("unused")
}

func (*closeTestStore) AttachDaemon(
	context.Context,
	actorctl.AttachDaemonRequest,
) (actorctl.ValueCommit[actorctl.AttachDaemonResult], error) {
	return actorctl.ValueCommit[actorctl.AttachDaemonResult]{}, errors.New("unused")
}

func (*closeTestStore) ResolveTerminal(
	context.Context,
	actorctl.TerminalCommand,
	[]storespec.ActorControlRow,
) (actorctl.TerminalPlan, error) {
	return actorctl.TerminalPlan{}, errors.New("unused")
}

func (*closeTestStore) CommitTerminal(
	context.Context,
	actorctl.TerminalCommand,
	actorctl.TerminalPlan,
) (actorctl.ValueCommit[actorctl.TerminalResult], error) {
	return actorctl.ValueCommit[actorctl.TerminalResult]{}, errors.New("unused")
}

func TestHomeCloseTimeoutDoesNotCrossCommandOwnerAndRetryCompletes(t *testing.T) {
	store := newCloseTestStore()
	controller, err := actorctl.New(store)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := controller.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	host, err := actorhost.New(actorhost.Config{
		Domain:       "server",
		PollInterval: time.Millisecond,
		BodyBuilder:  func(actorhost.BodyBuildInput) actorrt.Actor { return closeTestActor{} },
	})
	if err != nil {
		t.Fatal(err)
	}
	home := &Home{
		controller:   controller,
		serverHost:   host,
		systemKernel: systemkernel.New(),
		closeDone:    make(chan struct{}),
		logger:       slog.New(slog.DiscardHandler),
	}
	home.actors = newActorSystem(home, home.logger)

	restarted := make(chan error, 1)
	go func() {
		restarted <- home.actors.Restart(
			context.Background(),
			actorctl.RestartRequest{ActorID: "agent"},
		)
	}()
	<-store.entered

	if err := home.closeInternalWithin("timeout-test", time.Millisecond); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("first Close error=%v, want deadline exceeded", err)
	}
	select {
	case <-home.closeDone:
		t.Fatal("Home consumed runtime teardown while command owner was not drained")
	default:
	}
	if _, active, err := controller.Lookup("agent"); err != nil || !active {
		t.Fatalf("Controller was torn down across failed Quiesce: active=%v err=%v", active, err)
	}

	close(store.release)
	if err := <-restarted; err != nil {
		t.Fatal(err)
	}
	if err := home.closeInternalWithin("retry-test", time.Second); err != nil {
		t.Fatalf("retry Close: %v", err)
	}
	select {
	case <-home.closeDone:
	default:
		t.Fatal("retry Close did not complete runtime teardown")
	}
	if _, _, err := controller.Lookup("agent"); !errors.Is(err, actorctl.ErrClosed) {
		t.Fatalf("Controller remains live after retry Close: %v", err)
	}
}
