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

// closeTestStore is a record port whose UpdateDefinition blocks until released,
// so one admitted command can be held inside the Controller while Close runs.
type closeTestStore struct {
	agent   storespec.ActorRecord
	entered chan struct{}
	release chan struct{}
}

func newCloseTestStore() *closeTestStore {
	return &closeTestStore{
		agent: storespec.ActorRecord{
			ID: "agent", Kind: actor.KindAgent,
			Definition: storespec.ActorDefinition{Class: "test"},
			Placement:  storespec.NewServerPlacement(),
		},
		entered: make(chan struct{}, 1),
		release: make(chan struct{}),
	}
}

func (s *closeTestStore) RestoreActive(context.Context) ([]storespec.ActorRecord, error) {
	return []storespec.ActorRecord{s.agent}, nil
}

func (*closeTestStore) Insert(
	context.Context,
	storespec.ActorDraft,
) (storespec.ActorRecord, error) {
	return storespec.ActorRecord{}, errors.New("unused")
}

func (s *closeTestStore) UpdateDefinition(
	ctx context.Context,
	id actor.ActorID,
	def storespec.ActorDefinition,
) (storespec.ActorRecord, error) {
	if id != s.agent.ID {
		return storespec.ActorRecord{}, actorctl.ErrInactive
	}
	select {
	case s.entered <- struct{}{}:
	default:
	}
	select {
	case <-s.release:
		updated := s.agent.Clone()
		updated.Definition = def
		return updated, nil
	case <-ctx.Done():
		return storespec.ActorRecord{}, ctx.Err()
	}
}

func (*closeTestStore) Deregister(context.Context, []actor.ActorID) error {
	return errors.New("unused")
}

func (*closeTestStore) InstallEntry(storespec.ActorRecord) {}

func TestHomeCloseTimeoutDoesNotCrossCommandOwnerAndRetryCompletes(t *testing.T) {
	store := newCloseTestStore()
	controller, err := actorctl.New(store, func() int64 { return 1 })
	if err != nil {
		t.Fatal(err)
	}
	if err := controller.Start(t.Context()); err != nil {
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
		nowMs:        func() int64 { return 1 },
	}
	home.actors = newActorSystem(home, home.logger)

	applied := make(chan error, 1)
	go func() {
		applied <- home.actors.ApplyDeclaration(
			context.Background(),
			actorctl.DeclarationChange{
				ActorID:    "agent",
				Definition: storespec.ActorDefinition{Class: "test-v2"},
			},
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
	// The ledger lock covers the whole change (commit and publication), so the
	// in-flight command owns it until it finishes; reads join afterwards.
	close(store.release)
	if err := <-applied; err != nil {
		t.Fatal(err)
	}
	if _, active, err := controller.LookupActive(context.Background(), "agent"); err != nil || !active {
		t.Fatalf("Controller was torn down across failed Quiesce: active=%v err=%v", active, err)
	}
	if err := home.closeInternalWithin("retry-test", time.Second); err != nil {
		t.Fatalf("retry Close: %v", err)
	}
	select {
	case <-home.closeDone:
	default:
		t.Fatal("retry Close did not complete runtime teardown")
	}
	if _, _, err := controller.LookupActive(context.Background(), "agent"); !errors.Is(err, actorctl.ErrClosed) {
		t.Fatalf("Controller remains live after retry Close: %v", err)
	}
}
