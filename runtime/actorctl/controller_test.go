package actorctl

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/wanpengxie/atoll/lib/actorcaps"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/runtime/actorhost"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

// fakeRecordStore is the record port a Controller drives. It speaks only record
// language — there is no authorization coordinate anywhere in it.
type fakeRecordStore struct {
	mu       sync.Mutex
	restored []storespec.ActorRecord
	durable  map[actor.ActorID]storespec.ActorRecord
	entries  map[actor.ActorID]storespec.ActorRecord
	nextID   int
	insertN  int
}

func newFakeRecordStore(restored ...storespec.ActorRecord) *fakeRecordStore {
	return &fakeRecordStore{
		restored: restored,
		durable:  make(map[actor.ActorID]storespec.ActorRecord),
		entries:  make(map[actor.ActorID]storespec.ActorRecord),
	}
}

func (s *fakeRecordStore) RestoreActive(context.Context) ([]storespec.ActorRecord, error) {
	out := make([]storespec.ActorRecord, len(s.restored))
	for i, record := range s.restored {
		out[i] = record.Clone()
		s.durable[record.ID] = record.Clone()
	}
	return out, nil
}

func (s *fakeRecordStore) Insert(
	_ context.Context,
	draft storespec.ActorDraft,
) (storespec.ActorRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.insertN++
	// Semantic-key replay: an existing active record wins.
	for _, record := range s.durable {
		if draft.SourceDeclID != "" && record.SourceDeclID == draft.SourceDeclID {
			return record.Clone(), nil
		}
		if draft.Principal != "" && record.Principal == draft.Principal && record.Kind == draft.Kind {
			return record.Clone(), nil
		}
	}
	s.nextID++
	id := draft.ID
	if id == "" {
		id = actor.ActorID(string(draft.Kind) + ":minted:" + string(rune('a'+s.nextID)))
	}
	record := storespec.ActorRecord{
		ID: id, Kind: draft.Kind, Principal: draft.Principal,
		SourceDeclID: draft.SourceDeclID, CreatedAt: draft.CreatedAt,
		Definition: draft.Definition.Clone(), Placement: draft.Placement,
	}
	s.durable[id] = record.Clone()
	return record.Clone(), nil
}

func (s *fakeRecordStore) UpdateDefinition(
	_ context.Context,
	id actor.ActorID,
	def storespec.ActorDefinition,
) (storespec.ActorRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	record, ok := s.durable[id]
	if !ok {
		return storespec.ActorRecord{}, storespec.ErrActorNotFound
	}
	record.Definition = def.Clone()
	s.durable[id] = record.Clone()
	return record.Clone(), nil
}

func (s *fakeRecordStore) Deregister(_ context.Context, ids []actor.ActorID) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, id := range ids {
		delete(s.durable, id)
		delete(s.entries, id)
	}
	return nil
}

func (s *fakeRecordStore) InstallEntry(record storespec.ActorRecord) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.entries[record.ID]; exists {
		panic("entry already installed")
	}
	s.entries[record.ID] = record.Clone()
}

func newTestController(t *testing.T, store Store) *Controller {
	t.Helper()
	controller, err := New(store, func() int64 { return 1 })
	if err != nil {
		t.Fatal(err)
	}
	if err := controller.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	return controller
}

func seedParent(t *testing.T) (*Controller, *fakeRecordStore, actor.ActorID) {
	t.Helper()
	store := newFakeRecordStore(storespec.ActorRecord{
		ID: "agent:parent", Kind: actor.KindAgent, SourceDeclID: "decl:parent",
		Definition: storespec.ActorDefinition{Class: "agent"},
		Placement:  storespec.NewServerPlacement(),
	})
	return newTestController(t, store), store, "agent:parent"
}

func currentAttempt(t *testing.T, c *Controller, id actor.ActorID) string {
	t.Helper()
	value, ok, err := c.lookup(id)
	if err != nil || !ok {
		t.Fatalf("lookup %q: ok=%v err=%v", id, ok, err)
	}
	return string(value.Attempt)
}

// --- fork replay table -------------------------------------------------------

func TestForkReplayReturnsTheFirstChildForever(t *testing.T) {
	ctx := context.Background()
	controller, store, parent := seedParent(t)
	attempt := currentAttempt(t, controller, parent)

	request := ForkRequest{
		CallerActorID: parent,
		CallerAttempt: attemptKeyOf(attempt),
		RequestID:     "req-1",
		Spec:          actorcaps.ForkSpec{Kind: actor.KindAgent, Class: "worker"},
	}
	first, err := controller.Fork(ctx, request)
	if err != nil {
		t.Fatalf("first fork: %v", err)
	}
	child := first.Result.ChildActorID
	if child == "" {
		t.Fatal("fork produced no child")
	}

	// A retry inside the same process returns the first result, never a second
	// child.
	second, err := controller.Fork(ctx, request)
	if err != nil {
		t.Fatalf("retry fork: %v", err)
	}
	if second.Result.ChildActorID != child {
		t.Fatalf("retry child=%q want %q", second.Result.ChildActorID, child)
	}

	// Even after the child is terminated, the same RequestID still answers with
	// the original id: one request can never produce two live children.
	if _, err := controller.End(ctx, EndRequest{Target: child, CallerActorID: child}); err != nil {
		t.Fatalf("end child: %v", err)
	}
	third, err := controller.Fork(ctx, request)
	if err != nil {
		t.Fatalf("post-terminal fork: %v", err)
	}
	if third.Result.ChildActorID != child {
		t.Fatalf("post-terminal child=%q want %q", third.Result.ChildActorID, child)
	}
	if active, _ := controller.IsActive(ctx, child); active {
		t.Fatal("the replayed id must not resurrect the dead child")
	}

	// The replay table is never pruned.
	controller.ledger.RLock()
	rows := len(controller.forks)
	controller.ledger.RUnlock()
	if rows != 1 {
		t.Fatalf("fork replay rows=%d want 1 (never pruned)", rows)
	}
	if len(store.entries) != 0 {
		t.Fatalf("terminated entry survived: %v", store.entries)
	}
}

func TestForkReplayRejectsADifferentPayload(t *testing.T) {
	ctx := context.Background()
	controller, _, parent := seedParent(t)
	attempt := attemptKeyOf(currentAttempt(t, controller, parent))

	if _, err := controller.Fork(ctx, ForkRequest{
		CallerActorID: parent, CallerAttempt: attempt, RequestID: "req-1",
		Spec: actorcaps.ForkSpec{Kind: actor.KindAgent, Class: "worker"},
	}); err != nil {
		t.Fatalf("first fork: %v", err)
	}
	if _, err := controller.Fork(ctx, ForkRequest{
		CallerActorID: parent, CallerAttempt: attempt, RequestID: "req-1",
		Spec: actorcaps.ForkSpec{Kind: actor.KindAgent, Class: "other"},
	}); !errors.Is(err, ErrForkConflict) {
		t.Fatalf("conflicting replay err=%v want ErrForkConflict", err)
	}
}

func TestForkLeavesNoDurableFootprint(t *testing.T) {
	ctx := context.Background()
	controller, store, parent := seedParent(t)
	attempt := attemptKeyOf(currentAttempt(t, controller, parent))
	before := store.insertN

	result, err := controller.Fork(ctx, ForkRequest{
		CallerActorID: parent, CallerAttempt: attempt, RequestID: "req-1",
		Spec: actorcaps.ForkSpec{Kind: actor.KindAgent, Class: "worker"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if store.insertN != before {
		t.Fatal("fork wrote a durable row")
	}
	if _, ok := store.entries[result.Result.ChildActorID]; !ok {
		t.Fatal("fork child was not installed into the entry table")
	}
}

// --- restart / declaration ---------------------------------------------------

func TestRestartIsEdgeTriggeredAndTouchesNoRecord(t *testing.T) {
	ctx := context.Background()
	controller, store, parent := seedParent(t)
	first := currentAttempt(t, controller, parent)
	before := store.durable[parent]

	for range 2 {
		if _, err := controller.Restart(ctx, RestartRequest{ActorID: parent}); err != nil {
			t.Fatalf("restart: %v", err)
		}
	}
	second := currentAttempt(t, controller, parent)
	if second == first {
		t.Fatal("restart must mint a new term every call")
	}
	if store.durable[parent].Definition.Class != before.Definition.Class {
		t.Fatal("restart changed the record")
	}
}

func TestApplyDeclarationEqualValueIsANoOp(t *testing.T) {
	ctx := context.Background()
	controller, _, parent := seedParent(t)
	attempt := currentAttempt(t, controller, parent)

	if err := mustApply(controller, ctx, parent, "agent", nil); err != nil {
		t.Fatal(err)
	}
	if currentAttempt(t, controller, parent) != attempt {
		t.Fatal("an equal definition must not mint a new term")
	}
	if err := mustApply(controller, ctx, parent, "agent-v2", nil); err != nil {
		t.Fatal(err)
	}
	if currentAttempt(t, controller, parent) == attempt {
		t.Fatal("a changed definition must mint a new term")
	}
}

func mustApply(c *Controller, ctx context.Context, id actor.ActorID, class string, config []byte) error {
	_, err := c.ApplyDeclaration(ctx, DeclarationChange{
		ActorID:    id,
		Definition: storespec.ActorDefinition{Class: class, Config: config},
	})
	return err
}

// --- terminal ----------------------------------------------------------------

func TestTerminalSetIsExactlyTheExplicitTarget(t *testing.T) {
	ctx := context.Background()
	controller, _, parent := seedParent(t)
	attempt := attemptKeyOf(currentAttempt(t, controller, parent))
	child, err := controller.Fork(ctx, ForkRequest{
		CallerActorID: parent, CallerAttempt: attempt, RequestID: "req-1",
		Spec: actorcaps.ForkSpec{Kind: actor.KindAgent, Class: "worker"},
	})
	if err != nil {
		t.Fatal(err)
	}

	// Ending the parent must NOT spread to its fork: there is no lineage.
	transition, err := controller.End(ctx, EndRequest{Target: parent, CallerActorID: parent})
	if err != nil {
		t.Fatal(err)
	}
	if len(transition.Result.Ended) != 1 || transition.Result.Ended[0] != parent {
		t.Fatalf("terminal set=%v want exactly the target", transition.Result.Ended)
	}
	if active, _ := controller.IsActive(ctx, child.Result.ChildActorID); !active {
		t.Fatal("the fork child was cascaded away with its parent")
	}
}

func TestEndOnlyAcceptsTheTargetOrTheSystemFace(t *testing.T) {
	ctx := context.Background()
	controller, _, parent := seedParent(t)
	if _, err := controller.End(ctx, EndRequest{
		Target: parent, CallerActorID: "agent:stranger",
	}); !errors.Is(err, ErrEndForbidden) {
		t.Fatalf("stranger End err=%v want ErrEndForbidden", err)
	}
	if _, err := controller.End(ctx, EndRequest{
		Target: parent, CallerActorID: actor.SystemActorID,
	}); err != nil {
		t.Fatalf("system face End: %v", err)
	}
}

// --- kernel is not a member --------------------------------------------------

func TestKernelIsNeverAMember(t *testing.T) {
	ctx := context.Background()
	controller, _, _ := seedParent(t)
	if active, err := controller.IsActive(ctx, actor.SystemActorID); err != nil || active {
		t.Fatalf("kernel active=%v err=%v; it has no record", active, err)
	}
	// A lifecycle command aimed at the kernel finds no member and takes the
	// ordinary verdict — there is no reserved-system branch to hit.
	if _, err := controller.Restart(ctx, RestartRequest{ActorID: actor.SystemActorID}); !errors.Is(err, ErrInactive) {
		t.Fatalf("kernel restart err=%v want the ordinary inactive verdict", err)
	}
}

// --- deep copy ---------------------------------------------------------------

func TestRecordHandoffCopiesConfig(t *testing.T) {
	ctx := context.Background()
	store := newFakeRecordStore(storespec.ActorRecord{
		ID: "agent:a", Kind: actor.KindAgent, SourceDeclID: "decl:a",
		Definition: storespec.ActorDefinition{Class: "agent", Config: []byte(`{"n":1}`)},
		Placement:  storespec.NewServerPlacement(),
	})
	controller := newTestController(t, store)

	record, ok, err := controller.LookupActive(ctx, "agent:a")
	if err != nil || !ok {
		t.Fatal(err)
	}
	record.Definition.Config[2] = 'X'
	again, _, _ := controller.LookupActive(ctx, "agent:a")
	if string(again.Definition.Config) != `{"n":1}` {
		t.Fatalf("mutating a handed-out record reached the ledger: %s", again.Definition.Config)
	}
}

func attemptKeyOf(raw string) actorhost.AttemptKey { return actorhost.AttemptKey(raw) }
