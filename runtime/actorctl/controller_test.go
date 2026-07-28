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
	updateN  int
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
	// The real registry mints inside the insert transaction and no draft can ask
	// for a name; this stand-in does the same.
	id := actor.ActorID(string(draft.Kind) + ":minted:" + string(rune('a'+s.nextID)))
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
	s.updateN++
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
	c.ledger.RLock()
	defer c.ledger.RUnlock()
	value, ok := c.actors[id]
	if !ok {
		t.Fatalf("actor %q is not in the ledger", id)
	}
	return string(value.Attempt)
}

// --- fork replay table -------------------------------------------------------

// Humans are born by admission alone: a fork has no principal source, so a
// human child would be a member the human roster cannot recognize — refused
// at the mint point, mirroring the durable side's "human ⇔ principal" weld.
func TestForkRefusesAHumanChild(t *testing.T) {
	ctx := context.Background()
	controller, _, parent := seedParent(t)

	_, err := controller.Fork(ctx, ForkRequest{
		CallerActorID: parent,
		CallerAttempt: attemptKeyOf(currentAttempt(t, controller, parent)),
		RequestID:     "req-human",
		Spec:          actorcaps.ForkSpec{Kind: actor.KindHuman, Class: "whatever"},
	})
	if !errors.Is(err, ErrForkInvalid) {
		t.Fatalf("err=%v, want ErrForkInvalid", err)
	}
}

// The verdict is the door's first step: a stale term is refused before the
// replay table is consulted, while the current term retrying the same
// RequestID still lands on the replay row.
func TestForkReplayRequiresACurrentCaller(t *testing.T) {
	ctx := context.Background()
	controller, _, parent := seedParent(t)
	g1 := attemptKeyOf(currentAttempt(t, controller, parent))

	request := ForkRequest{
		CallerActorID: parent,
		CallerAttempt: g1,
		RequestID:     "req-stale",
		Spec:          actorcaps.ForkSpec{Kind: actor.KindAgent, Class: "worker"},
	}
	first, err := controller.Fork(ctx, request)
	if err != nil {
		t.Fatalf("first fork: %v", err)
	}

	if _, err := controller.Restart(ctx, RestartRequest{ActorID: parent}); err != nil {
		t.Fatalf("restart: %v", err)
	}

	// The old term replays: refused at the gate, not answered from the table.
	if _, err := controller.Fork(ctx, request); !errors.Is(err, ErrStaleAttempt) {
		t.Fatalf("stale replay err=%v, want ErrStaleAttempt", err)
	}

	// The current term replays the same RequestID: same child, no second one.
	request.CallerAttempt = attemptKeyOf(currentAttempt(t, controller, parent))
	replay, err := controller.Fork(ctx, request)
	if err != nil {
		t.Fatalf("current replay: %v", err)
	}
	if replay.Result.ChildActorID != first.Result.ChildActorID {
		t.Fatalf("replay child=%q want %q",
			replay.Result.ChildActorID, first.Result.ChildActorID)
	}
}

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
	if _, err := controller.End(ctx, EndRequest{
		Target: child, CallerActorID: child,
		CallerAttempt: attemptKeyOf(currentAttempt(t, controller, child)),
	}); err != nil {
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

// The equal-value short circuit is the ONE home of equality: two consecutive
// reconcile rounds over an unchanged declaration write no row and mint no term,
// so the 30-second pull loop can never produce a term storm.
func TestApplyDeclarationEqualValueIsANoOp(t *testing.T) {
	ctx := context.Background()
	controller, store, parent := seedParent(t)
	attempt := currentAttempt(t, controller, parent)

	for round := range 2 {
		if err := mustApply(controller, ctx, parent, "agent", nil); err != nil {
			t.Fatal(err)
		}
		if currentAttempt(t, controller, parent) != attempt {
			t.Fatalf("round %d: an equal definition minted a new term", round)
		}
	}
	store.mu.Lock()
	updates := store.updateN
	store.mu.Unlock()
	if updates != 0 {
		t.Fatalf("equal definition wrote the row %d times, want 0", updates)
	}

	if err := mustApply(controller, ctx, parent, "agent-v2", nil); err != nil {
		t.Fatal(err)
	}
	if currentAttempt(t, controller, parent) == attempt {
		t.Fatal("a changed definition must mint a new term")
	}
	store.mu.Lock()
	updates = store.updateN
	store.mu.Unlock()
	if updates != 1 {
		t.Fatalf("changed definition wrote the row %d times, want 1", updates)
	}
}

// --- narrow projections ------------------------------------------------------

// Every public projection is question-shaped: identity roster, declaration
// instances, identity facts. None of them hands out a record, and none carries
// a field the asking question does not need.
func TestNarrowProjectionsAnswerOneQuestionEach(t *testing.T) {
	ctx := context.Background()
	store := newFakeRecordStore(
		storespec.ActorRecord{
			ID: "agent:a", Kind: actor.KindAgent, SourceDeclID: "decl:x",
			Definition: storespec.ActorDefinition{Class: "agent"},
			Placement:  storespec.NewServerPlacement(),
		},
		storespec.ActorRecord{
			ID: "agent:b", Kind: actor.KindAgent, SourceDeclID: "decl:x",
			Definition: storespec.ActorDefinition{Class: "agent"},
			Placement:  storespec.NewServerPlacement(),
		},
		storespec.ActorRecord{
			ID: "human:c", Kind: actor.KindHuman, Principal: "carol",
			Definition: storespec.ActorDefinition{Class: "human"},
			Placement:  storespec.NewServerPlacement(),
		},
	)
	controller := newTestController(t, store)

	identities, err := controller.ActiveIdentities()
	if err != nil || len(identities) != 3 {
		t.Fatalf("active identities=%+v err=%v", identities, err)
	}
	if identities[0].ID != "agent:a" || identities[2].ID != "human:c" {
		t.Fatalf("active identities are not in canonical id order: %+v", identities)
	}

	instances, err := controller.DeclaredInstances("decl:x")
	if err != nil || len(instances) != 2 ||
		instances[0] != "agent:a" || instances[1] != "agent:b" {
		t.Fatalf("declared instances=%v err=%v", instances, err)
	}
	// A human carries no declaration source, so no declaration owns it.
	if instances, err := controller.DeclaredInstances(""); err != nil || len(instances) != 0 {
		t.Fatalf("empty decl id matched %v err=%v", instances, err)
	}
	if instances, err := controller.DeclaredInstances("decl:absent"); err != nil || len(instances) != 0 {
		t.Fatalf("absent decl id matched %v err=%v", instances, err)
	}

	facts, found, err := controller.ActorFacts(ctx, "human:c")
	if err != nil || !found || facts.Kind != actor.KindHuman || facts.Principal != "carol" {
		t.Fatalf("actor facts=%+v found=%v err=%v", facts, found, err)
	}
	if _, found, err := controller.ActorFacts(ctx, actor.SystemActorID); err != nil || found {
		t.Fatalf("kernel answered identity facts: found=%v err=%v", found, err)
	}

	// ResolvePrincipal is ActorFacts' principal read backwards, and it answers
	// off the same ledger — the Platform door no longer holds a registry face to
	// ask instead.
	if id, found, err := controller.ResolvePrincipal("carol"); err != nil || !found || id != "human:c" {
		t.Fatalf("resolve principal carol=(%s,%v,%v)", id, found, err)
	}
	if id, found, err := controller.ResolvePrincipal("nobody"); err != nil || found {
		t.Fatalf("an unknown principal resolved: (%s,%v,%v)", id, found, err)
	}
	// Both agents above carry no principal. Asking for nothing must not hand one
	// of them back — the same empty-matches-empty hole the introduction verdict
	// had to close.
	if id, found, err := controller.ResolvePrincipal(""); err != nil || found {
		t.Fatalf("the empty principal resolved to %q (found=%v err=%v)", id, found, err)
	}

	// The declaration reconcile list is the pull loop's own comparison input:
	// it carries the definition, the roster deliberately does not.
	declared, err := controller.DeclaredReconcileList()
	if err != nil || len(declared) != 2 {
		t.Fatalf("declared reconcile list=%+v err=%v", declared, err)
	}
	if declared[0].Definition.Class != "agent" || declared[0].SourceDeclID != "decl:x" {
		t.Fatalf("declared reconcile entry=%+v", declared[0])
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
	transition, err := controller.End(ctx, EndRequest{
		Target: parent, CallerActorID: parent, CallerAttempt: attempt,
	})
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

// End is self-termination and nothing else. The caller proves its own current
// term first and may only name itself as the target — in that order, the same
// one every other self-acting arm uses.
func TestEndOnlyAcceptsTheTargetItself(t *testing.T) {
	ctx := context.Background()
	controller, _, parent := seedParent(t)
	parentAttempt := attemptKeyOf(currentAttempt(t, controller, parent))

	// A member in good standing, acting as its current term, aimed at someone
	// else. This is the case ErrEndForbidden exists for: identity is proven, the
	// permission is what is missing.
	sibling, err := controller.Fork(ctx, ForkRequest{
		CallerActorID: parent, CallerAttempt: parentAttempt, RequestID: "req-sibling",
		Spec: actorcaps.ForkSpec{Kind: actor.KindAgent, Class: "worker"},
	})
	if err != nil {
		t.Fatal(err)
	}
	siblingID := sibling.Result.ChildActorID
	if _, err := controller.End(ctx, EndRequest{
		Target: parent, CallerActorID: siblingID,
		CallerAttempt: attemptKeyOf(currentAttempt(t, controller, siblingID)),
	}); !errors.Is(err, ErrEndForbidden) {
		t.Fatalf("a member ending someone else err=%v want ErrEndForbidden", err)
	}

	// A caller who is nobody is refused before the permission question is even
	// reached: it can present no current term.
	if _, err := controller.End(ctx, EndRequest{
		Target: parent, CallerActorID: "agent:stranger",
		CallerAttempt: parentAttempt,
	}); !errors.Is(err, ErrInactive) {
		t.Fatalf("stranger End err=%v want ErrInactive", err)
	}

	// The zero-value request. Both gates used to be sentinel-skipped by exactly
	// this shape, and it ended the target.
	if _, err := controller.End(ctx, EndRequest{Target: parent}); err == nil {
		t.Fatal("a zero-value EndRequest ended the target")
	}
	// The same, one field at a time: neither omission may read as authority.
	if _, err := controller.End(ctx, EndRequest{
		Target: parent, CallerActorID: parent,
	}); !errors.Is(err, ErrStaleAttempt) {
		t.Fatalf("End with no attempt err=%v want ErrStaleAttempt", err)
	}
	if _, err := controller.End(ctx, EndRequest{
		Target: parent, CallerAttempt: parentAttempt,
	}); !errors.Is(err, ErrInactive) {
		t.Fatalf("End with no caller err=%v want ErrInactive", err)
	}

	// The system face holds no actor record, so it can present no current term.
	// Removal by anyone other than the target is TerminalRemove's business.
	if _, err := controller.End(ctx, EndRequest{
		Target: parent, CallerActorID: actor.SystemActorID,
		CallerAttempt: parentAttempt,
	}); err == nil {
		t.Fatal("the system face ended an actor through End")
	}

	// And the target itself, presenting its own term, still succeeds.
	if _, err := controller.End(ctx, EndRequest{
		Target: parent, CallerActorID: parent, CallerAttempt: parentAttempt,
	}); err != nil {
		t.Fatalf("the target ending itself: %v", err)
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
	store := newFakeRecordStore(storespec.ActorRecord{
		ID: "agent:a", Kind: actor.KindAgent, SourceDeclID: "decl:a",
		Definition: storespec.ActorDefinition{Class: "agent", Config: []byte(`{"n":1}`)},
		Placement:  storespec.NewServerPlacement(),
	})
	controller := newTestController(t, store)

	// The only projections that hand out a Config alias are the declaration
	// reconcile list and the execution-domain desired level; both must copy.
	instances, err := controller.DeclaredReconcileList()
	if err != nil || len(instances) != 1 {
		t.Fatalf("declared reconcile list: n=%d err=%v", len(instances), err)
	}
	instances[0].Definition.Config[2] = 'X'

	desired, err := controller.DesiredFor("server", "server")
	if err != nil || len(desired) != 1 {
		t.Fatalf("desired: n=%d err=%v", len(desired), err)
	}
	body, ok := desired[0].(actorhost.BodyDesired)
	if !ok {
		t.Fatalf("desired[0] is %T, want BodyDesired", desired[0])
	}
	if string(body.ExecutionSpec.Config) != `{"n":1}` {
		t.Fatalf("mutating a handed-out projection reached the ledger: %s", body.ExecutionSpec.Config)
	}
	body.ExecutionSpec.Config[2] = 'Y'

	again, err := controller.DeclaredReconcileList()
	if err != nil || len(again) != 1 {
		t.Fatalf("declared reconcile list: n=%d err=%v", len(again), err)
	}
	if string(again[0].Definition.Config) != `{"n":1}` {
		t.Fatalf("mutating a handed-out projection reached the ledger: %s", again[0].Definition.Config)
	}
}

func attemptKeyOf(raw string) actorhost.AttemptKey { return actorhost.AttemptKey(raw) }
