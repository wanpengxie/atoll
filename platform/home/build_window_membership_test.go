package home

import (
	"context"
	"encoding/json"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/lib/actorbase"
	"github.com/wanpengxie/atoll/platform"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/runtime/actorhost"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

const (
	buildWindowClass    = "build-window-park"
	buildWindowDoomed   = "decl-build-window-doomed"
	buildWindowSurvivor = "decl-build-window-survivor"
	// buildWindowSettle bounds the negative observation "the voided build never
	// became a live body". It is deliberately generous: the positive control in
	// the same test has already proved the released builds are being processed.
	buildWindowSettle = 3 * time.Second
)

// buildWindowFixture parks the class lookup — the innermost step of a body
// build, run before the Unit is published — so a test can hold a build open and
// move membership truth underneath it.
type buildWindowFixture struct {
	mu      sync.Mutex
	arms    int
	started map[actor.ActorID]int

	entered  chan actor.ActorID
	release  chan struct{}
	stop     chan struct{}
	unparks  sync.Once
	stopOnce sync.Once
}

func newBuildWindowFixture(arms int) *buildWindowFixture {
	return &buildWindowFixture{
		arms:    arms,
		started: make(map[actor.ActorID]int),
		entered: make(chan actor.ActorID, 8),
		release: make(chan struct{}),
		stop:    make(chan struct{}),
	}
}

func (f *buildWindowFixture) unpark()   { f.unparks.Do(func() { close(f.release) }) }
func (f *buildWindowFixture) stopBody() { f.stopOnce.Do(func() { close(f.stop) }) }

func (f *buildWindowFixture) arm(n int) {
	f.mu.Lock()
	f.arms = n
	f.mu.Unlock()
}

func (f *buildWindowFixture) startCount(id actor.ActorID) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.started[id]
}

func (f *buildWindowFixture) BuildClass(
	_ channel.ID, id actor.ActorID, class string, _ json.RawMessage,
) (platform.ActorFactory, bool) {
	if class != buildWindowClass {
		return platform.ActorFactory{}, false
	}
	f.mu.Lock()
	park := f.arms > 0
	if park {
		f.arms--
	}
	f.mu.Unlock()
	if park {
		f.entered <- id
		<-f.release
	}
	return platform.ActorFactory{Proc: actorbase.Def{New: func() (actorbase.Proc, error) {
		return func(sys actorbase.Sys) error {
			f.mu.Lock()
			f.started[sys.Self()]++
			f.mu.Unlock()
			select {
			case <-f.stop:
			case <-sys.Life().Done():
			}
			return nil
		}, nil
	}}}, true
}

func buildWindowDeclaration(source string) DeclareRequest {
	return DeclareRequest{
		SourceDeclID: source, Kind: actor.KindAgent, Class: buildWindowClass,
		Placement: storespec.NewServerPlacement(), CreatedAt: time.Now().UnixMilli(),
	}
}

func openBuildWindowHome(
	t *testing.T,
	name string,
	fixture *buildWindowFixture,
	sources ...string,
) *Home {
	t.Helper()
	declarations := make([]DeclareRequest, 0, len(sources))
	for _, source := range sources {
		declarations = append(declarations, buildWindowDeclaration(source))
	}
	h, err := Open(Config{
		ChannelID:             channel.ID(name),
		DBPath:                filepath.Join(t.TempDir(), "channel.sqlite"),
		CompositionResolver:   fixture,
		IntroductionResolver:  inertIntroductionResolver{},
		ReconcileInterval:     time.Hour,
		Bootstrap:             true,
		BootstrapDeclarations: declarations,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = h.closeInternal("test") })
	return h
}

func buildWindowInstance(t *testing.T, h *Home, source string) actor.ActorID {
	t.Helper()
	instances, err := activeMembersForSource(h.controller, source)
	if err != nil || len(instances) != 1 {
		t.Fatalf("declaration %q instances=%v err=%v", source, instances, err)
	}
	return instances[0]
}

// buildWindowNeverPublished is the whole verdict of both tests below: within the
// settle window, the parked build's result never became this actor's live body.
func buildWindowNeverPublished(t *testing.T, h *Home, id actor.ActorID) {
	t.Helper()
	published := lifecycleObserveWithin(buildWindowSettle, func() bool {
		snapshot, ok := h.serverHost.Inspect(id)
		return ok && snapshot.Actual == actorhost.ActualBody
	})
	if published {
		t.Fatalf("%s: a build voided by removal was published as the live body", id)
	}
	if _, live := h.actors.Stat(id); live {
		t.Fatalf("%s: a removed member is addressable as a live actor", id)
	}
	if active, err := h.controller.IsActive(context.Background(), id); err != nil || active {
		t.Fatalf("%s: removed member active=%v err=%v", id, active, err)
	}
}

// T20, path 1 — the ordinary reconcile build. A member declared at boot has its
// body under construction when it is removed from the channel. The build result
// is a Unit that nobody may ever run: it must be retired unpublished, and its
// Proc must never draw a breath. The survivor declared beside it is the control
// — same fixture, same park, same release — so "nothing started" cannot be an
// artefact of the harness.
func TestBuildVoidedByRemovalIsNeverPublished_ReconcilePath(t *testing.T) {
	fixture := newBuildWindowFixture(2)
	h := openBuildWindowHome(t, "build-window-reconcile", fixture,
		buildWindowDoomed, buildWindowSurvivor)
	ctx := context.Background()

	parked := map[actor.ActorID]struct{}{}
	for range 2 {
		parked[restartRecv(t, "a body build to park in the class table", fixture.entered)] = struct{}{}
	}
	doomed := buildWindowInstance(t, h, buildWindowDoomed)
	survivor := buildWindowInstance(t, h, buildWindowSurvivor)
	for _, id := range []actor.ActorID{doomed, survivor} {
		if _, ok := parked[id]; !ok {
			t.Fatalf("%s never entered its build window: parked=%v", id, parked)
		}
	}

	// Membership truth moves while both builds are held open.
	if _, err := h.opEntry.remove(ctx, removeRequest{
		Target: doomed, InitiatorActorID: doomed,
	}); err != nil {
		t.Fatalf("remove the member under construction: %v", err)
	}
	restartEventually(t, "the host to drop the removed member's desired row", func() bool {
		_, ok := h.serverHost.Inspect(doomed)
		return !ok
	})

	fixture.unpark()

	// The control settles first: the released builds ARE being processed.
	restartEventually(t, "the surviving member's body to start", func() bool {
		return fixture.startCount(survivor) == 1
	})
	buildWindowNeverPublished(t, h, doomed)

	// Closing the Home joins the body-builder group, so after this line the
	// voided build has provably finished deciding what to do with its Unit.
	if err := h.closeInternal("test-build-window"); err != nil {
		t.Fatalf("close after a voided build: %v", err)
	}
	if starts := fixture.startCount(doomed); starts != 0 {
		t.Fatalf("the voided build ran its Proc %d times", starts)
	}
	if starts := fixture.startCount(survivor); starts != 1 {
		t.Fatalf("the surviving member started %d times, want 1", starts)
	}
}

// T20, path 2 — the replacement build. The same window opens a second time
// whenever a live body exits and the level is still desired, and that rebuild is
// exactly as capable of outliving its member's membership. Removing the member
// mid-rebuild must void that result too: the actor's Proc ran once, in its first
// life, and never again.
func TestBuildVoidedByRemovalIsNeverPublished_RebuildAfterExitPath(t *testing.T) {
	fixture := newBuildWindowFixture(0)
	h := openBuildWindowHome(t, "build-window-rebuild", fixture, buildWindowDoomed)
	ctx := context.Background()
	doomed := buildWindowInstance(t, h, buildWindowDoomed)

	restartEventually(t, "the first incarnation to start", func() bool {
		return fixture.startCount(doomed) == 1
	})

	// Arm the window, then let the running body exit: the level is unchanged, so
	// the host immediately builds a replacement — and parks in it.
	fixture.arm(1)
	fixture.stopBody()
	if got := restartRecv(t, "the replacement build to park", fixture.entered); got != doomed {
		t.Fatalf("the replacement build parked for %s, want %s", got, doomed)
	}

	if _, err := h.opEntry.remove(ctx, removeRequest{
		Target: doomed, InitiatorActorID: doomed,
	}); err != nil {
		t.Fatalf("remove the member under reconstruction: %v", err)
	}
	restartEventually(t, "the host to drop the removed member's desired row", func() bool {
		_, ok := h.serverHost.Inspect(doomed)
		return !ok
	})

	fixture.unpark()
	buildWindowNeverPublished(t, h, doomed)

	if err := h.closeInternal("test-build-window"); err != nil {
		t.Fatalf("close after a voided rebuild: %v", err)
	}
	if starts := fixture.startCount(doomed); starts != 1 {
		t.Fatalf("the member's Proc ran %d times, want exactly its one first life", starts)
	}
}
