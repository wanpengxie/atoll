package home

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/platform/channelspec"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/runtime/actorctl"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

const (
	declPullClass  = "routing-live"
	declPullSource = "decl-pull-gates"
	declPullSeed   = `{"model":"v1"}`
)

// declPullSpace is a fully programmable stand-in for the space half of the
// declaration pull loop. Every verdict that ends a round WITHOUT touching truth
// — the resolver could not answer, the declaration is gone, the class registry
// could not answer, the class is unknown, the class disagrees with the record's
// kind — is a value this type can be moved to between rounds.
type declPullSpace struct {
	mu         sync.Mutex
	resolveErr error
	class      string
	config     string
	kind       actor.Kind
	kindFound  bool
	kindErr    error
	resolves   int
	classKinds int

	// The park half. When armed, the FIRST resolve announces itself on entered
	// and blocks until release is closed. It deliberately ignores the caller's
	// ctx: the point is a resolve that is genuinely still in flight while truth
	// moves underneath it, and a stub that gave up when the loop's own resolve
	// budget expired would be testing the budget instead.
	armed   bool
	entered chan struct{}
	release chan struct{}
	unparks sync.Once
}

func newDeclPullSpace() *declPullSpace {
	return &declPullSpace{
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}
}

func (r *declPullSpace) ResolveDeclaration(
	context.Context, channel.ID, string,
) (channelspec.DeclarationFacts, error) {
	r.mu.Lock()
	r.resolves++
	park := r.armed
	r.armed = false
	resolveErr, class, config := r.resolveErr, r.class, r.config
	r.mu.Unlock()
	if park {
		close(r.entered)
		<-r.release
	}
	if resolveErr != nil {
		return channelspec.DeclarationFacts{}, resolveErr
	}
	return channelspec.DeclarationFacts{
		Visibility: "public", Class: class, Config: json.RawMessage(config),
	}, nil
}

func (r *declPullSpace) ClassKind(context.Context, string) (actor.Kind, bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.classKinds++
	return r.kind, r.kindFound, r.kindErr
}
func (r *declPullSpace) ClassPlacement(context.Context, string) (channelspec.PlacementKind, bool, error) {
	return channelspec.PlacementServer, true, nil
}
func (r *declPullSpace) AdmitIntroduction(context.Context, channel.ID, channelspec.DeclarationFacts) error {
	return nil
}

func (r *declPullSpace) setFacts(class, config string) {
	r.mu.Lock()
	r.class, r.config = class, config
	r.mu.Unlock()
}

func (r *declPullSpace) setResolveErr(err error) {
	r.mu.Lock()
	r.resolveErr = err
	r.mu.Unlock()
}

func (r *declPullSpace) setClassKind(kind actor.Kind, found bool, err error) {
	r.mu.Lock()
	r.kind, r.kindFound, r.kindErr = kind, found, err
	r.mu.Unlock()
}

func (r *declPullSpace) arm() {
	r.mu.Lock()
	r.armed = true
	r.mu.Unlock()
}

func (r *declPullSpace) unpark() { r.unparks.Do(func() { close(r.release) }) }

func (r *declPullSpace) counts() (resolves, classKinds int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.resolves, r.classKinds
}

func declPullDeclaration() DeclareRequest {
	config := json.RawMessage(declPullSeed)
	return DeclareRequest{
		SourceDeclID: declPullSource, Seed: declPullSource, Kind: actor.KindAgent, Class: declPullClass,
		Config: &config, Placement: storespec.NewServerPlacement(),
		CreatedAt: time.Now().UnixMilli(),
	}
}

func openDeclPullHome(
	t *testing.T,
	name string,
	space *declPullSpace,
	logger *slog.Logger,
) *Home {
	t.Helper()
	h, err := Open(Config{
		ChannelID:             channel.ID(name),
		DBPath:                filepath.Join(t.TempDir(), "channel.sqlite"),
		CompositionResolver:   routingResolver{},
		IntroductionResolver:  space,
		ReconcileInterval:     time.Hour,
		Logger:                logger,
		Bootstrap:             true,
		BootstrapDeclarations: []DeclareRequest{declPullDeclaration()},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = h.closeInternal("test") })
	return h
}

func declPullOnlyInstance(t *testing.T, h *Home) actor.ActorID {
	t.Helper()
	instances, err := activeMembersForSource(h.controller, declPullSource)
	if err != nil || len(instances) != 1 {
		t.Fatalf("declaration %q instances=%v err=%v", declPullSource, instances, err)
	}
	return instances[0]
}

// T15. The pull loop has five ways to end a round without writing anything, and
// until now not one of them had ever been taken in a test. Each round below
// hands the loop a definition that DIFFERS from the record's, so "the record
// did not move" can only mean the round was skipped — and the control round at
// the end, identical except that the space now agrees about the class kind,
// proves the definition on offer really was appliable the whole time.
func TestDeclarationPullSkipsEveryUnusableSpaceAnswer(t *testing.T) {
	space := newDeclPullSpace()
	space.setFacts(declPullClass, `{"model":"v2"}`)
	space.setResolveErr(errors.New("space rpc failed"))
	space.setClassKind(actor.KindAgent, true, nil)

	h := openDeclPullHome(t, "decl-pull-skips", space, nil)
	ctx := context.Background()
	id := declPullOnlyInstance(t, h)
	base, baseSpec := serverTerm(t, h, id)
	if string(baseSpec.Config) != declPullSeed {
		t.Fatalf("bootstrap spec = %s, want %s", baseSpec.Config, declPullSeed)
	}

	rounds := []struct {
		name       string
		arrange    func()
		asksTheMap bool
	}{
		{"resolver fault", func() {
			space.setResolveErr(errors.New("space rpc failed"))
		}, false},
		{"declaration gone", func() {
			space.setResolveErr(channelspec.ErrDeclarationNotFound)
		}, false},
		{"class registry fault", func() {
			space.setResolveErr(nil)
			space.setClassKind("", false, errors.New("class registry down"))
		}, true},
		{"class unknown", func() {
			space.setClassKind("", false, nil)
		}, true},
		{"class kind disagrees with the record", func() {
			space.setClassKind(actor.KindTool, true, nil)
		}, true},
	}
	for _, round := range rounds {
		round.arrange()
		resolvesBefore, kindsBefore := space.counts()
		h.reconcileDeclarations(ctx)
		resolvesAfter, kindsAfter := space.counts()
		if resolvesAfter != resolvesBefore+1 {
			t.Fatalf("%s: the loop asked the space %d times, want exactly 1",
				round.name, resolvesAfter-resolvesBefore)
		}
		wantKinds := kindsBefore
		if round.asksTheMap {
			wantKinds++
		}
		if kindsAfter != wantKinds {
			t.Fatalf("%s: class-kind lookups %d→%d, want →%d",
				round.name, kindsBefore, kindsAfter, wantKinds)
		}
		term, spec := serverTerm(t, h, id)
		if term != base || string(spec.Config) != string(baseSpec.Config) {
			t.Fatalf("%s: the loop applied a definition it had to skip: term %q→%q config %s→%s",
				round.name, base, term, baseSpec.Config, spec.Config)
		}
	}

	// The control: the same offered definition, the same loop, one space answer
	// changed — and now it lands.
	space.setClassKind(actor.KindAgent, true, nil)
	h.reconcileDeclarations(ctx)
	term, spec := serverTerm(t, h, id)
	if term == base {
		t.Fatal("a fully agreeing space answer minted no new term; the skips above prove nothing")
	}
	if string(spec.Config) != `{"model":"v2"}` {
		t.Fatalf("applied spec = %s, want the offered definition", spec.Config)
	}
}

// T16. The pull loop reads its comparison inputs, then leaves the ledger to go
// ask the space. While it is out there the instance it is holding can die and
// the SAME declaration can be introduced again, minting a different instance.
// The answer that comes back must be spent on the identity it was fetched for
// and nothing else: the reborn twin carries its own definition and its own
// term, and neither may be overwritten by an answer fetched for its dead
// predecessor.
func TestDeclarationPullCannotCrossFromARemovedInstanceToItsRebornTwin(t *testing.T) {
	probe := newLifecycleLogProbe("", nil)
	space := newDeclPullSpace()
	space.setResolveErr(channelspec.ErrDeclarationNotFound)
	space.setClassKind(actor.KindAgent, true, nil)

	h := openDeclPullHome(t, "decl-pull-aba", space, slog.New(probe))
	ctx := context.Background()
	stale := declPullOnlyInstance(t, h)

	// Arm one in-flight resolve carrying a definition that WOULD be applied.
	space.setFacts(declPullClass, `{"model":"pulled"}`)
	space.setResolveErr(nil)
	space.arm()
	t.Cleanup(space.unpark)
	go h.reconcileDeclarations(ctx)
	restartRecv(t, "the declaration pull to park inside the space resolve", space.entered)

	// From here on every other round of the loop is a no-op, so the only
	// declaration answer still in flight is the one held for the stale id.
	space.setResolveErr(channelspec.ErrDeclarationNotFound)

	// Truth moves underneath the parked resolve.
	if _, err := h.opEntry.remove(ctx, removeRequest{
		Target: stale, InitiatorActorID: stale,
	}); err != nil {
		t.Fatalf("remove the in-flight instance: %v", err)
	}
	born, err := h.actors.Introduce(ctx, actorctl.IntroduceRequest{
		DeclID: declPullSource, Seed: declPullSource, Kind: actor.KindAgent,
		Definition: storespec.ActorDefinition{
			Class: declPullClass, Config: json.RawMessage(declPullSeed),
		},
		Placement: storespec.NewServerPlacement(),
	})
	if err != nil {
		t.Fatalf("re-introduce the same declaration: %v", err)
	}
	fresh := born.ActorID
	if fresh == stale {
		t.Fatalf("re-declaring a removed instance handed back its identity %s", fresh)
	}
	freshTerm, freshSpec := serverTerm(t, h, fresh)

	space.unpark()
	restartEventually(t, "the stale instance's apply to be refused", func() bool {
		return probe.count("platform.declaration_pull.apply_failed") >= 1
	})

	term, spec := serverTerm(t, h, fresh)
	if term != freshTerm {
		t.Fatalf("the reborn twin was re-termed by its predecessor's pull: %q→%q", freshTerm, term)
	}
	if string(spec.Config) != string(freshSpec.Config) {
		t.Fatalf("the reborn twin took its predecessor's pulled definition: %s", spec.Config)
	}
	if active, err := h.controller.IsActive(ctx, stale); err != nil || active {
		t.Fatalf("the removed instance active=%v err=%v", active, err)
	}
	if active, err := h.controller.IsActive(ctx, fresh); err != nil || !active {
		t.Fatalf("the reborn twin active=%v err=%v", active, err)
	}
}
