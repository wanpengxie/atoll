package home

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

type pullTestResolver struct {
	mu      sync.Mutex
	facts   channel.DeclarationFacts
	err     error
	block   chan struct{}
	entered chan struct{}
}

func (r *pullTestResolver) ResolveDeclaration(context.Context, channel.ID, string) (channel.DeclarationFacts, error) {
	r.mu.Lock()
	facts, err := r.facts, r.err
	block, entered := r.block, r.entered
	r.block, r.entered = nil, nil
	if entered != nil {
		close(entered)
	}
	r.mu.Unlock()
	if block != nil {
		<-block
	}
	return facts, err
}

func (r *pullTestResolver) ClassKind(_ context.Context, class string) (actor.Kind, bool, error) {
	if class == "pull-tool" {
		return actor.KindTool, true, nil
	}
	return actor.KindAgent, true, nil
}

func (r *pullTestResolver) DaemonFacts(context.Context, string) (channel.DaemonFacts, error) {
	return channel.DaemonFacts{}, nil
}

func (r *pullTestResolver) set(facts channel.DeclarationFacts, err error) {
	r.mu.Lock()
	r.facts, r.err = facts, err
	r.mu.Unlock()
}

func (r *pullTestResolver) blockNext() (<-chan struct{}, chan<- struct{}) {
	r.mu.Lock()
	r.block = make(chan struct{})
	r.entered = make(chan struct{})
	block, entered := r.block, r.entered
	r.mu.Unlock()
	return entered, block
}

func openPullTestHome(t *testing.T, resolver *pullTestResolver) *Home {
	t.Helper()
	h, err := Open(Config{
		ChannelID: "declaration-pull", DBPath: filepath.Join(t.TempDir(), "channel.sqlite"), Bootstrap: true,
		CompositionResolver: emptyCompositionResolver{}, IntroductionResolver: resolver, ReconcileInterval: time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = h.closeInternal("test") })
	return h
}

func declarePullActor(t *testing.T, h *Home, source string, config string, placement storespec.Placement, idle time.Duration, createdAt int64) storespec.ActorControlRow {
	t.Helper()
	raw := json.RawMessage(config)
	result, err := h.declare(context.Background(), DeclareRequest{
		SourceDeclID: source, Kind: actor.KindAgent, Class: "pull-agent", Config: &raw,
		Placement: placement, TIdle: idle.Milliseconds(), CreatedAt: createdAt,
	})
	if err != nil {
		t.Fatal(err)
	}
	return result.Row
}

func TestDeclarationPullConvergesAndPreservesChannelFields(t *testing.T) {
	ctx := context.Background()
	resolver := &pullTestResolver{facts: channel.DeclarationFacts{Class: "pull-agent", Config: json.RawMessage(`{"value":"a"}`)}}
	h := openPullTestHome(t, resolver)
	owner, err := h.admitChannelOwner(ctx, "owner")
	if err != nil {
		t.Fatal(err)
	}
	placement, _ := storespec.NewDaemonPlacement("daemon-a")
	row := declarePullActor(t, h, "decl-a", `{"value":"a"}`, placement, 7*time.Second, 10)

	resolver.set(channel.DeclarationFacts{Class: "pull-agent", Config: json.RawMessage(`{"value":"b"}`)}, nil)
	h.reconcileDeclarations(ctx)
	current, active, err := h.controlIndex.LookupActive(ctx, row.ID)
	if err != nil || !active || current.CurrentDeclVersion != 2 || string(current.Config) != `{"value":"b"}` || current.Placement != placement || current.TIdle != 7*time.Second {
		t.Fatalf("A→B row=(%+v,%v,%v)", current, active, err)
	}
	resolver.set(channel.DeclarationFacts{Class: "pull-agent", Config: json.RawMessage(`{"value":"a"}`)}, nil)
	h.reconcileDeclarations(ctx)
	current, _, _ = h.controlIndex.LookupActive(ctx, row.ID)
	if current.CurrentDeclVersion != 3 || string(current.Config) != `{"value":"a"}` {
		t.Fatalf("B→A row=%+v", current)
	}
	h.reconcileDeclarations(ctx)
	equal, _, _ := h.controlIndex.LookupActive(ctx, row.ID)
	if equal.CurrentDeclVersion != 3 {
		t.Fatalf("equal pull wrote version %d", equal.CurrentDeclVersion)
	}

	if human, active, err := h.controlIndex.LookupActive(ctx, owner); err != nil || !active || human.SourceDeclID != "" || human.CurrentDeclVersion != 1 {
		t.Fatalf("human was pulled: row=%+v active=%v err=%v", human, active, err)
	}
}

func TestDeclarationPullSkipsResolverFailureAbsenceAndKindMismatch(t *testing.T) {
	ctx := context.Background()
	resolver := &pullTestResolver{facts: channel.DeclarationFacts{Class: "pull-agent", Config: json.RawMessage(`{"value":"a"}`)}}
	h := openPullTestHome(t, resolver)
	row := declarePullActor(t, h, "decl-skip", `{"value":"a"}`, storespec.NewServerPlacement(), 0, 10)

	for _, failure := range []error{errors.New("realm unavailable"), channel.ErrDeclarationNotFound} {
		resolver.set(channel.DeclarationFacts{}, failure)
		h.reconcileDeclarations(ctx)
		got, _, _ := h.controlIndex.LookupActive(ctx, row.ID)
		if got.CurrentDeclVersion != 1 {
			t.Fatalf("resolver failure %v mutated version to %d", failure, got.CurrentDeclVersion)
		}
	}
	resolver.set(channel.DeclarationFacts{Class: "pull-tool", Config: json.RawMessage(`{"value":"wrong-kind"}`)}, nil)
	h.reconcileDeclarations(ctx)
	got, _, _ := h.controlIndex.LookupActive(ctx, row.ID)
	if got.CurrentDeclVersion != 1 {
		t.Fatalf("kind mismatch mutated version to %d", got.CurrentDeclVersion)
	}
}

func TestDeclarationPullAttemptCannotCrossActorLifetime(t *testing.T) {
	ctx := context.Background()
	resolver := &pullTestResolver{facts: channel.DeclarationFacts{Class: "pull-agent", Config: json.RawMessage(`{"value":"a"}`)}}
	h := openPullTestHome(t, resolver)
	first := declarePullActor(t, h, "decl-aba", `{"value":"a"}`, storespec.NewServerPlacement(), 0, 10)
	resolver.set(channel.DeclarationFacts{Class: "pull-agent", Config: json.RawMessage(`{"value":"b"}`)}, nil)
	entered, release := resolver.blockNext()
	done := make(chan struct{})
	go func() {
		h.reconcileDeclarations(ctx)
		close(done)
	}()
	<-entered
	if err := h.systemEndHandle().End(ctx, first.ID, "test_remove"); err != nil {
		t.Fatal(err)
	}
	second := declarePullActor(t, h, "decl-aba", `{"value":"a"}`, storespec.NewServerPlacement(), 0, 20)
	if second.ID == first.ID {
		t.Fatal("reintroduction reused ActorID")
	}
	close(release)
	<-done
	got, active, err := h.controlIndex.LookupActive(ctx, second.ID)
	if err != nil || !active || got.CurrentDeclVersion != 1 || string(got.Config) != `{"value":"a"}` {
		t.Fatalf("old attempt crossed lifetime: row=%+v active=%v err=%v", got, active, err)
	}
	h.reconcileDeclarations(ctx)
	got, _, _ = h.controlIndex.LookupActive(ctx, second.ID)
	if got.CurrentDeclVersion != 2 || string(got.Config) != `{"value":"b"}` {
		t.Fatalf("new lifetime did not converge: %+v", got)
	}
}

func TestDeclarationPullRecomposesLatestPlacementAndIdleInsideGate(t *testing.T) {
	ctx := context.Background()
	resolver := &pullTestResolver{facts: channel.DeclarationFacts{Class: "pull-agent", Config: json.RawMessage(`{"value":"a"}`)}}
	h := openPullTestHome(t, resolver)
	placementA, _ := storespec.NewDaemonPlacement("daemon-a")
	row := declarePullActor(t, h, "decl-fields", `{"value":"a"}`, placementA, time.Second, 10)
	resolver.set(channel.DeclarationFacts{Class: "pull-agent", Config: json.RawMessage(`{"value":"b"}`)}, nil)
	entered, release := resolver.blockNext()
	done := make(chan struct{})
	go func() {
		h.reconcileDeclarations(ctx)
		close(done)
	}()
	<-entered
	placementB, _ := storespec.NewDaemonPlacement("daemon-b")
	latest, err := h.editDeclaration(ctx, storespec.DeclEditBundle{
		ActorID: row.ID, Class: row.Class, Config: row.Config, Placement: placementB, TIdle: 9 * time.Second, CreatedAt: 20,
	})
	if err == nil {
		_, err = h.applyDeclaration(ctx, row.ID, latest.CurrentDeclVersion)
	}
	if err != nil {
		t.Fatal(err)
	}
	close(release)
	<-done
	got, active, err := h.controlIndex.LookupActive(ctx, row.ID)
	if err != nil || !active || string(got.Config) != `{"value":"b"}` || got.Placement != placementB || got.TIdle != 9*time.Second {
		t.Fatalf("pull overwrote latest channel fields: row=%+v active=%v err=%v", got, active, err)
	}
}
