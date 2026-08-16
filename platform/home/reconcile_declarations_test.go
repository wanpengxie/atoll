package home

import (
	"context"
	"encoding/json"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/wanpengxie/atoll/platform"
	"github.com/wanpengxie/atoll/platform/channelspec"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/runtime/actorhost"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

// mutableDeclarationResolver is the space half of the declaration pull loop: it
// answers "what should this declaration be right now" and can be moved between
// rounds.
type mutableDeclarationResolver struct {
	mu     sync.Mutex
	class  string
	config json.RawMessage
	reads  int
}

func (r *mutableDeclarationResolver) ResolveDeclaration(
	context.Context, channel.ID, string,
) (channelspec.DeclarationFacts, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.reads++
	return channelspec.DeclarationFacts{
		Visibility: "public", Class: r.class,
		Config: append(json.RawMessage(nil), r.config...),
	}, nil
}

func (r *mutableDeclarationResolver) ClassKind(_ context.Context, class string) (actor.Kind, bool, error) {
	if class == "routing-live" {
		return actor.KindAgent, true, nil
	}
	return "", false, nil
}
func (r *mutableDeclarationResolver) ClassPlacement(context.Context, string) (channel.PlacementKind, bool, error) {
	return channel.PlacementServer, true, nil
}
func (r *mutableDeclarationResolver) AdmitIntroduction(context.Context, channel.ID, channelspec.DeclarationFacts) error {
	return nil
}

func (r *mutableDeclarationResolver) BuildClass(
	channel.ID, actor.ActorID, string, json.RawMessage,
) (platform.ActorFactory, bool) {
	return platform.ActorFactory{}, false
}

func (r *mutableDeclarationResolver) set(config string) {
	r.mu.Lock()
	r.config = json.RawMessage(config)
	r.mu.Unlock()
}

// serverTerm reads the one server-desired row's term and execution spec — the
// observable consequence of a declaration change.
func serverTerm(t *testing.T, h *Home, id actor.ActorID) (actorhost.AttemptKey, actorhost.ExecutionSpec) {
	t.Helper()
	desired, err := h.controller.DesiredFor("server", "server")
	if err != nil {
		t.Fatal(err)
	}
	for _, row := range desired {
		body, ok := row.(actorhost.BodyDesired)
		if ok && body.ActorID == id {
			return body.AttemptKey, body.ExecutionSpec
		}
	}
	t.Fatalf("no server desired row for %s: %+v", id, desired)
	return "", actorhost.ExecutionSpec{}
}

// The declaration pull loop is level-triggered over the Controller's
// question-shaped reconcile projection. A changed declaration mints exactly one
// new term carrying the new execution spec; repeating the SAME declaration mints
// none — the equal-value no-op lives in the command, so a 30-second loop can
// never produce a term storm.
func TestDeclarationPullAppliesChangeAndIsQuietWhenEqual(t *testing.T) {
	resolver := &mutableDeclarationResolver{class: "routing-live", config: json.RawMessage(`{"model":"v1"}`)}
	h, err := Open(Config{
		ChannelID:            "decl-pull",
		DBPath:               filepath.Join(t.TempDir(), "channel.sqlite"),
		CompositionResolver:  resolver,
		IntroductionResolver: resolver,
		ReconcileInterval:    time.Hour,
		Bootstrap:            true,
		BootstrapDeclarations: []DeclareRequest{{
			SourceDeclID: "decl:pull", Class: "routing-live", Kind: actor.KindAgent,
			Placement: storespec.NewServerPlacement(), CreatedAt: time.Now().UnixMilli(),
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = h.closeInternal("test") })
	ctx := context.Background()

	instances, err := h.controller.DeclaredInstances("decl:pull")
	if err != nil || len(instances) != 1 {
		t.Fatalf("declared instances=%v err=%v", instances, err)
	}
	id := instances[0]

	// Round 1 moves the record onto the space's current definition.
	h.reconcileDeclarations(ctx)
	applied, spec := serverTerm(t, h, id)
	if string(spec.Config) != `{"model":"v1"}` {
		t.Fatalf("first pull did not apply the declaration: %s", spec.Config)
	}

	// Round 2 over the SAME declaration changes nothing at all.
	h.reconcileDeclarations(ctx)
	quiet, quietSpec := serverTerm(t, h, id)
	if quiet != applied || string(quietSpec.Config) != string(spec.Config) {
		t.Fatalf("an unchanged declaration minted a new term: %q → %q", applied, quiet)
	}

	// A changed declaration is a new term over the new spec.
	resolver.set(`{"model":"v2"}`)
	h.reconcileDeclarations(ctx)
	changed, changedSpec := serverTerm(t, h, id)
	if changed == applied {
		t.Fatal("a changed declaration did not mint a new term")
	}
	if string(changedSpec.Config) != `{"model":"v2"}` {
		t.Fatalf("new term carries the stale spec: %s", changedSpec.Config)
	}

	// And the loop settles again immediately afterwards.
	h.reconcileDeclarations(ctx)
	settled, _ := serverTerm(t, h, id)
	if settled != changed {
		t.Fatalf("the loop kept churning after convergence: %q → %q", changed, settled)
	}
}
