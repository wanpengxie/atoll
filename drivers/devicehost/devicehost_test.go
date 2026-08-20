package devicehost

import (
	"fmt"
	"io"
	"log/slog"
	"testing"

	"github.com/wanpengxie/atoll/platform"
	"github.com/wanpengxie/atoll/platform/channelspec"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/registry"
)

// Test-only classes: one that builds, one whose constructor always errors, one
// that derives a different id — the three answers a class constructor can give
// a body build (real classes need creds/config to fail deterministically).
func init() {
	registry.Register("test-ok-daemon", registry.ClassDecl{
		Kind:      actor.KindAgent,
		Placement: channelspec.PlacementDaemon,
		New: func(spec registry.InstanceSpec, _ registry.Deps) (platform.ActorDecl, error) {
			return platform.ActorDecl{ID: spec.ID, Kind: actor.KindAgent}, nil
		},
	})
	registry.Register("test-fail-daemon", registry.ClassDecl{
		Kind:      actor.KindAgent,
		Placement: channelspec.PlacementDaemon,
		New: func(registry.InstanceSpec, registry.Deps) (platform.ActorDecl, error) {
			return platform.ActorDecl{}, fmt.Errorf("forced build failure")
		},
	})
	// A constructor that REWRITES the id (like device deriving its id from the
	// device identity, ignoring spec.ID) — used to prove the builder is keyed on the
	// plan InstanceID, not the built decl.ID, so a drift is caught as no_builder
	// rather than filed under an unreachable derived id.
	registry.Register("test-rewrite-id-daemon", registry.ClassDecl{
		Kind:      actor.KindAgent,
		Placement: channelspec.PlacementDaemon,
		New: func(spec registry.InstanceSpec, _ registry.Deps) (platform.ActorDecl, error) {
			return platform.ActorDecl{ID: spec.ID + ":derived", Kind: actor.KindAgent}, nil
		},
	})
}

func testFactories() classFactories {
	return classFactories{
		chID: "c", deviceName: "dev",
		logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
}

// The factory is a pure function of (id, class, config) — the spec the Host's
// own desired carries into each build. There is no plan snapshot behind it, so
// there is no generation to disagree with: the two-ledger split (a builder
// table on one plan, desired on another, meeting at a silent no_builder loop)
// has nothing to happen to. What remains to pin is the resolution itself and
// its two refusals.
func TestClassFactoriesResolvesFromTheBuildInputAlone(t *testing.T) {
	f := testFactories()

	if _, ok := f.BuildClass("agent:a", "test-ok-daemon", nil); !ok {
		t.Fatal("a registered class did not resolve")
	}
	// A class this daemon binary does not carry (version skew) fails this one
	// body, not a plan: there is no plan here to fail.
	if _, ok := f.BuildClass("agent:a", "not-registered", nil); ok {
		t.Fatal("an unregistered class resolved a factory")
	}
	if _, ok := f.BuildClass("agent:a", "test-fail-daemon", nil); ok {
		t.Fatal("a failing constructor resolved a factory")
	}
}

// A constructor that rewrites the id (device deriving its own from the device
// identity) would produce a body claiming an identity the plan never named.
// With no table to file it under, the only leak path is being built — refuse.
func TestClassFactoriesRefusesAConstructorThatDerivesAnotherID(t *testing.T) {
	f := testFactories()
	if _, ok := f.BuildClass("agent:a", "test-rewrite-id-daemon", nil); ok {
		t.Fatal("a factory was handed out for an identity the plan never named")
	}
}
