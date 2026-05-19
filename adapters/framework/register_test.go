package framework

import (
	"errors"
	"testing"

	"github.com/wanpengxie/ActOS/kernel/actor"
	"github.com/wanpengxie/ActOS/kernel/adapter"
)

func TestRegisterAndBuild(t *testing.T) {
	ResetRegistry()
	defer ResetRegistry()
	called := false
	Register("test", func(deps Deps) adapter.Module {
		called = true
		return &stubModule{decl: adapter.Declaration{
			Name:         "test",
			ActorID:      "tool:test",
			Types:        []string{"x.y"},
			Binding:      actor.BindingInProcess,
			MaxPendingMs: 1,
		}}
	})

	names := RegisteredNames()
	if len(names) != 1 || names[0] != "test" {
		t.Fatalf("RegisteredNames got %v", names)
	}

	mods, err := BuildAllRegistered(Deps{})
	if err != nil {
		t.Fatalf("BuildAllRegistered: %v", err)
	}
	if len(mods) != 1 {
		t.Fatalf("expected 1 module, got %d", len(mods))
	}
	if !called {
		t.Fatalf("factory not invoked")
	}
}

func TestBuildRegisteredUnknownName(t *testing.T) {
	ResetRegistry()
	defer ResetRegistry()
	_, err := BuildRegistered(Deps{}, "nope")
	if err == nil {
		t.Fatalf("expected error")
	}
}

func TestBuildAllRegisteredEmpty(t *testing.T) {
	ResetRegistry()
	defer ResetRegistry()
	_, err := BuildAllRegistered(Deps{})
	if !errors.Is(err, ErrNoRegisteredModules) {
		t.Fatalf("expected ErrNoRegisteredModules, got %v", err)
	}
}

func TestRegisterDuplicatePanics(t *testing.T) {
	ResetRegistry()
	defer ResetRegistry()
	Register("a", func(Deps) adapter.Module { return nil })
	defer func() {
		if r := recover(); r == nil {
			t.Fatalf("expected panic on duplicate")
		}
	}()
	Register("a", func(Deps) adapter.Module { return nil })
}

func TestDepsFromManagerConfig(t *testing.T) {
	cfg := ManagerConfig{
		ChannelID:     "channel:x",
		ActorRegistry: newMemoryActorRegistry(),
		HarnessChain:  newFakeChain(),
		RequestLookup: NewMemoryRequestLookup(nil),
	}
	deps := DepsFromManagerConfig(cfg)
	if deps.Logger == nil {
		t.Fatalf("Logger nil after applyDefaults")
	}
	if deps.Metrics == nil {
		t.Fatalf("Metrics nil after applyDefaults")
	}
	if deps.StateStore == nil {
		t.Fatalf("StateStore nil after applyDefaults")
	}
	_ = actor.KindTool // keep import live
}
