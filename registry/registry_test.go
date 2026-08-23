package registry

import (
	"encoding/json"
	"errors"
	"testing"

	"github.com/wanpengxie/atoll/lib/actorbase"
	"github.com/wanpengxie/atoll/lib/introspect"
	"github.com/wanpengxie/atoll/platform"
	"github.com/wanpengxie/atoll/platform/channelspec"
	"github.com/wanpengxie/atoll/protocol/actor"
)

func TestValidateConfig(t *testing.T) {
	const class = "registry-validate-config-test"
	called := false
	Register(class, ClassDecl{
		Kind: actor.KindAgent, Placement: channelspec.PlacementServer,
		ValidateConfig: func(raw json.RawMessage) error {
			called = true
			if string(raw) != `{"ok":true}` {
				return errors.New("ok required")
			}
			return nil
		},
	})
	if err := ValidateConfig(class, json.RawMessage(`{"ok":true}`)); err != nil {
		t.Fatal(err)
	}
	if !called {
		t.Fatal("validator was not called")
	}
	if err := ValidateConfig(class, json.RawMessage(`[]`)); err == nil {
		t.Fatal("non-object config accepted")
	}
	if err := ValidateConfig(class, json.RawMessage(`{"ok":false}`)); err == nil {
		t.Fatal("validator rejection ignored")
	}
	if err := ValidateConfig("registry-validate-missing", json.RawMessage(`{}`)); !errors.Is(err, ErrUnknownClass) {
		t.Fatalf("unknown class must surface ErrUnknownClass (callers branch on it), got %v", err)
	}
	if err := ValidateConfig(class, json.RawMessage(`{"ok":false}`)); errors.Is(err, ErrUnknownClass) {
		t.Fatal("config rejection must not claim unknown class")
	}
	if _, err := Build("registry-validate-missing", InstanceSpec{}, Deps{}); !errors.Is(err, ErrUnknownClass) {
		t.Fatalf("Build unknown class must surface ErrUnknownClass, got %v", err)
	}
}

func TestClassDefaultConfigIsMergedForValidationAndBuild(t *testing.T) {
	const class = "registry-default-config-test"
	var built json.RawMessage
	Register(class, ClassDecl{
		Kind: actor.KindAgent, Placement: channelspec.PlacementServer,
		DefaultConfig: func() json.RawMessage {
			return json.RawMessage(`{"model":"base","nested":{"from":"default"},"selections":[{"model":"base"}]}`)
		},
		ValidateConfig: func(raw json.RawMessage) error {
			var got map[string]json.RawMessage
			if err := json.Unmarshal(raw, &got); err != nil || len(got["model"]) == 0 || len(got["nested"]) == 0 || len(got["selections"]) == 0 {
				return errors.New("effective defaults missing")
			}
			return nil
		},
		New: func(spec InstanceSpec, _ Deps) (platform.ActorDecl, error) {
			built = append(json.RawMessage(nil), spec.Config...)
			return platform.ActorDecl{ID: spec.ID, Kind: actor.KindAgent}, nil
		},
	})

	effective, err := ResolveConfig(class, json.RawMessage(`{"model":"instance","selections":[{"model":"instance"}]}`))
	if err != nil {
		t.Fatal(err)
	}
	var got map[string]json.RawMessage
	if err := json.Unmarshal(effective, &got); err != nil {
		t.Fatal(err)
	}
	if string(got["model"]) != `"instance"` || string(got["nested"]) != `{"from":"default"}` || string(got["selections"]) != `[{"model":"instance"}]` {
		t.Fatalf("effective config=%s", effective)
	}
	if _, err := Build(class, InstanceSpec{ID: "agent:default:1", Config: json.RawMessage(`{}`)}, Deps{}); err != nil {
		t.Fatal(err)
	}
	if !json.Valid(built) || string(built) == `{}` {
		t.Fatalf("Build did not receive class defaults: %s", built)
	}
	first, ok := ClassDefaultConfig(class)
	if !ok {
		t.Fatal("class default config not published")
	}
	first[0] = '['
	second, _ := ClassDefaultConfig(class)
	if second[0] != '{' {
		t.Fatal("published class default aliases registry storage")
	}
}

func TestRegisterRejectsZeroPlacement(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("zero placement registration did not panic")
		}
	}()
	Register("registry-zero-placement-test", ClassDecl{Kind: actor.KindTool})
}

func TestRegisterRejectsGateErrorCodesButNotClassNames(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("gate error code registration did not panic")
		}
	}()
	Register("device", ClassDecl{
		Kind: actor.KindTool, Placement: channelspec.PlacementServer,
		Manifest: introspect.Manifest{Class: "device", Interfaces: []string{"actor"}, Words: map[string]introspect.WordSpec{
			"device.read": {ErrorCodes: []string{"endpoint_not_found"}},
		}},
	})
}

// Build must respect a constructor-projected INSTANCE manifest (per-instance
// words such as agent.select's selections schema) and fall back to the class
// manifest only when the constructor declares none. The old unconditional
// overwrite silently erased every instance projection. The constructor here
// derives the projection from InstanceSpec.Config — the same shape the real
// agent chain uses (decl config → per-instance schema) — so two instances of
// ONE class must not cross values, and Class must survive as the registered
// class (actor.describe answers the real class, never a generic one).
func TestBuildRespectsConstructorInstanceManifest(t *testing.T) {
	const class = "manifest-precedence-test"
	classManifest := introspect.Manifest{Class: class, Words: map[string]introspect.WordSpec{
		"noop.ping": {Description: "class-level word"},
	}}
	Register(class, ClassDecl{Kind: actor.KindAgent, Placement: channelspec.PlacementServer, Manifest: classManifest, New: func(spec InstanceSpec, ctx Deps) (platform.ActorDecl, error) {
		m := introspect.Manifest{Class: class, Words: map[string]introspect.WordSpec{
			"noop.ping": {Description: "instance word", InputSchema: append(json.RawMessage(nil), spec.Config...)},
		}}
		return platform.ActorDecl{ID: spec.ID, Kind: actor.KindAgent, Factory: platform.ActorFactory{Proc: actorbase.Def{Manifest: m}}}, nil
	}})

	schemaOf := func(id, config string) string {
		t.Helper()
		built, err := Build(class, InstanceSpec{ID: actor.ActorID(id), Config: json.RawMessage(config)}, Deps{})
		if err != nil {
			t.Fatal(err)
		}
		if built.Factory.Proc.Manifest.Class != class {
			t.Fatalf("instance %s manifest class = %q, want registered class %q", id, built.Factory.Proc.Manifest.Class, class)
		}
		return string(built.Factory.Proc.Manifest.Words["noop.ping"].InputSchema)
	}
	if got := schemaOf("a", `{"const":"a"}`); got != `{"const":"a"}` {
		t.Fatalf("instance manifest overwritten by class manifest: %s", got)
	}
	if got := schemaOf("b", `{"const":"b"}`); got != `{"const":"b"}` {
		t.Fatalf("same-class second instance crossed values: %s", got)
	}

	// A constructor that declares no manifest still inherits the class one.
	Register("manifest-fallback-test", ClassDecl{Kind: actor.KindAgent, Placement: channelspec.PlacementServer, Manifest: classManifest, New: func(spec InstanceSpec, ctx Deps) (platform.ActorDecl, error) {
		return platform.ActorDecl{ID: spec.ID, Kind: actor.KindAgent}, nil
	}})
	fallback, err := Build("manifest-fallback-test", InstanceSpec{}, Deps{})
	if err != nil {
		t.Fatal(err)
	}
	if fallback.Factory.Proc.Manifest.Words["noop.ping"].Description != "class-level word" {
		t.Fatal("class manifest fallback lost")
	}

	// A generic body (peeractor-style) legitimately declares its own Class —
	// Build keeps the instance words but normalizes Class to the registered
	// class, so describe stays consistent with the directory row.
	Register("manifest-generic-body-test", ClassDecl{Kind: actor.KindAgent, Placement: channelspec.PlacementServer, Manifest: classManifest, New: func(spec InstanceSpec, ctx Deps) (platform.ActorDecl, error) {
		m := introspect.Manifest{Class: "generic-body", Words: map[string]introspect.WordSpec{"body.word": {Description: "instance body word"}}}
		return platform.ActorDecl{ID: spec.ID, Kind: actor.KindAgent, Factory: platform.ActorFactory{Proc: actorbase.Def{Manifest: m}}}, nil
	}})
	generic, err := Build("manifest-generic-body-test", InstanceSpec{}, Deps{})
	if err != nil {
		t.Fatal(err)
	}
	if generic.Factory.Proc.Manifest.Class != "manifest-generic-body-test" {
		t.Fatalf("instance class not normalized to registered class: %q", generic.Factory.Proc.Manifest.Class)
	}
	if generic.Factory.Proc.Manifest.Words["body.word"].Description != "instance body word" {
		t.Fatal("instance words lost during class normalization")
	}
}
