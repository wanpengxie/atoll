package registry

import (
	"encoding/json"
	"errors"
	"testing"

	"github.com/wanpengxie/atoll/lib/introspect"
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
