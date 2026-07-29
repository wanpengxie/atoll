package registry

import (
	"encoding/json"
	"errors"
	"testing"

	"github.com/wanpengxie/atoll/protocol/actor"
)

func TestValidateConfig(t *testing.T) {
	const class = "registry-validate-config-test"
	called := false
	Register(class, ClassDecl{
		Kind: actor.KindAgent,
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
	if err := ValidateConfig("registry-validate-missing", json.RawMessage(`{}`)); err == nil {
		t.Fatal("unknown class accepted")
	}
}
