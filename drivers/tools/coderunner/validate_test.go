package coderunner

import (
	"encoding/json"
	"io"
	"log/slog"
	"reflect"
	"strings"
	"testing"

	"github.com/wanpengxie/atoll/lib/introspect"
)

func TestDecodeValidateModeRules(t *testing.T) {
	modeOne := &coderunnerActor{cfg: Config{}}
	got, err := modeOne.decodeValidate(json.RawMessage(`{"requires":["echo","system"]}`))
	if err != nil || !reflect.DeepEqual(got, []string{"echo", "system"}) {
		t.Fatalf("mode one = %v, %v", got, err)
	}
	if _, err := modeOne.decodeValidate(json.RawMessage(`{}`)); err == nil || !strings.Contains(err.Error(), "requires") {
		t.Fatalf("mode one without requires must be refused, got %v", err)
	}
	if _, err := modeOne.decodeValidate(json.RawMessage(`{"requires":["Bad Name"]}`)); err == nil {
		t.Fatal("malformed requirement accepted")
	}
	if _, err := modeOne.decodeValidate(json.RawMessage(`{"requires":[],"program":"x"}`)); err == nil {
		t.Fatal("program is code, not config: it must be refused by code.validate")
	}

	fixed := &coderunnerActor{cfg: Config{Program: "export async function run(){}", Requires: []string{"echo"}}}
	got, err = fixed.decodeValidate(json.RawMessage(`{}`))
	if err != nil || !reflect.DeepEqual(got, []string{"echo"}) {
		t.Fatalf("fixed member must validate its own config, got %v, %v", got, err)
	}
	got, err = fixed.decodeValidate(nil)
	if err != nil || !reflect.DeepEqual(got, []string{"echo"}) {
		t.Fatalf("fixed member with empty payload = %v, %v", got, err)
	}
	if _, err := fixed.decodeValidate(json.RawMessage(`{"requires":["echo"]}`)); err == nil {
		t.Fatal("fixed member must refuse a requires override")
	}
}

func TestResolveDirectoryReportsAmbiguity(t *testing.T) {
	catalog := introspect.Catalog{Actors: []introspect.CatalogEntry{
		{ID: "tool:echo-z:3", Name: "echo", Present: true},
		{ID: "tool:echo-a:1", Name: "echo", Present: true},
		{ID: "tool:github:2", Name: "github", Present: true},
	}}
	declarations := []declarationRow{{Name: "echo", DefaultClass: "echo"}, {Name: "github", DefaultClass: "mcp"}}
	res := resolveDirectoryDetailed([]string{"echo", "mcp:github", "mcp:nope"}, catalog, declarations, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if res.resolved["echo"] != "tool:echo-a:1" || res.resolved["mcp:github"] != "tool:github:2" {
		t.Fatalf("resolved=%v", res.resolved)
	}
	if !reflect.DeepEqual(res.missing, []string{"mcp:nope"}) {
		t.Fatalf("missing=%v", res.missing)
	}
	if !reflect.DeepEqual(res.ambiguous(), map[string][]string{"echo": {"tool:echo-a:1", "tool:echo-z:3"}}) {
		t.Fatalf("ambiguous=%v", res.ambiguous())
	}
}
