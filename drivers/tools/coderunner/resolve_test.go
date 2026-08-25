package coderunner

import (
	"io"
	"log/slog"
	"reflect"
	"testing"

	"github.com/wanpengxie/atoll/lib/introspect"
	"github.com/wanpengxie/atoll/protocol/actor"
)

func TestResolveDirectory(t *testing.T) {
	catalog := introspect.Catalog{Actors: []introspect.CatalogEntry{
		{ID: "tool:echo-z:3", Name: "echo", Present: true},
		{ID: "tool:echo-a:1", Name: "echo", Present: true},
		{ID: "tool:github:2", Name: "github", Present: true},
		{ID: "tool:gone:1", Name: "gone", Present: false},
	}}
	declarations := []declarationRow{
		{Name: "echo", DefaultClass: "echo"},
		{Name: "github", DefaultClass: "mcp"},
		{Name: "gone", DefaultClass: "mcp"},
	}
	requires := []string{"echo", "mcp:github", "system", "mcp:missing", "mcp:gone"}
	got, missing := resolveDirectory(requires, catalog, declarations, slog.New(slog.NewTextHandler(io.Discard, nil)))
	want := map[string]actor.ActorID{
		"echo": "tool:echo-a:1", "mcp:github": "tool:github:2", "system": actor.SystemActorID,
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("resolved=%v want=%v", got, want)
	}
	if !reflect.DeepEqual(missing, []string{"mcp:missing", "mcp:gone"}) {
		t.Fatalf("missing=%v", missing)
	}
}

func TestAllowedTargetAcceptsRequirementAndResolvedID(t *testing.T) {
	actors := map[string]actor.ActorID{"mcp:github": "tool:github:2"}
	for _, value := range []string{"mcp:github", "tool:github:2"} {
		if got, ok := allowedTarget(value, actors); !ok || got != "tool:github:2" {
			t.Fatalf("allowedTarget(%q)=(%q,%v)", value, got, ok)
		}
	}
	if _, ok := allowedTarget("tool:other:1", actors); ok {
		t.Fatal("undeclared actor was accepted")
	}
}
