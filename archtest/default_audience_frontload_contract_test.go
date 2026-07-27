package archtest

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"
)

func TestDefaultAudienceRetiredMechanismsCannotReturn(t *testing.T) {
	forbidden := []string{
		"ResolveAudience",
		"StepAudienceResolve",
		"channel_routing",
		"DefaultSourceDeclID",
		"MakeDefault",
		"ChannelRouting",
	}
	for _, path := range phaseAProductionFiles(t, "..") {
		body, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		for _, word := range forbidden {
			if strings.Contains(string(body), word) {
				t.Errorf("%s retains retired default-audience mechanism %q", filepath.Clean(path), word)
			}
		}
	}

	for _, path := range phaseAProductionFiles(t, "../runtime/storespec") {
		body, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		for _, verb := range []string{"DefaultAgent", "SetDefaultAgent"} {
			if strings.Contains(string(body), verb) {
				t.Errorf("%s retains retired storespec routing verb %q", filepath.Clean(path), verb)
			}
		}
	}
}

func TestDefaultAudienceHarnessDepsStayPolicyFree(t *testing.T) {
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "../runtime/harness/deps.go", nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]bool{
		"ChannelID": true,
		"Log":       true,
		"Presence":  true,
		"NowMs":     true,
		"Logger":    true,
	}
	found := false
	ast.Inspect(file, func(n ast.Node) bool {
		spec, ok := n.(*ast.TypeSpec)
		if !ok || spec.Name.Name != "Deps" {
			return true
		}
		found = true
		fields := spec.Type.(*ast.StructType).Fields.List
		got := make(map[string]bool)
		for _, field := range fields {
			for _, name := range field.Names {
				got[name.Name] = true
			}
		}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("harness.Deps fields=%v want closed policy-free set=%v", got, want)
		}
		return false
	})
	if !found {
		t.Fatal("harness.Deps not found")
	}
}

func TestSystemEventEnvelopeConstructionHasOneHomeMouth(t *testing.T) {
	allowed := map[string]bool{
		"lib/behavior/call.go":           true,
		"lib/behavior/respond.go":        true,
		"lib/behavior/serve.go":          true,
		"runtime/schedule/engine.go":     true,
		"platform/home/actor_effects.go": true,
	}
	fset := token.NewFileSet()
	for _, path := range phaseAProductionFiles(t, "..") {
		file, err := parser.ParseFile(fset, path, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		rel, err := filepath.Rel("..", path)
		if err != nil {
			t.Fatal(err)
		}
		rel = filepath.ToSlash(rel)
		ast.Inspect(file, func(n ast.Node) bool {
			literal, ok := n.(*ast.CompositeLit)
			if !ok {
				return true
			}
			selector, ok := literal.Type.(*ast.SelectorExpr)
			if !ok || selector.Sel.Name != "Envelope" {
				return true
			}
			pkg, ok := selector.X.(*ast.Ident)
			if ok && pkg.Name == "message" && !allowed[rel] {
				t.Errorf("%s constructs message.Envelope outside the five allowed files", rel)
			}
			return true
		})
	}
}

func TestPlatformDoesNotHardCodeBoostRouting(t *testing.T) {
	fset := token.NewFileSet()
	for _, path := range phaseAProductionFiles(t, "../platform") {
		file, err := parser.ParseFile(fset, path, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		ast.Inspect(file, func(n ast.Node) bool {
			literal, ok := n.(*ast.BasicLit)
			if !ok || literal.Kind != token.STRING {
				return true
			}
			value, err := strconv.Unquote(literal.Value)
			if err == nil && value == "sys:boost" {
				t.Errorf("%s hard-codes boost routing", filepath.Clean(path))
			}
			return true
		})
	}
}
