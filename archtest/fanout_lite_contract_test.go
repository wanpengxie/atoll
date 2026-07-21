package archtest

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestFanoutLiteResolverIsClosedAndReadOnly(t *testing.T) {
	fset := token.NewFileSet()
	requirements, err := parser.ParseFile(fset, "../platform/home/requirements.go", nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]bool{
		"ResolveDeclaration": true,
		"ClassKind":          true,
		"DaemonFacts":        true,
	}
	found := false
	ast.Inspect(requirements, func(n ast.Node) bool {
		spec, ok := n.(*ast.TypeSpec)
		if !ok || spec.Name.Name != "IntroductionResolver" {
			return true
		}
		found = true
		iface, ok := spec.Type.(*ast.InterfaceType)
		if !ok {
			t.Fatal("IntroductionResolver is not an interface")
		}
		got := make(map[string]bool)
		for _, field := range iface.Methods.List {
			for _, name := range field.Names {
				got[name.Name] = true
			}
		}
		for name := range got {
			if !want[name] {
				t.Errorf("IntroductionResolver gained non-read method %q", name)
			}
		}
		for name := range want {
			if !got[name] {
				t.Errorf("IntroductionResolver lost method %q", name)
			}
		}
		return false
	})
	if !found {
		t.Fatal("IntroductionResolver interface not found")
	}

	composition, err := parser.ParseFile(fset, "../app/composition.go", nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	checked := map[string]bool{"ResolveDeclaration": false, "DaemonFacts": false}
	for _, decl := range composition.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || fn.Recv == nil {
			continue
		}
		if _, ok := checked[fn.Name.Name]; !ok {
			continue
		}
		checked[fn.Name.Name] = true
		ast.Inspect(fn.Body, func(n ast.Node) bool {
			sel, ok := n.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			switch sel.Sel.Name {
			case "Exec", "ExecContext", "Begin", "BeginTx", "Prepare", "PrepareContext":
				t.Errorf("compositionResolver.%s contains write-capable call %s", fn.Name.Name, sel.Sel.Name)
			}
			return true
		})
	}
	for name, ok := range checked {
		if !ok {
			t.Errorf("compositionResolver.%s implementation not found", name)
		}
	}
}

func TestFanoutLiteLegacyMechanismsCannotReturn(t *testing.T) {
	if _, err := os.Stat("../app/fanout.go"); !os.IsNotExist(err) {
		t.Fatalf("retired app/fanout.go exists or cannot be checked: %v", err)
	}

	forbidden := []string{
		"RenderSeq",
		"ApplyDeclVersion",
		"RevokeDeclTargets",
		"RevokeDaemon",
		"DerivedFanoutRef",
		"DerivedFinalizeRef",
		"StageDeclarationEditForTest",
		"submitEdit",
		"deliverFinalize",
		"finalizeProvision",
		"fanoutWorker",
	}
	for _, path := range phaseAProductionFiles(t, "..") {
		body, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		for _, symbol := range forbidden {
			if strings.Contains(string(body), symbol) {
				t.Errorf("%s retains retired fanout-lite symbol %q", filepath.Clean(path), symbol)
			}
		}
	}

	routes, err := os.ReadFile("../app/app.go")
	if err != nil {
		t.Fatal(err)
	}
	routeText := string(routes)
	if !strings.Contains(routeText, `"/channels/:chID/decls/:declID/config"`) {
		t.Error("declaration-keyed overlay route is absent")
	}
	if strings.Contains(routeText, `"/channels/:chID/actors/:actorID/config"`) {
		t.Error("retired actor-instance config route returned")
	}

	schema, err := os.ReadFile("../app/store.go")
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(schema), "'edit'") {
		t.Error("admission operation schema retains retired edit operation")
	}
}
