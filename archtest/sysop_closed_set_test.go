package archtest

import (
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"testing"
)

// TestSysOpMethodSetIsClosedAtSevenValueOperations pins the two-family split
// on the realm-facing bundle: SysOp is the VALUE-OPERATION face and stays a
// closed set of exactly the seven typed words — observations (reads) belong to
// View and must never widen SysOp (the R5-P2 drift: a serialized read landed
// on SysOp because it wanted the operation lock; locking is an implementation
// privacy, not a family membership).
func TestSysOpMethodSetIsClosedAtSevenValueOperations(t *testing.T) {
	want := map[string]bool{
		"Admit":             true,
		"Introduce":         true,
		"AttachDaemon":      true,
		"DetachDaemon":      true,
		"ApplyDeclVersion":  true,
		"RevokeDeclTargets": true,
		"RevokeDaemon":      true,
	}
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, filepath.Join("..", "platform", "channelhost", "bundle.go"), nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	ast.Inspect(file, func(n ast.Node) bool {
		spec, ok := n.(*ast.TypeSpec)
		if !ok || spec.Name.Name != "SysOp" {
			return true
		}
		iface, ok := spec.Type.(*ast.InterfaceType)
		if !ok {
			t.Fatalf("SysOp is not an interface")
		}
		found = true
		got := map[string]bool{}
		for _, method := range iface.Methods.List {
			for _, name := range method.Names {
				got[name.Name] = true
			}
		}
		for name := range got {
			if !want[name] {
				t.Errorf("SysOp gained method %q outside the frozen seven-word value set", name)
			}
		}
		for name := range want {
			if !got[name] {
				t.Errorf("SysOp lost frozen method %q", name)
			}
		}
		return false
	})
	if !found {
		t.Fatal("SysOp interface not found in channelhost/bundle.go")
	}
}
