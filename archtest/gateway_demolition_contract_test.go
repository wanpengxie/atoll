package archtest

import (
	"bytes"
	"go/ast"
	"go/parser"
	"go/printer"
	"go/token"
	"io/fs"
	"sort"
	"strings"
	"testing"
)

func TestGatewayPublicContractIsSinglePath(t *testing.T) {
	fset := token.NewFileSet()
	pkgs, err := parser.ParseDir(fset, "../drivers/gateway", func(info fs.FileInfo) bool {
		return !strings.HasSuffix(info.Name(), "_test.go")
	}, 0)
	if err != nil {
		t.Fatal(err)
	}
	var fields []string
	type signature struct{ in, out []string }
	sigs := make(map[string]signature)
	var entitlementSnapshot signature
	for _, pkg := range pkgs {
		for _, file := range pkg.Files {
			for _, decl := range file.Decls {
				switch decl := decl.(type) {
				case *ast.GenDecl:
					for _, spec := range decl.Specs {
						ts, ok := spec.(*ast.TypeSpec)
						if !ok {
							continue
						}
						if ts.Name.Name == "Config" {
							st := ts.Type.(*ast.StructType)
							for _, field := range st.Fields.List {
								for _, name := range field.Names {
									fields = append(fields, name.Name)
								}
							}
						}
						if ts.Name.Name == "EntitlementResolver" {
							iface := ts.Type.(*ast.InterfaceType)
							for _, method := range iface.Methods.List {
								if len(method.Names) != 1 || method.Names[0].Name != "Snapshot" {
									continue
								}
								fn := method.Type.(*ast.FuncType)
								entitlementSnapshot = signature{fieldTypes(fset, fn.Params), fieldTypes(fset, fn.Results)}
							}
						}
					}
				case *ast.FuncDecl:
					key := decl.Name.Name
					if decl.Recv != nil {
						key = receiverName(decl.Recv.List[0].Type) + "." + key
					}
					sigs[key] = signature{fieldTypes(fset, decl.Type.Params), fieldTypes(fset, decl.Type.Results)}
				}
			}
		}
	}
	sort.Strings(fields)
	wantFields := []string{"Logger", "Resolver"}
	if strings.Join(fields, ",") != strings.Join(wantFields, ",") {
		t.Fatalf("gateway.Config fields = %v, want %v", fields, wantFields)
	}
	want := map[string]signature{
		"New":              {[]string{"Config"}, []string{"*Gateway", "error"}},
		"Gateway.Start":    {nil, nil},
		"Gateway.Close":    {nil, []string{"error"}},
		"Gateway.Attach":   {[]string{"string", "map[channel.ID]int64"}, []string{"*Session", "error"}},
		"Session.Upstream": {[]string{"subjectgate.Frame"}, []string{"subjectgate.Frame"}},
	}
	for name, expected := range want {
		got, ok := sigs[name]
		if !ok || strings.Join(got.in, ",") != strings.Join(expected.in, ",") || strings.Join(got.out, ",") != strings.Join(expected.out, ",") {
			t.Errorf("%s shape = %v, want %v", name, got, expected)
		}
	}
	wantSnapshot := signature{
		in:  []string{"context.Context", "string"},
		out: []string{"[]Route", "[]channel.ID", "error"},
	}
	if strings.Join(entitlementSnapshot.in, ",") != strings.Join(wantSnapshot.in, ",") ||
		strings.Join(entitlementSnapshot.out, ",") != strings.Join(wantSnapshot.out, ",") {
		t.Errorf("EntitlementResolver.Snapshot shape = %v, want %v", entitlementSnapshot, wantSnapshot)
	}
}

func fieldTypes(fset *token.FileSet, list *ast.FieldList) []string {
	if list == nil {
		return nil
	}
	var out []string
	for _, field := range list.List {
		n := len(field.Names)
		if n == 0 {
			n = 1
		}
		for i := 0; i < n; i++ {
			out = append(out, nodeText(fset, field.Type))
		}
	}
	return out
}

func nodeText(fset *token.FileSet, node ast.Node) string {
	var b bytes.Buffer
	_ = printer.Fprint(&b, fset, node)
	return b.String()
}

func receiverName(expr ast.Expr) string {
	if star, ok := expr.(*ast.StarExpr); ok {
		expr = star.X
	}
	if id, ok := expr.(*ast.Ident); ok {
		return id.Name
	}
	return ""
}

func TestGatewayRetiredDeclarationsAbsent(t *testing.T) {
	fset := token.NewFileSet()
	pkgs, err := parser.ParseDir(fset, "../drivers/gateway", nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	forbidden := map[string]struct{}{
		"Clock": {}, "Timer": {}, "ErrGatewayClosed": {}, "LeakedPumps": {},
		"PokeHub": {}, "NewPokeHub": {}, "ChannelFailure": {}, "droppedCount": {},
	}
	forbiddenMethods := map[string]struct{}{
		"Gateway.entryFor": {}, "Gateway.LeakedPumps": {},
		"lane.drain": {}, "lane.isClosed": {}, "lane.DroppedCount": {},
		"cursor.snapshot": {},
	}
	forbiddenFields := map[string]struct{}{
		"presenceOn": {}, "leakedPumps": {}, "droppedCount": {},
	}
	for _, pkg := range pkgs {
		for path, file := range pkg.Files {
			ast.Inspect(file, func(n ast.Node) bool {
				switch n := n.(type) {
				case *ast.TypeSpec:
					if _, bad := forbidden[n.Name.Name]; bad {
						t.Errorf("%s declares retired type %s", path, n.Name.Name)
					}
					if st, ok := n.Type.(*ast.StructType); ok {
						for _, field := range st.Fields.List {
							for _, name := range field.Names {
								if _, bad := forbiddenFields[name.Name]; bad {
									t.Errorf("%s declares retired struct field %s.%s", path, n.Name.Name, name.Name)
								}
							}
						}
					}
				case *ast.ValueSpec:
					for _, name := range n.Names {
						if _, bad := forbidden[name.Name]; bad {
							t.Errorf("%s declares retired value %s", path, name.Name)
						}
					}
				case *ast.FuncDecl:
					key := n.Name.Name
					if n.Recv != nil {
						key = receiverName(n.Recv.List[0].Type) + "." + key
					}
					if _, bad := forbiddenMethods[key]; bad {
						t.Errorf("%s declares retired method %s", path, key)
					}
					if n.Recv == nil {
						if _, bad := forbidden[n.Name.Name]; bad {
							t.Errorf("%s declares retired function/method %s", path, n.Name.Name)
						}
					}
				}
				return true
			})
		}
	}
}

func TestGatewayOwnersHaveNoProductionTestHooks(t *testing.T) {
	targets := map[string]struct{}{"Gateway": {}, "Session": {}, "App": {}}
	for _, dir := range []string{"../drivers/gateway", "../app"} {
		fset := token.NewFileSet()
		pkgs, err := parser.ParseDir(fset, dir, func(info fs.FileInfo) bool {
			return !strings.HasSuffix(info.Name(), "_test.go")
		}, 0)
		if err != nil {
			t.Fatal(err)
		}
		for _, pkg := range pkgs {
			for path, file := range pkg.Files {
				for _, decl := range file.Decls {
					gen, ok := decl.(*ast.GenDecl)
					if !ok {
						continue
					}
					for _, spec := range gen.Specs {
						ts, ok := spec.(*ast.TypeSpec)
						if !ok {
							continue
						}
						if _, wanted := targets[ts.Name.Name]; !wanted {
							continue
						}
						st, ok := ts.Type.(*ast.StructType)
						if !ok {
							continue
						}
						for _, field := range st.Fields.List {
							for _, name := range field.Names {
								lower := strings.ToLower(name.Name)
								if strings.Contains(lower, "hook") || strings.Contains(lower, "failpoint") || strings.Contains(lower, "testonly") {
									t.Errorf("%s: %s.%s recreates a production test hook", path, ts.Name.Name, name.Name)
								}
							}
						}
					}
				}
			}
		}
	}
}

func TestChannelHostOwnsHomeRegistryAndClose(t *testing.T) {
	fset := token.NewFileSet()
	pkgs, err := parser.ParseDir(fset, "../app", func(info fs.FileInfo) bool {
		return !strings.HasSuffix(info.Name(), "_test.go")
	}, 0)
	if err != nil {
		t.Fatal(err)
	}
	for _, pkg := range pkgs {
		for path, file := range pkg.Files {
			ast.Inspect(file, func(n ast.Node) bool {
				sel, ok := n.(*ast.SelectorExpr)
				if ok && sel.Sel.Name == "homes" {
					t.Errorf("%s still reaches a Home registry", path)
				}
				return true
			})
		}
	}
}

func isAppMuCall(call *ast.CallExpr) bool {
	method, ok := call.Fun.(*ast.SelectorExpr)
	if !ok || (method.Sel.Name != "Lock" && method.Sel.Name != "Unlock" &&
		method.Sel.Name != "RLock" && method.Sel.Name != "RUnlock") {
		return false
	}
	mu, ok := method.X.(*ast.SelectorExpr)
	if !ok || mu.Sel.Name != "mu" {
		return false
	}
	recv, ok := mu.X.(*ast.Ident)
	return ok && recv.Name == "a"
}

func isReceiverCall(sel *ast.SelectorExpr, recv, method string) bool {
	id, ok := sel.X.(*ast.Ident)
	return ok && id.Name == recv && sel.Sel.Name == method
}

func isAppHomes(expr ast.Expr) bool {
	sel, ok := expr.(*ast.SelectorExpr)
	if !ok || sel.Sel.Name != "homes" {
		return false
	}
	id, ok := sel.X.(*ast.Ident)
	return ok && id.Name == "a"
}
