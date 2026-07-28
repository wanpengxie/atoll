package archtest

import (
	"fmt"
	"go/ast"
	"go/token"
	"strings"
	"testing"
)

// spec §13.3: "SystemKernel Unit 只有 one-shot transfer，无 getter/rebuild".
//
// The Kernel receives exactly one already-Prepared SystemActor Unit, adopts it,
// and from then on the Unit is unreachable: callers reach the system actor only
// through the Kernel's own narrow verbs (Deliver/CancelRequest/Stat/…), which
// all re-check `closing` and liveness under the Kernel lock. Handing the raw
// `*actorrt.Unit` back out — `func (k *Kernel) Unit() *actorrt.Unit` — is a
// single line, is the classic "expose internals for testing/observability"
// patch, and destroys the property: the holder can Start/Stop/Deliver behind
// the Kernel's back, so the Kernel's fatal watcher and its Close drain stop
// being the whole truth about the system actor's life.
//
// Rebuild is the same violation on the write side: if anything other than the
// one adoption in Start may install a Unit, "the Kernel owns one incarnation of
// the system actor" stops holding, and a second system body can exist while the
// first is still being watched.
//
// No existing wall looks at systemkernel at all — the package is reachable from
// Platform and its exported method set is completely unguarded.

const systemKernelPkg = "../runtime/systemkernel"

// systemKernelUnitFieldName is the one private handle the whole wall is about.
const systemKernelUnitFieldName = "unit"

// isUnitPointerType reports whether an AST type expression is exactly
// `*actorrt.Unit`. It deliberately does NOT match actorrt.UnitStat /
// actorrt.UnitState / actorrt.UnitEventSink: those are read-only projections and
// callbacks, which the Kernel may legitimately expose.
func isUnitPointerType(expr ast.Expr) bool {
	star, ok := expr.(*ast.StarExpr)
	if !ok {
		return false
	}
	selector, ok := star.X.(*ast.SelectorExpr)
	if !ok {
		return false
	}
	pkg, ok := selector.X.(*ast.Ident)
	return ok && pkg.Name == "actorrt" && selector.Sel.Name == "Unit"
}

// systemKernelExportedUnitEscapes reports every exported declaration other than
// the single transfer entry point whose signature mentions the raw Unit. A
// getter shows up as a result; a "re-adopt this Unit" verb shows up as a second
// parameter site.
func systemKernelExportedUnitEscapes(files map[string]*ast.File) []string {
	const transferEntryPoint = "Start"
	var v []string
	for path, file := range files {
		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || !fn.Name.IsExported() {
				continue
			}
			if fn.Type.Results != nil {
				for _, result := range fn.Type.Results.List {
					if isUnitPointerType(result.Type) {
						v = append(v, fmt.Sprintf(
							"%s: exported %s returns the raw *actorrt.Unit — the transfer is one-shot, the Unit never comes back out",
							path, fn.Name.Name))
					}
				}
			}
			if fn.Name.Name == transferEntryPoint {
				continue
			}
			if fn.Type.Params == nil {
				continue
			}
			for _, param := range fn.Type.Params.List {
				if isUnitPointerType(param.Type) {
					v = append(v, fmt.Sprintf(
						"%s: exported %s accepts a *actorrt.Unit — %s is the ONLY transfer entry point",
						path, fn.Name.Name, transferEntryPoint))
				}
			}
		}
	}
	return v
}

// systemKernelUnitInstallations reports every write to the private unit handle
// that is not the one adoption in Start or a release to nil. A rebuild — even a
// well-meant "restart the system actor in place" — puts a second incarnation
// behind a Kernel that is still watching the first.
func systemKernelUnitInstallations(files map[string]*ast.File, fset *token.FileSet) []string {
	var v []string
	adoptions := 0
	for path, file := range files {
		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				continue
			}
			ast.Inspect(fn.Body, func(node ast.Node) bool {
				assign, ok := node.(*ast.AssignStmt)
				if !ok {
					return true
				}
				for i, lhs := range assign.Lhs {
					selector, ok := lhs.(*ast.SelectorExpr)
					if !ok || selector.Sel.Name != systemKernelUnitFieldName {
						continue
					}
					if i >= len(assign.Rhs) {
						continue
					}
					rhs, isIdent := assign.Rhs[i].(*ast.Ident)
					switch {
					case isIdent && rhs.Name == "nil":
						// Release: the Kernel lets go, it never re-installs.
					case isIdent && fn.Name.Name == "Start" && rhs.Name == "unit":
						adoptions++
					default:
						v = append(v, fmt.Sprintf(
							"%s:%d installs a Unit handle in %s — adoption happens once, in Start, from the transferred parameter",
							path, fset.Position(assign.Pos()).Line, fn.Name.Name))
					}
				}
				return true
			})
		}
	}
	if adoptions != 1 {
		v = append(v, fmt.Sprintf(
			"one-shot adoptions in Start=%d, want exactly 1 — the wall lost its footing", adoptions))
	}
	return v
}

// systemKernelRebuilds reports any attempt to construct a Unit inside the
// Kernel. The Kernel is a custodian, not a factory: it is handed a body that
// was built by the assembly root, and "prepare a fresh one" is exactly the
// rebuild the spec forbids.
func systemKernelRebuilds(files map[string]*ast.File, fset *token.FileSet) []string {
	var v []string
	for path, file := range files {
		ast.Inspect(file, func(node ast.Node) bool {
			call, ok := node.(*ast.CallExpr)
			if !ok {
				return true
			}
			selector, ok := call.Fun.(*ast.SelectorExpr)
			if !ok || selector.Sel.Name != "Prepare" {
				return true
			}
			if pkg, ok := selector.X.(*ast.Ident); ok && pkg.Name == "actorrt" {
				v = append(v, fmt.Sprintf(
					"%s:%d builds a Unit inside the Kernel — the Kernel adopts a transferred body, it never rebuilds one",
					path, fset.Position(call.Pos()).Line))
			}
			return true
		})
	}
	return v
}

func TestSystemKernelUnitIsOneShotTransferOnly(t *testing.T) {
	files, fset := loadArchWallPackage(t, systemKernelPkg)

	// Footing: the handle the wall guards must actually be there.
	found := false
	for _, file := range files {
		ast.Inspect(file, func(node ast.Node) bool {
			spec, ok := node.(*ast.TypeSpec)
			if !ok || spec.Name.Name != "Kernel" {
				return true
			}
			structType, ok := spec.Type.(*ast.StructType)
			if !ok {
				return true
			}
			for _, field := range structType.Fields.List {
				for _, name := range field.Names {
					if name.Name == systemKernelUnitFieldName && isUnitPointerType(field.Type) {
						found = true
					}
				}
			}
			return true
		})
	}
	if !found {
		t.Fatalf("systemkernel.Kernel has no %q *actorrt.Unit field — the one-shot transfer wall lost its subject",
			systemKernelUnitFieldName)
	}

	failViolations(t, "SystemKernel exposes no Unit getter and no second transfer entry point",
		systemKernelExportedUnitEscapes(files))
	failViolations(t, "SystemKernel adopts one Unit exactly once",
		systemKernelUnitInstallations(files, fset))
	failViolations(t, "SystemKernel never rebuilds its Unit",
		systemKernelRebuilds(files, fset))
}

// TestSystemKernelUnitTransferWallTripsOnGetterAndRebuild is the trip proof.
// The getter case is applied to the REAL kernel.go so the wall is shown to fire
// on production source, not only on a miniature.
func TestSystemKernelUnitTransferWallTripsOnGetterAndRebuild(t *testing.T) {
	const kernelPath = systemKernelPkg + "/kernel.go"

	t.Run("exported getter on production source", func(t *testing.T) {
		const anchor = "func (k *Kernel) Stat() (actorrt.UnitStat, bool) {"
		const withGetter = `// Unit exposes the adopted body "for tests and observability".
func (k *Kernel) Unit() *actorrt.Unit {
	k.mu.Lock()
	defer k.mu.Unlock()
	return k.unit
}

func (k *Kernel) Stat() (actorrt.UnitStat, bool) {`
		files, _ := patchArchWallSource(t, kernelPath, archWallPatch{old: anchor, new: withGetter})
		if got := systemKernelExportedUnitEscapes(files); len(got) == 0 {
			t.Fatal("one-shot transfer wall stayed green on an exported Unit getter")
		}
	})

	t.Run("second transfer entry point on production source", func(t *testing.T) {
		const anchor = "func (k *Kernel) IsRunning() bool {"
		const withAdopt = `func (k *Kernel) Adopt(unit *actorrt.Unit) error {
	return k.Start(unit)
}

func (k *Kernel) IsRunning() bool {`
		files, _ := patchArchWallSource(t, kernelPath, archWallPatch{old: anchor, new: withAdopt})
		if got := systemKernelExportedUnitEscapes(files); len(got) == 0 {
			t.Fatal("one-shot transfer wall stayed green on a second Unit-accepting entry point")
		}
	})

	fixtures := []struct {
		name  string
		src   string
		check func(map[string]*ast.File, *token.FileSet) []string
	}{
		{
			name: "in-place rebuild reinstalls the handle",
			src: `package systemkernel
func (k *Kernel) Start(unit *actorrt.Unit) error {
	k.unit = unit
	return nil
}
func (k *Kernel) Restart(fresh *actorrt.Unit) error {
	k.mu.Lock()
	defer k.mu.Unlock()
	k.unit = fresh
	return nil
}`,
			check: systemKernelUnitInstallations,
		},
		{
			name: "kernel prepares its own replacement body",
			src: `package systemkernel
func (k *Kernel) Start(unit *actorrt.Unit) error {
	k.unit = unit
	return nil
}
func (k *Kernel) respawn() error {
	fresh, err := actorrt.Prepare(actorrt.UnitConfig{}, nil, k)
	if err != nil {
		return err
	}
	_ = fresh
	return nil
}`,
			check: systemKernelRebuilds,
		},
	}
	for _, tc := range fixtures {
		t.Run(tc.name, func(t *testing.T) {
			files, fset := parseArchWallFixtureSource(t, "systemkernel_fixture.go", tc.src)
			if got := tc.check(files, fset); len(got) == 0 {
				t.Fatalf("wall did not trip on the break form %q", tc.name)
			}
		})
	}
}

// TestSystemKernelUnitTransferWallHasNoStrayHandleWriters double-checks the
// message the wall prints stays informative: the private field name it keys on
// must appear in kernel.go, so a rename cannot silently empty the check.
func TestSystemKernelUnitTransferWallHasNoStrayHandleWriters(t *testing.T) {
	source, err := readFile(systemKernelPkg + "/kernel.go")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(source, "k."+systemKernelUnitFieldName+" = unit") {
		t.Fatalf("kernel.go no longer adopts via k.%s = unit — retune the one-shot transfer wall",
			systemKernelUnitFieldName)
	}
}
