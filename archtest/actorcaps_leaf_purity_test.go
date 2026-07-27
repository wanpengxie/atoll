package archtest

import (
	"fmt"
	"go/ast"
	"go/token"
	"strconv"
	"strings"
	"testing"
)

// spec §13.3: "`lib/actorcaps` 终态不 import actorrt/actorctl/storespec；
// `Caps.Lifecycle` 与 `ForkSpec/LifecycleHandle` 均由 actorcaps 自有；
// `ForkSpec.Placement` 只使用 `protocol/channel.Placement` public DTO".
//
// actorcaps is the ACTOR-FACING vocabulary: it is what a body author writes
// against. Everything it names becomes part of the surface an actor can see, so
// the package has to stay a leaf over public protocol DTOs. The three banned
// imports are the three ways a control-plane or physical-plane concept gets
// dragged into that surface:
//
//   - actorrt  — the physical incarnation runtime; if it leaks in, an actor's
//     declared capability starts speaking about Units/Incarnations.
//   - actorctl — the control plane; if it leaks in, the actor's fork request
//     starts carrying the controller's internal shapes.
//   - storespec — durable rows; if it leaks in, a body author is writing
//     against storage normalization.
//
// Two facts make this worth a wall rather than trusting the compiler:
//
//  1. Only the actorctl direction is protected by anything today, and that
//     protection is accidental — actorctl already imports actorcaps, so the
//     reverse edge is a cyclic import Go rejects. actorrt and storespec have NO
//     cycle: `import ".../runtime/actorrt"` for one error constant compiles
//     right now and no test notices.
//  2. `ForkSpec.Placement` is the concrete field where the leak would land,
//     because placement is exactly the field that has a public DTO
//     (`protocol/channel.Placement`) AND a control-plane normalization. Swapping
//     the field's type to the internal one is a one-token edit that keeps
//     everything compiling in the control plane and silently moves store
//     normalization into the actor's declaration.

const actorCapsPkg = "../lib/actorcaps"

// actorCapsBannedImports is the spec's list, by suffix so a vendoring or module
// rename cannot slip past.
var actorCapsBannedImports = []string{
	"/runtime/actorrt",
	"/runtime/actorctl",
	"/runtime/storespec",
}

func actorCapsImportViolations(files map[string]*ast.File) []string {
	var v []string
	for path, file := range files {
		for _, spec := range file.Imports {
			importPath, err := strconv.Unquote(spec.Path.Value)
			if err != nil {
				continue
			}
			for _, banned := range actorCapsBannedImports {
				if strings.HasSuffix(importPath, banned) || strings.Contains(importPath, banned+"/") {
					v = append(v, fmt.Sprintf(
						"%s imports %q — the actor-facing capability vocabulary stays a leaf over public protocol DTOs",
						path, importPath))
				}
			}
		}
	}
	return v
}

// actorCapsForeignPlacement checks the one field the leak would actually land
// on: ForkSpec.Placement must be `*channel.Placement`, and `channel` must be the
// public protocol package.
func actorCapsForeignPlacement(files map[string]*ast.File, fset *token.FileSet) []string {
	var v []string
	seen := false
	for path, file := range files {
		// Which local name refers to protocol/channel in this file.
		channelAlias := ""
		for _, spec := range file.Imports {
			importPath, err := strconv.Unquote(spec.Path.Value)
			if err != nil {
				continue
			}
			if !strings.HasSuffix(importPath, "/protocol/channel") {
				continue
			}
			if spec.Name != nil {
				channelAlias = spec.Name.Name
			} else {
				channelAlias = "channel"
			}
		}
		ast.Inspect(file, func(node ast.Node) bool {
			spec, ok := node.(*ast.TypeSpec)
			if !ok || spec.Name.Name != "ForkSpec" {
				return true
			}
			structType, ok := spec.Type.(*ast.StructType)
			if !ok || structType.Fields == nil {
				return true
			}
			for _, field := range structType.Fields.List {
				named := ""
				if len(field.Names) > 0 {
					named = field.Names[0].Name
				}
				if named != "Placement" {
					continue
				}
				seen = true
				star, ok := field.Type.(*ast.StarExpr)
				if !ok {
					v = append(v, fmt.Sprintf("%s:%d ForkSpec.Placement is not the public *channel.Placement DTO",
						path, fset.Position(field.Pos()).Line))
					continue
				}
				selector, ok := star.X.(*ast.SelectorExpr)
				if !ok {
					v = append(v, fmt.Sprintf("%s:%d ForkSpec.Placement is not a package-qualified DTO",
						path, fset.Position(field.Pos()).Line))
					continue
				}
				pkg, ok := selector.X.(*ast.Ident)
				if !ok || channelAlias == "" || pkg.Name != channelAlias || selector.Sel.Name != "Placement" {
					v = append(v, fmt.Sprintf(
						"%s:%d ForkSpec.Placement is %s.%s — an actor declares a child with the public protocol/channel.Placement, never a control-plane or store shape",
						path, fset.Position(field.Pos()).Line, expressionText(fset, selector.X), selector.Sel.Name))
				}
			}
			return true
		})
	}
	if !seen {
		v = append(v, "lib/actorcaps declares no ForkSpec.Placement field — the placement DTO wall lost its subject")
	}
	return v
}

// actorCapsForeignOwnership checks the "均由 actorcaps 自有" half: the lifecycle
// contract and the fork declaration are this package's own types, so
// `Caps.Lifecycle` must be a package-local identifier, not a selector into some
// other package's interface.
func actorCapsForeignOwnership(files map[string]*ast.File, fset *token.FileSet) []string {
	var v []string
	ownTypes := map[string]bool{}
	for _, file := range files {
		ast.Inspect(file, func(node ast.Node) bool {
			if spec, ok := node.(*ast.TypeSpec); ok {
				ownTypes[spec.Name.Name] = true
			}
			return true
		})
	}
	for _, required := range []string{"ForkSpec", "LifecycleHandle", "Caps"} {
		if !ownTypes[required] {
			v = append(v, fmt.Sprintf("lib/actorcaps no longer declares %s — the lifecycle vocabulary left its home", required))
		}
	}
	for path, file := range files {
		ast.Inspect(file, func(node ast.Node) bool {
			spec, ok := node.(*ast.TypeSpec)
			if !ok || spec.Name.Name != "Caps" {
				return true
			}
			structType, ok := spec.Type.(*ast.StructType)
			if !ok || structType.Fields == nil {
				return true
			}
			for _, field := range structType.Fields.List {
				if len(field.Names) == 0 || field.Names[0].Name != "Lifecycle" {
					continue
				}
				ident, ok := field.Type.(*ast.Ident)
				if !ok || !ownTypes[ident.Name] {
					v = append(v, fmt.Sprintf(
						"%s:%d Caps.Lifecycle is %s — the lifecycle capability contract is actorcaps' own type",
						path, fset.Position(field.Pos()).Line, expressionText(fset, field.Type)))
				}
			}
			return true
		})
	}
	return v
}

func TestActorCapsStaysALeafOverPublicDTOs(t *testing.T) {
	files, fset := loadArchWallPackage(t, actorCapsPkg)
	failViolations(t, "lib/actorcaps imports no runtime control/physical/store package",
		actorCapsImportViolations(files))
	failViolations(t, "ForkSpec.Placement is the public protocol DTO",
		actorCapsForeignPlacement(files, fset))
	failViolations(t, "the lifecycle vocabulary is actorcaps' own",
		actorCapsForeignOwnership(files, fset))
}

// TestActorCapsLeafWallTripsOnRuntimeLeak is the trip proof. The import case is
// applied to the real lifecycle.go, because that is the file where a "just one
// error constant" edit would land.
func TestActorCapsLeafWallTripsOnRuntimeLeak(t *testing.T) {
	const path = actorCapsPkg + "/lifecycle.go"

	t.Run("one runtime import for one error constant", func(t *testing.T) {
		const anchor = `	"github.com/wanpengxie/atoll/protocol/channel"`
		const patched = `	"github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/runtime/actorrt"`
		files, _ := patchArchWallSource(t, path, archWallPatch{old: anchor, new: patched})
		if got := actorCapsImportViolations(files); len(got) == 0 {
			t.Fatal("actorcaps leaf wall stayed green on a runtime/actorrt import")
		}
	})

	t.Run("placement retyped to a control-plane shape", func(t *testing.T) {
		const anchor = "	Placement *channel.Placement"
		const patched = "	Placement *actorctl.Placement"
		files, fset := patchArchWallSource(t, path, archWallPatch{old: anchor, new: patched})
		if got := actorCapsForeignPlacement(files, fset); len(got) == 0 {
			t.Fatal("placement DTO wall stayed green on a control-plane placement type")
		}
	})

	t.Run("placement flattened to a bare string", func(t *testing.T) {
		const anchor = "	Placement *channel.Placement"
		const patched = "	Placement string"
		files, fset := patchArchWallSource(t, path, archWallPatch{old: anchor, new: patched})
		if got := actorCapsForeignPlacement(files, fset); len(got) == 0 {
			t.Fatal("placement DTO wall stayed green on a non-DTO placement field")
		}
	})

	t.Run("lifecycle contract borrowed from another package", func(t *testing.T) {
		src := `package actorcaps
type ForkSpec struct{}
type LifecycleHandle interface{}
type Caps struct {
	Lifecycle actorctl.LifecycleHandle
}`
		files, fset := parseArchWallFixtureSource(t, "actorcaps_fixture.go", src)
		if got := actorCapsForeignOwnership(files, fset); len(got) == 0 {
			t.Fatal("ownership wall stayed green on a foreign Caps.Lifecycle type")
		}
	})
}
