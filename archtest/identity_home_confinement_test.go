package archtest

import (
	"fmt"
	"go/ast"
	"go/token"
	"sort"
	"strings"
	"testing"
)

// spec §13.3:
//   - "`ActorDefinition`、`ActiveActor`、`PreparedRun`、`IdentityAdmission`、
//     `ActorAuthority` 与 wire 不得携 IdentityHome"                        (audit #13)
//   - "AccessDoor/Resource/Schedule/Harness/managedcaps/systemcaps/View 不得
//     import 或分支 IdentityHome"                                          (audit #16)
//
// Identity storage home — whether an ActorID's identity lives in the durable
// registry or only in this process — is a fact of the identity store and of
// nothing else. The moment a coordination DTO carries it, every downstream that
// receives the DTO gains the ability to branch on it, and "same identity"
// silently stops being one predicate. That is why the ban is on the FIELD, not
// on a call: a field is a standing invitation.
//
// Both break forms the audit named are one line and compile cleanly:
//
//	#13: add `Home IdentityHome` to IdentityAdmission "so admission resolves the
//	     storage location once and saves a later lookup". Nothing in the tree
//	     does field-level scanning of these DTOs today.
//	#16: add a "memory-home vs durable-home takes a different validation branch"
//	     to accessdoor or harness. Five of the six named packages have no
//	     forbidden-word wall over them at all (only Schedule's handle.go is
//	     covered).
//
// The field/ident scans below are AST-level so a field named `Home` of ANY type
// trips #13, and so comments and doc prose (which legitimately discuss timer
// homes) never do.

// identityHomeDTOs are the coordination values the spec names by hand.
var identityHomeDTOs = map[string]bool{
	"ActorDefinition":    true,
	"ActiveActor":        true,
	"PreparedRun":        true,
	"IdentityAdmission":  true,
	"ActorAuthority":     true,
	"IdentityAuthority":  true,
	"ActorRecord":        true,
	"ActorDraft":         true,
	"ActorFacts":         true,
	"PenBasis":           true,
	"BodyBuildInput":     true,
	"IdentityCurrent":    true,
	"ExecutionSpec":      true,
	"ForkSpec":           true,
	"LifecycleHandle":    true,
	"IdentityAdmissions": true,
}

// identityHomeDTOsRequired must actually be found, otherwise a rename would
// silently empty the wall.
var identityHomeDTOsRequired = []string{"ActorDefinition", "IdentityAdmission", "PreparedRun"}

// identityHomeVocabulary are the identity-storage-home names. Timer storage
// home (`TimerHomeDurable` / `TimerHomeMemory`) is a DIFFERENT axis — where a
// timer row lives — and is legitimate inside the schedule engine, so it is
// exempted there and only there.
var identityHomeVocabulary = []string{
	"identityhome",
	"homeof",
	"homereader",
	"homedurable",
	"homememory",
	"memoryhome",
	"durablehome",
	"storagehome",
	"actorhome",
	"identitystore",
}

func identityHomeNameHit(name string) string {
	lower := strings.ToLower(name)
	for _, word := range identityHomeVocabulary {
		if strings.Contains(lower, word) {
			return word
		}
	}
	return ""
}

// identityHomeDTOViolations is #13: none of the named coordination values may
// carry a storage-home field, by name or by type.
func identityHomeDTOViolations(
	files map[string]*ast.File,
	fset *token.FileSet,
	found map[string]bool,
) []string {
	var v []string
	for path, file := range files {
		ast.Inspect(file, func(node ast.Node) bool {
			spec, ok := node.(*ast.TypeSpec)
			if !ok || !identityHomeDTOs[spec.Name.Name] {
				return true
			}
			found[spec.Name.Name] = true
			structure, ok := spec.Type.(*ast.StructType)
			if !ok {
				return false
			}
			for _, field := range structure.Fields.List {
				typeText := expressionText(fset, field.Type)
				names := []string{}
				for _, name := range field.Names {
					names = append(names, name.Name)
				}
				if len(names) == 0 {
					names = append(names, typeText) // embedded field
				}
				for _, name := range names {
					if strings.Contains(strings.ToLower(name), "home") ||
						strings.Contains(strings.ToLower(typeText), "home") {
						v = append(v, fmt.Sprintf(
							"%s:%d %s carries identity storage home %s %s — a coordination value that names where an identity lives lets every consumer branch on it",
							path, fset.Position(field.Pos()).Line, spec.Name.Name, name, typeText))
					}
				}
			}
			return false
		})
		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Recv == nil || len(fn.Recv.List) == 0 {
				continue
			}
			receiver := strings.TrimPrefix(expressionText(fset, fn.Recv.List[0].Type), "*")
			if !identityHomeDTOs[receiver] {
				continue
			}
			if identityHomeNameHit(fn.Name.Name) != "" ||
				strings.EqualFold(fn.Name.Name, "home") {
				v = append(v, fmt.Sprintf(
					"%s:%d %s.%s answers an identity storage home question",
					path, fset.Position(fn.Pos()).Line, receiver, fn.Name.Name))
			}
			if fn.Type.Results == nil {
				continue
			}
			for _, result := range fn.Type.Results.List {
				text := expressionText(fset, result.Type)
				if identityHomeNameHit(text) != "" {
					v = append(v, fmt.Sprintf(
						"%s:%d %s.%s returns identity storage home %s",
						path, fset.Position(fn.Pos()).Line, receiver, fn.Name.Name, text))
				}
			}
		}
	}
	return v
}

// identityHomeConsumerViolations is #16: the six enforcement/read surfaces may
// neither name nor branch on identity storage home. Nothing there can name it
// ⇒ nothing there can branch on it.
func identityHomeConsumerViolations(
	files map[string]*ast.File,
	fset *token.FileSet,
	timerHomeExempt bool,
) []string {
	var v []string
	report := func(path string, pos token.Pos, what, name string) {
		v = append(v, fmt.Sprintf(
			"%s:%d names identity storage home via %s %q — this surface must not learn, let alone branch on, where an identity is stored",
			path, fset.Position(pos).Line, what, name))
	}
	for path, file := range files {
		ast.Inspect(file, func(node ast.Node) bool {
			switch value := node.(type) {
			case *ast.SelectorExpr:
				name := value.Sel.Name
				if timerHomeExempt && strings.Contains(strings.ToLower(name), "timerhome") {
					return true
				}
				if identityHomeNameHit(name) != "" {
					report(path, value.Pos(), "selector", name)
					return true
				}
				if name == "Home" && !timerHomeExempt {
					report(path, value.Pos(), "selector", name)
				}
			case *ast.Ident:
				if timerHomeExempt && strings.Contains(strings.ToLower(value.Name), "timerhome") {
					return true
				}
				if identityHomeNameHit(value.Name) != "" {
					report(path, value.Pos(), "identifier", value.Name)
				}
			case *ast.Field:
				for _, name := range value.Names {
					if name.Name == "Home" && !timerHomeExempt {
						report(path, name.Pos(), "field", name.Name)
					}
				}
			}
			return true
		})
	}
	return v
}

// identityHomeConsumerRoots are the six surfaces the spec names. "Resource" is
// runtime/resourcespec (the resource kind/spec authority) and "View" is the
// Home read projection.
var identityHomeConsumerRoots = []struct {
	root        string
	timerExempt bool
}{
	{root: "../runtime/accessdoor"},
	{root: "../runtime/resourcespec"},
	{root: "../runtime/schedule", timerExempt: true},
	{root: "../runtime/harness"},
	{root: "../runtime/managedcaps"},
	{root: "../runtime/systemcaps"},
}

func TestIdentityHomeStaysInsideTheIdentityStore(t *testing.T) {
	found := map[string]bool{}
	var dto []string
	for _, root := range []string{"../runtime", "../platform", "../lib", "../protocol"} {
		files, fset := loadArchWallPackage(t, root)
		dto = append(dto, identityHomeDTOViolations(files, fset, found)...)
	}
	var missing []string
	for _, name := range identityHomeDTOsRequired {
		if !found[name] {
			missing = append(missing, name)
		}
	}
	sort.Strings(missing)
	if len(missing) > 0 {
		t.Fatalf("coordination DTOs %v not found — the field wall has nothing to hold", missing)
	}
	failViolations(t, "coordination DTOs carry no identity storage home (spec §13.3)", dto)

	var consumer []string
	for _, surface := range identityHomeConsumerRoots {
		files, fset := loadArchWallPackage(t, surface.root)
		consumer = append(consumer,
			identityHomeConsumerViolations(files, fset, surface.timerExempt)...)
	}
	viewFiles, viewFset := loadArchWallSingleFile(t, "../platform/home/view.go")
	consumer = append(consumer, identityHomeConsumerViolations(viewFiles, viewFset, false)...)
	failViolations(t,
		"AccessDoor/Resource/Schedule/Harness/managedcaps/systemcaps/View ⊥ identity storage home",
		consumer)
}

func TestIdentityHomeWallTripsOnTheOneLineField(t *testing.T) {
	t.Run("DTO gains a storage home field", func(t *testing.T) {
		files, fset := parseArchWallFixtureSource(t, "identity_home_fixture.go", `package storespec
type IdentityAdmission struct {
	ID   actor.ActorID
	Kind actor.Kind
	Home IdentityHome
}`)
		found := map[string]bool{}
		if got := identityHomeDTOViolations(files, fset, found); len(got) == 0 {
			t.Fatal("wall did not trip on `Home IdentityHome` added to IdentityAdmission")
		}
	})

	t.Run("DTO gains an unexported home field", func(t *testing.T) {
		files, fset := parseArchWallFixtureSource(t, "identity_home_fixture.go", `package actorctl
type PreparedRun struct {
	id   actor.ActorID
	home identityPlace
}`)
		found := map[string]bool{}
		if got := identityHomeDTOViolations(files, fset, found); len(got) == 0 {
			t.Fatal("wall did not trip on an unexported storage home field")
		}
	})

	t.Run("DTO answers a home question", func(t *testing.T) {
		files, fset := parseArchWallFixtureSource(t, "identity_home_fixture.go", `package storespec
type ActorDefinition struct {
	Class string
}
func (d ActorDefinition) HomeOf() string { return "" }`)
		found := map[string]bool{}
		if got := identityHomeDTOViolations(files, fset, found); len(got) == 0 {
			t.Fatal("wall did not trip on a storage-home accessor")
		}
	})

	t.Run("enforcement surface branches on storage home", func(t *testing.T) {
		files, fset := parseArchWallFixtureSource(t, "identity_home_fixture.go", `package accessdoor
func (d *Door) admit(a storespec.IdentityAdmission) bool {
	if a.Home == storespec.HomeMemory {
		return false
	}
	return true
}`)
		if got := identityHomeConsumerViolations(files, fset, false); len(got) == 0 {
			t.Fatal("wall did not trip on a memory-home/durable-home validation branch")
		}
	})

	t.Run("enforcement surface imports the vocabulary under a new name", func(t *testing.T) {
		files, fset := parseArchWallFixtureSource(t, "identity_home_fixture.go", `package harness
type penGate struct {
	homes identityHomeReader
}`)
		if got := identityHomeConsumerViolations(files, fset, false); len(got) == 0 {
			t.Fatal("wall did not trip on an identity-home reader held by harness")
		}
	})

	t.Run("real IdentityAdmission gains the field", func(t *testing.T) {
		files, fset := patchArchWallSource(t, "../runtime/storespec/actor_record.go",
			archWallPatch{
				old: "type IdentityAdmission struct {\n\tID   actor.ActorID\n\tKind actor.Kind\n}",
				new: "type IdentityAdmission struct {\n\tID   actor.ActorID\n\tKind actor.Kind\n\tHome IdentityHome\n}",
			})
		found := map[string]bool{}
		if got := identityHomeDTOViolations(files, fset, found); len(got) == 0 {
			t.Fatal("DTO wall stayed green after the real IdentityAdmission gained a storage home")
		}
	})

	t.Run("real accessdoor gains a home branch", func(t *testing.T) {
		files, fset := patchArchWallSource(t, "../runtime/accessdoor/door.go",
			archWallPatch{
				old: "func (d *door) driver(",
				new: `func (d *door) identityHomeBranch(a storespec.IdentityAdmission) bool {
	if a.Home == storespec.HomeMemory {
		return false
	}
	return true
}

func (d *door) driver(`,
			})
		if got := identityHomeConsumerViolations(files, fset, false); len(got) == 0 {
			t.Fatal("consumer wall stayed green after accessdoor branched on identity storage home")
		}
	})

	t.Run("timer home stays legal inside the schedule engine only", func(t *testing.T) {
		src := `package schedule
func (e *Engine) schedule(req ScheduleReq) {
	switch req.Home {
	case TimerHomeDurable:
	case TimerHomeMemory:
	}
}`
		files, fset := parseArchWallFixtureSource(t, "identity_home_fixture.go", src)
		if got := identityHomeConsumerViolations(files, fset, true); len(got) != 0 {
			t.Fatalf("timer storage home must stay legal in schedule: %v", got)
		}
		files, fset = parseArchWallFixtureSource(t, "identity_home_fixture.go", src)
		if got := identityHomeConsumerViolations(files, fset, false); len(got) == 0 {
			t.Fatal("the same branch must be illegal outside the schedule engine")
		}
	})
}
