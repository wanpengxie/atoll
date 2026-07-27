package archtest

import (
	"fmt"
	"go/ast"
	"go/token"
	"testing"
)

// spec §13.3: "actorrt Prepare builder 只取得 exact read-only Self；actorhost
// BodyBuildInput 同时携同一 Self 与 exact ActualCurrent；无 exported binder".
//
// A body is built once, for one incarnation, and every authority it receives has
// to be welded to THAT incarnation. Two of those weldings ride in the same
// struct: `Self` (this is who you are) and `Current` (ask whether you are still
// the live one). The whole point is that they are the SAME incarnation. If they
// are not, the body holds a currency probe that answers about somebody else:
// a retired body can be told "yes, you are current" and keep writing, or a live
// one can be told "no" and quietly stop — and neither shows up as a type error,
// because both fields still have exactly the right types.
//
// The only way to keep them the same is to keep them SINGLE-SOURCED: both must
// come from the one read-only Incarnation that `actorrt.Prepare` hands the
// builder closure. The moment either side is filled from a second read — a
// cached snapshot, a fresh `h.states[id]` lookup, a "the incarnation we recorded
// a moment ago" — coherence is a timing accident.
//
// The existing walls miss this entirely: `TestHarden03BBodyConstructionUsesOnly
// ExactSnapshot` checks that lookups are by exact key rather than by ActorID,
// and `TestHarden03BNoExportedLifecycleBinder` checks the "no exported binder"
// clause. Nothing looks at where Self and Current get their values.
//
// So this wall pins the SOURCE, not the shape:
//
//   - every ActualCurrent is minted inside a Prepare builder closure, from that
//     closure's own incarnation parameter — there is no other place to mint one;
//   - every BodyBuildInput takes Self from that same parameter and Current from
//     an ActualCurrent minted from it;
//   - the builder closure receives `actorrt.Incarnation` (a read-only value),
//     never the Unit itself.

// prepareBuilder is one `actorrt.Prepare(cfg, func(self actorrt.Incarnation) …)`
// closure: the lexical region in which — and only in which — an incarnation
// exists to be welded.
type prepareBuilder struct {
	param string
	start token.Pos
	end   token.Pos
}

func prepareBuildersIn(file *ast.File) []prepareBuilder {
	var builders []prepareBuilder
	ast.Inspect(file, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return true
		}
		selector, ok := call.Fun.(*ast.SelectorExpr)
		if !ok || selector.Sel.Name != "Prepare" {
			return true
		}
		if pkg, ok := selector.X.(*ast.Ident); !ok || pkg.Name != "actorrt" {
			return true
		}
		for _, arg := range call.Args {
			lit, ok := arg.(*ast.FuncLit)
			if !ok || lit.Type.Params == nil || len(lit.Type.Params.List) != 1 {
				continue
			}
			param := lit.Type.Params.List[0]
			name := "_"
			if len(param.Names) > 0 {
				name = param.Names[0].Name
			}
			builders = append(builders, prepareBuilder{param: name, start: lit.Pos(), end: lit.End()})
		}
		return true
	})
	return builders
}

func enclosingPrepareBuilder(builders []prepareBuilder, pos token.Pos) (prepareBuilder, bool) {
	for _, builder := range builders {
		if builder.start <= pos && pos <= builder.end {
			return builder, true
		}
	}
	return prepareBuilder{}, false
}

// prepareBuilderInputViolations reports every incoherent or second-sourced weld.
func prepareBuilderInputViolations(files map[string]*ast.File, fset *token.FileSet) []string {
	var v []string
	for path, file := range files {
		builders := prepareBuildersIn(file)

		// The builder must be handed a read-only incarnation, not a body.
		ast.Inspect(file, func(node ast.Node) bool {
			call, ok := node.(*ast.CallExpr)
			if !ok {
				return true
			}
			selector, ok := call.Fun.(*ast.SelectorExpr)
			if !ok || selector.Sel.Name != "Prepare" {
				return true
			}
			if pkg, ok := selector.X.(*ast.Ident); !ok || pkg.Name != "actorrt" {
				return true
			}
			for _, arg := range call.Args {
				lit, ok := arg.(*ast.FuncLit)
				if !ok || lit.Type.Params == nil {
					continue
				}
				for _, param := range lit.Type.Params.List {
					text := expressionText(fset, param.Type)
					if text != "actorrt.Incarnation" && text != "Incarnation" {
						v = append(v, fmt.Sprintf(
							"%s:%d Prepare builder takes %s — the builder gets the exact read-only Self, never a body handle",
							path, fset.Position(param.Pos()).Line, text))
					}
				}
			}
			return true
		})

		// Which local names hold an ActualCurrent, and which incarnation each
		// was minted from.
		currentSource := map[string]string{}
		ast.Inspect(file, func(node ast.Node) bool {
			assign, ok := node.(*ast.AssignStmt)
			if !ok {
				return true
			}
			for i, rhs := range assign.Rhs {
				lit, ok := rhs.(*ast.CompositeLit)
				if !ok || i >= len(assign.Lhs) {
					continue
				}
				name, ok := lit.Type.(*ast.Ident)
				if !ok || name.Name != "ActualCurrent" {
					continue
				}
				target, ok := assign.Lhs[i].(*ast.Ident)
				if !ok {
					continue
				}
				currentSource[target.Name] = compositeFieldText(fset, lit, "self")
			}
			return true
		})

		ast.Inspect(file, func(node ast.Node) bool {
			lit, ok := node.(*ast.CompositeLit)
			if !ok {
				return true
			}
			name, ok := lit.Type.(*ast.Ident)
			if !ok {
				return true
			}
			line := fset.Position(lit.Pos()).Line
			switch name.Name {
			case "ActualCurrent":
				builder, inside := enclosingPrepareBuilder(builders, lit.Pos())
				if !inside {
					v = append(v, fmt.Sprintf(
						"%s:%d mints an ActualCurrent outside a Prepare builder — a currency probe can only be welded where the incarnation is handed over",
						path, line))
					return true
				}
				if got := compositeFieldText(fset, lit, "self"); got != builder.param {
					v = append(v, fmt.Sprintf(
						"%s:%d ActualCurrent.self is %q, not the builder's incarnation %q — the probe would answer about a different body",
						path, line, got, builder.param))
				}
			case "BodyBuildInput":
				builder, inside := enclosingPrepareBuilder(builders, lit.Pos())
				if !inside {
					v = append(v, fmt.Sprintf(
						"%s:%d builds a BodyBuildInput outside a Prepare builder — Self has no exact source there",
						path, line))
					return true
				}
				self := compositeFieldText(fset, lit, "Self")
				if self != builder.param {
					v = append(v, fmt.Sprintf(
						"%s:%d BodyBuildInput.Self is %q, not the builder's incarnation %q — a second read of 'who am I' is a coherence accident",
						path, line, self, builder.param))
				}
				current := compositeFieldExpr(lit, "Current")
				if current == nil {
					v = append(v, fmt.Sprintf(
						"%s:%d BodyBuildInput carries no Current — Self and the currency probe travel together",
						path, line))
					return true
				}
				source := ""
				switch value := current.(type) {
				case *ast.Ident:
					source = currentSource[value.Name]
				case *ast.CompositeLit:
					source = compositeFieldText(fset, value, "self")
				}
				if source != builder.param {
					v = append(v, fmt.Sprintf(
						"%s:%d BodyBuildInput.Current was minted from %q while Self is %q — one body must not carry two incarnations",
						path, line, source, builder.param))
				}
			}
			return true
		})
	}
	return v
}

// compositeFieldExpr returns the value expression of one keyed field.
func compositeFieldExpr(lit *ast.CompositeLit, field string) ast.Expr {
	for _, element := range lit.Elts {
		kv, ok := element.(*ast.KeyValueExpr)
		if !ok {
			continue
		}
		if key, ok := kv.Key.(*ast.Ident); ok && key.Name == field {
			return kv.Value
		}
	}
	return nil
}

// compositeFieldText renders a keyed field's value; a value that is not a plain
// identifier renders as its source text, which is exactly what the wall reports
// when someone substitutes a second read.
func compositeFieldText(fset *token.FileSet, lit *ast.CompositeLit, field string) string {
	expr := compositeFieldExpr(lit, field)
	if expr == nil {
		return ""
	}
	if ident, ok := expr.(*ast.Ident); ok {
		return ident.Name
	}
	return expressionText(fset, expr)
}

func TestBodyBuildInputSelfAndCurrentShareOneIncarnation(t *testing.T) {
	files, fset := loadArchWallPackage(t, "../runtime/actorhost")

	// Footing: there must be a real weld to guard.
	welds := 0
	for _, file := range files {
		ast.Inspect(file, func(node ast.Node) bool {
			lit, ok := node.(*ast.CompositeLit)
			if !ok {
				return true
			}
			if name, ok := lit.Type.(*ast.Ident); ok && name.Name == "BodyBuildInput" {
				welds++
			}
			return true
		})
	}
	if welds == 0 {
		t.Fatal("runtime/actorhost constructs no BodyBuildInput — the Self/Current coherence wall lost its subject")
	}

	failViolations(t, "Self and ActualCurrent are single-sourced from the Prepare builder's incarnation",
		prepareBuilderInputViolations(files, fset))

	// The other Prepare callsite (assembly's system body) must also receive the
	// read-only incarnation.
	homeFiles, homeFset := loadArchWallPackage(t, "../platform/home")
	failViolations(t, "every Prepare builder takes the exact read-only Self",
		prepareBuilderInputViolations(homeFiles, homeFset))
}

// TestBodyBuildInputCoherenceWallTripsOnASecondRead is the trip proof. The
// first three cases are applied to the real host.go build path.
func TestBodyBuildInputCoherenceWallTripsOnASecondRead(t *testing.T) {
	const path = "../runtime/actorhost/host.go"

	t.Run("Self filled from a cached snapshot", func(t *testing.T) {
		files, fset := patchArchWallSource(t, path, archWallPatch{
			old: "			Self:          self,",
			new: "			Self:          h.snapshotSelf(id),",
		})
		if got := prepareBuilderInputViolations(files, fset); len(got) == 0 {
			t.Fatal("coherence wall stayed green on a second-read Self")
		}
	})

	t.Run("ActualCurrent minted from a live re-read", func(t *testing.T) {
		files, fset := patchArchWallSource(t, path, archWallPatch{
			old: "		current = ActualCurrent{host: h, id: id, key: job.key, self: self}",
			new: "		current = ActualCurrent{host: h, id: id, key: job.key, self: h.liveSelf(id)}",
		})
		if got := prepareBuilderInputViolations(files, fset); len(got) == 0 {
			t.Fatal("coherence wall stayed green on an ActualCurrent minted from a second read")
		}
	})

	t.Run("builder handed the body instead of the incarnation", func(t *testing.T) {
		files, fset := patchArchWallSource(t, path, archWallPatch{
			old: "	}, func(self actorrt.Incarnation) actorrt.Actor {",
			new: "	}, func(self *actorrt.Unit) actorrt.Actor {",
		})
		if got := prepareBuilderInputViolations(files, fset); len(got) == 0 {
			t.Fatal("read-only Self wall stayed green on a builder that receives the Unit")
		}
	})

	fixtures := []struct {
		name string
		src  string
	}{
		{
			name: "Current wired from a different incarnation",
			src: `package actorhost
func (h *HostSupervisor) build(id actor.ActorID, job *buildJob) {
	var current ActualCurrent
	unit, err := actorrt.Prepare(actorrt.UnitConfig{}, func(self actorrt.Incarnation) actorrt.Actor {
		stale := ActualCurrent{host: h, id: id, key: job.key, self: h.lastSelf}
		current = stale
		input := BodyBuildInput{
			ActorID: id,
			Self:    self,
			Current: stale,
		}
		return h.builder(input)
	}, h)
	_, _ = unit, err
	_ = current
}`,
		},
		{
			name: "ActualCurrent minted outside any builder",
			src: `package actorhost
func (h *HostSupervisor) probeFor(id actor.ActorID, key AttemptKey, self actorrt.Incarnation) ActualCurrent {
	return ActualCurrent{host: h, id: id, key: key, self: self}
}`,
		},
		{
			name: "BodyBuildInput assembled outside any builder",
			src: `package actorhost
func (h *HostSupervisor) inputFor(id actor.ActorID, job *buildJob, self actorrt.Incarnation) BodyBuildInput {
	return BodyBuildInput{ActorID: id, Self: self, Current: h.probeFor(id, job.key, self)}
}`,
		},
	}
	for _, tc := range fixtures {
		t.Run(tc.name, func(t *testing.T) {
			files, fset := parseArchWallFixtureSource(t, "actorhost_build_fixture.go", tc.src)
			if got := prepareBuilderInputViolations(files, fset); len(got) == 0 {
				t.Fatalf("wall did not trip on the break form %q", tc.name)
			}
		})
	}
}
