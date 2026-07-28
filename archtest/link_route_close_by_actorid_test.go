package archtest

import (
	"fmt"
	"go/ast"
	"go/token"
	"strings"
	"testing"
)

// spec §13.3: "link 无按 ActorID 关闭 route 并暗示 termination 的 API".
//
// The physical layer owns objects, not identities. A stream or a binding is a
// thing that was opened, and the only handle on it is the object itself. That
// is what makes "close this route" an honest statement: it says nothing about
// the actor, only about one physical child that this session created.
//
// An API keyed by ActorID says something completely different. `CloseForActor
// (id)` reads as "this actor is finished", and callers will use it that way —
// but link has no authority over an actor's life, and it cannot even tell G1's
// stream from G2's. A by-ActorID close therefore does two illegitimate things at
// once: it invents a termination verdict link is not entitled to make, and it
// resolves that verdict against whichever generation happens to be in the set.
//
// The existing guard (`TestHarden03BPhysicalOwnersUseExactObjectIdentity`)
// forbids two literal spellings, `HasStream(` and `DetachStream(`. That is the
// textbook "钉死名不钉活名" failure: `CloseForActor`, `DropActorRoutes`,
// `RetireActor` — any of them is the same API under a new name, and all of them
// are green today.
//
// So this wall pins the CAPABILITY instead, along the two structural steps any
// such API must take:
//
//	step 1 — a physical child must become findable BY id: either the child
//	         carries an ActorID, or some index maps ActorID to children;
//	step 2 — an ActorID-parameterised function must SCAN or MUTATE the child
//	         registry to pick its victim.
//
// Opening (`OpenActorStream(ctx, id, key)`) legitimately takes an ActorID and
// legitimately INSERTS the child it just built — it never reads the set to find
// somebody else's.

const linkPhysicalPkg = "../platform/internal/link"

// linkChildRegistries are the session's two exact-object child sets.
var linkChildRegistries = map[string]bool{"streams": true, "bindings": true}

// linkPhysicalChildTypes are the objects a route close would have to reach.
var linkPhysicalChildTypes = map[string]bool{"ActorStream": true, "Binding": true}

func isActorIDType(expr ast.Expr, fset *token.FileSet) bool {
	return expressionText(fset, expr) == "actor.ActorID"
}

// registryRootIsChildSet reports whether an expression addresses one of the
// session's child registries (`s.streams`, `s.bindings`, or the bare field).
func registryRootIsChildSet(expr ast.Expr) bool {
	switch value := expr.(type) {
	case *ast.SelectorExpr:
		return linkChildRegistries[value.Sel.Name]
	case *ast.Ident:
		return linkChildRegistries[value.Name]
	}
	return false
}

// linkByActorIDRouteCloseViolations implements step 2: no ActorID-parameterised
// function may read the child registry.
func linkByActorIDRouteCloseViolations(files map[string]*ast.File, fset *token.FileSet) []string {
	var v []string
	for path, file := range files {
		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil || fn.Type.Params == nil {
				continue
			}
			takesActorID := false
			for _, param := range fn.Type.Params.List {
				if isActorIDType(param.Type, fset) {
					takesActorID = true
				}
			}
			if !takesActorID {
				continue
			}
			where := fmt.Sprintf("%s:%s", path, fn.Name.Name)

			// An insertion's LHS index expression is a write, not a lookup;
			// collect those positions so the rvalue scan below can skip them.
			inserted := map[token.Pos]bool{}
			ast.Inspect(fn.Body, func(node ast.Node) bool {
				assign, ok := node.(*ast.AssignStmt)
				if !ok {
					return true
				}
				for _, lhs := range assign.Lhs {
					if index, ok := lhs.(*ast.IndexExpr); ok {
						inserted[index.Pos()] = true
					}
				}
				return true
			})

			ast.Inspect(fn.Body, func(node ast.Node) bool {
				switch value := node.(type) {
				case *ast.RangeStmt:
					if registryRootIsChildSet(value.X) {
						v = append(v, fmt.Sprintf(
							"%s:%d scans the physical child registry while holding an ActorID — link resolves routes by object identity, never by actor",
							where, fset.Position(value.Pos()).Line))
					}
				case *ast.IndexExpr:
					if !inserted[value.Pos()] && registryRootIsChildSet(value.X) {
						v = append(v, fmt.Sprintf(
							"%s:%d looks a physical child up in the registry while holding an ActorID — that is a by-ActorID route handle",
							where, fset.Position(value.Pos()).Line))
					}
				case *ast.CallExpr:
					ident, ok := value.Fun.(*ast.Ident)
					if !ok || ident.Name != "delete" || len(value.Args) == 0 {
						return true
					}
					if registryRootIsChildSet(value.Args[0]) {
						v = append(v, fmt.Sprintf(
							"%s:%d unregisters a physical child while holding an ActorID — closing a route must not be reachable through an actor identity",
							where, fset.Position(value.Pos()).Line))
					}
				}
				return true
			})
		}
	}
	return v
}

// linkChildIdentityViolations implements step 1: a physical child carries no
// ActorID, and nothing indexes children by one.
func linkChildIdentityViolations(files map[string]*ast.File, fset *token.FileSet) []string {
	var v []string
	for path, file := range files {
		ast.Inspect(file, func(node ast.Node) bool {
			spec, ok := node.(*ast.TypeSpec)
			if !ok {
				return true
			}
			structType, ok := spec.Type.(*ast.StructType)
			if !ok || structType.Fields == nil {
				return true
			}
			for _, field := range structType.Fields.List {
				line := fset.Position(field.Pos()).Line

				if linkPhysicalChildTypes[spec.Name.Name] && isActorIDType(field.Type, fset) {
					name := "<embedded>"
					if len(field.Names) > 0 {
						name = field.Names[0].Name
					}
					v = append(v, fmt.Sprintf(
						"%s:%d %s carries an ActorID in %s — a physical child is identified by the object, so no by-actor match is possible",
						path, line, spec.Name.Name, name))
				}

				mapType, ok := field.Type.(*ast.MapType)
				if !ok || !isActorIDType(mapType.Key, fset) {
					continue
				}
				value := expressionText(fset, mapType.Value)
				for child := range linkPhysicalChildTypes {
					if strings.Contains(value, child) || strings.Contains(value, "ActorEndpoint") {
						v = append(v, fmt.Sprintf(
							"%s:%d %s indexes physical children by ActorID (map[actor.ActorID]%s) — that index IS the forbidden by-ActorID route API",
							path, line, spec.Name.Name, value))
						break
					}
				}
			}
			return true
		})
	}
	return v
}

func TestLinkHasNoByActorIDRouteCloseAPI(t *testing.T) {
	files, fset := loadArchWallPackage(t, linkPhysicalPkg)

	// Footing: both structural steps must have something to guard — there are
	// ActorID-parameterised functions, and the child registries exist.
	takers := 0
	for _, file := range files {
		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Type.Params == nil {
				continue
			}
			for _, param := range fn.Type.Params.List {
				if isActorIDType(param.Type, fset) {
					takers++
					break
				}
			}
		}
	}
	if takers == 0 {
		t.Fatal("no ActorID-parameterised function in platform/internal/link — retune the by-ActorID route wall")
	}
	source, err := readFile(linkPhysicalPkg + "/physical.go")
	if err != nil {
		t.Fatal(err)
	}
	for _, registry := range []string{"bindings map[*Binding]struct{}", "streams  map[*ActorStream]struct{}"} {
		if !strings.Contains(source, registry) {
			t.Fatalf("physical child registry %q is gone — retune the by-ActorID route wall", registry)
		}
	}

	failViolations(t, "link resolves no route through an ActorID",
		linkByActorIDRouteCloseViolations(files, fset))
	failViolations(t, "no physical child is findable by ActorID",
		linkChildIdentityViolations(files, fset))
}

// TestLinkByActorIDRouteWallTripsOnRenamedAPI is the trip proof, and it is
// deliberately built out of names the existing literal guard does NOT know.
func TestLinkByActorIDRouteWallTripsOnRenamedAPI(t *testing.T) {
	t.Run("CloseForActor on the real session", func(t *testing.T) {
		const path = linkPhysicalPkg + "/physical.go"
		const anchor = "// ChildCounts is a diagnostic snapshot."
		const patched = `// CloseForActor tears down whatever this session holds for one actor.
func (s *AuthenticatedLinkSession) CloseForActor(id actor.ActorID) error {
	s.mu.Lock()
	var victim *ActorStream
	for stream := range s.streams {
		if stream.owner == id {
			victim = stream
		}
	}
	s.mu.Unlock()
	if victim == nil {
		return nil
	}
	return victim.Close()
}

// ChildCounts is a diagnostic snapshot.`
		files, fset := patchArchWallSource(t, path, archWallPatch{old: anchor, new: patched})
		if got := linkByActorIDRouteCloseViolations(files, fset); len(got) == 0 {
			t.Fatal("by-ActorID route wall stayed green on a renamed close-by-actor API")
		}
	})

	fixtures := []struct {
		name  string
		src   string
		check func(map[string]*ast.File, *token.FileSet) []string
	}{
		{
			name: "unregister keyed by actor",
			src: `package link
func (s *AuthenticatedLinkSession) DropActorRoutes(id actor.ActorID) {
	s.mu.Lock()
	delete(s.streams, s.index[id])
	s.mu.Unlock()
}`,
			check: linkByActorIDRouteCloseViolations,
		},
		{
			name: "direct registry lookup by actor",
			src: `package link
func (s *AuthenticatedLinkSession) RetireActor(id actor.ActorID) error {
	stream := s.streams[id]
	return stream.Close()
}`,
			check: linkByActorIDRouteCloseViolations,
		},
		{
			name: "child grows an ActorID so it can be matched",
			src: `package link
type ActorStream struct {
	session *AuthenticatedLinkSession
	owner   actor.ActorID
}`,
			check: linkChildIdentityViolations,
		},
		{
			name: "session indexes children by actor",
			src: `package link
type AuthenticatedLinkSession struct {
	routes map[actor.ActorID]*ActorStream
}`,
			check: linkChildIdentityViolations,
		},
	}
	for _, tc := range fixtures {
		t.Run(tc.name, func(t *testing.T) {
			files, fset := parseArchWallFixtureSource(t, "link_route_fixture.go", tc.src)
			if got := tc.check(files, fset); len(got) == 0 {
				t.Fatalf("wall did not trip on the break form %q", tc.name)
			}
		})
	}

	t.Run("opening and registering a fresh child stays allowed", func(t *testing.T) {
		src := `package link
func (s *AuthenticatedLinkSession) OpenActorStream(ctx context.Context, id actor.ActorID, key actorhost.AttemptKey) (*ActorStream, error) {
	resource, err := s.opener(ctx, id, key)
	if err != nil {
		return nil, err
	}
	stream := newActorStream(s, resource)
	s.mu.Lock()
	s.streams[stream] = struct{}{}
	s.mu.Unlock()
	return stream, nil
}`
		files, fset := parseArchWallFixtureSource(t, "link_open_fixture.go", src)
		if got := linkByActorIDRouteCloseViolations(files, fset); len(got) != 0 {
			t.Fatalf("by-ActorID route wall fired on a legitimate open+register: %v", got)
		}
	})
}
