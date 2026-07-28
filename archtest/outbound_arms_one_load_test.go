package archtest

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

// spec §13.3: "OutboundSlot 以一次 atomic immutable-bundle pointer swap 同时发布五个
// arms；facade 每笔调用 one-load/one-call，生产路径不得包含 error-triggered
// reload/retry loop".
//
// The pre-existing wall (harden03b_final_contract_test.go) pins only the FIRST
// half — the literal presence of `atomic.Pointer[OutboundArmsBundle]`, `Swap`
// and `Load`. That is the part a static structure gets right once and never
// breaks by accident. The second half is a RUNTIME BEHAVIOUR shape, and it is
// the one a well-meaning patch breaks: wrapping a facade call in
// `for attempts := 0; attempts < 3; attempts++ { … }` to "stabilise flaky
// sends" reads like a robustness fix, not like an architecture violation.
//
// A retry/reload inside a facade call is a violation because the bundle is the
// body's generation identity: re-loading after an error can silently move the
// call onto a SUCCESSOR generation's arms, i.e. execute a dead incarnation's
// side effect on the live one's stream. one-load/one-call is what makes "this
// call belonged to exactly this generation" decidable.
//
// KEYED ON SHAPE, NOT ON NAMES. An earlier form of this wall held an allowlist
// of five helper names (loadConnected/loadIdentity/loadAttempt/loadPhysical/
// publishObsNow) and could therefore be walked around two ways: call
// `slot.arms.Load()` directly (a form production already uses in the daemon
// converge path), or rename one helper and watch the function it guarded fall
// out of the checked set entirely. Both are the "钉死名不钉活名" defect. So the
// sets are DERIVED from the package instead:
//
//   - a bundle ACQUISITION is `<x>.arms.Load()`, or a call to a loader;
//   - a LOADER is any function that returns `*OutboundArmsBundle` AND performs
//     an acquisition — i.e. a function whose job is to hand a generation to its
//     caller. Computed to a fixpoint, so a chain of delegating helpers is one;
//   - a FACADE is any function that calls a loader. That is exactly "the code
//     that gets handed a generation to act on", regardless of spelling.
//
// The daemon's own converge machinery loads directly under the lock and returns
// nothing, so it is an acquisition site but not a loader and not a facade: it is
// held to one-load, and to no loop around the load, but not to one-arm-call —
// comparing generations is its job, invoking arms is not.

// outboundArmsBundleType is the swapped immutable value. A function handing one
// of these back is, by shape, a bundle loader.
const outboundArmsBundleType = "OutboundArmsBundle"

// outboundArmFields is the five-arm bundle the spec says is published in ONE
// swap. A composite literal that arms some but not all of them is a partial
// publication, which is the same defect the atomic swap exists to prevent.
var outboundArmFields = []string{"Pen", "Access", "State", "Schedule", "Lifecycle"}

// outboundAnchorFacade is the function the spec clause is named against. The
// footing check requires it to still be inside the derived facade set, so
// renaming it out of coverage turns the wall red instead of quiet.
const outboundAnchorFacade = "outboundPen.Write"

// loadArchWallPackage parses one production package with a single shared
// FileSet so positions from different files stay comparable.
func loadArchWallPackage(t *testing.T, root string) (map[string]*ast.File, *token.FileSet) {
	t.Helper()
	fset := token.NewFileSet()
	files := map[string]*ast.File{}
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			if skipDirs[entry.Name()] {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		file, perr := parser.ParseFile(fset, path, nil, 0)
		if perr != nil {
			return fmt.Errorf("parse %s: %w", path, perr)
		}
		files[filepath.ToSlash(path)] = file
		return nil
	})
	if err != nil {
		t.Fatalf("load %s: %v", root, err)
	}
	if len(files) == 0 {
		t.Fatalf("load %s: no production files found", root)
	}
	return files, fset
}

// loadArchWallSingleFile parses one named production file.
func loadArchWallSingleFile(t *testing.T, path string) (map[string]*ast.File, *token.FileSet) {
	t.Helper()
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, path, nil, 0)
	if err != nil {
		t.Fatalf("parse %s: %v", path, err)
	}
	return map[string]*ast.File{filepath.ToSlash(path): file}, fset
}

// parseArchWallFixtureSource parses one synthetic package fragment so a wall can
// be shown to actually trip on the break form it claims to stop.
func parseArchWallFixtureSource(t *testing.T, name, src string) (map[string]*ast.File, *token.FileSet) {
	t.Helper()
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, name, src, 0)
	if err != nil {
		t.Fatalf("parse fixture %s: %v", name, err)
	}
	return map[string]*ast.File{name: file}, fset
}

// archWallPatch is one in-memory edit of a real production file. It is how a
// wall proves it stops the break form on the ACTUAL code rather than on a
// hand-written miniature: the edit is applied to the real source, the result is
// parsed, and the wall must report it. Each `old` must occur exactly once, so
// the proof fails loudly if the production shape drifts instead of silently
// testing nothing.
type archWallPatch struct {
	old string
	new string
}

func patchArchWallSource(t *testing.T, path string, patches ...archWallPatch) (map[string]*ast.File, *token.FileSet) {
	t.Helper()
	source, err := readFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	for _, patch := range patches {
		if count := strings.Count(source, patch.old); count != 1 {
			t.Fatalf("%s: anchor %q occurs %d times, want exactly 1 — the trip proof lost its footing",
				path, patch.old, count)
		}
		source = strings.Replace(source, patch.old, patch.new, 1)
	}
	fset := token.NewFileSet()
	file, perr := parser.ParseFile(fset, path, source, 0)
	if perr != nil {
		t.Fatalf("parse patched %s: %v", path, perr)
	}
	return map[string]*ast.File{filepath.ToSlash(path): file}, fset
}

// archWallRootIdent returns the leftmost identifier of a selector chain, e.g.
// "bundle" for bundle.Pen.Write.
func archWallRootIdent(expr ast.Expr) string {
	for {
		switch value := expr.(type) {
		case *ast.SelectorExpr:
			expr = value.X
		case *ast.CallExpr:
			expr = value.Fun
		case *ast.IndexExpr:
			expr = value.X
		case *ast.ParenExpr:
			expr = value.X
		case *ast.Ident:
			return value.Name
		default:
			return ""
		}
	}
}

// ---------------------------------------------------------------------------
// shape derivation
// ---------------------------------------------------------------------------

// outboundIsDirectArmsLoad recognises `<anything>.arms.Load()` — the raw
// acquisition, which production uses inside the daemon's converge machinery and
// which the name-keyed wall was blind to.
func outboundIsDirectArmsLoad(call *ast.CallExpr) bool {
	outer, ok := call.Fun.(*ast.SelectorExpr)
	if !ok || outer.Sel.Name != "Load" {
		return false
	}
	inner, ok := outer.X.(*ast.SelectorExpr)
	return ok && inner.Sel.Name == "arms"
}

// outboundCalleeName is the callee's identifier at a call site. archtest has no
// type information, so calls resolve by name; this is the same parser-level
// approximation the rest of the package runs on.
func outboundCalleeName(call *ast.CallExpr) string {
	switch fun := call.Fun.(type) {
	case *ast.SelectorExpr:
		return fun.Sel.Name
	case *ast.Ident:
		return fun.Name
	}
	return ""
}

// outboundReturnsBundle reports whether a function hands a bundle back to its
// caller — the shape that makes it a loader rather than a consumer.
func outboundReturnsBundle(fn *ast.FuncDecl) bool {
	if fn.Type.Results == nil {
		return false
	}
	for _, result := range fn.Type.Results.List {
		star, ok := result.Type.(*ast.StarExpr)
		if !ok {
			continue
		}
		if ident, ok := star.X.(*ast.Ident); ok && ident.Name == outboundArmsBundleType {
			return true
		}
	}
	return false
}

// outboundArmsModel is the derived call-graph view of one package.
type outboundArmsModel struct {
	// loaders / facades are keyed by bare function name because that is all a
	// call site carries without type information.
	loaders map[string]bool
	facades map[string]bool
	// facadeQualified carries receiver-qualified names ("outboundPen.Write") for
	// the footing anchor and for readable violation messages.
	facadeQualified map[string]bool
}

func outboundDeclKey(fn *ast.FuncDecl) string {
	if recv := recvBaseTypeName(fn); recv != "" {
		return recv + "." + fn.Name.Name
	}
	return fn.Name.Name
}

// outboundAcquisitions counts bundle acquisitions inside a node: direct
// `arms.Load()` calls plus calls to known loaders.
func outboundAcquisitions(node ast.Node, loaders map[string]bool) int {
	count := 0
	ast.Inspect(node, func(inner ast.Node) bool {
		call, ok := inner.(*ast.CallExpr)
		if !ok {
			return true
		}
		if outboundIsDirectArmsLoad(call) || loaders[outboundCalleeName(call)] {
			count++
		}
		return true
	})
	return count
}

func outboundLoaderCalls(node ast.Node, loaders map[string]bool) int {
	count := 0
	ast.Inspect(node, func(inner ast.Node) bool {
		call, ok := inner.(*ast.CallExpr)
		if !ok {
			return true
		}
		if loaders[outboundCalleeName(call)] {
			count++
		}
		return true
	})
	return count
}

func outboundFuncDecls(files map[string]*ast.File) []*ast.FuncDecl {
	var out []*ast.FuncDecl
	for _, file := range files {
		for _, decl := range file.Decls {
			if fn, ok := decl.(*ast.FuncDecl); ok && fn.Body != nil {
				out = append(out, fn)
			}
		}
	}
	return out
}

// buildOutboundArmsModel derives the loader set to a fixpoint, then the facade
// set that sits on top of it.
func buildOutboundArmsModel(files map[string]*ast.File) outboundArmsModel {
	decls := outboundFuncDecls(files)
	model := outboundArmsModel{
		loaders:         map[string]bool{},
		facades:         map[string]bool{},
		facadeQualified: map[string]bool{},
	}
	for changed := true; changed; {
		changed = false
		for _, fn := range decls {
			if model.loaders[fn.Name.Name] || !outboundReturnsBundle(fn) {
				continue
			}
			if outboundAcquisitions(fn.Body, model.loaders) > 0 {
				model.loaders[fn.Name.Name] = true
				changed = true
			}
		}
	}
	for _, fn := range decls {
		if model.loaders[fn.Name.Name] {
			continue
		}
		if outboundLoaderCalls(fn.Body, model.loaders) > 0 {
			model.facades[fn.Name.Name] = true
			model.facadeQualified[outboundDeclKey(fn)] = true
		}
	}
	return model
}

// outboundBundleBindings returns the identifiers a function binds to an acquired
// bundle, so "one call on the loaded bundle" can be counted without keying on
// the local variable being spelled `bundle`.
func outboundBundleBindings(fn *ast.FuncDecl, loaders map[string]bool) map[string]bool {
	bound := map[string]bool{}
	ast.Inspect(fn.Body, func(node ast.Node) bool {
		assign, ok := node.(*ast.AssignStmt)
		if !ok || len(assign.Rhs) != 1 || len(assign.Lhs) == 0 {
			return true
		}
		call, ok := assign.Rhs[0].(*ast.CallExpr)
		if !ok {
			return true
		}
		if !outboundIsDirectArmsLoad(call) && !loaders[outboundCalleeName(call)] {
			return true
		}
		if ident, ok := assign.Lhs[0].(*ast.Ident); ok && ident.Name != "_" {
			bound[ident.Name] = true
		}
		return true
	})
	return bound
}

// ---------------------------------------------------------------------------
// the three legs
// ---------------------------------------------------------------------------

// outboundFacadeViolations enforces one-load/one-call: any function that
// acquires a bundle acquires it exactly once; any function that was HANDED a
// bundle by a loader calls exactly one arm on it and contains no loop at all.
func outboundFacadeViolations(files map[string]*ast.File, fset *token.FileSet) []string {
	var v []string
	model := buildOutboundArmsModel(files)
	for path, file := range files {
		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				continue
			}
			acquisitions := outboundAcquisitions(fn.Body, model.loaders)
			if acquisitions == 0 {
				continue
			}
			where := fmt.Sprintf("%s:%s", path, outboundDeclKey(fn))
			if acquisitions != 1 {
				v = append(v, fmt.Sprintf(
					"%s acquires the arms bundle %d times — one call is one load (an error-triggered reload can move the call onto a successor generation)",
					where, acquisitions))
			}
			loops := 0
			ast.Inspect(fn.Body, func(node ast.Node) bool {
				switch node.(type) {
				case *ast.ForStmt, *ast.RangeStmt:
					loops++
				}
				return true
			})
			if model.loaders[fn.Name.Name] {
				// A loader delegates at most once and never loops while doing it.
				if loops > 0 {
					v = append(v, fmt.Sprintf("%s loops while acquiring the bundle", where))
				}
				continue
			}
			if !model.facades[fn.Name.Name] {
				// Direct-load plumbing (the daemon converge path): held to
				// one-load and to no loop, but it compares generations rather
				// than invoking arms, so one-call does not apply.
				if loops > 0 {
					v = append(v, fmt.Sprintf(
						"%s loops around a direct arms.Load() — re-reading the bundle in a loop is the retry shape the swap exists to make impossible",
						where))
				}
				continue
			}
			bound := outboundBundleBindings(fn, model.loaders)
			armCalls := 0
			ast.Inspect(fn.Body, func(node ast.Node) bool {
				call, ok := node.(*ast.CallExpr)
				if !ok {
					return true
				}
				selector, ok := call.Fun.(*ast.SelectorExpr)
				if !ok {
					return true
				}
				if bound[archWallRootIdent(selector.X)] {
					armCalls++
				}
				return true
			})
			if armCalls != 1 {
				v = append(v, fmt.Sprintf(
					"%s makes %d calls on the loaded bundle — one facade call is one call", where, armCalls))
			}
			if loops > 0 {
				v = append(v, fmt.Sprintf(
					"%s contains a loop around a bundle acquisition — production facades carry no retry/reload loop",
					where))
			}
		}
	}
	return v
}

// outboundRetryLoopViolations bans the retry loop wherever it is written, not
// just inside a facade method: no counted loop anywhere in the package may
// contain a bundle acquisition or a facade attempt, and the one legitimate range
// loop (draining held observations onto a freshly opened stream) may attempt
// each held value once.
func outboundRetryLoopViolations(files map[string]*ast.File, fset *token.FileSet) []string {
	var v []string
	model := buildOutboundArmsModel(files)
	countAttempts := func(node ast.Node) int {
		attempts := 0
		ast.Inspect(node, func(inner ast.Node) bool {
			call, ok := inner.(*ast.CallExpr)
			if !ok {
				return true
			}
			name := outboundCalleeName(call)
			if outboundIsDirectArmsLoad(call) || model.loaders[name] || model.facades[name] {
				attempts++
			}
			return true
		})
		return attempts
	}
	for path, file := range files {
		ast.Inspect(file, func(node ast.Node) bool {
			switch loop := node.(type) {
			case *ast.ForStmt:
				if countAttempts(loop.Body) > 0 {
					v = append(v, fmt.Sprintf(
						"%s: counted loop at %d re-acquires or re-attempts the arms bundle — an error-triggered retry loop is not a production shape here",
						path, fset.Position(loop.Pos()).Line))
				}
			case *ast.RangeStmt:
				attempts := countAttempts(loop.Body)
				if attempts == 0 {
					return true
				}
				if _, plain := loop.X.(*ast.Ident); !plain {
					v = append(v, fmt.Sprintf(
						"%s: range at %d attempts the wire over a computed sequence",
						path, fset.Position(loop.Pos()).Line))
				}
				if attempts > 1 {
					v = append(v, fmt.Sprintf(
						"%s: range at %d makes %d wire attempts per element — that is a retry",
						path, fset.Position(loop.Pos()).Line, attempts))
				}
			}
			return true
		})
	}
	return v
}

// outboundPartialBundleViolations keeps the swapped value whole: any bundle
// literal that arms one capability arms all five, so a single pointer swap can
// never publish a half-armed generation.
func outboundPartialBundleViolations(files map[string]*ast.File, fset *token.FileSet) []string {
	var v []string
	for path, file := range files {
		ast.Inspect(file, func(node ast.Node) bool {
			lit, ok := node.(*ast.CompositeLit)
			if !ok {
				return true
			}
			name, ok := lit.Type.(*ast.Ident)
			if !ok || name.Name != outboundArmsBundleType {
				return true
			}
			present := map[string]bool{}
			for _, element := range lit.Elts {
				kv, ok := element.(*ast.KeyValueExpr)
				if !ok {
					continue
				}
				key, ok := kv.Key.(*ast.Ident)
				if !ok {
					continue
				}
				present[key.Name] = true
			}
			armed, missing := 0, []string{}
			for _, arm := range outboundArmFields {
				if present[arm] {
					armed++
				} else {
					missing = append(missing, arm)
				}
			}
			if armed > 0 && len(missing) > 0 {
				v = append(v, fmt.Sprintf(
					"%s:%d publishes a partial arms bundle (missing %s) — the five arms go live in one swap",
					path, fset.Position(lit.Pos()).Line, strings.Join(missing, ",")))
			}
			return true
		})
	}
	return v
}

func TestOutboundFacadeIsOneLoadOneCallWithoutRetry(t *testing.T) {
	files, fset := loadArchWallPackage(t, "../platform/compute")

	// Footing: the derived sets must be non-empty AND must still contain the
	// function the spec clause is named against. A rename that moves
	// outboundPen.Write out of the facade set turns this red instead of leaving
	// the checks quietly guarding a smaller set.
	model := buildOutboundArmsModel(files)
	if len(model.loaders) == 0 {
		t.Fatal("no bundle loader derived from platform/compute — nothing returns *OutboundArmsBundle off an arms.Load()")
	}
	if len(model.facades) == 0 {
		t.Fatal("no arms facade derived from platform/compute — no function is handed a bundle by a loader")
	}
	if !model.facadeQualified[outboundAnchorFacade] {
		names := make([]string, 0, len(model.facadeQualified))
		for name := range model.facadeQualified {
			names = append(names, name)
		}
		sort.Strings(names)
		t.Fatalf("%s is no longer in the derived facade set (found: %s) — the one-load/one-call wall lost its named anchor",
			outboundAnchorFacade, strings.Join(names, ", "))
	}

	failViolations(t, "outbound facade one-load/one-call", outboundFacadeViolations(files, fset))
	failViolations(t, "outbound production path carries no reload/retry loop",
		outboundRetryLoopViolations(files, fset))
	failViolations(t, "outbound arms bundle publishes all five arms at once",
		outboundPartialBundleViolations(files, fset))
}

// outboundFixturePrelude gives a synthetic fragment the loader declaration the
// shape derivation needs. It is deliberately spelled with names that appear
// NOWHERE in production, so a fixture that trips proves the wall found the
// loader by shape rather than by recognising a blessed name.
const outboundFixturePrelude = `package compute

func (s *OutboundSlot) fetchArms() (*OutboundArmsBundle, error) {
	bundle := s.arms.Load()
	if bundle == nil {
		return nil, ErrOutboundDisconnected
	}
	return bundle, nil
}

func (s *OutboundSlot) fetchCurrentArms() (*OutboundArmsBundle, error) {
	if !s.attempt.IsCurrent() {
		return nil, ErrOutboundNotCurrent
	}
	return s.fetchArms()
}
`

// TestOutboundFacadeWallTripsOnTheRetryPatch is the trip proof: each fixture is
// a break form the audit named, and the wall must report it.
func TestOutboundFacadeWallTripsOnTheRetryPatch(t *testing.T) {
	cases := []struct {
		name  string
		src   string
		check func(map[string]*ast.File, *token.FileSet) []string
	}{
		{
			name: "renamed helper plus counted retry loop (the allowlist escape)",
			src: outboundFixturePrelude + `
func (p outboundPen) Write(ctx context.Context, env *message.Envelope) (harness.WriteResult, error) {
	var result harness.WriteResult
	var err error
	for attempts := 0; attempts < 3; attempts++ {
		var bundle *OutboundArmsBundle
		bundle, err = p.slot.fetchCurrentArms()
		if err != nil {
			continue
		}
		result, err = bundle.Pen.Write(ctx, env)
		if err == nil {
			break
		}
	}
	return result, err
}`,
			check: outboundFacadeViolations,
		},
		{
			name: "renamed helper plus counted retry loop seen by the loop wall too",
			src: outboundFixturePrelude + `
func (p outboundPen) Write(ctx context.Context, env *message.Envelope) (harness.WriteResult, error) {
	var result harness.WriteResult
	var err error
	for attempts := 0; attempts < 3; attempts++ {
		bundle, lerr := p.slot.fetchCurrentArms()
		if lerr != nil {
			continue
		}
		result, err = bundle.Pen.Write(ctx, env)
		if err == nil {
			break
		}
	}
	return result, err
}`,
			check: outboundRetryLoopViolations,
		},
		{
			name: "direct slot.arms.Load() retry loop, no helper at all",
			src: `package compute
func (p outboundPen) Write(ctx context.Context, env *message.Envelope) (harness.WriteResult, error) {
	var result harness.WriteResult
	var err error
	for attempts := 0; attempts < 3; attempts++ {
		bundle := p.slot.arms.Load()
		if bundle == nil || bundle.Stream == nil {
			continue
		}
		result, err = bundle.Pen.Write(ctx, env)
		if err == nil {
			break
		}
	}
	return result, err
}`,
			check: outboundFacadeViolations,
		},
		{
			name: "direct slot.arms.Load() retry loop seen by the loop wall too",
			src: `package compute
func (p outboundPen) Write(ctx context.Context, env *message.Envelope) (harness.WriteResult, error) {
	var result harness.WriteResult
	var err error
	for attempts := 0; attempts < 3; attempts++ {
		bundle := p.slot.arms.Load()
		if bundle == nil {
			continue
		}
		result, err = bundle.Pen.Write(ctx, env)
		if err == nil {
			break
		}
	}
	return result, err
}`,
			check: outboundRetryLoopViolations,
		},
		{
			name: "loop-free error-triggered reload",
			src: outboundFixturePrelude + `
func (p outboundPen) Write(ctx context.Context, env *message.Envelope) (harness.WriteResult, error) {
	bundle, err := p.slot.fetchCurrentArms()
	if err != nil {
		bundle, err = p.slot.fetchCurrentArms()
		if err != nil {
			return harness.WriteResult{}, err
		}
	}
	return bundle.Pen.Write(ctx, env)
}`,
			check: outboundFacadeViolations,
		},
		{
			name: "second arm call on one load",
			src: outboundFixturePrelude + `
func (a outboundResourceAccess) Stat(ctx context.Context, id resource.ResourceID) (accessdoor.StatResult, error) {
	arms, err := a.slot.fetchCurrentArms()
	if err != nil {
		return accessdoor.StatResult{}, err
	}
	if _, serr := arms.Access.Stat(ctx, id); serr != nil {
		return arms.Access.Stat(ctx, id)
	}
	return accessdoor.StatResult{}, nil
}`,
			check: outboundFacadeViolations,
		},
		{
			name: "partially armed bundle publication",
			src: `package compute
func publish(session *link.AuthenticatedLinkSession, stream *link.ActorStream, raw link.Arms) {
	next := &OutboundArmsBundle{
		Session: session,
		Stream:  stream,
		Pen:     raw.Pen,
	}
	_ = next
}`,
			check: outboundPartialBundleViolations,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			files, fset := parseArchWallFixtureSource(t, "outbound_fixture.go", tc.src)
			if got := tc.check(files, fset); len(got) == 0 {
				t.Fatalf("wall did not trip on the break form %q", tc.name)
			}
		})
	}
}

// TestOutboundFacadeWallTripsOnPatchedProductionSource applies the audit's
// breaks to the real outbound.go and requires the wall to catch them there. The
// first two are the escapes the name-keyed wall permitted.
func TestOutboundFacadeWallTripsOnPatchedProductionSource(t *testing.T) {
	const path = "../platform/compute/outbound.go"
	const original = `	bundle, err := p.slot.loadAttempt()
	if err != nil {
		result := harness.WriteResult{}
		if env != nil {
			result.MessageID = env.ID
		}
		return result, err
	}
	return bundle.Pen.Write(ctx, env)`

	// Escape (b): rename the helper the old allowlist named, then retry. The
	// rename is applied to the declaration AND to this one call site; every other
	// caller keeps the old name, which is exactly what a half-finished rename
	// looks like.
	renameDecl := archWallPatch{
		old: "func (s *OutboundSlot) loadAttempt() (*OutboundArmsBundle, error) {",
		new: "func (s *OutboundSlot) loadAttemptArms() (*OutboundArmsBundle, error) {",
	}
	renamedRetry := archWallPatch{
		old: original,
		new: `	var bundle *OutboundArmsBundle
	var err error
	for attempts := 0; attempts < 3; attempts++ {
		bundle, err = p.slot.loadAttemptArms()
		if err == nil {
			break
		}
	}
	if err != nil {
		result := harness.WriteResult{}
		if env != nil {
			result.MessageID = env.ID
		}
		return result, err
	}
	return bundle.Pen.Write(ctx, env)`,
	}
	files, fset := patchArchWallSource(t, path, renameDecl, renamedRetry)
	if got := outboundFacadeViolations(files, fset); len(got) == 0 {
		t.Fatal("one-load/one-call wall stayed green on a RENAMED loader wrapped in a retry loop")
	}
	if got := outboundRetryLoopViolations(files, fset); len(got) == 0 {
		t.Fatal("retry-loop wall stayed green on a RENAMED loader wrapped in a retry loop")
	}
	// And the footing must notice the anchor left the facade set.
	if buildOutboundArmsModel(files).facadeQualified[outboundAnchorFacade] {
		t.Log("note: outboundPen.Write still reaches a loader after the rename, so the anchor holds by itself")
	}

	// Escape (a): skip the helpers entirely and load the bundle directly, the
	// form the daemon converge path already uses.
	directRetry := archWallPatch{
		old: original,
		new: `	var bundle *OutboundArmsBundle
	var err error
	for attempts := 0; attempts < 3; attempts++ {
		bundle = p.slot.arms.Load()
		if bundle != nil {
			break
		}
		err = ErrOutboundDisconnected
	}
	if err != nil {
		result := harness.WriteResult{}
		if env != nil {
			result.MessageID = env.ID
		}
		return result, err
	}
	return bundle.Pen.Write(ctx, env)`,
	}
	files, fset = patchArchWallSource(t, path, directRetry)
	if got := outboundFacadeViolations(files, fset); len(got) == 0 {
		t.Fatal("one-load/one-call wall stayed green on a direct slot.arms.Load() retry loop")
	}
	if got := outboundRetryLoopViolations(files, fset); len(got) == 0 {
		t.Fatal("retry-loop wall stayed green on a direct slot.arms.Load() retry loop")
	}

	// The other half: no loop at all, just a reload on error.
	const reloadPatched = `	bundle, err := p.slot.loadAttempt()
	if err != nil {
		bundle, err = p.slot.loadAttempt()
	}
	if err != nil {
		result := harness.WriteResult{}
		if env != nil {
			result.MessageID = env.ID
		}
		return result, err
	}
	return bundle.Pen.Write(ctx, env)`
	files, fset = patchArchWallSource(t, path, archWallPatch{old: original, new: reloadPatched})
	if got := outboundFacadeViolations(files, fset); len(got) == 0 {
		t.Fatal("one-load/one-call wall stayed green on a loop-free error-triggered reload")
	}
}
