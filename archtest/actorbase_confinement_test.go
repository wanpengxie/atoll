package archtest

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

// actorbase-spec-v1.md §4 S3 / §5 red line ③: the eight out-generation call
// construction faces (home, compute, declarations, and plugin registries)
// registry.Constructor / app-human / cmd-daemon / test fixtures) were reshaped
// so production downstream never again needs to name actorcaps.Caps — it
// speaks platform.ActorFactory with a single actorbase.Def model. These four locks are
// the S3 "断净防绕清单" DoD: import confinement / plugin-dir reinforcement /
// zero old-shape residual / direct-Actor-implementer allowlist.

const actorcapsPkg = platformModulePrefix + "lib/actorcaps"

// actorcapsAllowedPrefix is the repo-wide allowlist for naming the whole Caps
// bundle. Importing actorcaps vocabulary such as ForkSpec or LifecycleHandle
// is intentionally broader; only Caps itself is the private assembly seam.
//
// runtime/actorctl joined the allowlist when the Server managed Caps final
// construction moved into it (harden03B value-ledger gate收口): actorctl is now
// the SOLE constructor of the Server managed body's five-arm Caps — it welds the
// value-ledger gate onto each arm and hands the finished bundle to a narrow
// business builder. Naming actorcaps.Caps there is that construction locus, not
// a downstream leak; the platform assembly root now only injects minters.
var actorcapsAllowedPrefix = []string{"../platform/", "../lib/actorbase/", "../lib/actorcaps/", "../archtest/", "../runtime/managedcaps/", "../runtime/systemcaps/"}

// TestActorcapsConfinedToPlatformAndActorbase checks the actual Caps selector,
// not the package import. Only the platform assembly root, lib/actorbase, and
// the runtime/actorctl managed-Caps constructor may name the whole Caps bundle.
func TestActorcapsConfinedToPlatformAndActorbase(t *testing.T) {
	var v []string
	for _, root := range []string{"../app", "../cmd", "../drivers", "../lib", "../platform", "../registry", "../runtime"} {
		fset := token.NewFileSet()
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
			if !strings.HasSuffix(path, ".go") {
				return nil
			}
			slash := filepath.ToSlash(path)
			for _, prefix := range actorcapsAllowedPrefix {
				if strings.HasPrefix(slash, prefix) {
					return nil
				}
			}
			file, err := parser.ParseFile(fset, path, nil, 0)
			if err != nil {
				return err
			}
			ast.Inspect(file, func(node ast.Node) bool {
				selector, ok := node.(*ast.SelectorExpr)
				if !ok || selector.Sel.Name != "Caps" {
					return true
				}
				pkg, ok := selector.X.(*ast.Ident)
				if ok && pkg.Name == "actorcaps" {
					v = append(v, fmt.Sprintf("%s names the private actorcaps.Caps assembly bundle", fset.Position(selector.Pos())))
				}
				return true
			})
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	failViolations(t, "actorcaps.Caps confinement", v)
}

// pluginDirs are the domain-implementable actor packages (spec §2: "域层没人
// 碰 caps;降档=换身份换目录(逃生门纪律,archtest 砌死插件目录对执法包的
// import)") — the plugin boundary itself, scanned as its own dedicated
// invariant distinct from ①'s repo-wide sweep (belt and suspenders: a plugin
// author reaching for actorcaps is a domain-layer offence even if some other
// future non-plugin downstream were ever exempted from ①).
var pluginDirs = []string{"../drivers/tools/", "../drivers/agents/provider/"}

// TestPluginDirsForbidCapsAndDoorImports — ② the plugin-directory-scoped
// reinforcement: no actor/engine implementation package may import
// actorcaps.Caps (the whole caps bundle) or reach directly for the raw
// access/schedule minting surfaces (runtime/accessdoor, runtime/schedule) —
// today's plugin authors reach only for actorbase.Sys, so a plugin needing raw
// caps is exactly the "降档" signal
// spec §2 names: it has stopped being a thin adapter.
func TestPluginDirsForbidCapsAndDoorImports(t *testing.T) {
	forbidden := map[string]string{
		actorcapsPkg: "the whole caps bundle",
		platformModulePrefix + "runtime/accessdoor": "the raw access-plane minting surface",
		platformModulePrefix + "runtime/schedule":   "the raw schedule-engine minting surface",
	}
	var v []string
	walkImportsAll(t, func(slash, imp string) {
		inPlugin := false
		for _, p := range pluginDirs {
			if strings.HasPrefix(slash, p) {
				inPlugin = true
				break
			}
		}
		if !inPlugin {
			return
		}
		// A plugin _test.go implementing an actorbase.Sys double must name
		// schedule.TimerID (Sys.After's return type) once the adapter migrated
		// to Proc (期10 S3) — a test double is not a production adapter reaching
		// for a minting surface, the same "_test.go doubles are exempt"
		// principle rule ④ already uses. The whole caps bundle stays banned even
		// in production here (and repo-wide via rule ①); only the raw
		// accessdoor/schedule minting surfaces are relaxed for test doubles.
		if strings.HasSuffix(slash, "_test.go") && imp != actorcapsPkg {
			return
		}
		if why, banned := forbidden[imp]; banned {
			v = append(v, fmt.Sprintf(
				"%s imports %q (%s) — plugin dirs are adapters over harness.Pen only (逃生门纪律: reaching for more means changing身份/目录, not importing the enforcement package from here)",
				slash, imp, why))
		}
	})
	failViolations(t, "plugin dirs ⊥ actorcaps/accessdoor/schedule minting surfaces", v)
}

// oldFactoryShape is the retired signature text (spec: "func(actorcaps.Caps)
// actorrt.Actor 零残留"). A source-text scan (not go/types) matches this
// package's existing tripwire stance: the realistic accident (a stray old-
// shape factory literal reintroduced by a careless merge/rebase) is exactly
// what this catches; a deliberately obfuscated evasion is a review-layer
// offence.
const oldFactoryShape = "func(actorcaps.Caps) actorrt.Actor"

// TestNoRetiredFactoryShapeResidual — ③ of the four (numbered ② in the DoD
// list; kept in this file for the shared walk helper): zero residual of the
// retired bare factory signature across the explicit scan surface the
// spec names, including platform: no test or production assembly receives an
// exemption to recreate the retired representation.
func TestNoRetiredFactoryShapeResidual(t *testing.T) {
	scanRoots := []string{"../registry", "../drivers", "../cmd", "../app", "../platform"}
	var v []string
	for _, root := range scanRoots {
		err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if d.IsDir() {
				if skipDirs[d.Name()] {
					return filepath.SkipDir
				}
				return nil
			}
			if !strings.HasSuffix(path, ".go") {
				return nil
			}
			b, rerr := readFile(path)
			if rerr != nil {
				return rerr
			}
			if strings.Contains(b, oldFactoryShape) {
				v = append(v, fmt.Sprintf("%s: contains the retired shape %q", filepath.ToSlash(path), oldFactoryShape))
			}
			return nil
		})
		if err != nil {
			t.Fatalf("walk %s: %v", root, err)
		}
	}
	failViolations(t, "zero residual of the retired func(actorcaps.Caps) actorrt.Actor shape", v)
}

// directActorImplementAllowlist is ④'s exact allowlist (spec: "直接实现
// actorrt.Actor 者仅引擎+测试 stub"): lib/actorbase's engine is the ONE
// production implementer — the migration queue is now EMPTY. _test.go stubs are
// exempt everywhere (a test double is not a production implementer). echo, kimi
// (drivers/tools/kimi), sysactor, the plugin adapters xhs + drivers/tools/device (期10 S3),
// and now the two agent-provider engines claude + go-kimi (期10 S5) have all
// migrated to actorbase.Proc — the compatibility queue is zeroed and the cell's
// per-request machine铲除 (spec §3 / 红线6).
var directActorImplementAllowlist = []string{
	"../lib/actorbase/engine.go", // the sanctioned engine (spec §3) — the ONLY entry
	// ../app/human.go is GONE: the old humanFront (a direct actorrt.Actor
	// implementer holding a Pen) was整删 with the subjectgate door (S4) — the
	// human is now embodied by the platform-internal Proc cell (humancell.go), and
	// app/human.go carries only routing policy (no Receive, no Pen).
}

// receiveSig is the actorrt.Actor.Receive method signature text this AST scan
// matches on (method name + parameter shape) — a source-text-adjacent tripwire,
// same stance as the rest of archtest: it does not resolve types, it flags any
// NEW `func (...) Receive(ctx context.Context, env *message.Envelope) error`
// method declared outside the allowlist.
func TestDirectActorImplementersAllowlisted(t *testing.T) {
	allowed := map[string]bool{}
	for _, p := range directActorImplementAllowlist {
		allowed[p] = true
	}
	fset := token.NewFileSet()
	var v []string
	roots := []string{"../registry", "../drivers", "../cmd", "../app", "../platform", "../lib/actorbase"}
	for _, root := range roots {
		err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if d.IsDir() {
				if skipDirs[d.Name()] {
					return filepath.SkipDir
				}
				return nil
			}
			if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
				return nil
			}
			slash := filepath.ToSlash(path)
			file, perr := parser.ParseFile(fset, path, nil, 0)
			if perr != nil {
				return fmt.Errorf("parse %s: %w", path, perr)
			}
			for _, decl := range file.Decls {
				fn, ok := decl.(*ast.FuncDecl)
				if !ok || fn.Recv == nil || fn.Name.Name != "Receive" {
					continue
				}
				if !isActorReceiveSig(fn) {
					continue
				}
				if allowed[slash] {
					continue
				}
				v = append(v, fmt.Sprintf(
					"%s declares a Receive(ctx, *message.Envelope) error method — a NEW direct actorrt.Actor implementer outside the engine (lib/actorbase) + the named S5/S5b migration queue; build it over platform.ActorFactory{Proc: actorbase.Def{...}} instead",
					slash))
			}
			return nil
		})
		if err != nil {
			t.Fatalf("walk %s: %v", root, err)
		}
	}
	failViolations(t, "direct actorrt.Actor implementers (engine + named migration queue only)", v)
}

// isActorReceiveSig reports whether fn's signature is
// Receive(context.Context, *message.Envelope) error — a light structural
// match (param count/shape + result count), not a go/types resolution; good
// enough to catch the realistic case (a genuine actorrt.Actor implementation)
// without false-positives on an unrelated same-named method (this repo has no
// such collision today — grounding grep).
func isActorReceiveSig(fn *ast.FuncDecl) bool {
	if fn.Type.Params == nil || len(fn.Type.Params.List) != 2 {
		return false
	}
	if fn.Type.Results == nil || len(fn.Type.Results.List) != 1 {
		return false
	}
	// second param must be a pointer type (the *message.Envelope shape).
	_, ptr := fn.Type.Params.List[1].Type.(*ast.StarExpr)
	return ptr
}

// walkImportsAll is walkImports (agent_layering_test.go) but INCLUDING
// _test.go files — ①'s ban is explicitly "含 _test.go".
func walkImportsAll(t *testing.T, fn func(slash, importPath string)) {
	t.Helper()
	fset := token.NewFileSet()
	err := filepath.WalkDir("..", func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			if skipDirs[d.Name()] {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") {
			return nil
		}
		f, perr := parser.ParseFile(fset, path, nil, parser.ImportsOnly)
		if perr != nil {
			return fmt.Errorf("parse %s: %w", path, perr)
		}
		slash := filepath.ToSlash(path)
		for _, imp := range f.Imports {
			p, uerr := strconv.Unquote(imp.Path.Value)
			if uerr != nil {
				continue
			}
			fn(slash, p)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk: %v", err)
	}
}

// readFile is a tiny os.ReadFile wrapper (string, not []byte).
func readFile(path string) (string, error) {
	b, err := os.ReadFile(path)
	return string(b), err
}
