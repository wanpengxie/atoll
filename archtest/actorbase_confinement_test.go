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
// faces (CapsFactoryBuilder / ComputeBuilder / Home.Spawn / ActorDecl /
// registry.Constructor / app-human / cmd-daemon / test fixtures) were reshaped
// so downstream never again needs to name actorcaps.Caps — it speaks
// platform.ActorFactory (harness.Pen for the pre-actorbase "Legacy" shape,
// actorbase.Sys/Proc for the migrated shape) instead. These four locks are
// the S3 "断净防绕清单" DoD: import confinement / plugin-dir reinforcement /
// zero old-shape residual / direct-Actor-implementer allowlist.

const actorcapsPkg = platformModulePrefix + "lib/actorcaps"

// actorcapsAllowedPrefix is ①'s repo-wide allowlist (spec: "允许范围仅
// platform+lib/actorbase+archtest"): the assembly root that welds the caps
// bundle (platform), the ONE package that consumes it to mint a live Sys
// (lib/actorbase — engine.go's New), and this archtest package's own doubles.
var actorcapsAllowedPrefix = []string{"../platform/", "../lib/actorbase/", "../lib/actorcaps/", "../archtest/"}

// TestActorcapsConfinedToPlatformAndActorbase — ① import confinement,
// repo-wide, _test.go included (spec: "含 _test.go 扫描"). A downstream
// package never names actorcaps.Caps: the platform-level ActorFactory (a
// harness.Pen-taking Legacy factory or an actorbase.Def) is the only shape a
// consumer constructs.
func TestActorcapsConfinedToPlatformAndActorbase(t *testing.T) {
	var v []string
	walkImportsAll(t, func(slash, imp string) {
		if imp != actorcapsPkg {
			return
		}
		for _, p := range actorcapsAllowedPrefix {
			if strings.HasPrefix(slash, p) {
				return
			}
		}
		v = append(v, fmt.Sprintf(
			"%s imports %q — actorcaps.Caps is confined to platform (the assembly seam) + lib/actorbase (the caps→Sys weld) + archtest; a consumer speaks platform.ActorFactory (harness.Pen / actorbase.Sys) instead",
			slash, imp))
	})
	failViolations(t, "actorcaps.Caps import confinement (platform + lib/actorbase + archtest only)", v)
}

// pluginDirs are the domain-implementable actor packages (spec §2: "域层没人
// 碰 caps;降档=换身份换目录(逃生门纪律,archtest 砌死插件目录对执法包的
// import)") — the plugin boundary itself, scanned as its own dedicated
// invariant distinct from ①'s repo-wide sweep (belt and suspenders: a plugin
// author reaching for actorcaps is a domain-layer offence even if some other
// future non-plugin downstream were ever exempted from ①).
var pluginDirs = []string{"../actors/", "../agent/provider/"}

// TestPluginDirsForbidCapsAndDoorImports — ② the plugin-directory-scoped
// reinforcement: no actor/engine implementation package may import
// actorcaps.Caps (the whole caps bundle) or reach directly for the raw
// access/schedule minting surfaces (runtime/accessdoor, runtime/schedule) —
// today's plugin authors reach for harness.Pen alone (grep-confirmed, S3
// grounding), so a plugin needing more than Pen is exactly the "降档" signal
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

// TestNoOldCapsFactoryShapeResidual — ③ of the four (numbered ② in the DoD
// list; kept in this file for the shared walk helper): zero residual of the
// retired bare factory shape ActorDecl.Factory / CapsFactoryBuilder.Lookup /
// ComputeBuilder.Lookup used to carry, across the explicit scan surface the
// spec names (registry/actors/agent providers/cmd) plus app (the human front-
// end entry point) — platform itself is exempt (ActorFactory.fullCaps, the
// platform-only test seam CapsFactory() builds, is a deliberately DIFFERENT,
// narrower-scoped shape that never leaves this package).
func TestNoOldCapsFactoryShapeResidual(t *testing.T) {
	scanRoots := []string{"../registry", "../actors", "../agent", "../cmd", "../app"}
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
// production implementer this period; the rest are the S5/S5b migration
// queue named explicitly in the spec (echo → actors/kimi(+xhs/device, same
// adapter shape) → sysactor → agent providers) — each entry SHRINKS this set
// as it migrates to actorbase.Proc, it never grows. _test.go stubs are exempt
// everywhere (a test double is not a production implementer). echo has
// migrated (spec §4 S5a) and is gone from this list.
var directActorImplementAllowlist = []string{
	"../lib/actorbase/engine.go",                // the sanctioned engine (spec §3)
	"../actors/xhs/actor.go",                    // S5/S5b migration queue
	"../actors/device/actor.go",                 // S5/S5b migration queue
	"../actors/kimi/actor.go",                   // S5/S5b migration queue
	"../agent/provider/kimi/bridge.go",          // S5b migration queue
	"../agent/provider/claudecode/bridge.go",    // S5b migration queue
	"../platform/internal/sysactor/sysactor.go", // S5b migration queue (ring0 special)
	"../app/human.go",                           // S3's app-human call-site adaptation only (Legacy shape); Sys verb table cannot express its arbitrary-audience/TTL/parent_id envelope needs without a behaviour rewrite — see actorbase-spec-v1.md S3 slice report
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
	roots := []string{"../registry", "../actors", "../agent", "../cmd", "../app", "../platform", "../lib/actorbase"}
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
