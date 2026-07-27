package archtest

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"strings"
	"testing"
)

// The assembly surface is runtime.ChannelStores: the bundle runtime.OpenChannel
// hands back. Six of its faces are raw write capability that exists to be given
// away exactly once —
//
//	Log              → harness (the pen's only ingredient; a direct Append
//	                   skips the welded Sender, the envelope shape check, the
//	                   reserved-word rules, response pairing and the terminal
//	                   TOCTOU latch)
//	Actors           → actorstore/Controller (a direct Insert/Deregister skips
//	                   the ledger lock and the verdict)
//	Assembly.State   → accessdoor  (skips the state door's admission)
//	Assembly.Timers  → schedule    (skips the fire gate)
//	Assembly.Resources/KV → accessdoor
//
// Once each organ has taken its own, nothing above them needs the bundle again.
// The two tests below hold that shape structurally rather than by comment: the
// bundle may be born in exactly one place, and it may never be stored. Together
// they confine it to a single function body, so "assembly-period threading is
// not holding" (actorstore spec §0.5) is a fact the compiler enforces instead
// of a promise a future edit can quietly break.
//
// What this is NOT: a ban on Platform assembling runtime organs. Platform IS the
// channel composition root (harden03b v5.1) and OpenChannel's own doc confines
// assembly to the platform tree. The root is supposed to hold every ingredient
// for the length of assembly. It is not supposed to keep them afterwards.
//
// (Non-test source only, as everywhere in this package: a test that asserts a
// physical row legitimately opens its own handle.)

const channelStoresType = "ChannelStores"

// TestAssemblyBundleNeverOutlivesItsFunction — no non-test declaration in the
// platform tree may NAME runtime.ChannelStores. A struct field would let the
// bundle outlive assembly (every raw write face reachable from any method on
// that struct, forever); a parameter or result would let it travel further down
// the tree, handing a nominal claim on the raw log to a callee that needs one
// registry face. Both are the same defect: the ingredient list living past the
// moment of assembly. Open() names no type — `cs := runtime.OpenChannel(...)`
// infers it — so the legitimate use does not trip this, and every illegitimate
// one must write the name.
func TestAssemblyBundleNeverOutlivesItsFunction(t *testing.T) {
	fset := token.NewFileSet()
	var violations []string

	err := filepath.WalkDir("../platform", func(path string, d fs.DirEntry, err error) error {
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
		file, perr := parser.ParseFile(fset, path, nil, 0)
		if perr != nil {
			t.Fatalf("parse %s: %v", path, perr)
		}
		runtimeAliases := runtimeRootAliases(file)
		if len(runtimeAliases) == 0 {
			return nil
		}
		ast.Inspect(file, func(n ast.Node) bool {
			sel, ok := n.(*ast.SelectorExpr)
			if !ok || sel.Sel.Name != channelStoresType {
				return true
			}
			ident, ok := sel.X.(*ast.Ident)
			if !ok || !runtimeAliases[ident.Name] {
				return true
			}
			violations = append(violations, fmt.Sprintf(
				"%s:%d names runtime.ChannelStores",
				filepath.ToSlash(path), fset.Position(sel.Pos()).Line))
			return true
		})
		return nil
	})
	if err != nil {
		t.Fatalf("walk platform: %v", err)
	}
	if len(violations) > 0 {
		t.Fatalf("the assembly bundle must not be stored or passed on — take the "+
			"one face you need (storespec.ActorRegistryStore, storespec.GenesisStore, …) "+
			"so the raw write faces die with Open:\n  %s", strings.Join(violations, "\n  "))
	}
}

// TestAssemblyBundleHasOneBirthplace — runtime.OpenChannel may be called from
// exactly one non-test site. A second call site is a second place holding all
// six raw faces, and the first test above cannot see it (a local needs no type
// name). Growing a legitimate second channel-opening path means moving this
// constant, deliberately, with the same "hand it away, keep nothing" discipline
// Open follows.
func TestAssemblyBundleHasOneBirthplace(t *testing.T) {
	const birthplace = "../platform/home/open.go"

	fset := token.NewFileSet()
	var callSites []string

	err := filepath.WalkDir("..", func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			if skipDirs[d.Name()] || d.Name() == "archtest" {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		file, perr := parser.ParseFile(fset, path, nil, 0)
		if perr != nil {
			return nil // not our concern here; other walls parse the whole tree
		}
		runtimeAliases := runtimeRootAliases(file)
		if len(runtimeAliases) == 0 {
			return nil
		}
		ast.Inspect(file, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			sel, ok := call.Fun.(*ast.SelectorExpr)
			if !ok || sel.Sel.Name != "OpenChannel" {
				return true
			}
			ident, ok := sel.X.(*ast.Ident)
			if !ok || !runtimeAliases[ident.Name] {
				return true
			}
			callSites = append(callSites, fmt.Sprintf("%s:%d",
				filepath.ToSlash(path), fset.Position(call.Pos()).Line))
			return true
		})
		return nil
	})
	if err != nil {
		t.Fatalf("walk repo: %v", err)
	}
	for _, site := range callSites {
		if !strings.HasPrefix(site, birthplace+":") {
			t.Errorf("runtime.OpenChannel called at %s — the assembly bundle has one "+
				"birthplace (%s); a second holder of the raw log / actor store / leaf "+
				"ports is exactly what this wall exists to prevent", site, birthplace)
		}
	}
	if len(callSites) == 0 {
		t.Fatalf("no runtime.OpenChannel call site found — this wall has lost its subject")
	}
}

// runtimeRootAliases returns the local names under which a file imports the
// runtime ROOT package (normally "runtime", but an alias is legal Go).
func runtimeRootAliases(file *ast.File) map[string]bool {
	const runtimeRootPkg = platformModulePrefix + "runtime"

	aliases := map[string]bool{}
	for _, imp := range file.Imports {
		path := strings.Trim(imp.Path.Value, `"`)
		if path != runtimeRootPkg {
			continue
		}
		if imp.Name != nil {
			aliases[imp.Name.Name] = true
			continue
		}
		aliases["runtime"] = true
	}
	return aliases
}
