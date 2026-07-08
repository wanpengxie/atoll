package archtest

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

// storageHostPkg is cmd/daemon's own disk-touching storage host — the ONE
// package §4.1's Allocator/Streamer/Reclaimer/Scrubber (file-kind bytes at
// rest) live in. Today it sits under cmd/daemon/internal/, so Go's own
// internal/ visibility rule already makes it physically unimportable from
// cmd/server — but that closure is an ACCIDENT of package placement, not a
// decision anyone wrote down. §8.2's red line ("server 零存储...archtest 钉
// 死") wants the assertion to survive the accident: if storagehost (or its
// disk-touching guts) is ever moved out of internal/ — a refactor with zero
// reason to consult this test — cmd/server's assembly closure must still be
// mechanically forbidden from reaching it.
const storageHostPkg = platformModulePrefix + "cmd/daemon/internal/storagehost"

// storageHostNameFragment is the substring guard: even a FUTURE rename that
// moves storagehost out from under internal/ (e.g.
// "cmd/daemon/storagehost", or hoisted to "lib/storagehost") keeps carrying
// its own name — disk-touching code doesn't usually get renamed away from a
// name that says what it is. A pure path-equality check would go silently
// blind the moment the directory moves; this substring check does not.
const storageHostNameFragment = "storagehost"

// serverClosureRoots are the two assembly entry points §8.2 names: the
// SERVER BINARY's own composition root (cmd/server, whose import closure is
// literally everything that ends up in the atoll-server binary) and the
// PLATFORM package (the channel-home assembly layer platform.Open lives in
// — already part of cmd/server's closure via app, walked again here by name
// so a future entry point that reaches platform WITHOUT routing through
// cmd/server — e.g. a second server-shaped binary — is covered too).
var serverClosureRoots = []string{"cmd/server", "platform"}

// TestServerAssemblyNeverImportsStorageHost pins 期11 spec §8.2 ("server 零
// 存储...archtest 钉死") + §9 DoD#6 mechanically: walk cmd/server's and
// platform's FULL transitive same-module import closure and fail if any
// package on it is (or is renamed to look like) the daemon's disk-touching
// storage host. This upgrades "cmd/server physically cannot import
// storagehost" from a side effect of Go's internal/ rule into an explicit,
// self-checking assertion that stays red-on-violation even after a
// refactor that lifts the visibility barrier.
func TestServerAssemblyNeverImportsStorageHost(t *testing.T) {
	fset := token.NewFileSet()

	for _, root := range serverClosureRoots {
		rootPkg := platformModulePrefix + root
		visited := map[string]bool{}
		var walk func(pkgPath, via string)
		walk = func(pkgPath, via string) {
			if visited[pkgPath] {
				return
			}
			visited[pkgPath] = true

			if pkgPath == storageHostPkg || strings.Contains(strings.ToLower(pkgPath), storageHostNameFragment) {
				t.Fatalf(
					"server assembly closure (root %q) reaches %q via %q — server=BIOS, no disk-touching storage host code may ever be reachable from the server binary or the platform channel-home assembly layer (期11 spec §8.2 red line)",
					root, pkgPath, via)
			}

			if !strings.HasPrefix(pkgPath, platformModulePrefix) {
				return // external dependency: not part of this module's own package graph
			}
			dir := "../" + strings.TrimPrefix(pkgPath, platformModulePrefix)
			entries, err := os.ReadDir(dir)
			if err != nil {
				// A package path with no matching directory (e.g. a module-relative
				// alias) is not something this walk can resolve further; that is not
				// itself a violation.
				return
			}
			var files []string
			for _, e := range entries {
				if e.IsDir() {
					continue
				}
				name := e.Name()
				if !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
					continue
				}
				files = append(files, name)
			}
			sort.Strings(files) // deterministic walk order for reproducible failure messages
			for _, name := range files {
				for _, imp := range importsOf(t, fset, filepath.Join(dir, name)) {
					walk(imp, pkgPath+" ("+name+")")
				}
			}
		}
		walk(rootPkg, "<root>")
	}
}

// TestNoDiskWriteStorageHostOutsideDaemonRuntime is server-zero-storage's
// second, independent leg: §8.2 also names the MECHANISM ("file 落盘代码
// （Allocator/Streamer/os.Root 使用面）只存在于 daemon 运行时包"), not merely
// the storagehost package name. This scans the WHOLE repo (mirroring this
// package's established confinement idiom, e.g.
// TestRuntimeAssemblyConfinedToPlatform) for actual `os.Root` TYPE
// references — the chroot-shaped disk handle §4.6 names as the resource
// axis's ONE local-file-write primitive — and fails if anything outside
// cmd/daemon's own tree touches it. An AST selector-expression walk (not a
// text scan): this codebase's own doc comments legitimately quote
// "os.Root" in prose all over (§3.4/§5 cross-references) — a naive
// substring scan would misfire on every one of them, so this checks actual
// `os.Root` SelectorExprs in code (comments are never part of the AST's
// expression tree), which only ever appear where the type is genuinely
// referenced. Unlike the closure walk above (which asks "can server REACH a
// storage host"), this asks "does a storage host exist anywhere it
// shouldn't" — together they cover both directions of §8.2's red line.

// daemonRuntimeDir is the one directory allowed to construct/hold os.Root
// handles — cmd/daemon's own tree (storagehost lives at
// cmd/daemon/internal/storagehost today; this allowlist is path-based, not
// package-name-based, so it survives storagehost moving anywhere still
// under cmd/daemon, while still catching a hoist out of cmd/daemon entirely).
const daemonRuntimeDir = "../cmd/daemon/"

func TestNoDiskWriteStorageHostOutsideDaemonRuntime(t *testing.T) {
	fset := token.NewFileSet()
	var violations []string

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
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		rel := filepath.ToSlash(path)
		if strings.HasPrefix(rel, daemonRuntimeDir) {
			return nil // the one allowed home
		}
		file, perr := parser.ParseFile(fset, path, nil, 0)
		if perr != nil {
			return fmt.Errorf("parse %s: %w", path, perr)
		}
		ast.Inspect(file, func(n ast.Node) bool {
			sel, ok := n.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			x, ok := sel.X.(*ast.Ident)
			if !ok || x.Name != "os" || sel.Sel.Name != "Root" {
				return true
			}
			violations = append(violations, fmt.Sprintf("%s: references os.Root outside cmd/daemon's runtime tree", fset.Position(sel.Pos())))
			return true
		})
		return nil
	})
	if err != nil {
		t.Fatalf("walk: %v", err)
	}
	if len(violations) > 0 {
		t.Fatalf("disk-touching os.Root usage found outside cmd/daemon (期11 spec §8.2 — file falloff code lives ONLY in the daemon runtime):\n  %s", strings.Join(violations, "\n  "))
	}
}
