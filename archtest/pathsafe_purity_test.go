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

// pathsafePkg is lib/pathsafe's import path — a plain stdlib-shaped string
// util (character-replace), NOT collision-free (":" -> "-" and "-" collide;
// "/" and "\\" both -> "_" and collide). 期11 S6 account item ⑦'s explicit
// two-way decision: adopt it (with an added collision check) as channelID's
// path-segment home, OR judge it dead for that role. Resolved toward the
// latter — channelID's real, already-shipped path-segment safety is
// cmd/daemon/internal/storagehost's assertPathSegment (an ALLOW-LIST charset
// assert, collision-free BY CONSTRUCTION: an illegal id is rejected outright,
// never lossily rewritten into a colliding neighbor). Adopting pathsafe here
// would be a strict regression for zero benefit. This test pins that
// resolution: pathsafe must never be imported by the resource axis's
// storage-name-generating packages.
const pathsafePkg = platformModulePrefix + "lib/pathsafe"

// storageNamingDirs are the packages that turn opaque ids (channelID, coord)
// into filesystem path segments for the resource axis (期11 §1.6/§4.2) — the
// ONLY place a "storage name generator" could legitimately live.
var storageNamingDirs = []string{"../cmd/daemon/internal/storagehost", "../runtime/resourcespec"}

// TestPathsafeNeverAStorageNameGenerator (期11 S6 account item ⑦): lib/pathsafe
// is a generic id-to-path-segment convenience util, but it must NEVER be the
// mechanism behind a durable storage name (channelID/coord path segments) —
// its lossy character-replace shape can collide two distinct ids into the
// same on-disk name, silently mixing data across channels/resources. The
// resource axis's actual path-segment safety is assertPathSegment's
// allow-list charset assert (collision-free by construction, no rewrite).
func TestPathsafeNeverAStorageNameGenerator(t *testing.T) {
	fset := token.NewFileSet()
	var violations []string

	for _, dir := range storageNamingDirs {
		err := filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
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
			for _, imp := range importsOf(t, fset, path) {
				if imp == pathsafePkg {
					violations = append(violations, fmt.Sprintf(
						"%s imports %q — pathsafe's lossy character-replace is not collision-free; storage-name path segments must go through assertPathSegment's allow-list charset assert instead", filepath.ToSlash(path), imp))
				}
			}
			return nil
		})
		if err != nil {
			t.Fatalf("walk %s: %v", dir, err)
		}
	}
	if len(violations) > 0 {
		t.Fatalf("pathsafe used as a storage-name generator (期11 S6 account item ⑦, judged dead for this role):\n  %s", strings.Join(violations, "\n  "))
	}
}

// storagehostDir is the daemon's disk-touching storage host — the package that
// turns opaque ids into on-disk path segments.
const storagehostDir = "../cmd/daemon/internal/storagehost"

// TestPathSegmentAssertCentralizedInRootGo is the POSITIVE half of "pathsafe
// 集中 root.go" (S-3 nail 4): the negative test above forbids the WRONG tool
// (lib/pathsafe); this pins that the RIGHT one — assertPathSegment, the
// allow-list charset assert every path-segment constructor must funnel through
// — is defined exactly ONCE and ONLY in root.go. A second, scattered copy
// elsewhere in the package (a future constructor that hand-rolls its own,
// possibly weaker, segment check instead of calling the shared assert) is the
// violation this catches: path-segment safety must not fragment into
// divergent per-call implementations.
func TestPathSegmentAssertCentralizedInRootGo(t *testing.T) {
	fset := token.NewFileSet()
	var definedIn []string

	err := filepath.WalkDir(storagehostDir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		file, perr := parser.ParseFile(fset, path, nil, 0)
		if perr != nil {
			return fmt.Errorf("parse %s: %w", path, perr)
		}
		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if ok && fn.Recv == nil && fn.Name.Name == "assertPathSegment" {
				definedIn = append(definedIn, filepath.ToSlash(path))
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk %s: %v", storagehostDir, err)
	}

	if len(definedIn) != 1 {
		t.Fatalf("assertPathSegment defined %d times (%v) — path-segment safety must live in exactly one place (root.go), not fragment across the package", len(definedIn), definedIn)
	}
	if !strings.HasSuffix(definedIn[0], "/root.go") {
		t.Fatalf("assertPathSegment defined in %q, want cmd/daemon/internal/storagehost/root.go — the S-3 'pathsafe 集中 root.go' centralization nail", definedIn[0])
	}
}
