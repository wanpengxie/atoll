package archtest

import (
	"fmt"
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
