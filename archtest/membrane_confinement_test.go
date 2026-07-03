package archtest

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

// live-membrane CONSTRUCTOR confinement — the WHEN-validity twin of the
// minter/harness confinement walls.
//
// livePen / liveAccess / liveSchedule are the death-after-write membranes: each
// wraps a raw, ALREADY-welded capability (a minted harness.Pen /
// accessdoor.AccessHandle / schedule.ScheduleHandle) and gates every call on the
// welded incarnation still being live. The membrane TYPES are unexported (only
// NewLivePen / NewLiveAccess / NewLiveSchedule can produce one), so the struct
// literal is compiler-confined to package link. This wall locks the CONSTRUCTOR
// symbols one layer earlier: the membrane must be woven where the raw handle is
// minted (minting the handle and wrapping it in the live membrane happen in the
// same step) — the single caps assembler
// (platform/home.go buildCaps) and the port emitSink path
// (platform/internal/link)
// — and nowhere else. A downstream cell holds only the woven membrane it was born
// with; it never re-constructs one (that would let a raw handle escape unwrapped,
// or re-weld a membrane to the wrong incarnation).
//
// Allowlist granularity = a PRECISE per-file set (membraneWeaveAllowlist below),
// NOT the whole platform tree. A blanket platform-prefix skip makes the wall
// vacuous — any platform file could re-construct a membrane and re-weld it to the
// wrong incarnation (or let a raw handle escape unwrapped). The membrane types are
// unexported so only NewLive* can produce one, and link IS internal to platform;
// this symbol lock is the tripwire that (a) records the weave-at-assembly invariant
// at the type-reference layer and (b) re-arms should link ever be de-internalised
// (same "kept as a tripwire" stance as harness.Chain). The residual shadowing /
// dot-import evasion is a review-layer offence, not worth a go/types-grade pass
// pre-launch — same as the sibling confinement walls.
var membraneConstructors = map[string]bool{
	"NewLivePen":      true,
	"NewLiveAccess":   true,
	"NewLiveSchedule": true,
}

// membraneWeaveAllowlist is the EXACT set of files permitted to reference the
// live-membrane constructors via the `link.` qualifier. Cored out by ACTUAL
// reference (not by tree prefix):
//
//   - The link package itself DEFINES the constructors; a link file never imports
//     link, so a same-package reference carries no `link.` qualifier and the
//     import-resolution below already exempts it (local=="" → skip). It therefore
//     needs no entry here (accept.go's port-path weave is such a same-package use
//     — and, since S7, so is livearms.go's NewLiveArms, the daemon-side counterpart
//     of buildCaps: it weaves NewLivePen/NewLiveAccess/NewLiveSchedule over the
//     RebindableArms facades, gated on the DAEMON's own incarnation).
//   - platform/home.go is the SINGLE caps assembler (buildCaps) — the one
//     external legitimate weave site.
//
// Everything else — spawnhandle.go (delegates to the assembler, weaves nothing),
// compute.go (delegates to link.NewLiveArms, weaves nothing directly), and any
// future platform file — is OUT: adding a NewLive* reference anywhere else must
// turn this test red.
var membraneWeaveAllowlist = map[string]bool{
	"../platform/home.go": true,
}

// TestLiveMembraneConstructionConfinedToPlatform — SYMBOL-level lock: only the
// platform tree may reference the live-membrane constructors (livePen /
// liveAccess / liveSchedule are woven at the caps assembler + port path, never
// re-constructed downstream).
func TestLiveMembraneConstructionConfinedToPlatform(t *testing.T) {
	const linkPkg = platformModulePrefix + "platform/internal/link"

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
		slash := filepath.ToSlash(path)
		if membraneWeaveAllowlist[slash] {
			return nil // the SINGLE sanctioned assembler (buildCaps); link is exempted below by import-resolution
		}
		file, perr := parser.ParseFile(fset, path, nil, 0)
		if perr != nil {
			return fmt.Errorf("parse %s: %w", path, perr)
		}

		// Resolve this file's local qualifier for the link import.
		local := ""
		for _, imp := range file.Imports {
			p, uerr := strconv.Unquote(imp.Path.Value)
			if uerr != nil || p != linkPkg {
				continue
			}
			if imp.Name != nil {
				local = imp.Name.Name
			} else {
				local = "link"
			}
			break
		}
		if local == "" || local == "_" || local == "." {
			return nil // not imported under a qualifier we can match
		}

		ast.Inspect(file, func(n ast.Node) bool {
			sel, ok := n.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			x, ok := sel.X.(*ast.Ident)
			if !ok || x.Name != local {
				return true
			}
			if membraneConstructors[sel.Sel.Name] {
				pos := fset.Position(sel.Pos())
				violations = append(violations, fmt.Sprintf(
					"%s:%d references %s.%s — the live-membrane must be woven at the caps assembler / port path (发 handle 与 live 膜 wrap 同一步); a downstream never re-constructs one",
					slash, pos.Line, local, sel.Sel.Name))
			}
			return true
		})
		return nil
	})
	if err != nil {
		t.Fatalf("walk: %v", err)
	}
	if len(violations) > 0 {
		t.Fatalf("live-membrane construction confinement (only platform may reference link.NewLivePen/NewLiveAccess/NewLiveSchedule):\n  %s",
			strings.Join(violations, "\n  "))
	}
}
