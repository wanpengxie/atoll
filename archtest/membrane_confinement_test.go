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
// (platform/home/caps.go buildCaps) and the port emitSink path
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
	// NewLiveArms is the daemon-side caps assembler (S7): it bundles the
	// RebindableArms facades through the three membranes above, gated on the
	// daemon cell's own incarnation. Same exported-constructor exposure, same
	// lock: it must be woven exactly once, at the compute build closure.
	"NewLiveArms": true,
}

// membraneWeaveAllowlist is the EXACT per-file, PER-SYMBOL set permitted to
// reference the live-membrane constructors via the `link.` qualifier. Cored out
// by ACTUAL reference (not by tree prefix):
//
//   - The link package itself DEFINES the constructors; a link file never imports
//     link, so a same-package reference carries no `link.` qualifier and the
//     import-resolution below already exempts it (local=="" → skip). It therefore
//     needs no entry here (accept.go's port-path weave is such a same-package use
//     — and, since S7, so is livearms.go's NewLiveArms body: it weaves
//     NewLivePen/NewLiveAccess/NewLiveSchedule over the RebindableArms facades,
//     gated on the DAEMON's own incarnation).
//   - platform/home/caps.go is the SINGLE home-side caps assembler (buildCaps) —
//     it may weave the three raw membranes, never the daemon bundle. (platform
//     拓扑批 T3: buildCaps moved out of home.go into its own file; T5b: home
//     成包, path moved under platform/home/.)
//   - platform/compute/ring.go is the SINGLE daemon-side assembly site (the
//     build closure inside computeRing.buildOne, ex-compute.go) — it may call
//     link.NewLiveArms, and ONLY that: it never touches the raw per-plane
//     constructors directly. (T5b: compute 成包, path moved under
//     platform/compute/.)
//
// Everything else — spawnhandle.go (delegates to the assembler, weaves nothing)
// and any future platform file — is OUT: adding a NewLive* reference anywhere
// else, or a raw-membrane reference in ring.go / a NewLiveArms reference in
// caps.go, must turn this test red.
var membraneWeaveAllowlist = map[string]map[string]bool{
	"../platform/home/caps.go": {
		"NewLivePen":      true,
		"NewLiveAccess":   true,
		"NewLiveSchedule": true,
	},
	"../platform/compute/ring.go": {
		"NewLiveArms": true,
	},
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
		allowedSymbols := membraneWeaveAllowlist[slash] // per-symbol; nil for every non-assembler file
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
			if membraneConstructors[sel.Sel.Name] && !allowedSymbols[sel.Sel.Name] {
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
		t.Fatalf("live-membrane construction confinement (link.NewLivePen/NewLiveAccess/NewLiveSchedule only in caps.go's buildCaps; link.NewLiveArms only in ring.go's build closure):\n  %s",
			strings.Join(violations, "\n  "))
	}
}
