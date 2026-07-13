package archtest

import (
	"go/ast"
	"go/parser"
	"go/token"
	"testing"
)

// TestSubjectgateFilesNeverMintOrConstruct pins the 门不构建/不现铸 red line
// at the source level (mint-call-site assertion): the human cell's frame
// interpreter (platform/internal/humancell/humancell.go +
// humancell_verbs.go — the gateway 期 successor to the removed HumanHandle
// door, quarantined to platform/internal/humancell by platform 拓扑批 T2) and
// the expiry reaper (platform/home/expiry.go) may NEVER mint a
// capability or construct an embodiment — the interpreter only drives the cell's
// OWN welded caps through the identity-dimension Sys verbs (P2 能力取用), and the
// reaper writes through the home's existing systemPen (D3 — never mint-as-caller).
// The forbidden call names are checked as selector suffixes so a re-plumb through
// any holder (h.minter.Mint / schedMinter.Mint / rt.Spawn / EnsureLive) trips it.
//
// Scope is deliberately these three files, not the platform tree: buildCaps /
// the supply ring / the scheduler's reviver are the LEGITIMATE mint/spawn
// sites and live elsewhere.
func TestSubjectgateFilesNeverMintOrConstruct(t *testing.T) {
	forbidden := map[string]string{
		"Mint":          "capability minting belongs to buildCaps (P2 能力取用不现铸)",
		"MintState":     "capability minting belongs to buildCaps (P2 能力取用不现铸)",
		"Spawn":         "embodiment supply belongs to the ring (门不构建)",
		"SpawnIfAbsent": "embodiment supply belongs to the ring (门不构建)",
		"EnsureLive":    "revive belongs to the ring/reviver (绝不 revive-to-close / revive-to-write)",
	}
	for _, file := range []string{
		"../platform/internal/humancell/humancell.go",
		"../platform/internal/humancell/humancell_verbs.go",
		"../platform/home/expiry.go",
	} {
		fset := token.NewFileSet()
		f, err := parser.ParseFile(fset, file, nil, 0)
		if err != nil {
			t.Fatalf("parse %s: %v", file, err)
		}
		ast.Inspect(f, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			sel, ok := call.Fun.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			if why, bad := forbidden[sel.Sel.Name]; bad {
				t.Errorf("%s: forbidden call %s(...) — %s",
					fset.Position(call.Pos()), sel.Sel.Name, why)
			}
			return true
		})
	}
}
