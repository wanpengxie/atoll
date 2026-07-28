package archtest

import (
	"go/ast"
	"go/token"
	"strings"
	"testing"
)

// A birth id is minted inside the transaction that inserts the row, and no
// caller can ask for a particular one.
//
// actorstore v4.7 §3.3 spells the birth out as one transaction — dedupe, mint,
// insert — returning the authoritative record. ActorDraft used to carry an ID
// field anyway, with a store branch that took the caller's value verbatim after
// an in-use check. No production site ever filled it, which is exactly what made
// it dangerous: writing one line was enough to name an actor from outside the
// transaction, and nothing would have failed.
//
// An id chosen outside the mint is an id decided before anyone checked, and it
// also escapes the naming the mint derives (kind, seed and creation instant,
// probing forward past names a tombstone still holds) — so it can occupy a value
// some later birth would have produced.
//
// How to break it: add an ID field back to ActorDraft, or any other field on it
// whose name ends in ID and is not one of the semantic keys a birth is deduped
// by (SourceDeclID).
func TestActorDraftCannotNameTheBirth(t *testing.T) {
	// The semantic keys a draft legitimately carries. They identify what the
	// birth is FOR, and the mint derives the id from them; they never are it.
	allowed := map[string]bool{"SourceDeclID": true}

	var violations []string
	found := false
	walkProductionGo(t, func(path string, file *ast.File, fset *token.FileSet) {
		ast.Inspect(file, func(node ast.Node) bool {
			spec, ok := node.(*ast.TypeSpec)
			if !ok || spec.Name.Name != "ActorDraft" {
				return true
			}
			structType, ok := spec.Type.(*ast.StructType)
			if !ok {
				return true
			}
			found = true
			for _, field := range structType.Fields.List {
				for _, name := range field.Names {
					if !strings.HasSuffix(name.Name, "ID") || allowed[name.Name] {
						continue
					}
					violations = append(violations, path+":"+
						fset.Position(name.Pos()).String()+" ActorDraft."+name.Name)
				}
			}
			return true
		})
	})
	if !found {
		t.Fatal("ActorDraft was not found under the production tree; this wall no longer guards anything")
	}
	if len(violations) != 0 {
		t.Fatalf("a draft may not name the actor it is asking to create (v4.7 §3.3); "+
			"the id is minted inside the insert transaction:\n%s",
			strings.Join(violations, "\n"))
	}
}
