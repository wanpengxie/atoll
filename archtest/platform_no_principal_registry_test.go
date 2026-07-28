package archtest

import (
	"go/ast"
	"go/token"
	"strings"
	"testing"
)

// Platform holds no actor-registry read face.
//
// actorstore v4.7 §12.2: Platform does not hold, implement or touch the actor
// store. The principal axis was the one place that had drifted — Home kept a
// storespec.PrincipalRegistry and View.ResolvePrincipal turned a login into a
// member by querying actor_registry directly. That is a second runtime truth:
// the registry can show a member the Controller has not published yet, and can
// still show one the Controller has already ended, so the same HTTP request
// answers differently depending on which ledger it happened to read.
//
// The read itself is legitimate and stayed — it moved onto the Controller
// (ResolvePrincipal there, answered off the value ledger under the ledger lock).
// What this wall forbids is the door reaching around it again. It is a name
// check on purpose: a future rename of the interface is not the failure mode
// worth guarding, a future assembly that wires the registry back into a
// platform-side field is.
//
// How to break it: give any type under platform/ a field of type
// storespec.PrincipalRegistry, or call LookupActivePrincipal from there.
func TestPlatformHoldsNoPrincipalRegistry(t *testing.T) {
	const (
		registryType = "PrincipalRegistry"
		registryRead = "LookupActivePrincipal"
	)
	var violations []string
	walkProductionGo(t, func(path string, file *ast.File, fset *token.FileSet) {
		if !strings.HasPrefix(path, "../platform/") {
			return
		}
		ast.Inspect(file, func(node ast.Node) bool {
			switch value := node.(type) {
			case *ast.SelectorExpr:
				// storespec.PrincipalRegistry as a field/param/result type, and
				// any .LookupActivePrincipal call, are the same violation: the
				// door is holding or using the registry face.
				if value.Sel.Name == registryType || value.Sel.Name == registryRead {
					violations = append(violations,
						path+":"+fset.Position(value.Pos()).String()+" refers to "+value.Sel.Name)
				}
			case *ast.Ident:
				if value.Name == registryRead {
					violations = append(violations,
						path+":"+fset.Position(value.Pos()).String()+" refers to "+registryRead)
				}
			}
			return true
		})
	})
	if len(violations) != 0 {
		t.Fatalf("platform is holding an actor-registry read face again (v4.7 §12.2); "+
			"the principal question belongs to the Controller:\n%s",
			strings.Join(violations, "\n"))
	}
}
