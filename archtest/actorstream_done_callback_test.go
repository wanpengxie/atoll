package archtest

import (
	"fmt"
	"go/ast"
	"go/token"
	"strings"
	"testing"
)

// spec §13.3: "ActorStream Done callback 只能 wake，不能清 slot/current；
// successor publication 后 predecessor callback 不得修改 successor".
//
// A slot outlives its streams. When a stream dies, some goroutine notices — and
// that goroutine is, by construction, LATE: by the time it runs, the converger
// may already have opened a successor stream and published a new arms bundle
// into the same slot. So the death notice is not authority about the slot's
// present state; it is authority about one dead object. Its entire licensed
// effect is "wake the converger, which will re-derive everything under the
// lock".
//
// Two ways to break that, both of which read as tidiness rather than as an
// architecture change:
//
//  1. "while we're here, clear the slot's cached current/arms" — the callback
//     tears down a bundle it does not own. If a successor was published in the
//     window, a live body's arms are yanked out from under it and the observable
//     result is a body that silently stops reaching the wire.
//  2. dropping the exact-identity guard — the callback checks "is this slot
//     connected?" instead of "is the published bundle still MY session and MY
//     stream?". Same outcome, arrived at by omission instead of by addition.
//
// The wall therefore says: inside the stream-death callback, the only state
// write is the retry hint, no bundle publication primitive may be called at all,
// and the exact-identity guard on both session and stream must be present.

// slotWakeOnlyField is the single write a death notice is allowed to make: a
// hint about WHEN to re-converge. It changes no capability and no identity.
const slotWakeOnlyField = "slot.retryAt"

// bundlePublicationOps are the atomic pointer primitives that publish or retract
// an arms generation. None of them belongs in a late death notice.
var bundlePublicationOps = map[string]bool{
	"Swap":             true,
	"Store":            true,
	"CompareAndSwap":   true,
	"CompareAndSwapP":  true,
	"CompareAndSwapPP": true,
}

// streamDeathCallbacks finds the functions that react to an ActorStream's Done
// channel. It keys on the SHAPE (a receive from `<param>.Done()` inside a
// select, where the param is a `*link.ActorStream`) rather than on the function
// name, so renaming the watcher does not slip past the wall.
func streamDeathCallbacks(file *ast.File) []*ast.FuncDecl {
	var found []*ast.FuncDecl
	for _, decl := range file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || fn.Body == nil || fn.Type.Params == nil {
			continue
		}
		streams := map[string]bool{}
		for _, param := range fn.Type.Params.List {
			star, ok := param.Type.(*ast.StarExpr)
			if !ok {
				continue
			}
			selector, ok := star.X.(*ast.SelectorExpr)
			if !ok || selector.Sel.Name != "ActorStream" {
				continue
			}
			for _, name := range param.Names {
				streams[name.Name] = true
			}
		}
		if len(streams) == 0 {
			continue
		}
		watches := false
		ast.Inspect(fn.Body, func(node ast.Node) bool {
			comm, ok := node.(*ast.CommClause)
			if !ok || comm.Comm == nil {
				return true
			}
			ast.Inspect(comm.Comm, func(inner ast.Node) bool {
				call, ok := inner.(*ast.CallExpr)
				if !ok {
					return true
				}
				selector, ok := call.Fun.(*ast.SelectorExpr)
				if !ok || selector.Sel.Name != "Done" {
					return true
				}
				if receiver, ok := selector.X.(*ast.Ident); ok && streams[receiver.Name] {
					watches = true
				}
				return true
			})
			return true
		})
		if watches {
			found = append(found, fn)
		}
	}
	return found
}

// streamDeathCallbackViolations enforces "wake only" on every stream-death
// callback in the package.
func streamDeathCallbackViolations(files map[string]*ast.File, fset *token.FileSet) []string {
	var v []string
	callbacks := 0
	for path, file := range files {
		for _, fn := range streamDeathCallbacks(file) {
			callbacks++
			where := fmt.Sprintf("%s:%s", path, fn.Name.Name)
			guardsSession, guardsStream := false, false

			ast.Inspect(fn.Body, func(node ast.Node) bool {
				switch value := node.(type) {
				case *ast.AssignStmt:
					// Short declarations introduce locals; only plain
					// assignment to an existing field mutates shared state.
					if value.Tok != token.ASSIGN {
						return true
					}
					for _, lhs := range value.Lhs {
						selector, ok := lhs.(*ast.SelectorExpr)
						if !ok {
							continue
						}
						text := expressionText(fset, selector)
						if text == slotWakeOnlyField {
							continue
						}
						v = append(v, fmt.Sprintf(
							"%s:%d writes %s — a stream death notice may only hint at re-convergence (%s), never edit slot state it no longer owns",
							where, fset.Position(value.Pos()).Line, text, slotWakeOnlyField))
					}
				case *ast.CallExpr:
					selector, ok := value.Fun.(*ast.SelectorExpr)
					if !ok {
						if ident, ok := value.Fun.(*ast.Ident); ok && ident.Name == "delete" {
							v = append(v, fmt.Sprintf(
								"%s:%d deletes from a registry — a late death notice never unregisters; the converger owns removal",
								where, fset.Position(value.Pos()).Line))
						}
						return true
					}
					if bundlePublicationOps[selector.Sel.Name] &&
						archWallRootIdent(selector.X) == "slot" {
						v = append(v, fmt.Sprintf(
							"%s:%d publishes/retracts an arms bundle (%s) — by now a successor generation may already be live in this slot",
							where, fset.Position(value.Pos()).Line, expressionText(fset, value.Fun)))
					}
					if selector.Sel.Name == "Close" {
						v = append(v, fmt.Sprintf(
							"%s:%d closes a physical child — the death notice reacts to a teardown, it does not perform one",
							where, fset.Position(value.Pos()).Line))
					}
				case *ast.BinaryExpr:
					if value.Op != token.EQL {
						return true
					}
					switch expressionText(fset, value) {
					case "bundle.Session == session":
						guardsSession = true
					case "bundle.Stream == stream":
						guardsStream = true
					}
				}
				return true
			})

			if !guardsSession || !guardsStream {
				v = append(v, fmt.Sprintf(
					"%s does not gate on the exact published bundle (session=%v stream=%v) — without both, a predecessor's death notice edits its successor's slot",
					where, guardsSession, guardsStream))
			}
		}
	}
	if callbacks == 0 {
		v = append(v, "no ActorStream death callback found — the wake-only wall lost its subject")
	}
	return v
}

func TestActorStreamDeathCallbackOnlyWakes(t *testing.T) {
	files, fset := loadArchWallPackage(t, "../platform/compute")
	failViolations(t, "an ActorStream death notice only wakes the converger",
		streamDeathCallbackViolations(files, fset))
}

// TestActorStreamDeathCallbackWallTripsOnTidyUp is the trip proof: every case is
// applied to the real converger, because the whole risk is that these edits look
// like housekeeping inside a callback someone is already editing.
func TestActorStreamDeathCallbackWallTripsOnTidyUp(t *testing.T) {
	const path = "../platform/compute/outbound.go"

	t.Run("clears the slot's arms while it is there", func(t *testing.T) {
		files, fset := patchArchWallSource(t, path, archWallPatch{
			old: "		bundle := slot.arms.Load()",
			new: `		bundle := slot.arms.Load()
		slot.arms.Store(disconnectedOutboundBundle)`,
		})
		if got := streamDeathCallbackViolations(files, fset); len(got) == 0 {
			t.Fatal("wake-only wall stayed green on a callback that retracts the arms bundle")
		}
	})

	t.Run("drops the exact-stream identity guard", func(t *testing.T) {
		files, fset := patchArchWallSource(t, path, archWallPatch{
			old: "			bundle.Session == session && bundle.Stream == stream {",
			new: "			bundle.Session == session {",
		})
		if got := streamDeathCallbackViolations(files, fset); len(got) == 0 {
			t.Fatal("wake-only wall stayed green after the exact-stream guard was dropped")
		}
	})

	t.Run("clears an unrelated slot field", func(t *testing.T) {
		files, fset := patchArchWallSource(t, path, archWallPatch{
			old: "			slot.retryAt = time.Now().Add(d.retry)\n		}\n		d.mu.Unlock()\n		d.Wake()",
			new: "			slot.retryAt = time.Now().Add(d.retry)\n			slot.pendingObs = nil\n		}\n		d.mu.Unlock()\n		d.Wake()",
		})
		if got := streamDeathCallbackViolations(files, fset); len(got) == 0 {
			t.Fatal("wake-only wall stayed green on a callback that clears held observations")
		}
	})

	t.Run("closes the successor stream on the way out", func(t *testing.T) {
		files, fset := patchArchWallSource(t, path, archWallPatch{
			old: "		d.mu.Unlock()\n		d.Wake()\n	case <-d.ctx.Done():",
			new: "		d.mu.Unlock()\n		if bundle != nil && bundle.Stream != nil {\n			_ = bundle.Stream.Close()\n		}\n		d.Wake()\n	case <-d.ctx.Done():",
		})
		if got := streamDeathCallbackViolations(files, fset); len(got) == 0 {
			t.Fatal("wake-only wall stayed green on a callback that tears a stream down")
		}
	})
}

// TestActorStreamDeathCallbackWallIsShapeKeyed proves the callback is located by
// shape: renaming the watcher must not lose the wall.
func TestActorStreamDeathCallbackWallIsShapeKeyed(t *testing.T) {
	src := `package compute
func (d *DaemonOutbound) reactToStreamCollapse(
	slot *OutboundSlot,
	session *link.AuthenticatedLinkSession,
	stream *link.ActorStream,
) {
	select {
	case <-stream.Done():
		d.mu.Lock()
		bundle := slot.arms.Load()
		slot.arms.Store(disconnectedOutboundBundle)
		_ = bundle
		d.mu.Unlock()
		d.Wake()
	case <-d.ctx.Done():
	}
}`
	files, fset := parseArchWallFixtureSource(t, "outbound_watch_fixture.go", src)
	got := streamDeathCallbackViolations(files, fset)
	if len(got) == 0 {
		t.Fatal("wake-only wall did not find a renamed stream-death callback")
	}
	if !strings.Contains(strings.Join(got, "\n"), "reactToStreamCollapse") {
		t.Fatalf("wall found something other than the renamed callback: %v", got)
	}
}
