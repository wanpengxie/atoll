package archtest

import (
	"fmt"
	"go/ast"
	"go/token"
	"strconv"
	"strings"
	"testing"
)

// Two adjacent spec §13.3 clauses about the same object, neither of which has a
// wall today:
//
//	#27 "daemon stable outbound facade 只存在于 `platform/compute`，Server Body
//	     不构造 OutboundSlot"
//	#28 "OutboundSlot registry 以 exact slot object 为 identity，不得使用
//	     `map[ActorID]slot` 或把 slot 写入 HostState/Plan/Store"
//
// An OutboundSlot is a DAEMON-side membrane: it is the stable arm a remote body
// holds while the physical stream underneath it is replaced. Two properties make
// it work, and both are properties of WHERE it may exist.
//
// #27 — one home. The Server assembles bodies that run in-process; their
// capabilities are minted locally and their route, when they have one, is a
// Binding. If Server assembly could also open an outbound slot ("just a local
// one, as an optimisation"), the Server would hold a second, parallel capability
// path to its own bodies — with its own currency rules and its own teardown —
// alongside the Host's. `platform/home` and `platform/compute` are sibling
// packages under the same parent, so this import is a low-friction thing to
// write. The one test that walks platform/home's whole import closure
// (`server_zero_storage_test.go`) is hunting for the daemon's storage host and
// nothing else, and it deliberately leaves platform/compute out of its roots as
// "a different fence" — so a home→compute edge sails straight through it.
// Nothing else looks at all.
//
// #28 — object identity, and nothing derived from it. The slot registry is
// keyed by the slot POINTER precisely so a G1 and a G2 slot for the same
// ActorID can coexist while one drains and the other lives. Two ways to lose
// that, and only the first is currently guarded (by a literal string check):
//
//	(a) rekey the registry by ActorID — G1 and G2 collide, and one of them is
//	    silently overwritten;
//	(b) write the slot into durable/plan/host state — a Plan row or a HostState
//	    entry now carries a live capability handle. That handle outlives the
//	    generation it was minted for, and the row it rides in is copied,
//	    compared and re-published by machinery that has no idea it is holding a
//	    body's arms. The half that is protected today (HostState) is protected
//	    only INDIRECTLY, by actorhost's import wall; the Plan/Store half is
//	    wide open — the `platform` root package imports `platform/compute`
//	    with nothing objecting.

const outboundFacadePkg = "../platform/compute"

// outboundFacadeConsumers are the only paths allowed to name the daemon
// outbound facade: the facade itself and the daemon composition root that
// starts it.
var outboundFacadeConsumers = []string{outboundFacadePkg + "/", "../cmd/daemon/"}

// outboundFacadeSymbols is the facade's whole vocabulary. Naming ANY of them
// outside the two sanctioned paths means a second assembly grew a daemon
// outbound path.
var outboundFacadeSymbols = map[string]bool{
	"OutboundSlot":       true,
	"DaemonOutbound":     true,
	"PreparedOutbound":   true,
	"OutboundArmsBundle": true,
	"NewDaemonOutbound":  true,
}

// outboundSlotStateSinks are the three structures the spec names: a slot must
// never become part of Host state, of a Plan row, or of anything durable.
var outboundSlotStateSinks = []string{
	"../runtime/actorhost",
	"../platform",
	"../runtime/actorstore",
	"../runtime/storespec",
}

func pathIsOutboundFacadeConsumer(path string) bool {
	slashed := strings.ReplaceAll(path, "\\", "/")
	for _, allowed := range outboundFacadeConsumers {
		if strings.Contains(slashed, strings.TrimPrefix(allowed, "../")) {
			return true
		}
	}
	return false
}

// outboundFacadeHomeViolations enforces #27: the facade has one home, and only
// the daemon root may import it.
func outboundFacadeHomeViolations(files map[string]*ast.File, fset *token.FileSet) []string {
	var v []string
	for path, file := range files {
		consumer := pathIsOutboundFacadeConsumer(path)
		for _, spec := range file.Imports {
			importPath, err := strconv.Unquote(spec.Path.Value)
			if err != nil {
				continue
			}
			if !strings.HasSuffix(importPath, "/platform/compute") {
				continue
			}
			if !consumer {
				v = append(v, fmt.Sprintf(
					"%s imports the daemon outbound facade %q — the Server assembles local bodies; it never opens an outbound slot",
					path, importPath))
			}
		}
		if consumer {
			continue
		}
		ast.Inspect(file, func(node ast.Node) bool {
			switch value := node.(type) {
			case *ast.SelectorExpr:
				if pkg, ok := value.X.(*ast.Ident); ok && pkg.Name == "compute" &&
					outboundFacadeSymbols[value.Sel.Name] {
					v = append(v, fmt.Sprintf(
						"%s:%d names compute.%s — the daemon outbound facade lives in platform/compute alone",
						path, fset.Position(value.Pos()).Line, value.Sel.Name))
				}
			case *ast.TypeSpec:
				if outboundFacadeSymbols[value.Name.Name] {
					v = append(v, fmt.Sprintf(
						"%s:%d declares a second %s — one facade, one home",
						path, fset.Position(value.Pos()).Line, value.Name.Name))
				}
			}
			return true
		})
	}
	return v
}

// typeMentionsOutboundSlot reports whether a type expression reaches the slot,
// however it is wrapped.
func typeMentionsOutboundSlot(expr ast.Expr, fset *token.FileSet) bool {
	return strings.Contains(expressionText(fset, expr), "OutboundSlot")
}

// outboundSlotStateSinkViolations enforces #28's second half: no Host/Plan/Store
// structure may carry a slot, in a field or across a signature.
func outboundSlotStateSinkViolations(files map[string]*ast.File, fset *token.FileSet) []string {
	var v []string
	for path, file := range files {
		if pathIsOutboundFacadeConsumer(path) {
			continue
		}
		ast.Inspect(file, func(node ast.Node) bool {
			switch value := node.(type) {
			case *ast.TypeSpec:
				structType, ok := value.Type.(*ast.StructType)
				if !ok || structType.Fields == nil {
					return true
				}
				for _, field := range structType.Fields.List {
					if !typeMentionsOutboundSlot(field.Type, fset) {
						continue
					}
					name := "<embedded>"
					if len(field.Names) > 0 {
						name = field.Names[0].Name
					}
					v = append(v, fmt.Sprintf(
						"%s:%d %s.%s stores an OutboundSlot — a live capability handle must not become Host/Plan/Store state",
						path, fset.Position(field.Pos()).Line, value.Name.Name, name))
				}
			case *ast.FuncDecl:
				fields := []*ast.Field{}
				if value.Type.Params != nil {
					fields = append(fields, value.Type.Params.List...)
				}
				if value.Type.Results != nil {
					fields = append(fields, value.Type.Results.List...)
				}
				for _, field := range fields {
					if typeMentionsOutboundSlot(field.Type, fset) {
						v = append(v, fmt.Sprintf(
							"%s:%d %s passes an OutboundSlot across its signature — the slot never leaves the daemon facade",
							path, fset.Position(field.Pos()).Line, value.Name.Name))
					}
				}
			}
			return true
		})
	}
	return v
}

// outboundSlotRegistryIdentityViolations enforces #28's first half inside the
// facade: the registry is keyed by the slot object, never by an ActorID.
func outboundSlotRegistryIdentityViolations(files map[string]*ast.File, fset *token.FileSet) []string {
	var v []string
	objectKeyed := 0
	for path, file := range files {
		ast.Inspect(file, func(node ast.Node) bool {
			mapType, ok := node.(*ast.MapType)
			if !ok {
				return true
			}
			key := expressionText(fset, mapType.Key)
			value := expressionText(fset, mapType.Value)
			if key == "*OutboundSlot" {
				objectKeyed++
				return true
			}
			if strings.Contains(value, "OutboundSlot") {
				v = append(v, fmt.Sprintf(
					"%s:%d indexes slots by %s — G1 and G2 slots for one ActorID must be able to coexist, so identity is the slot object",
					path, fset.Position(mapType.Pos()).Line, key))
			}
			return true
		})
	}
	if objectKeyed == 0 {
		v = append(v, "platform/compute holds no map[*OutboundSlot] registry — the exact-object identity wall lost its subject")
	}
	return v
}

func TestDaemonOutboundFacadeHasOneHome(t *testing.T) {
	// Footing: the vocabulary the wall confines must actually be declared in
	// the home it is confined to.
	homeFiles, _ := loadArchWallPackage(t, outboundFacadePkg)
	declared := map[string]bool{}
	for _, file := range homeFiles {
		ast.Inspect(file, func(node ast.Node) bool {
			switch value := node.(type) {
			case *ast.TypeSpec:
				declared[value.Name.Name] = true
			case *ast.FuncDecl:
				if value.Recv == nil {
					declared[value.Name.Name] = true
				}
			}
			return true
		})
	}
	for symbol := range outboundFacadeSymbols {
		if !declared[symbol] {
			t.Errorf("platform/compute no longer declares %s — the facade-home wall is confining a name that moved", symbol)
		}
	}

	for _, root := range []string{"../app", "../cmd", "../drivers", "../lib", "../platform", "../registry", "../runtime"} {
		files, fset := loadArchWallPackage(t, root)
		failViolations(t, "the daemon outbound facade lives only in platform/compute",
			outboundFacadeHomeViolations(files, fset))
	}
}

func TestOutboundSlotNeverBecomesHostPlanOrStoreState(t *testing.T) {
	for _, sink := range outboundSlotStateSinks {
		files, fset := loadArchWallPackage(t, sink)
		failViolations(t, "no OutboundSlot in "+sink,
			outboundSlotStateSinkViolations(files, fset))
	}
	files, fset := loadArchWallPackage(t, outboundFacadePkg)
	failViolations(t, "the slot registry is keyed by the slot object",
		outboundSlotRegistryIdentityViolations(files, fset))
}

// TestOutboundSlotConfinementWallTripsOnSiblingReuse is the trip proof for both
// clauses, applied to the two real files the break would land in.
func TestOutboundSlotConfinementWallTripsOnSiblingReuse(t *testing.T) {
	t.Run("Server assembly opens a local outbound slot", func(t *testing.T) {
		const path = "../platform/home/open.go"
		files, fset := patchArchWallSource(t, path,
			archWallPatch{
				old: `	"github.com/wanpengxie/atoll/platform/internal/hostcommon"`,
				new: `	"github.com/wanpengxie/atoll/platform/compute"
	"github.com/wanpengxie/atoll/platform/internal/hostcommon"`,
			},
			archWallPatch{
				old: "	h.opEntry = &opEntry{home: h}",
				new: `	h.opEntry = &opEntry{home: h}
	// "while we're here, give the server body a local outbound slot too"
	h.localOutbound = compute.NewDaemonOutbound(compute.DaemonOutboundConfig{Parent: ctx})`,
			},
		)
		if got := outboundFacadeHomeViolations(files, fset); len(got) == 0 {
			t.Fatal("facade-home wall stayed green on Server assembly importing platform/compute")
		}
	})

	t.Run("Plan row caches a slot for reconnect", func(t *testing.T) {
		const path = "../platform/plan.go"
		files, fset := patchArchWallSource(t, path,
			archWallPatch{
				old: `	"github.com/wanpengxie/atoll/runtime/actorhost"`,
				new: `	"github.com/wanpengxie/atoll/platform/compute"
	"github.com/wanpengxie/atoll/runtime/actorhost"`,
			},
			archWallPatch{
				old: "	Config     json.RawMessage      `json:\"config_json,omitempty\"`",
				new: "	Config     json.RawMessage      `json:\"config_json,omitempty\"`\n	// cached across reconnects\n	slot       *compute.OutboundSlot",
			},
		)
		if got := outboundSlotStateSinkViolations(files, fset); len(got) == 0 {
			t.Fatal("state-sink wall stayed green on a Plan row carrying an OutboundSlot")
		}
		if got := outboundFacadeHomeViolations(files, fset); len(got) == 0 {
			t.Fatal("facade-home wall stayed green on the platform root importing platform/compute")
		}
	})

	fixtures := []struct {
		name  string
		src   string
		check func(map[string]*ast.File, *token.FileSet) []string
	}{
		{
			name: "registry rekeyed by ActorID",
			src: `package compute
type DaemonOutbound struct {
	slots map[actor.ActorID]*OutboundSlot
}`,
			check: outboundSlotRegistryIdentityViolations,
		},
		{
			name: "HostState grows a slot field",
			src: `package actorhost
type hostState struct {
	desired *desiredValue
	slot    *compute.OutboundSlot
}`,
			check: outboundSlotStateSinkViolations,
		},
		{
			name: "store row round-trips a slot",
			src: `package actorstore
func (s *Store) AttachSlot(id actor.ActorID, slot *compute.OutboundSlot) error {
	return nil
}`,
			check: outboundSlotStateSinkViolations,
		},
		{
			name: "a second facade grows elsewhere",
			src: `package home
type OutboundSlot struct{}`,
			check: outboundFacadeHomeViolations,
		},
	}
	for _, tc := range fixtures {
		t.Run(tc.name, func(t *testing.T) {
			files, fset := parseArchWallFixtureSource(t, "outbound_sink_fixture.go", tc.src)
			if got := tc.check(files, fset); len(got) == 0 {
				t.Fatalf("wall did not trip on the break form %q", tc.name)
			}
		})
	}
}
