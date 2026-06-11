// Package archtest enforces repo-level structure constraints that the type
// system alone cannot: it runs as a normal test package, so `go test ./...`
// (and `make lint`) trips on violations.
//
// Scope note: these are TRIPWIRES for accidental drift, not defenses against
// adversarial evasion. Computed strings, consts, or field-by-field mutation
// of a zero-value struct can bypass the AST checks — deliberately dodging a
// lint is a review-layer offence, not something worth a go/types-grade
// analyzer pre-launch. The realistic accident (hand-rolled composite literal,
// redeclared json tag) is exactly what these catch.
package archtest

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"
)

// contractJSONTags is the actor.* self-answer contract field set. These wire
// keys are owned by lib/introspect — the ONE home of the contract. Any struct
// outside lib/introspect declaring one of these json tags is recreating a
// contract shape locally, which is exactly the drift that made tool docs,
// typed shapes, and actor implementations diverge (three homes, zero compile
// coupling). Downstream constructs introspect types; it never redeclares them.
//
// Generic keys (actor_id, type, description) are deliberately NOT guarded:
// they appear legitimately all over payloads and DTOs; the six distinctive
// keys are the tripwire.
var contractJSONTags = map[string]bool{
	"skill_doc":       true,
	"allowed_kinds":   true,
	"max_pending_ms":  true,
	"payload_fields":  true,
	"payload_example": true,
	"error_codes":     true,
}

// allowedDir is the single home of the contract shapes.
const allowedDir = "lib/introspect"

// skipDirs are non-Go / vendored / data trees.
var skipDirs = map[string]bool{
	".git": true, "node_modules": true, "web": true, "bin": true, ".dalek": true,
}

func TestContractShapesLiveOnlyInIntrospect(t *testing.T) {
	fset := token.NewFileSet()
	var violations []string

	root := ".."
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
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
		rel := filepath.ToSlash(path)
		if strings.Contains(rel, allowedDir+"/") {
			return nil
		}
		isTest := strings.HasSuffix(path, "_test.go")

		file, perr := parser.ParseFile(fset, path, nil, parser.SkipObjectResolution)
		if perr != nil {
			return fmt.Errorf("parse %s: %w", path, perr)
		}
		ast.Inspect(file, func(n ast.Node) bool {
			switch node := n.(type) {
			case *ast.StructType:
				// A struct json tag redeclaring a contract key = a parallel
				// typed shape.
				for _, f := range node.Fields.List {
					if f.Tag == nil {
						continue
					}
					tag := strings.Trim(f.Tag.Value, "`")
					jsonTag := reflect.StructTag(tag).Get("json")
					name, _, _ := strings.Cut(jsonTag, ",")
					if contractJSONTags[name] {
						violations = append(violations,
							fmt.Sprintf("%s: struct field redeclares contract json tag %q", fset.Position(f.Pos()), name))
					}
				}
			case *ast.BasicLit:
				// A bare string literal equal to a contract key in non-test
				// code = a map/MarshalJSON-based clone of the shape (tests
				// legitimately decode wire JSON by key).
				if isTest || node.Kind != token.STRING {
					return true
				}
				if v, uerr := strconv.Unquote(node.Value); uerr == nil && contractJSONTags[v] {
					violations = append(violations,
						fmt.Sprintf("%s: string literal %q builds a contract shape outside %s", fset.Position(node.Pos()), v, allowedDir))
				}
			}
			return true
		})
		return nil
	})
	if err != nil {
		t.Fatalf("walk %s: %v", root, err)
	}

	if len(violations) > 0 {
		t.Fatalf("actor.* contract shapes live ONLY in %s — construct introspect.Describe/DescribeType/TypeMeta instead of redeclaring fields:\n  %s",
			allowedDir, strings.Join(violations, "\n  "))
	}
}

// TestEnvelopeConstructionLivesOnlyInBehavior enforces the no-parallel-
// primitives law: lib/behavior is the ONE home of envelope construction
// (BuildRequest / BuildResponseFromRequest / BuildEvent). Adapters (metatool,
// actors) compose those builders; a hand-rolled non-empty message.Envelope{…}
// literal anywhere else in lib/ or actors/ is a parallel primitive — exactly
// the split (callkit / hand-rolled agent envelopes) this repo just removed.
// Zero-value returns (message.Envelope{}) are allowed. runtime/ is exempt
// (the substrate itself is the authority); app/ joins the scan once
// handleSendMessage migrates to BuildRequest in default_agent v0.
func TestEnvelopeConstructionLivesOnlyInBehavior(t *testing.T) {
	fset := token.NewFileSet()
	var violations []string

	for _, root := range []string{"../lib", "../actors"} {
		err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
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
			if strings.Contains(filepath.ToSlash(path), "lib/behavior/") {
				return nil
			}
			file, perr := parser.ParseFile(fset, path, nil, parser.SkipObjectResolution)
			if perr != nil {
				return fmt.Errorf("parse %s: %w", path, perr)
			}
			ast.Inspect(file, func(n ast.Node) bool {
				cl, ok := n.(*ast.CompositeLit)
				if !ok || len(cl.Elts) == 0 {
					return true
				}
				sel, ok := cl.Type.(*ast.SelectorExpr)
				if !ok || sel.Sel.Name != "Envelope" {
					return true
				}
				if pkg, ok := sel.X.(*ast.Ident); ok && pkg.Name == "message" {
					violations = append(violations,
						fmt.Sprintf("%s: hand-rolled message.Envelope literal", fset.Position(cl.Pos())))
				}
				return true
			})
			return nil
		})
		if err != nil {
			t.Fatalf("walk %s: %v", root, err)
		}
	}

	if len(violations) > 0 {
		t.Fatalf("envelope construction lives ONLY in lib/behavior (BuildRequest/BuildResponseFromRequest/BuildEvent) — compose the builders instead:\n  %s",
			strings.Join(violations, "\n  "))
	}
}

// importsOf returns the import paths of one .go file.
func importsOf(t *testing.T, fset *token.FileSet, path string) []string {
	t.Helper()
	file, err := parser.ParseFile(fset, path, nil, parser.ImportsOnly)
	if err != nil {
		t.Fatalf("parse imports %s: %v", path, err)
	}
	var out []string
	for _, imp := range file.Imports {
		p, uerr := strconv.Unquote(imp.Path.Value)
		if uerr == nil {
			out = append(out, p)
		}
	}
	return out
}

// platformModulePrefix is this module's import path prefix; the dependency
// guards reason about same-module package paths.
const platformModulePrefix = "github.com/wanpengxie/ActOS/"

// TestPlatformDependencyDirection enforces the platform reshape's dependency
// figure (platform-redesign-construction §1): platform is the assembly layer
// BELOW app/cmd and ABOVE the substrate. It must never import downstream
// (actors/, app/) — that would let a domain actor or the HTTP layer back-flow
// into the physical layer. The wire VOCABULARY (runtime/ipc + the mux) lives
// ONLY in platform/link: no other platform package may carry it (the computebus
// drift this reshape removed must not regrow a second home).
func TestPlatformDependencyDirection(t *testing.T) {
	fset := token.NewFileSet()
	var violations []string

	root := "../platform"
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
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
		inLink := strings.Contains(rel, "/platform/link/")
		for _, imp := range importsOf(t, fset, path) {
			if !strings.HasPrefix(imp, platformModulePrefix) {
				continue
			}
			sub := strings.TrimPrefix(imp, platformModulePrefix)
			// Downstream back-flow: platform must not import domain/app.
			if strings.HasPrefix(sub, "actors/") || strings.HasPrefix(sub, "app/") || sub == "app" {
				violations = append(violations,
					fmt.Sprintf("%s: platform imports downstream %q (dependency direction violated)", rel, imp))
			}
			// Wire vocabulary containment: only platform/link may carry the port
			// wire (runtime/ipc) — its mux is the single home of link framing.
			if !inLink && (sub == "runtime/ipc" || strings.HasPrefix(sub, "runtime/ipc/")) {
				violations = append(violations,
					fmt.Sprintf("%s: platform package outside link imports the wire %q (frame vocabulary lives only in platform/link)", rel, imp))
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk %s: %v", root, err)
	}

	if len(violations) > 0 {
		t.Fatalf("platform dependency direction (construction §1):\n  %s", strings.Join(violations, "\n  "))
	}
}
