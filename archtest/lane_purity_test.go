package archtest

import (
	"bytes"
	"fmt"
	"go/ast"
	"go/parser"
	"go/printer"
	"go/token"
	"io/fs"
	"path/filepath"
	"strings"
	"testing"
)

// TestLaneYamuxNeverInExportedSignature pins 期11 spec §5.2's transport wall:
// "yamux 类型不得出现在任何 exported 签名" — platform/internal/link is the
// ONE package allowed to import github.com/hashicorp/yamux (it is the lane
// transport's implementor), but every yamux type (*yamux.Session,
// *yamux.Stream, yamux.Config, …) must stay confined to unexported
// functions/methods/fields. Downstream sees only the package's own contract
// types (LocalFileOpener, accessdoor.FileAccess's plain io.ReadWriteCloser
// Stream field, error codes) — never a yamux type leaking through an
// exported func result, method result, or exported struct field. A plain
// source-text scan (this codebase's own established archtest idiom — see
// e.g. actorbase_confinement_test.go's doc — not a go/types-grade pass):
// render every EXPORTED top-level declaration's signature/field list and
// check for the substring "yamux".
func TestLaneYamuxNeverInExportedSignature(t *testing.T) {
	const linkDir = "../platform/internal/link"
	fset := token.NewFileSet()
	var violations []string

	err := filepath.WalkDir(linkDir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		f, perr := parser.ParseFile(fset, path, nil, 0)
		if perr != nil {
			return perr
		}
		for _, decl := range f.Decls {
			switch d := decl.(type) {
			case *ast.FuncDecl:
				if !d.Name.IsExported() {
					continue
				}
				// A method whose RECEIVER is unexported-named but type is
				// exported (e.g. (a *Acceptor)) still counts if the METHOD
				// NAME itself is exported — that is genuinely part of the
				// package's public API surface.
				if snippet := render(fset, d.Type); strings.Contains(strings.ToLower(snippet), "yamux") {
					violations = append(violations, fmt.Sprintf("%s: exported func/method %q signature contains a yamux type: %s", path, d.Name.Name, snippet))
				}
			case *ast.GenDecl:
				if d.Tok != token.TYPE {
					continue
				}
				for _, spec := range d.Specs {
					ts, ok := spec.(*ast.TypeSpec)
					if !ok || !ts.Name.IsExported() {
						continue
					}
					st, ok := ts.Type.(*ast.StructType)
					if !ok {
						// The other exported-type leak forms: an exported
						// INTERFACE whose method takes/returns a yamux type, and
						// an exported type ALIAS or DEFINED type over a yamux
						// type (`type Foo = yamux.Session` / `type Foo
						// yamux.Session`). render(ts.Type) covers all of them —
						// interface method signatures are rendered inside the
						// InterfaceType, and an alias/defined type renders its
						// underlying yamux reference directly.
						if snippet := render(fset, ts.Type); strings.Contains(strings.ToLower(snippet), "yamux") {
							violations = append(violations, fmt.Sprintf("%s: exported type %q definition (interface method / alias / defined type) contains a yamux type: %s", path, ts.Name.Name, snippet))
						}
						continue
					}
					for _, field := range st.Fields.List {
						// An embedded/unexported field name is still part of
						// the struct's public field LIST if the field itself
						// has no explicit unexported name (embedding) or is
						// itself exported; skip only genuinely unexported
						// named fields (the struct's own private state).
						exported := len(field.Names) == 0
						for _, n := range field.Names {
							if n.IsExported() {
								exported = true
							}
						}
						if !exported {
							continue
						}
						if snippet := render(fset, field.Type); strings.Contains(strings.ToLower(snippet), "yamux") {
							violations = append(violations, fmt.Sprintf("%s: exported struct %q field type contains yamux: %s", path, ts.Name.Name, snippet))
						}
					}
				}
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk %s: %v", linkDir, err)
	}
	if len(violations) > 0 {
		t.Fatalf("yamux type(s) leaked into an exported signature (期11 spec §5.2 red line):\n%s", strings.Join(violations, "\n"))
	}
}

// render prints an AST node's source form for the textual yamux-substring
// check above.
func render(fset *token.FileSet, n ast.Node) string {
	var buf bytes.Buffer
	_ = printer.Fprint(&buf, fset, n)
	return buf.String()
}
