package archtest

import (
	"fmt"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

// forbiddenProtoStdlib are the stdlib seams protocol/ may NOT reach for: they
// imply state / IO / transport, which protocol (pure types + closed-set
// vocabularies + pure functions) categorically does not do. Prefix-matched, so
// database/sql/driver and net/http/httptest are caught too. The set mirrors
// protocol/doc.go's layering rule (proto-foundation): "no context, database/sql,
// net/http, or any SQL/driver/transport package".
var forbiddenProtoStdlib = []string{"context", "database/sql", "net/http"}

// TestProtocolPurityAndDirection mechanically enforces protocol/'s two load-
// bearing axioms — purity and dependency direction — that protocol/doc.go
// declares but, until this guard, only a manual `git grep` and review upheld:
//
//   - DIRECTION: protocol/ may import ONLY stdlib and other protocol/ packages.
//     It may NOT import any other atoll package (runtime/lib/platform/app) —
//     everything depends on protocol, never the reverse. A reversed arrow
//     (protocol importing runtime) inverts the whole layering and still
//     compiles; this is the single most corrosive base-layer defect, so it is
//     locked here.
//   - PURITY: even within the stdlib, protocol/ may not reach for the
//     state/IO/transport seams (context / database/sql / net/http) — protocol is
//     pure types, not an engine.
//
// Unlike the struct-literal tripwires in this package, an import path is a
// MANDATORY string literal in Go — the AST sees every import, and there is no
// computed-import escape hatch. So this check is a true structural boundary, not
// a drift tripwire: a PR that adds a forbidden import turns this test red, full
// stop. (The agent-PR consumer model needs exactly this: a comment + manual grep
// is zero enforcement when review is also an agent.)
func TestProtocolPurityAndDirection(t *testing.T) {
	const protoPkg = platformModulePrefix + "protocol" // ".../atoll/protocol"
	protoPrefix := protoPkg + "/"                      // ".../atoll/protocol/"

	fset := token.NewFileSet()
	var violations []string

	err := filepath.WalkDir("../protocol", func(path string, d fs.DirEntry, err error) error {
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

		file, perr := parser.ParseFile(fset, path, nil, parser.ImportsOnly)
		if perr != nil {
			return fmt.Errorf("parse %s: %w", path, perr)
		}
		rel := filepath.ToSlash(path)
		for _, imp := range file.Imports {
			p, uerr := strconv.Unquote(imp.Path.Value)
			if uerr != nil {
				violations = append(violations, fmt.Sprintf("%s: unparseable import %s", rel, imp.Path.Value))
				continue
			}

			// Intra-protocol import — always allowed (the only permitted atoll edge).
			if p == protoPkg || strings.HasPrefix(p, protoPrefix) {
				continue
			}
			// Any other atoll package = a reversed dependency arrow.
			if strings.HasPrefix(p, platformModulePrefix) {
				violations = append(violations, fmt.Sprintf(
					"%s imports %q — protocol must not import any non-protocol atoll package (dependency direction: all layers depend on protocol, never the reverse)", rel, p))
				continue
			}
			// External (non-stdlib) module: a dot in the first path segment.
			first := p
			if i := strings.IndexByte(p, '/'); i >= 0 {
				first = p[:i]
			}
			if strings.Contains(first, ".") {
				violations = append(violations, fmt.Sprintf(
					"%s imports external module %q — protocol is pure (stdlib + protocol/ only)", rel, p))
				continue
			}
			// Stdlib: forbid the state/IO/transport seams.
			for _, f := range forbiddenProtoStdlib {
				if p == f || strings.HasPrefix(p, f+"/") {
					violations = append(violations, fmt.Sprintf(
						"%s imports %q — protocol takes no state/IO/transport seam (no context / database/sql / net/http)", rel, p))
					break
				}
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk protocol: %v", err)
	}

	if len(violations) > 0 {
		t.Fatalf("protocol purity / dependency-direction (protocol/ imports stdlib-minus-{context,database/sql,net/http} or protocol/ only):\n  %s",
			strings.Join(violations, "\n  "))
	}
}
