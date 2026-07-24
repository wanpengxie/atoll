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

// TestIPCWireLeafPurity mechanically enforces that runtime/ipc is a
// PROTOCOL-ONLY wire leaf: it may import only stdlib and protocol/* — never a
// sibling runtime package (harness / storespec / actorrt / store) and never the
// runtime assembly root.
//
// Why this is load-bearing: ipc is the port wire protocol (length-prefixed JSON
// frames). actorrt/port imports it for the host-side wire codec, so ANYTHING ipc
// imports rides into actorrt's compile closure. The relay-only remote PROXY PEN
// (RemoteWriter) used to live here and dragged runtime/harness in — which forced
// actorrt to transitively depend on harness, contradicting the model that
// actorrt and harness are SIBLING axes above protocol/storespec. A remote pen is
// a platform ASSEMBLY concern — the composition of the harness.Pen contract and
// the wire protocol — so it now lives in platform/internal/link beside its
// host-side counterpart (emitSink). This wall pins ipc back to a pure wire leaf
// so the regression cannot recur: a PR that makes ipc import harness (or any
// runtime sibling) turns this test red, full stop. A doc comment is zero
// enforcement when review is also an agent.
func TestIPCWireLeafPurity(t *testing.T) {
	const protoPkg = platformModulePrefix + "protocol" // ".../atoll/protocol"
	protoPrefix := protoPkg + "/"                      // ".../atoll/protocol/"
	const ipcPkg = platformModulePrefix + "runtime/ipc"

	fset := token.NewFileSet()
	var violations []string

	err := filepath.WalkDir("../runtime/ipc", func(path string, d fs.DirEntry, err error) error {
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
			// protocol/* — the only atoll edge a wire leaf may take (it speaks
			// pure envelope / actor types on the wire).
			if p == protoPkg || strings.HasPrefix(p, protoPrefix) {
				continue
			}
			// ipc itself (intra-package) — always fine.
			if p == ipcPkg {
				continue
			}
			// Any other atoll package = a sibling/up dependency a wire leaf must
			// not carry (harness is the one this wall exists to forbid).
			if strings.HasPrefix(p, platformModulePrefix) {
				violations = append(violations, fmt.Sprintf(
					"%s imports %q — runtime/ipc is a protocol-only wire leaf; it must not import a sibling runtime package (a remote pen needing harness is a platform assembly concern, it lives in platform/internal/link)", rel, p))
				continue
			}
			// External (non-stdlib) module: a dot in the first path segment.
			first := p
			if i := strings.IndexByte(p, '/'); i >= 0 {
				first = p[:i]
			}
			if strings.Contains(first, ".") {
				violations = append(violations, fmt.Sprintf(
					"%s imports external module %q — runtime/ipc is a pure wire leaf (stdlib + protocol/ only)", rel, p))
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk runtime/ipc: %v", err)
	}
	if len(violations) > 0 {
		t.Fatalf("ipc wire-leaf purity (runtime/ipc imports stdlib + protocol/ only):\n  %s",
			strings.Join(violations, "\n  "))
	}
}
