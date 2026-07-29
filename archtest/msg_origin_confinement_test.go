package archtest

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"strings"
	"testing"
)

// logOriginConstructors is the EXACT set of production functions allowed to
// build a log-origin actorbase.Msg — file path → function names.
//
// A log-origin Msg is a terminal-only write handle whose authority is the LOG,
// not the serve ledger: it is how an OFF-PROCESS subject answers, since a
// person holds no mailbox and their frame carries only a bare request id. That
// is the whole justification, and it is exhausted by the two frame steps
// below. Everything inside the process HAS a mailbox handle, so anyone else
// reaching for this shape is either (a) routing around a closed serve-ledger
// gate, or (b) about to thread a Ctx() that promises no request scope into
// ordinary work — which would silently outrun the request's deadline and
// cancel without ever erroring.
//
// actorbase.NewMsg is exported and its origin parameter is a plain argument,
// so nothing in the type system says this. The doc says it; this test is what
// makes the doc load-bearing.
var logOriginConstructors = map[string][]string{
	"../platform/internal/humancell/humancell.go": {"interpretResolve", "interpretCancel"},
}

// TestLogOriginMsgIsConfinedToTheFrameInterpreter walks every PRODUCTION .go
// file and flags any actorbase.NewMsg call that does not pass the literal
// OriginMailbox constant, unless it sits in one of the two allowed frame
// steps.
//
// The check is deliberately phrased as "not literally OriginMailbox" rather
// than "literally OriginLog": a computed origin (a variable, a function call, a
// field) is an evasion of exactly this lock, and it must be a violation rather
// than an unrecognised expression this test quietly walks past. There is no
// legitimate reason for the argument to be anything but one of the two
// constants, spelled out at the call site.
//
// _test.go files are exempt, the same stance the rest of this package takes: a
// test fixture standing in for the frame interpreter (or asserting the gate's
// own behaviour) is not a production consumer, and lib/actorbase's own tests
// must be able to construct both origins to test them at all.
//
// Scope note: OriginMailbox is NOT confined here. It is minted only by the
// engine's own delivery path today, but the risk this test exists to answer is
// asymmetric — a mailbox Msg is gated on the serve ledger and carries the
// request's real ctx, so forging one gets you a STRICTER handle, not a looser
// one. §9's TestRecvOnlyEverProducesMailboxOrigin covers the other direction
// (the delivery path never mints a log-origin handle) behaviourally.
func TestLogOriginMsgIsConfinedToTheFrameInterpreter(t *testing.T) {
	allowed := map[string]map[string]bool{}
	for file, fns := range logOriginConstructors {
		set := map[string]bool{}
		for _, fn := range fns {
			set[fn] = true
		}
		allowed[file] = set
	}
	seen := map[string]map[string]bool{}

	fset := token.NewFileSet()
	var v []string

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

		file, perr := parser.ParseFile(fset, path, nil, parser.SkipObjectResolution)
		if perr != nil {
			return fmt.Errorf("parse %s: %w", path, perr)
		}

		ast.Inspect(file, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok || !isNewMsgCall(call.Fun) {
				return true
			}
			origin := ""
			if len(call.Args) > 0 {
				origin = originArgName(call.Args[0])
			}
			if origin == "OriginMailbox" {
				return true
			}
			fn := enclosingFuncName(file, call.Pos())
			if allowed[slash][fn] {
				if seen[slash] == nil {
					seen[slash] = map[string]bool{}
				}
				seen[slash][fn] = true
				return true
			}
			shown := describeOrigin(origin)
			v = append(v, fmt.Sprintf(
				"%s: %s builds a Msg with origin %s — a log-origin Msg is the OFF-PROCESS subject's terminal-only write handle (no mailbox, authority is the log, Ctx() promises no request scope). In-process code holds a real delivery handle; use it. If a second frame interpreter genuinely appears, add it to logOriginConstructors and say why here",
				fset.Position(call.Pos()), funcLabel(fn), shown))
			return true
		})
		return nil
	})
	if err != nil {
		t.Fatalf("walk: %v", err)
	}
	failViolations(t, "log-origin actorbase.Msg construction is confined to the frame interpreter", v)

	// The allowlist must stay EARNED. A stale entry (the function renamed, the
	// call moved) would silently widen the lock to a name nothing matches, and
	// the next real violation in that file would then only need to reuse the
	// dead name.
	for file, fns := range logOriginConstructors {
		for _, fn := range fns {
			if !seen[file][fn] {
				t.Errorf("logOriginConstructors allows %s in %s, but no log-origin NewMsg call is there — remove the stale entry (an allowlist nobody uses is a hole waiting for a name collision)", fn, file)
			}
		}
	}
}

// isNewMsgCall matches both spellings: actorbase.NewMsg from outside the
// package, and a bare NewMsg from inside lib/actorbase. The repo has exactly
// one NewMsg symbol (grounding grep), so a name match needs no type
// resolution; a same-named helper appearing elsewhere would produce a
// false positive here, which is the safe direction.
func isNewMsgCall(fun ast.Expr) bool {
	switch f := fun.(type) {
	case *ast.Ident:
		return f.Name == "NewMsg"
	case *ast.SelectorExpr:
		return f.Sel.Name == "NewMsg"
	}
	return false
}

// originArgName returns the constant name of an origin argument spelled as a
// literal (OriginMailbox / actorbase.OriginLog / …), or "" for anything
// computed — which the caller treats as a violation, not as unknown.
func originArgName(arg ast.Expr) string {
	switch a := arg.(type) {
	case *ast.Ident:
		return a.Name
	case *ast.SelectorExpr:
		return a.Sel.Name
	}
	return ""
}

// describeOrigin turns the argument's spelling into words for the failure
// message, keeping the distinction between "spelled OriginLog" and "not
// spelled out at all" visible — the second is a worse offence, since it
// defeats every source-level reading of who holds which handle.
func describeOrigin(origin string) string {
	switch origin {
	case "OriginLog", "OriginUnset":
		return origin
	case "":
		return "a computed expression"
	default:
		return fmt.Sprintf("%s — a name, not one of the two spelled-out constants", origin)
	}
}

// enclosingFuncName names the top-level func (or method) a position sits in,
// including calls nested inside a func literal — the enclosing DECLARATION is
// what an allowlist can meaningfully name. "" means package level (a var
// initialiser), which no allowlist entry can match.
func enclosingFuncName(file *ast.File, pos token.Pos) string {
	for _, decl := range file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok {
			continue
		}
		if pos >= fn.Pos() && pos <= fn.End() {
			return fn.Name.Name
		}
	}
	return ""
}

func funcLabel(fn string) string {
	if fn == "" {
		return "a package-level initialiser"
	}
	return fn
}
