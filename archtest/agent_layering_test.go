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

// Agent subsystem layering locks.
//
// The agent is its OWN top-level subsystem (agent/), NOT a tool driver under
// drivers/tools/. These tests pin the dependency direction so it can't rot back:
//
//   {drivers/*, agent/, app} → registry → platform (substrate, blind upward)
//
//   1. Engine SDKs (go-kimi / go-claude-agent-sdk) are quarantined to
//      agent/provider/* — agent core / app / substrate stay engine-agnostic.
//   2. Substrate (protocol/runtime/lib/platform) stays blind to agent AND to the
//      domain registry (registry → platform, never the reverse).
//   3. agent never depends up on app (cmd → app → agent, one-way).
//   4. app uses agent's public face only, never agent/internal/*.
//
// Same file-walk tripwire stance as the rest of archtest: imports-only parse,
// path-prefix classification, _test.go excluded (tests legitimately reach across
// for fixtures). skipDirs / platformModulePrefix are shared from contract_test.go.

const (
	agentPkg     = platformModulePrefix + "agent"
	registryPkg  = platformModulePrefix + "registry"
	appPkg       = platformModulePrefix + "app"
	engineKimi   = "github.com/wanpengxie/go-kimi"
	engineClaude = "github.com/wanpengxie/go-claude-agent-sdk"
)

// walkImports parses every non-test .go file under the repo root and calls fn
// with (repo-relative slash path, imported package path) for each import.
func walkImports(t *testing.T, fn func(slash, importPath string)) {
	t.Helper()
	fset := token.NewFileSet()
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
		f, perr := parser.ParseFile(fset, path, nil, parser.ImportsOnly)
		if perr != nil {
			return fmt.Errorf("parse %s: %w", path, perr)
		}
		slash := filepath.ToSlash(path)
		for _, imp := range f.Imports {
			p, uerr := strconv.Unquote(imp.Path.Value)
			if uerr != nil {
				continue
			}
			fn(slash, p)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk: %v", err)
	}
}

func failViolations(t *testing.T, rule string, v []string) {
	t.Helper()
	if len(v) > 0 {
		t.Fatalf("%s:\n  %s", rule, strings.Join(v, "\n  "))
	}
}

// TestEngineQuarantine — only agent/provider/* may import the LLM engine SDKs.
func TestEngineQuarantine(t *testing.T) {
	var v []string
	walkImports(t, func(slash, imp string) {
		if !strings.HasPrefix(imp, engineKimi) && !strings.HasPrefix(imp, engineClaude) {
			return
		}
		if strings.HasPrefix(slash, "../agent/provider/") {
			return // the legitimate quarantine
		}
		v = append(v, fmt.Sprintf(
			"%s imports %q — LLM engine SDKs are quarantined to agent/provider/*; agent core / app / substrate stay engine-agnostic (the brain-agnostic discipline, one scale down)",
			slash, imp))
	})
	failViolations(t, "engine quarantine (only agent/provider/* imports the engine SDKs)", v)
}

// TestSubstrateBlindToAgent — substrate must not import the agent subsystem or
// the domain registry. registry → platform is one-way; if platform imported
// registry it would cycle.
func TestSubstrateBlindToAgent(t *testing.T) {
	subs := []string{"../protocol/", "../runtime/", "../lib/", "../platform/"}
	var v []string
	walkImports(t, func(slash, imp string) {
		inSub := false
		for _, s := range subs {
			if strings.HasPrefix(slash, s) {
				inSub = true
				break
			}
		}
		if !inSub {
			return
		}
		if imp == agentPkg || strings.HasPrefix(imp, agentPkg+"/") || imp == registryPkg {
			v = append(v, fmt.Sprintf(
				"%s imports %q — substrate must stay blind to the agent subsystem and the domain registry (composition flows {drivers,agent,app} → registry → platform, never back)",
				slash, imp))
		}
	})
	failViolations(t, "substrate ⊥ agent / registry", v)
}

// TestAgentNotImportApp — agent must never depend up on app.
func TestAgentNotImportApp(t *testing.T) {
	var v []string
	walkImports(t, func(slash, imp string) {
		if !strings.HasPrefix(slash, "../agent/") {
			return
		}
		if imp == appPkg || strings.HasPrefix(imp, appPkg+"/") {
			v = append(v, fmt.Sprintf(
				"%s imports %q — agent must never depend up on app (cmd → app → agent is one-way)",
				slash, imp))
		}
	})
	failViolations(t, "agent ⊥ app", v)
}

// TestAppNotImportAgentInternal — app may use agent's public face only, never
// agent/internal/* (Go's internal/ visibility already enforces this; the archtest
// documents the rule and guards future agent/internal/ extractions).
func TestAppNotImportAgentInternal(t *testing.T) {
	internalPkg := agentPkg + "/internal"
	var v []string
	walkImports(t, func(slash, imp string) {
		if !strings.HasPrefix(slash, "../app/") {
			return
		}
		if imp == internalPkg || strings.HasPrefix(imp, internalPkg+"/") {
			v = append(v, fmt.Sprintf(
				"%s imports %q — app may use agent's public face only, never agent/internal/*",
				slash, imp))
		}
	})
	failViolations(t, "app ⊥ agent/internal", v)
}
