package archtest

import (
	"fmt"
	"strings"
	"testing"
)

func TestAgentDriverDependencyWalls(t *testing.T) {
	var bad []string
	for _, f := range productionFiles(t) {
		for _, p := range f.imports {
			rel, ok := atollPath(p)
			if !ok {
				continue
			}
			switch {
			case hasPathPrefix(f.dir, "drivers/agents/provider"):
				if hasPathPrefix(rel, "drivers/agents/base") || hasPathPrefix(rel, "lib/actorbase") || hasPathPrefix(rel, "runtime/actorhost") || hasPathPrefix(rel, "runtime/actorrt") {
					bad = append(bad, fmt.Sprintf("%s imports %s", f.path, p))
				}
			case hasPathPrefix(f.dir, "drivers/agents/base"):
				if hasPathPrefix(rel, "drivers/agents/runtime") || hasPathPrefix(rel, "drivers/agents/driverproto") || hasPathPrefix(rel, "drivers/agents/provider") {
					bad = append(bad, fmt.Sprintf("%s imports %s", f.path, p))
				}
			case hasPathPrefix(f.dir, "drivers/agents/runtime") || hasPathPrefix(f.dir, "drivers/agents/driverproto"):
				if hasPathPrefix(rel, "runtime/actorhost") || hasPathPrefix(rel, "runtime/actorrt") || hasPathPrefix(rel, "lib/actorbase") {
					bad = append(bad, fmt.Sprintf("%s imports %s", f.path, p))
				}
			}
		}
		if hasPathPrefix(f.dir, "drivers/agents/provider") && (strings.HasSuffix(f.path, "/engine.go") || strings.Contains(f.path, "legacy") || strings.Contains(f.path, "compat")) {
			bad = append(bad, f.path+": forbidden provider lifecycle/legacy file")
		}
	}
	if len(bad) > 0 {
		t.Fatalf("agent driver dependency wall violations:\n  %s", strings.Join(bad, "\n  "))
	}
}
