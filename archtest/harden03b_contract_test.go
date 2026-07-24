package archtest

import (
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

func TestHarden03BPhaseBDependencyWalls(t *testing.T) {
	if _, err := os.Stat("../runtime/actorspec"); !os.IsNotExist(err) {
		t.Fatalf("runtime/actorspec must not exist, stat error=%v", err)
	}

	forbiddenImports := []string{
		"/platform/compute",
		"/platform/internal/link",
		"/runtime/actorctl",
		"/runtime/ipc",
		"/runtime/storespec",
	}
	err := filepath.WalkDir("../runtime/actorhost", func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		file, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.ImportsOnly)
		if err != nil {
			return err
		}
		for _, spec := range file.Imports {
			importPath, err := strconv.Unquote(spec.Path.Value)
			if err != nil {
				return err
			}
			for _, forbidden := range forbiddenImports {
				if strings.Contains(importPath, forbidden) {
					t.Errorf("%s imports forbidden owner %s", filepath.ToSlash(path), importPath)
				}
			}
		}
		source, err := readFile(path)
		if err != nil {
			return err
		}
		for _, forbidden := range []string{
			"actorrt.Runtime",
			"RebindableArms",
			"retryLifecycle",
			"SpawnIfAbsent",
		} {
			if strings.Contains(string(source), forbidden) {
				t.Errorf("%s references pre-cutover owner %q", filepath.ToSlash(path), forbidden)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestHarden03BPhaseCLifecycleCutoverIsAtomic(t *testing.T) {
	caps, err := readFile("../lib/actorcaps/caps.go")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(caps), "Lifecycle LifecycleHandle") {
		t.Fatal("Phase C must publish the terminal actorcaps LifecycleHandle")
	}
	if strings.Contains(string(caps), "actorrt.LifecycleHandle") {
		t.Fatal("Phase C left the pre-cutover actorrt lifecycle type in production Caps")
	}

	physical, err := readFile("../platform/internal/link/physical.go")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(physical), "Lifecycle actorcaps.LifecycleHandle") {
		t.Fatal("Phase C link wire must expose the terminal lifecycle facade")
	}
	if strings.Contains(string(physical), "actorrt.LifecycleHandle") {
		t.Fatal("Phase C link wire adapted the old lifecycle contract")
	}
}
