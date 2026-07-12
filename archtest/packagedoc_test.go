package archtest

import (
	"fmt"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

// docConventionRoots are the four layers owner unified this round: the package
// doc-comment of every production package here lives in doc.go and ONLY in
// doc.go. Downstream (app/, cmd/, drivers/) is deliberately NOT in scope — this
// tripwire guards the substrate/library layers, not the domain.
var docConventionRoots = []string{"protocol", "runtime", "lib", "platform"}

// TestPackageDocConvention enforces the doc.go-only package-comment convention:
// every production package under the four unified layers carries its `// Package`
// doc comment in exactly one file, and that file is named doc.go. A second
// // Package file, a stray // Package comment on a non-doc.go file, or a package
// with no doc.go home at all is the drift this tripwire catches.
//
// Scope note (same spirit as the other guards here): this is a TRIPWIRE for
// accidental drift, not a defense against adversarial evasion. It reads the AST
// package doc (the comment group immediately preceding the `package` clause), so
// a misattached free-floating comment that the parser does not bind as package
// doc would slip through — but the realistic accident (a // Package comment
// landing on identity.go instead of doc.go, or two doc.go-style files) is
// exactly what trips here.
func TestPackageDocConvention(t *testing.T) {
	fset := token.NewFileSet()

	// pkgDocFiles[pkgDir] = sorted list of production .go files in that dir
	// whose AST carries a package doc comment.
	pkgDocFiles := map[string][]string{}
	// pkgDirs is the set of dirs that contain at least one production .go file
	// (so we can flag a package that has NO package-doc home at all).
	pkgDirs := map[string]bool{}

	for _, root := range docConventionRoots {
		err := filepath.WalkDir("../"+root, func(path string, d fs.DirEntry, err error) error {
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
			dir := filepath.ToSlash(filepath.Dir(path))
			pkgDirs[dir] = true

			file, perr := parser.ParseFile(fset, path, nil, parser.ParseComments|parser.PackageClauseOnly)
			if perr != nil {
				return fmt.Errorf("parse %s: %w", path, perr)
			}
			if file.Doc != nil {
				pkgDocFiles[dir] = append(pkgDocFiles[dir], filepath.Base(path))
			}
			return nil
		})
		if err != nil {
			t.Fatalf("walk %s: %v", root, err)
		}
	}

	var violations []string
	dirs := make([]string, 0, len(pkgDirs))
	for dir := range pkgDirs {
		dirs = append(dirs, dir)
	}
	sort.Strings(dirs)

	for _, dir := range dirs {
		docFiles := pkgDocFiles[dir]
		sort.Strings(docFiles)
		switch {
		case len(docFiles) == 0:
			violations = append(violations,
				fmt.Sprintf("%s: package has no // Package doc comment — its package comment must live in doc.go", dir))
		case len(docFiles) > 1:
			violations = append(violations,
				fmt.Sprintf("%s: %d files carry a // Package doc comment (%s) — exactly one allowed, and it must be doc.go", dir, len(docFiles), strings.Join(docFiles, ", ")))
		case docFiles[0] != "doc.go":
			violations = append(violations,
				fmt.Sprintf("%s: // Package doc comment lives in %s — it must live in doc.go (only a bare `package X` clause elsewhere)", dir, docFiles[0]))
		}
	}

	if len(violations) > 0 {
		t.Fatalf("package doc-comment convention (doc.go-only across %s):\n  %s",
			strings.Join(docConventionRoots, ", "), strings.Join(violations, "\n  "))
	}
}
