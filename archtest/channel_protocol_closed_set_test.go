package archtest

import (
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"testing"
)

// TestChannelProtocolExportedTypesAreClosed keeps business-membrane and realm
// contracts out of protocol/channel.
func TestChannelProtocolExportedTypesAreClosed(t *testing.T) {
	want := map[string]bool{
		"ID": true, "Reader": true, "ReaderMode": true,
		"AdmitResult": true, "IntroduceResult": true, "RemoveResult": true,
		"Placement": true, "PlacementKind": true,
		"ResourceListQuery": true, "ResourceMeta": true, "ResourcePage": true,
		"ResourceRef": true, "ResourceFetch": true,
	}
	got := map[string]bool{}
	files, err := filepath.Glob(filepath.Join("..", "protocol", "channel", "*.go"))
	if err != nil {
		t.Fatal(err)
	}
	for _, path := range files {
		if filepath.Ext(path) != ".go" || filepath.Base(path) == "" {
			continue
		}
		f, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
		if err != nil {
			t.Fatalf("parse %s: %v", path, err)
		}
		for _, decl := range f.Decls {
			gen, ok := decl.(*ast.GenDecl)
			if !ok || gen.Tok != token.TYPE {
				continue
			}
			for _, raw := range gen.Specs {
				spec := raw.(*ast.TypeSpec)
				if spec.Name.IsExported() {
					got[spec.Name.Name] = true
				}
			}
		}
	}
	for name := range got {
		if !want[name] {
			t.Errorf("protocol/channel exports business type %s", name)
		}
	}
	for name := range want {
		if !got[name] {
			t.Errorf("protocol/channel lost allowed kernel type %s", name)
		}
	}
}
