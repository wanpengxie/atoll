package kimibridge

import (
	"testing"

	"github.com/wanpengxie/ActOS/kernel/adapter"
	"github.com/wanpengxie/ActOS/kernel/message"
)

func TestSchemaOnlyDeclarationsCoverAllTypes(t *testing.T) {
	decls := DeclarationTypeDeclarations()
	if len(decls) != len(AllTypes) {
		t.Fatalf("declarations=%d want all types=%d", len(decls), len(AllTypes))
	}
	for _, typ := range RequestResponseTypes {
		decl, ok := decls[typ]
		if !ok {
			t.Fatalf("missing declaration for %s", typ)
		}
		if !sameKinds(decl.AllowedKinds, []message.Kind{message.KindRequest, message.KindResponse}) {
			t.Fatalf("%s allowed kinds=%v", typ, decl.AllowedKinds)
		}
		if decl.TerminalConvention != string(adapter.TerminalPayloadStatus) {
			t.Fatalf("%s terminal convention=%q", typ, decl.TerminalConvention)
		}
		if action, ok := ActionForType(typ); !ok || action == "" {
			t.Fatalf("%s action=%q ok=%v", typ, action, ok)
		}
	}
	for _, typ := range EventOnlyTypes {
		decl, ok := decls[typ]
		if !ok {
			t.Fatalf("missing declaration for %s", typ)
		}
		if !sameKinds(decl.AllowedKinds, []message.Kind{message.KindEvent}) {
			t.Fatalf("%s allowed kinds=%v", typ, decl.AllowedKinds)
		}
	}
}

func sameKinds(got, want []message.Kind) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range got {
		if got[i] != want[i] {
			return false
		}
	}
	return true
}
