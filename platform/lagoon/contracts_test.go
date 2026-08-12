package lagoon

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
)

func TestRegistrarWordAndAdapterSurfacesAreClosed(t *testing.T) {
	seen := map[Word]bool{}
	for _, word := range WriteWords {
		if seen[word] || word == "" {
			t.Fatalf("duplicate/empty write word %q", word)
		}
		seen[word] = true
	}
	if len(WriteWords) != 15 {
		t.Fatalf("write words=%d", len(WriteWords))
	}
	for _, word := range ReadWords {
		if seen[word] || word == "" {
			t.Fatalf("duplicate/empty read word %q", word)
		}
		seen[word] = true
	}
	if len(ReadWords) != 6 {
		t.Fatalf("read words=%d", len(ReadWords))
	}
	if got := reflect.TypeOf((*SpaceOps)(nil)).Elem().NumMethod(); got != 14 {
		t.Fatalf("SpaceOps methods=%d", got)
	}
	if got := reflect.TypeOf((*SpaceQueries)(nil)).Elem().NumMethod(); got != 6 {
		t.Fatalf("SpaceQueries methods=%d", got)
	}
}

func TestCredentialReplyCannotExposeHash(t *testing.T) {
	raw, err := json.Marshal(CredentialReply{PrincipalID: "alice", Kind: "password", Status: CredentialActive})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(raw), "hash") || strings.Contains(string(raw), "secret") {
		t.Fatalf("credential reply leaks a secret-shaped field: %s", raw)
	}
}

func TestReplyDecodeValueRejectsMissingNullAndMalformed(t *testing.T) {
	for name, reply := range map[string]Reply{
		"missing":   {},
		"null":      {Value: json.RawMessage(`null`)},
		"malformed": {Value: json.RawMessage(`{"id":`)},
	} {
		t.Run(name, func(t *testing.T) {
			var out PrincipalRow
			if err := reply.DecodeValue(&out); err == nil {
				t.Fatal("invalid value decoded successfully")
			}
		})
	}
	var out PrincipalRow
	if err := (Reply{Value: json.RawMessage(`{"id":"alice"}`)}).DecodeValue(&out); err != nil || out.ID != "alice" {
		t.Fatalf("valid value=(%+v,%v)", out, err)
	}
}

func TestRegistryExportedSurfaceContainsNoMutationVerb(t *testing.T) {
	typ := reflect.TypeOf((*Registry)(nil))
	for i := 0; i < typ.NumMethod(); i++ {
		name := typ.Method(i).Name
		for _, prefix := range []string{"Create", "Insert", "Update", "Delete", "Set", "Attach", "Detach", "Retire", "Mint", "Claim"} {
			if strings.HasPrefix(name, prefix) {
				t.Fatalf("Registry exports mutation method %s", name)
			}
		}
	}
}
