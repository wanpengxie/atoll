package behavior

import (
	"encoding/json"
	"testing"

	"github.com/wanpengxie/ActOS/kernel/actor"
	"github.com/wanpengxie/ActOS/kernel/message"
)

func TestDeclarationCatalogFromDeclarationCopiesConventionFields(t *testing.T) {
	decl := Declaration{
		Name:         "xhs",
		ActorID:      actor.ActorID("tool:xhs"),
		Types:        []string{"example.publish"},
		Binding:      actor.BindingEmbedded,
		MaxPendingMs: 300_000,
		Description:  "XHS automation",
		SkillDoc:     "Use this actor to publish notes.",
		TypeDeclarations: map[string]TypeDeclaration{
			"example.publish": {
				AllowedKinds:   []message.Kind{message.KindRequest, message.KindResponse},
				Description:    "Publish a note",
				PayloadExample: json.RawMessage(`{"title":"hello"}`),
				PayloadFields: []FieldDoc{{
					Name:     "title",
					Required: true,
					Example:  "hello",
				}},
				ErrorCodes: []ErrorDoc{{
					Code:     "publish_timeout",
					Recovery: "Retry later",
				}},
				Notes: "Requires browser login.",
			},
		},
	}

	catalog := DeclarationCatalogFromDeclaration(decl)
	if catalog.Description != decl.Description || catalog.SkillDoc != decl.SkillDoc {
		t.Fatalf("actor convention fields not copied: %+v", catalog)
	}
	doc := catalog.Types["example.publish"]
	if doc.Description != "Publish a note" || string(doc.PayloadExample) != `{"title":"hello"}` {
		t.Fatalf("type convention fields not copied: %+v", doc)
	}
	if len(doc.PayloadFields) != 1 || doc.PayloadFields[0].Name != "title" {
		t.Fatalf("payload fields=%+v", doc.PayloadFields)
	}
	if len(doc.ErrorCodes) != 1 || doc.ErrorCodes[0].Code != "publish_timeout" {
		t.Fatalf("error codes=%+v", doc.ErrorCodes)
	}
}
