package adapter

import (
	"encoding/json"

	"github.com/wanpengxie/ActOS/kernel/actor"
)

// DeclarationConventionStateKey is the adapter_state key, within the
// framework adapter namespace, that stores actor-CLI convention metadata.
const DeclarationConventionStateKey = "actor_cli_declaration_v1"

// DeclarationCatalog is the durable product-layer projection of a
// Declaration used by actor-CLI discovery tools. It intentionally lives
// outside TypeRow/type_registry so Level A payload opacity stays intact.
type DeclarationCatalog struct {
	Name        string                       `json:"name"`
	ActorID     actor.ActorID                `json:"actor_id"`
	Description string                       `json:"description,omitempty"`
	SkillDoc    string                       `json:"skill_doc,omitempty"`
	Types       map[string]TypeConventionDoc `json:"types,omitempty"`
}

// TypeConventionDoc is the actor-CLI convention subset of TypeDeclaration.
type TypeConventionDoc struct {
	Description    string          `json:"description,omitempty"`
	PayloadExample json.RawMessage `json:"payload_example,omitempty"`
	PayloadFields  []FieldDoc      `json:"payload_fields,omitempty"`
	ErrorCodes     []ErrorDoc      `json:"error_codes,omitempty"`
	Notes          string          `json:"notes,omitempty"`
}

// DeclarationCatalogFromDeclaration projects optional convention fields
// out of a Declaration for persistence in adapter_state.
func DeclarationCatalogFromDeclaration(d Declaration) DeclarationCatalog {
	out := DeclarationCatalog{
		Name:        d.Name,
		ActorID:     d.ActorID,
		Description: d.Description,
		SkillDoc:    d.SkillDoc,
	}
	if len(d.Types) == 0 {
		return out
	}
	out.Types = make(map[string]TypeConventionDoc, len(d.Types))
	for _, typeName := range d.Types {
		td, ok := d.TypeDeclarations[typeName]
		if !ok {
			continue
		}
		out.Types[typeName] = TypeConventionDoc{
			Description:    td.Description,
			PayloadExample: cloneRawMessage(td.PayloadExample),
			PayloadFields:  append([]FieldDoc(nil), td.PayloadFields...),
			ErrorCodes:     append([]ErrorDoc(nil), td.ErrorCodes...),
			Notes:          td.Notes,
		}
	}
	if len(out.Types) == 0 {
		out.Types = nil
	}
	return out
}

func cloneRawMessage(raw json.RawMessage) json.RawMessage {
	if len(raw) == 0 {
		return nil
	}
	out := make(json.RawMessage, len(raw))
	copy(out, raw)
	return out
}
