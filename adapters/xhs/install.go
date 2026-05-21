package xhs

import (
	"github.com/wanpengxie/ActOS/kernel/actor"
	"github.com/wanpengxie/ActOS/kernel/actorreg"
	"github.com/wanpengxie/ActOS/kernel/adapter"
	"github.com/wanpengxie/ActOS/kernel/message"
)

// declarationTypeDeclarations builds the kernel/adapter.TypeDeclaration
// map the scaffold attaches to its Declaration. R/R types get
// AllowedKinds={request, response}, the event-only type gets
// AllowedKinds={event}.
//
// Level A (proto-layer0 §1.4.1 / proto-layer1 §1.3): payload schema is
// NOT declared at the protocol layer; the type_registry does not store
// payload schemas and the harness does not validate them.
func declarationTypeDeclarations() map[string]adapter.TypeDeclaration {
	out := make(map[string]adapter.TypeDeclaration, len(AllTypes))
	for _, t := range RequestResponseTypes {
		out[t] = adapter.TypeDeclaration{
			AllowedKinds:       []message.Kind{message.KindRequest, message.KindResponse},
			TerminalConvention: string(adapter.TerminalPayloadStatus),
		}
	}
	out[TypeNoteArchived] = adapter.TypeDeclaration{
		AllowedKinds:       []message.Kind{message.KindEvent},
		TerminalConvention: string(adapter.TerminalPayloadStatus),
	}
	return out
}

// DefaultActorSeed returns the kernel/actorreg.Record the saga inserts so
// framework.Manager.Install can locate tool:xhs-adapter at boot. T3
// will need to flip the Binding to ViaServerTransit when the device
// adapter replaces this scaffold.
func DefaultActorSeed() actorreg.Record {
	return actorreg.Record{
		ID:          DefaultAdapterActorID,
		Kind:        actor.KindTool,
		Binding:     actor.BindingEmbedded,
		DisplayName: "xhs",
	}
}
