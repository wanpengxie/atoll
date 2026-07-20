package home

import (
	"context"

	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
)

// IntroductionResolver is the exact-admission requirement port. Implementors
// must not call back into the same channel while resolving: OpEntry invokes it
// before the channel-store serial section and fails closed on any uncertainty.
type IntroductionResolver interface {
	ResolveDeclaration(context.Context, channel.ID, string) (channel.DeclarationFacts, error)
	ClassKind(context.Context, string) (actor.Kind, error)
}
