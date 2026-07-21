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
	DaemonFacts(context.Context, string) (channel.DaemonFacts, error)
	// ClassKind separates the two ways a lookup can end: found=false is the
	// DEFINITIVE "no such class" answer (a decisive unknown_class terminal for
	// the word that asked), while a non-nil error is an infrastructure fault —
	// the resolver could not answer at all — which callers must map to a
	// retryable refusal, never to a permanent terminal.
	ClassKind(context.Context, string) (actor.Kind, bool, error)
}
