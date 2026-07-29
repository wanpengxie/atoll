package home

import (
	"context"

	"github.com/wanpengxie/atoll/platform/channelspec"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
)

// IntroductionResolver is the realm-current-facts read port shared by exact
// introduction and level reconciliation. Implementors must not call back into
// the same channel or mutate realm state while resolving; callers read before
// the channel serial section and fail closed on any uncertainty.
type IntroductionResolver interface {
	ResolveDeclaration(context.Context, channel.ID, string) (channelspec.DeclarationFacts, error)
	DaemonFacts(context.Context, string) (channelspec.DaemonFacts, error)
	// ClassKind separates the two ways a lookup can end: found=false is the
	// DEFINITIVE "no such class" answer (a decisive unknown_class terminal for
	// the word that asked), while a non-nil error is an infrastructure fault —
	// the resolver could not answer at all — which callers must map to a
	// retryable refusal, never to a permanent terminal.
	ClassKind(context.Context, string) (actor.Kind, bool, error)
}
