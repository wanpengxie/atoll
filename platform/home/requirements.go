package home

import (
	"context"
	"errors"

	"github.com/wanpengxie/atoll/platform/channelspec"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
)

// IntroductionResolver is the space-current-facts read port shared by exact
// introduction and level reconciliation. Implementors must not call back into
// the same channel or mutate space state while resolving; callers read before
// the channel serial section and fail closed on any uncertainty.
type IntroductionResolver interface {
	ResolveDeclaration(context.Context, channel.ID, string) (channelspec.DeclarationFacts, error)
	// ClassKind separates the two ways a lookup can end: found=false is the
	// DEFINITIVE "no such class" answer (a decisive unknown_class terminal for
	// the word that asked), while a non-nil error is an infrastructure fault —
	// the resolver could not answer at all — which callers must map to a
	// retryable refusal, never to a permanent terminal.
	ClassKind(context.Context, string) (actor.Kind, bool, error)
	ClassPlacement(context.Context, string) (channelspec.PlacementKind, bool, error)
	AdmitIntroduction(context.Context, channel.ID, channelspec.DeclarationFacts) error
}

// DeclarationCatalogResolver is the optional bulk read used by directory and
// observation projections. Production implements it with one registry scan;
// focused test resolvers may implement only IntroductionResolver and use the
// local per-id fallback.
type DeclarationCatalogResolver interface {
	ResolveDeclarationCatalog(context.Context, channel.ID, []string) (map[string]channelspec.DeclarationFacts, error)
}

func resolveDeclarationCatalog(
	ctx context.Context,
	resolver IntroductionResolver,
	channelID channel.ID,
	declIDs []string,
) (map[string]channelspec.DeclarationFacts, error) {
	if bulk, ok := resolver.(DeclarationCatalogResolver); ok {
		return bulk.ResolveDeclarationCatalog(ctx, channelID, declIDs)
	}
	out := make(map[string]channelspec.DeclarationFacts, len(declIDs))
	for _, declID := range declIDs {
		facts, err := resolver.ResolveDeclaration(ctx, channelID, declID)
		if errors.Is(err, channelspec.ErrDeclarationNotFound) {
			continue
		}
		if err != nil {
			return nil, err
		}
		out[declID] = facts
	}
	return out, nil
}
