package app

import (
	"context"
	"strings"

	"github.com/wanpengxie/atoll/protocol/actor"
)

// ownedDeclarationWhere is the atomic ownership fence used by every
// declaration mutation. Keep it in the SQL statement: a detached pre-check
// would reopen a check/use authorization race.
const ownedDeclarationWhere = "id=? AND owner=?"

func declarationVisibleTo(visibility, owner, principal string) bool {
	return visibility == "public" || owner == principal
}

func (a *App) declarationClassKind(ctx context.Context, class string) (actor.Kind, bool, error) {
	class = strings.TrimSpace(class)
	if class == spaceToolClass {
		return "", false, nil
	}
	return (compositionResolver{app: a}).ClassKind(ctx, class)
}

func (a *App) declarationClassTransition(ctx context.Context, current, next string) (bool, error) {
	oldKind, oldFound, err := a.declarationClassKind(ctx, current)
	if err != nil {
		return false, err
	}
	newKind, newFound, err := a.declarationClassKind(ctx, next)
	if err != nil {
		return false, err
	}
	return oldFound && newFound && oldKind == newKind, nil
}
