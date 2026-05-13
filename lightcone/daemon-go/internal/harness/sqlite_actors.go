package harness

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/coagent-ai/daemon-go/internal/registry"
	pkgharness "github.com/coagent-ai/daemon-go/pkg/harness"
	"github.com/coagent-ai/daemon-go/pkg/v4types"
)

// SQLiteActors adapts a channel-local *sql.DB into a
// pkg/harness.ActorLookup using internal/registry's Get helper.
type SQLiteActors struct {
	db *sql.DB
}

// NewSQLiteActors wraps db. The caller owns db's lifecycle.
func NewSQLiteActors(db *sql.DB) *SQLiteActors {
	return &SQLiteActors{db: db}
}

// Get returns the actor_registry row for actorID, regardless of
// deregistration state (so the harness can produce the right reason
// when actor is deregistered vs missing). Returns (nil, nil) when no
// row exists — the harness treats both "missing" and "deregistered"
// the same way (sender_deregistered).
func (a *SQLiteActors) Get(ctx context.Context, actorID string) (*pkgharness.ActorMeta, error) {
	meta, err := registry.Get(ctx, a.db, actorID)
	if err != nil {
		if errors.Is(err, registry.ErrActorNotFound) {
			return nil, nil
		}
		return nil, fmt.Errorf("sqlite_actors: get %q: %w", actorID, err)
	}
	if meta == nil {
		return nil, nil
	}
	return &pkgharness.ActorMeta{
		ActorID:        meta.ActorID,
		Kind:           v4types.SenderKind(meta.Kind),
		Binding:        string(meta.Binding),
		DeregisteredAt: meta.DeregisteredAt,
	}, nil
}
