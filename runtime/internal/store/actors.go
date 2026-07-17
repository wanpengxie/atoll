package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/protocol/message"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

// actorRegistry owns durable declared identity reads plus the named admission,
// declaration, cascade, routing, and restart-journal transactions. It exposes
// no generic membership mutation surface.
type actorRegistry struct {
	db        *sql.DB
	channelID channel.ID
	onCommit  func()
}

func newActorRegistry(db *sql.DB, channelID channel.ID, onCommit func()) *actorRegistry {
	return &actorRegistry{db: db, channelID: channelID, onCommit: onCommit}
}

func (r *actorRegistry) Lookup(ctx context.Context, id actor.ActorID) (storespec.Record, bool, error) {
	const q = `SELECT actor_id, actor_kind, principal, COALESCE(actor_binding,''),
	                 created_at, COALESCE(deregistered_at,0)
	            FROM actor_registry WHERE actor_id=?`
	var rec storespec.Record
	var kind, binding string
	err := r.db.QueryRowContext(ctx, q, string(id)).Scan(
		&rec.ID, &kind, &rec.Principal, &binding, &rec.CreatedAt, &rec.DeregisteredAt,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return storespec.Record{}, false, nil
	}
	if err != nil {
		return storespec.Record{}, false, fmt.Errorf("store: actor lookup %q: %w", id, err)
	}
	if err := parseRecordIdentity(&rec, kind, binding); err != nil {
		return storespec.Record{}, false, err
	}
	return rec, true, nil
}

func (r *actorRegistry) LookupActivePrincipal(ctx context.Context, kind actor.Kind, principal string) (storespec.Record, bool, error) {
	const q = `SELECT actor_id, actor_kind, principal, COALESCE(actor_binding,''), created_at
	 FROM actor_registry WHERE actor_kind=? AND principal=? AND deregistered_at IS NULL`
	var rec storespec.Record
	var rawKind, binding string
	err := r.db.QueryRowContext(ctx, q, string(kind), principal).Scan(
		&rec.ID, &rawKind, &rec.Principal, &binding, &rec.CreatedAt,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return storespec.Record{}, false, nil
	}
	if err != nil {
		return storespec.Record{}, false, fmt.Errorf("store: principal lookup: %w", err)
	}
	if err := parseRecordIdentity(&rec, rawKind, binding); err != nil {
		return storespec.Record{}, false, err
	}
	return rec, true, nil
}

func parseRecordIdentity(rec *storespec.Record, rawKind, rawBinding string) error {
	kind, ok := actor.ParseKind(rawKind)
	if !ok {
		return fmt.Errorf("store: actor %q invalid kind %q (out of closed set)", rec.ID, rawKind)
	}
	rec.Kind = kind
	if rawBinding != "" {
		binding, ok := actor.ParseBinding(rawBinding)
		if !ok {
			return fmt.Errorf("store: actor %q invalid binding %q (out of closed set)", rec.ID, rawBinding)
		}
		rec.Binding = binding
	}
	return nil
}

// Exists is the durable-history query and deliberately includes ended rows.
func (r *actorRegistry) Exists(ctx context.Context, id actor.ActorID) (bool, error) {
	var one int
	err := r.db.QueryRowContext(ctx, `SELECT 1 FROM actor_registry WHERE actor_id=? LIMIT 1`, string(id)).Scan(&one)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("store: actor exists %q: %w", id, err)
	}
	return true, nil
}

func (r *actorRegistry) ListActive(ctx context.Context) ([]storespec.Record, error) {
	rows, err := r.db.QueryContext(ctx, `SELECT actor_id, actor_kind, principal,
		COALESCE(actor_binding,''), created_at FROM actor_registry
		WHERE deregistered_at IS NULL ORDER BY actor_id`)
	if err != nil {
		return nil, fmt.Errorf("store: list active actors: %w", err)
	}
	defer rows.Close()
	var out []storespec.Record
	for rows.Next() {
		var rec storespec.Record
		var kind, binding string
		if err := rows.Scan(&rec.ID, &kind, &rec.Principal, &binding, &rec.CreatedAt); err != nil {
			return nil, fmt.Errorf("store: list active actors scan: %w", err)
		}
		if err := parseRecordIdentity(&rec, kind, binding); err != nil {
			return nil, err
		}
		out = append(out, rec)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: list active actors rows: %w", err)
	}
	return out, nil
}

func validateMemberIdentity(id actor.ActorID, kind actor.Kind, binding actor.Binding) error {
	if id == "" {
		return errors.New("store: actor id required")
	}
	if _, ok := actor.ParseKind(string(kind)); !ok {
		return fmt.Errorf("store: actor %q kind %q not in the actor.Kind closed set", id, kind)
	}
	if binding != "" {
		if _, ok := actor.ParseBinding(string(binding)); !ok {
			return fmt.Errorf("store: actor %q binding %q not in the actor.Binding closed set", id, binding)
		}
	}
	return nil
}

func nullableBinding(binding actor.Binding) any {
	if binding == "" {
		return nil
	}
	return string(binding)
}

func actorRegisteredEnvelope(channelID channel.ID, id actor.ActorID, kind actor.Kind, binding actor.Binding, at int64) *message.Envelope {
	payload, _ := json.Marshal(map[string]any{
		"actor_id": id, "actor_kind": kind, "actor_binding": binding, "registered_at": at,
	})
	return &message.Envelope{
		ID: message.ID(uuid.NewString()), TS: at, TSReceived: at, ChannelID: channelID,
		Sender: message.Sender{Kind: actor.KindSystem, ID: actor.SystemActorID},
		Kind:   message.KindEvent, Type: actor.ReservedSystemActorRegistered,
		Payload: payload, Visibility: message.VisibilitySystem,
		Audience: message.Audience{actor.SystemActorID},
	}
}

var (
	_ storespec.PrincipalRegistry = (*actorRegistry)(nil)
	_ storespec.DurableHistory    = (*actorRegistry)(nil)
)
