// Package relation maintains non-authoritative reverse indexes fed by channel
// membrane commit events.
package relation

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/wanpengxie/atoll/platform/channelspec"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
)

type Store struct {
	db  *sql.DB
	now func() int64
}

func New(db *sql.DB) *Store {
	return &Store{db: db, now: func() int64 { return time.Now().UnixMilli() }}
}

type Instance struct {
	ChannelID channel.ID
	ActorID   actor.ActorID
}

func (s *Store) Apply(ctx context.Context, chID channel.ID, deltas []channelspec.RelationDelta) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	now := s.now()
	for _, delta := range deltas {
		if delta.ChannelID != "" && delta.ChannelID != chID {
			return fmt.Errorf("relation: delta channel %q does not match batch %q", delta.ChannelID, chID)
		}
		if delta.Reset {
			if err := deleteChannel(ctx, tx, chID); err != nil {
				return err
			}
			continue
		}
		switch delta.Kind {
		case channelspec.RelationJoined:
			if delta.Principal == "" || delta.ActorID == "" {
				return fmt.Errorf("relation: incomplete joined delta")
			}
			_, err = tx.ExecContext(ctx, `INSERT INTO principal_channels(principal,channel_id,actor_id,updated_at)
				VALUES (?,?,?,?) ON CONFLICT(principal,channel_id) DO UPDATE
				SET actor_id=excluded.actor_id,updated_at=excluded.updated_at`,
				delta.Principal, string(chID), string(delta.ActorID), now)
		case channelspec.RelationLeft:
			if delta.Principal == "" || delta.ActorID == "" {
				return fmt.Errorf("relation: incomplete left delta")
			}
			_, err = tx.ExecContext(ctx, `DELETE FROM principal_channels
				WHERE principal=? AND channel_id=? AND actor_id=?`,
				delta.Principal, string(chID), string(delta.ActorID))
		case channelspec.RelationIntroduced:
			if delta.DeclID == "" || delta.ActorID == "" {
				return fmt.Errorf("relation: incomplete introduced delta")
			}
			_, err = tx.ExecContext(ctx, `INSERT INTO channel_decl_instances(channel_id,decl_id,actor_id,updated_at)
				VALUES (?,?,?,?) ON CONFLICT(channel_id,actor_id) DO UPDATE
				SET decl_id=excluded.decl_id,updated_at=excluded.updated_at`,
				string(chID), delta.DeclID, string(delta.ActorID), now)
		case channelspec.RelationInstanceRemoved:
			if delta.ActorID == "" {
				return fmt.Errorf("relation: incomplete instance-removed delta")
			}
			_, err = tx.ExecContext(ctx, `DELETE FROM channel_decl_instances
				WHERE channel_id=? AND actor_id=?`, string(chID), string(delta.ActorID))
		case channelspec.RelationBound:
			if delta.DaemonID == "" {
				return fmt.Errorf("relation: incomplete bound delta")
			}
			_, err = tx.ExecContext(ctx, `INSERT INTO daemon_channels(channel_id,daemon_id,updated_at)
				VALUES (?,?,?) ON CONFLICT(channel_id,daemon_id) DO UPDATE
				SET updated_at=excluded.updated_at`, string(chID), delta.DaemonID, now)
		case channelspec.RelationUnbound:
			if delta.DaemonID == "" {
				return fmt.Errorf("relation: incomplete unbound delta")
			}
			_, err = tx.ExecContext(ctx, `DELETE FROM daemon_channels
				WHERE channel_id=? AND daemon_id=?`, string(chID), delta.DaemonID)
		case channelspec.RelationGone:
			err = deleteChannel(ctx, tx, chID)
		default:
			return fmt.Errorf("relation: unknown delta kind %q", delta.Kind)
		}
		if err != nil {
			return err
		}
	}
	return tx.Commit()
}

func deleteChannel(ctx context.Context, tx *sql.Tx, chID channel.ID) error {
	for _, table := range []string{"principal_channels", "channel_decl_instances", "daemon_channels"} {
		if _, err := tx.ExecContext(ctx, `DELETE FROM `+table+` WHERE channel_id=?`, string(chID)); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) InstancesOf(ctx context.Context, declID string) ([]Instance, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT channel_id,actor_id FROM channel_decl_instances
		WHERE decl_id=? ORDER BY channel_id,actor_id`, declID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Instance
	for rows.Next() {
		var item Instance
		if err := rows.Scan(&item.ChannelID, &item.ActorID); err != nil {
			return nil, err
		}
		out = append(out, item)
	}
	return out, rows.Err()
}

func (s *Store) BindingsOf(ctx context.Context, daemonID string) ([]channel.ID, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT channel_id FROM daemon_channels
		WHERE daemon_id=? ORDER BY channel_id`, daemonID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []channel.ID
	for rows.Next() {
		var id channel.ID
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		out = append(out, id)
	}
	return out, rows.Err()
}

func (s *Store) ReconcilePrincipal(ctx context.Context, chID channel.ID, principal string, actorID actor.ActorID, found bool) error {
	if found {
		return s.Apply(ctx, chID, []channelspec.RelationDelta{{
			Kind: channelspec.RelationJoined, ChannelID: chID,
			Principal: principal, ActorID: actorID,
		}})
	}
	_, err := s.db.ExecContext(ctx, `DELETE FROM principal_channels
		WHERE principal=? AND channel_id=? AND actor_id=?`,
		principal, string(chID), string(actorID))
	return err
}

func (s *Store) ReconcileBinding(ctx context.Context, chID channel.ID, daemonID string, bound bool) error {
	kind := channelspec.RelationUnbound
	if bound {
		kind = channelspec.RelationBound
	}
	return s.Apply(ctx, chID, []channelspec.RelationDelta{{
		Kind: kind, ChannelID: chID, DaemonID: daemonID,
	}})
}
