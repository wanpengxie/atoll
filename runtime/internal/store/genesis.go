package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/wanpengxie/atoll/runtime/storespec"
)

type genesisStore struct{ db *sql.DB }

func (s genesisStore) CreateGenesis(ctx context.Context, in storespec.ChannelGenesis) error {
	if in.ChannelID == "" || in.Type == "" || in.OwnerPrincipal == "" || in.CreatedAt <= 0 {
		return errors.New("store: invalid channel genesis")
	}
	_, err := s.db.ExecContext(ctx, `INSERT INTO channel_genesis
		(channel_id,type,owner_principal,parent_channel_id,initiator_principal,created_at)
		VALUES (?,?,?,?,?,?)`, in.ChannelID, in.Type, in.OwnerPrincipal,
		nullIfEmpty(in.ParentChannelID), nullIfEmpty(in.InitiatorPrincipal), in.CreatedAt)
	if err != nil {
		return fmt.Errorf("store: create channel genesis: %w", err)
	}
	return nil
}

func (s genesisStore) ReadGenesis(ctx context.Context) (storespec.ChannelGenesis, bool, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT channel_id,type,owner_principal,parent_channel_id,initiator_principal,created_at FROM channel_genesis`)
	if err != nil {
		return storespec.ChannelGenesis{}, false, fmt.Errorf("store: read channel genesis: %w", err)
	}
	defer rows.Close()
	var out storespec.ChannelGenesis
	count := 0
	for rows.Next() {
		count++
		var parent, initiator sql.NullString
		if err := rows.Scan(&out.ChannelID, &out.Type, &out.OwnerPrincipal, &parent, &initiator, &out.CreatedAt); err != nil {
			return storespec.ChannelGenesis{}, false, fmt.Errorf("store: read channel genesis: %w", err)
		}
		out.ParentChannelID = parent.String
		out.InitiatorPrincipal = initiator.String
	}
	if err := rows.Err(); err != nil {
		return storespec.ChannelGenesis{}, false, err
	}
	if count == 0 {
		return storespec.ChannelGenesis{}, false, nil
	}
	if count != 1 {
		return storespec.ChannelGenesis{}, false, errors.New("store: channel genesis must contain exactly one row")
	}
	return out, true, nil
}

func nullIfEmpty(value string) any {
	if value == "" {
		return nil
	}
	return value
}
