package catalog

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
)

// Channel is the public channel record.
type Channel struct {
	ID          string
	WorkspaceID string
	Name        string
	Type        string
	CreatedAt   int64
}

// CreateChannelInput carries the create call. Members is the
// bootstrap set written into channel_members atomically with the
// channel row + propagated to daemon via T1.9 control.create_channel
// initial_members.
type CreateChannelInput struct {
	WorkspaceID string
	Name        string
	Type        string
	CreatorID   string
	// Members lists the bootstrap members. The creator is auto-added
	// as owner if not in this list.
	Members []NewMember
}

// NewMember is one entry of CreateChannelInput.Members.
type NewMember struct {
	UserID           string
	ActorIDInChannel string // optional — auto-assigned if empty
	Role             string // "owner" | "member"; defaults to "member"
}

// CreateChannel inserts a channel row + every member row in a single
// transaction. Returns the channel + the canonical member rows (so
// the caller can hand them to placements.CreateChannel as
// initial_members).
func (s *Service) CreateChannel(ctx context.Context, in CreateChannelInput) (Channel, []ChannelMember, error) {
	name := strings.TrimSpace(in.Name)
	if name == "" {
		return Channel{}, nil, ErrInvalidName
	}
	if in.WorkspaceID == "" {
		return Channel{}, nil, fmt.Errorf("catalog: workspace_id required")
	}
	if in.CreatorID == "" {
		return Channel{}, nil, fmt.Errorf("catalog: creator_id required")
	}
	if in.Type == "" {
		in.Type = "group"
	}

	if err := s.assertWorkspaceMember(ctx, in.WorkspaceID, in.CreatorID); err != nil {
		return Channel{}, nil, err
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return Channel{}, nil, fmt.Errorf("begin: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	ch := Channel{
		ID: newID(), WorkspaceID: in.WorkspaceID, Name: name, Type: in.Type,
		CreatedAt: s.nowMs(),
	}
	if _, err := tx.ExecContext(
		ctx,
		`INSERT INTO channels (id, workspace_id, name, type, created_at) VALUES (?, ?, ?, ?, ?)`,
		ch.ID, ch.WorkspaceID, ch.Name, ch.Type, ch.CreatedAt,
	); err != nil {
		return Channel{}, nil, fmt.Errorf("insert channel: %w", err)
	}

	members := normaliseMembers(in)
	out := make([]ChannelMember, 0, len(members))
	for _, m := range members {
		row := ChannelMember{
			ChannelID:        ch.ID,
			UserID:           m.UserID,
			ActorIDInChannel: m.ActorIDInChannel,
			Role:             m.Role,
			JoinedAt:         ch.CreatedAt,
		}
		if row.ActorIDInChannel == "" {
			row.ActorIDInChannel = "user:" + row.UserID
		}
		if row.Role == "" {
			row.Role = "member"
		}
		if _, err := tx.ExecContext(
			ctx,
			`INSERT INTO channel_members (channel_id, user_id, actor_id_in_channel, role, joined_at)
			 VALUES (?, ?, ?, ?, ?)`,
			row.ChannelID, row.UserID, row.ActorIDInChannel, row.Role, row.JoinedAt,
		); err != nil {
			return Channel{}, nil, fmt.Errorf("insert member %s: %w", row.UserID, err)
		}
		out = append(out, row)
	}

	if err := tx.Commit(); err != nil {
		return Channel{}, nil, fmt.Errorf("commit: %w", err)
	}
	return ch, out, nil
}

// normaliseMembers ensures the creator is in the member list as
// owner; deduplicates by user_id.
func normaliseMembers(in CreateChannelInput) []NewMember {
	seen := map[string]bool{}
	out := make([]NewMember, 0, len(in.Members)+1)
	creator := NewMember{UserID: in.CreatorID, Role: "owner"}
	out = append(out, creator)
	seen[in.CreatorID] = true
	for _, m := range in.Members {
		if seen[m.UserID] {
			continue
		}
		seen[m.UserID] = true
		out = append(out, m)
	}
	return out
}

// GetChannel returns the channel + caller's actor_id (membership
// check).
func (s *Service) GetChannel(ctx context.Context, channelID, userID string) (Channel, ChannelMember, error) {
	var ch Channel
	err := s.db.QueryRowContext(
		ctx,
		`SELECT id, workspace_id, name, type, created_at FROM channels WHERE id = ?`,
		channelID,
	).Scan(&ch.ID, &ch.WorkspaceID, &ch.Name, &ch.Type, &ch.CreatedAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return Channel{}, ChannelMember{}, ErrChannelNotFound
		}
		return Channel{}, ChannelMember{}, err
	}

	member, err := s.GetChannelMember(ctx, channelID, userID)
	if err != nil {
		return Channel{}, ChannelMember{}, err
	}
	return ch, member, nil
}

// ListChannels returns every channel the user is a member of in a
// given workspace.
func (s *Service) ListChannels(ctx context.Context, workspaceID, userID string) ([]Channel, error) {
	if err := s.assertWorkspaceMember(ctx, workspaceID, userID); err != nil {
		return nil, err
	}
	rows, err := s.db.QueryContext(
		ctx,
		`SELECT c.id, c.workspace_id, c.name, c.type, c.created_at
		   FROM channels c
		   JOIN channel_members m ON m.channel_id = c.id
		  WHERE c.workspace_id = ?
		    AND m.user_id      = ?
		  ORDER BY c.created_at ASC`,
		workspaceID, userID,
	)
	if err != nil {
		return nil, fmt.Errorf("list channels: %w", err)
	}
	defer rows.Close()
	var out []Channel
	for rows.Next() {
		var c Channel
		if err := rows.Scan(&c.ID, &c.WorkspaceID, &c.Name, &c.Type, &c.CreatedAt); err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	return out, rows.Err()
}
