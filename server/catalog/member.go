package catalog

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/wanpengxie/ActOS/kernel/placement"
)

// ChannelMember is the join row from channel_members. ActorID is
// stable for the (channel, user) tuple — daemon harness uses it as
// envelope.sender.id when the human caller token path runs.
type ChannelMember struct {
	ChannelID        string
	UserID           string
	ActorIDInChannel string
	Role             string
	JoinedAt         int64
}

// GetChannelMember returns the row for (channelID, userID).
// ErrNotChannelMember when absent.
func (s *Service) GetChannelMember(ctx context.Context, channelID, userID string) (ChannelMember, error) {
	var m ChannelMember
	err := s.db.QueryRowContext(
		ctx,
		`SELECT channel_id, user_id, actor_id_in_channel, role, joined_at
		   FROM channel_members
		  WHERE channel_id = ? AND user_id = ?`,
		channelID, userID,
	).Scan(&m.ChannelID, &m.UserID, &m.ActorIDInChannel, &m.Role, &m.JoinedAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ChannelMember{}, ErrNotChannelMember
		}
		return ChannelMember{}, err
	}
	return m, nil
}

// ListChannelMembers returns every member of a channel.
func (s *Service) ListChannelMembers(ctx context.Context, channelID string) ([]ChannelMember, error) {
	rows, err := s.db.QueryContext(
		ctx,
		`SELECT channel_id, user_id, actor_id_in_channel, role, joined_at
		   FROM channel_members
		  WHERE channel_id = ?
		  ORDER BY joined_at ASC`,
		channelID,
	)
	if err != nil {
		return nil, fmt.Errorf("list members: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var out []ChannelMember
	for rows.Next() {
		var m ChannelMember
		if err := rows.Scan(&m.ChannelID, &m.UserID, &m.ActorIDInChannel, &m.Role, &m.JoinedAt); err != nil {
			return nil, err
		}
		out = append(out, m)
	}
	return out, rows.Err()
}

// AddChannelMember inserts a new (channel, user) row. ActorID auto-
// assigned when empty. Returns ErrMemberExists on uniqueness collision.
func (s *Service) AddChannelMember(ctx context.Context, channelID string, m NewMember) (ChannelMember, error) {
	if m.UserID == "" {
		return ChannelMember{}, fmt.Errorf("catalog: user_id required")
	}
	row := ChannelMember{
		ChannelID:        channelID,
		UserID:           m.UserID,
		ActorIDInChannel: m.ActorIDInChannel,
		Role:             m.Role,
		JoinedAt:         s.nowMs(),
	}
	if row.ActorIDInChannel == "" {
		row.ActorIDInChannel = "user:" + row.UserID
	}
	if row.Role == "" {
		row.Role = "member"
	}
	if _, err := s.db.ExecContext(
		ctx,
		`INSERT INTO channel_members (channel_id, user_id, actor_id_in_channel, role, joined_at)
		 VALUES (?, ?, ?, ?, ?)`,
		row.ChannelID, row.UserID, row.ActorIDInChannel, row.Role, row.JoinedAt,
	); err != nil {
		// modernc.org/sqlite returns CONSTRAINT errors via the SQL
		// driver; cheapest portable check is on the message.
		if isUniqueViolation(err) {
			return ChannelMember{}, ErrMemberExists
		}
		return ChannelMember{}, fmt.Errorf("insert member: %w", err)
	}
	return row, nil
}

// RemoveChannelMember deletes (channelID, userID). Idempotent — no
// error when row is absent.
func (s *Service) RemoveChannelMember(ctx context.Context, channelID, userID string) error {
	_, err := s.db.ExecContext(
		ctx,
		`DELETE FROM channel_members WHERE channel_id = ? AND user_id = ?`,
		channelID, userID,
	)
	if err != nil {
		return fmt.Errorf("delete member: %w", err)
	}
	return nil
}

// InitialMembersFor converts a normalised channel_members result set
// into the kernel placement initial_members slice used by the
// `control.create_channel` frame (T1.9).
func InitialMembersFor(members []ChannelMember, displayName func(userID string) string) []placement.InitialMember {
	out := make([]placement.InitialMember, 0, len(members))
	for _, m := range members {
		entry := placement.InitialMember{
			UserID:           m.UserID,
			ActorIDInChannel: m.ActorIDInChannel,
			Kind:             "human",
			Role:             m.Role,
		}
		if displayName != nil {
			entry.DisplayName = displayName(m.UserID)
		}
		out = append(out, entry)
	}
	return out
}

// isUniqueViolation returns true when err looks like a sqlite
// UNIQUE / PRIMARY KEY constraint failure (cheap portable check).
func isUniqueViolation(err error) bool {
	if err == nil {
		return false
	}
	s := err.Error()
	return contains(s, "UNIQUE constraint failed") || contains(s, "PRIMARY KEY")
}

func contains(haystack, needle string) bool {
	for i := 0; i+len(needle) <= len(haystack); i++ {
		if haystack[i:i+len(needle)] == needle {
			return true
		}
	}
	return false
}
