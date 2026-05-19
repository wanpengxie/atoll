// Package catalog owns the workspace / channel / member directory on
// the server side. Channel metadata only — placement state lives in
// server/placements (separate concern by spec §T6).
//
// Authoritative spec: .dalek/pm/m1.5-tickets.md §T6 (catalog 子目录) +
// L1 §3 channel namespace + T1.9 channel-member sync.
//
// Key invariant: member_actor_id is unique per channel (DB-level
// UNIQUE constraint) so daemon harness can rely on it for sender
// identification. The mapping (channel_id, user_id) → actor_id is the
// single source of truth — daemonbus update_members frame derives
// everything else from it.
package catalog

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/wanpengxie/ActOS/kernel/channel"
)

// Errors returned by Service.
var (
	ErrInvalidName        = errors.New("catalog: invalid name")
	ErrWorkspaceNotFound  = errors.New("catalog: workspace not found")
	ErrChannelNotFound    = errors.New("catalog: channel not found")
	ErrNotWorkspaceMember = errors.New("catalog: not a workspace member")
	ErrNotChannelMember   = errors.New("catalog: not a channel member")
	ErrMemberExists       = errors.New("catalog: member already exists")
)

// Service is the catalog facade.
type Service struct {
	db                  *sql.DB
	now                 func() time.Time
	subscriptionRevoker SubscriptionRevoker
}

// NewService constructs a Service.
func NewService(db *sql.DB) *Service {
	return &Service{db: db, now: time.Now}
}

// WithClock sets a custom clock (testing only).
func (s *Service) WithClock(now func() time.Time) *Service {
	s.now = now
	return s
}

// SubscriptionRevoker is implemented by pushhub.Service. Catalog calls it after
// a member row is durably removed so long-lived websocket subscriptions stop
// receiving that channel's messages.
type SubscriptionRevoker interface {
	RevokeChannelUser(channelID channel.ID, userID string)
}

// SetSubscriptionRevoker wires the optional live-subscription revocation hook.
func (s *Service) SetSubscriptionRevoker(r SubscriptionRevoker) {
	s.subscriptionRevoker = r
}

func (s *Service) nowMs() int64 { return s.now().UnixMilli() }

func newID() string { return uuid.NewString() }

// Workspace is the public workspace record.
type Workspace struct {
	ID        string
	Name      string
	OwnerID   string
	CreatedAt int64
}

// CreateWorkspaceInput carries the create call.
type CreateWorkspaceInput struct {
	Name    string
	OwnerID string
}

// CreateWorkspace creates a workspace + inserts an owner row in
// workspace_members. Atomic.
func (s *Service) CreateWorkspace(ctx context.Context, in CreateWorkspaceInput) (Workspace, error) {
	name := strings.TrimSpace(in.Name)
	if name == "" {
		return Workspace{}, ErrInvalidName
	}
	if in.OwnerID == "" {
		return Workspace{}, fmt.Errorf("catalog: owner_id required")
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return Workspace{}, fmt.Errorf("begin: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	ws := Workspace{
		ID: newID(), Name: name, OwnerID: in.OwnerID, CreatedAt: s.nowMs(),
	}
	if _, err := tx.ExecContext(
		ctx,
		`INSERT INTO workspaces (id, name, owner_id, created_at) VALUES (?, ?, ?, ?)`,
		ws.ID, ws.Name, ws.OwnerID, ws.CreatedAt,
	); err != nil {
		return Workspace{}, fmt.Errorf("insert workspace: %w", err)
	}
	if _, err := tx.ExecContext(
		ctx,
		`INSERT INTO workspace_members (workspace_id, user_id, role, joined_at) VALUES (?, ?, 'owner', ?)`,
		ws.ID, ws.OwnerID, ws.CreatedAt,
	); err != nil {
		return Workspace{}, fmt.Errorf("insert owner: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return Workspace{}, fmt.Errorf("commit: %w", err)
	}
	return ws, nil
}

// GetWorkspace returns a workspace by id (with membership check).
func (s *Service) GetWorkspace(ctx context.Context, id, userID string) (Workspace, error) {
	if err := s.assertWorkspaceMember(ctx, id, userID); err != nil {
		return Workspace{}, err
	}
	var ws Workspace
	err := s.db.QueryRowContext(
		ctx,
		`SELECT id, name, owner_id, created_at FROM workspaces WHERE id = ?`,
		id,
	).Scan(&ws.ID, &ws.Name, &ws.OwnerID, &ws.CreatedAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return Workspace{}, ErrWorkspaceNotFound
		}
		return Workspace{}, err
	}
	return ws, nil
}

// ListWorkspaces returns every workspace the user is a member of.
func (s *Service) ListWorkspaces(ctx context.Context, userID string) ([]Workspace, error) {
	rows, err := s.db.QueryContext(
		ctx,
		`SELECT w.id, w.name, w.owner_id, w.created_at
		   FROM workspaces w
		   JOIN workspace_members m ON m.workspace_id = w.id
		  WHERE m.user_id = ?
		  ORDER BY w.created_at ASC`,
		userID,
	)
	if err != nil {
		return nil, fmt.Errorf("list workspaces: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var out []Workspace
	for rows.Next() {
		var ws Workspace
		if err := rows.Scan(&ws.ID, &ws.Name, &ws.OwnerID, &ws.CreatedAt); err != nil {
			return nil, err
		}
		out = append(out, ws)
	}
	return out, rows.Err()
}

func (s *Service) assertWorkspaceMember(ctx context.Context, workspaceID, userID string) error {
	var role string
	err := s.db.QueryRowContext(
		ctx,
		`SELECT role FROM workspace_members WHERE workspace_id = ? AND user_id = ?`,
		workspaceID, userID,
	).Scan(&role)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ErrNotWorkspaceMember
		}
		return err
	}
	return nil
}
