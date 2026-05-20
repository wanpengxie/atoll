// Package placements is the server-side owner of the channel
// placement state machine — creating / active / orphan / stale. It
// runs the SQL-backed CAS dance from L2 §1.4.11 (ACK 完整字段匹配)
// plus the reconcile loop from T1.7 (cold start grace + heartbeat
// timeout).
//
// Authoritative spec: .dalek/pm/m1.5-tickets.md §T6 (placements) +
// kernel/placement contract.
//
// Concurrency: every method is safe under concurrent use — sqlite
// row-level locking + the CAS guard in CASActivate prevents lost
// updates.
package placements

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/wanpengxie/ActOS/kernel/channel"
	"github.com/wanpengxie/ActOS/kernel/placement"
)

// SQLStore implements kernel/placement.Store on top of the
// channel_placements sqlite table (migration 0003).
type SQLStore struct {
	db *sql.DB
}

// NewSQLStore constructs a store rooted at db.
func NewSQLStore(db *sql.DB) *SQLStore { return &SQLStore{db: db} }

// Reserve inserts the placement row in StateCreating (L2 §1.4.11.3
// step 1). Returns ErrPlacementExists on PRIMARY KEY collision.
//
// The three federation / tenancy columns (host_actor_id /
// federated_origin / tenant_id, reserved by m1.5-tickets §T10) are
// written as NULL when the corresponding placement field is empty;
// M1.5 demo callers therefore leave the columns NULL without any
// extra parameter.
func (s *SQLStore) Reserve(ctx context.Context, p placement.Placement) (placement.Placement, error) {
	if p.ChannelID == "" {
		return placement.Placement{}, errors.New("placements: channel_id required")
	}
	if p.State == "" {
		p.State = placement.StateCreating
	}

	_, err := s.db.ExecContext(
		ctx,
		`INSERT INTO channel_placements (
		   channel_id, daemon_id, state, owner_epoch, fencing_token,
		   create_request_id, daemon_connection_epoch, last_heartbeat_at,
		   created_at, activated_at, entered_state_at,
		   host_actor_id, federated_origin, tenant_id
		 ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		string(p.ChannelID), string(p.DaemonID), string(p.State),
		int64(p.OwnerEpoch), string(p.FencingToken),
		string(p.CreateRequestID), int64(p.DaemonConnectionEpoch),
		p.LastHeartbeatAt, p.CreatedAt, p.ActivatedAt, p.EnteredStateAt,
		nullableString(p.HostActorID),
		nullableString(p.FederatedOrigin),
		nullableString(string(p.TenantID)),
	)
	if err != nil {
		if isUniqueViolation(err) {
			return placement.Placement{}, &placement.ErrPlacementExists{ChannelID: p.ChannelID}
		}
		return placement.Placement{}, fmt.Errorf("placements: insert: %w", err)
	}
	return p, nil
}

// Get returns the placement row for channelID.
func (s *SQLStore) Get(ctx context.Context, channelID channel.ID) (placement.Placement, bool, error) {
	var p placement.Placement
	var state string
	var hostActor, fedOrigin, tenant sql.NullString
	err := s.db.QueryRowContext(
		ctx,
		`SELECT channel_id, daemon_id, state, owner_epoch, fencing_token,
		        create_request_id, daemon_connection_epoch, last_heartbeat_at,
		        created_at, activated_at, entered_state_at,
		        host_actor_id, federated_origin, tenant_id
		   FROM channel_placements WHERE channel_id = ?`,
		string(channelID),
	).Scan(
		(*string)(&p.ChannelID), (*string)(&p.DaemonID), &state,
		(*int64)(&p.OwnerEpoch), (*string)(&p.FencingToken),
		(*string)(&p.CreateRequestID), (*int64)(&p.DaemonConnectionEpoch),
		&p.LastHeartbeatAt, &p.CreatedAt, &p.ActivatedAt, &p.EnteredStateAt,
		&hostActor, &fedOrigin, &tenant,
	)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return placement.Placement{}, false, nil
		}
		return placement.Placement{}, false, fmt.Errorf("placements: get: %w", err)
	}
	p.State = placement.State(state)
	p.HostActorID = hostActor.String
	p.FederatedOrigin = fedOrigin.String
	p.TenantID = placement.TenantID(tenant.String)
	return p, true, nil
}

// ResolveDaemonForChannel returns the daemon owning channelID when
// the placement is active. ok=false means no active owner exists.
func (s *SQLStore) ResolveDaemonForChannel(ctx context.Context, channelID channel.ID) (placement.DaemonID, bool, error) {
	var daemonID string
	err := s.db.QueryRowContext(
		ctx,
		`SELECT daemon_id FROM channel_placements
		  WHERE channel_id = ? AND state = 'active'`,
		string(channelID),
	).Scan(&daemonID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return "", false, nil
		}
		return "", false, fmt.Errorf("placements: resolve daemon: %w", err)
	}
	return placement.DaemonID(daemonID), true, nil
}

// CASActivate runs the L2 §1.4.11.3 step 5 CAS — UPDATE state to
// active iff ALL of (create_request_id, owner_epoch, fencing_token,
// daemon_id) match the ACK + state='creating'.
//
// Returns ok=true on successful CAS; ok=false (no error) when the
// CAS lost — callers should treat this as "ACK rejected" and let the
// reconcile loop advance the row to orphan via create_timeout.
func (s *SQLStore) CASActivate(
	ctx context.Context,
	ack placement.CreateChannelAck,
	newConnectionEpoch placement.ConnectionEpoch,
	nowMs int64,
) (bool, error) {
	if ack.Status != placement.AckBound {
		// daemon explicitly rejected — never advance to active.
		return false, nil
	}
	res, err := s.db.ExecContext(
		ctx,
		`UPDATE channel_placements
		    SET state                    = 'active',
		        activated_at             = ?,
		        entered_state_at         = ?,
		        daemon_connection_epoch  = ?,
		        last_heartbeat_at        = ?
		  WHERE channel_id        = ?
		    AND create_request_id = ?
		    AND owner_epoch       = ?
		    AND fencing_token     = ?
		    AND daemon_id         = ?
		    AND state             = 'creating'`,
		nowMs, nowMs, int64(newConnectionEpoch), nowMs,
		string(ack.ChannelID), string(ack.CreateRequestID),
		int64(ack.OwnerEpoch), string(ack.FencingToken),
		string(ack.DaemonID),
	)
	if err != nil {
		return false, fmt.Errorf("placements: CASActivate: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("placements: CASActivate rows: %w", err)
	}
	return n == 1, nil
}

// MarkStale transitions an active placement to Stale (idempotent —
// non-active rows are left untouched).
func (s *SQLStore) MarkStale(ctx context.Context, channelID channel.ID, nowMs int64) error {
	_, err := s.db.ExecContext(
		ctx,
		`UPDATE channel_placements
		    SET state = 'stale',
		        entered_state_at = ?
		  WHERE channel_id = ? AND state = 'active'`,
		nowMs,
		string(channelID),
	)
	if err != nil {
		return fmt.Errorf("placements: MarkStale: %w", err)
	}
	return nil
}

// MarkOrphan transitions a creating placement to Orphan (idempotent).
func (s *SQLStore) MarkOrphan(ctx context.Context, channelID channel.ID, nowMs int64) error {
	_, err := s.db.ExecContext(
		ctx,
		`UPDATE channel_placements
		    SET state = 'orphan',
		        entered_state_at = ?
		  WHERE channel_id = ? AND state = 'creating'`,
		nowMs,
		string(channelID),
	)
	if err != nil {
		return fmt.Errorf("placements: MarkOrphan: %w", err)
	}
	return nil
}

// AcceptReclaim runs the L2 §1.4.11.4 reclaim path — daemon reports
// (channel_id, fencing_token, owner_epoch) on reconnect; server
// validates state='active' + full tuple (channel_id, daemon_id,
// owner_epoch, fencing_token) matches, then refreshes the
// connection_epoch + heartbeat_at.
//
// daemonID is the WS-authenticated owner identifier from
// Connection.DaemonID — pinned into the SQL WHERE alongside the
// (owner_epoch, fencing_token) tuple so a different daemon presenting
// the same epoch/token cannot hijack ownership (FIX-T4 / L2 §1.4.11.4
// + spec T1.4 invariant).
func (s *SQLStore) AcceptReclaim(
	ctx context.Context,
	channelID channel.ID,
	daemonID placement.DaemonID,
	req placement.ReclaimChannel,
	newConnectionEpoch placement.ConnectionEpoch,
	nowMs int64,
) (bool, error) {
	res, err := s.db.ExecContext(
		ctx,
		`UPDATE channel_placements
		    SET state                    = 'active',
		        entered_state_at         = ?,
		        daemon_connection_epoch  = ?,
		        last_heartbeat_at        = ?
		  WHERE channel_id    = ?
		    AND daemon_id     = ?
		    AND owner_epoch   = ?
		    AND fencing_token = ?
		    AND state IN ('active','stale')`,
		nowMs, int64(newConnectionEpoch), nowMs,
		string(channelID), string(daemonID),
		int64(req.OwnerEpoch), string(req.FencingToken),
	)
	if err != nil {
		return false, fmt.Errorf("placements: AcceptReclaim: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return false, err
	}
	return n == 1, nil
}

// Heartbeat refreshes last_heartbeat_at for an active placement.
// Used by daemonbus when the daemon emits control.heartbeat.
func (s *SQLStore) Heartbeat(ctx context.Context, channelID channel.ID, daemonID placement.DaemonID, nowMs int64) error {
	_, err := s.db.ExecContext(
		ctx,
		`UPDATE channel_placements
		    SET last_heartbeat_at = ?
		  WHERE channel_id = ? AND daemon_id = ? AND state = 'active'`,
		nowMs, string(channelID), string(daemonID),
	)
	if err != nil {
		return fmt.Errorf("placements: Heartbeat: %w", err)
	}
	return nil
}

// ListByState returns every placement in the named state. Used by
// the reconcile loop.
func (s *SQLStore) ListByState(ctx context.Context, state placement.State) ([]placement.Placement, error) {
	rows, err := s.db.QueryContext(
		ctx,
		`SELECT channel_id, daemon_id, state, owner_epoch, fencing_token,
		        create_request_id, daemon_connection_epoch, last_heartbeat_at,
		        created_at, activated_at, entered_state_at,
		        host_actor_id, federated_origin, tenant_id
		   FROM channel_placements WHERE state = ?`,
		string(state),
	)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	var out []placement.Placement
	for rows.Next() {
		var (
			p                            placement.Placement
			state                        string
			hostActor, fedOrigin, tenant sql.NullString
		)
		if err := rows.Scan(
			(*string)(&p.ChannelID), (*string)(&p.DaemonID), &state,
			(*int64)(&p.OwnerEpoch), (*string)(&p.FencingToken),
			(*string)(&p.CreateRequestID), (*int64)(&p.DaemonConnectionEpoch),
			&p.LastHeartbeatAt, &p.CreatedAt, &p.ActivatedAt, &p.EnteredStateAt,
			&hostActor, &fedOrigin, &tenant,
		); err != nil {
			return nil, err
		}
		p.State = placement.State(state)
		p.HostActorID = hostActor.String
		p.FederatedOrigin = fedOrigin.String
		p.TenantID = placement.TenantID(tenant.String)
		out = append(out, p)
	}
	return out, rows.Err()
}

// ListByDaemon returns every active placement owned by daemonID.
// Used by reclaim path to enumerate "channels you should be holding".
func (s *SQLStore) ListByDaemon(ctx context.Context, daemonID placement.DaemonID) ([]placement.Placement, error) {
	rows, err := s.db.QueryContext(
		ctx,
		`SELECT channel_id, daemon_id, state, owner_epoch, fencing_token,
		        create_request_id, daemon_connection_epoch, last_heartbeat_at,
		        created_at, activated_at, entered_state_at,
		        host_actor_id, federated_origin, tenant_id
		   FROM channel_placements WHERE daemon_id = ?`,
		string(daemonID),
	)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var out []placement.Placement
	for rows.Next() {
		var (
			p                            placement.Placement
			state                        string
			hostActor, fedOrigin, tenant sql.NullString
		)
		if err := rows.Scan(
			(*string)(&p.ChannelID), (*string)(&p.DaemonID), &state,
			(*int64)(&p.OwnerEpoch), (*string)(&p.FencingToken),
			(*string)(&p.CreateRequestID), (*int64)(&p.DaemonConnectionEpoch),
			&p.LastHeartbeatAt, &p.CreatedAt, &p.ActivatedAt, &p.EnteredStateAt,
			&hostActor, &fedOrigin, &tenant,
		); err != nil {
			return nil, err
		}
		p.State = placement.State(state)
		p.HostActorID = hostActor.String
		p.FederatedOrigin = fedOrigin.String
		p.TenantID = placement.TenantID(tenant.String)
		out = append(out, p)
	}
	return out, rows.Err()
}

// nullableString converts a Go string to sql.NullString — empty
// string maps to NULL so M1.5 demo callers leave the federation /
// tenancy columns NULL without extra parameters.
func nullableString(s string) sql.NullString {
	if s == "" {
		return sql.NullString{}
	}
	return sql.NullString{String: s, Valid: true}
}

// isUniqueViolation returns true for sqlite UNIQUE / PRIMARY KEY
// constraint failures (string-match — modernc/sqlite doesn't expose
// a typed error code in this version).
func isUniqueViolation(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return contains(msg, "UNIQUE constraint failed") ||
		contains(msg, "PRIMARY KEY")
}

func contains(haystack, needle string) bool {
	for i := 0; i+len(needle) <= len(haystack); i++ {
		if haystack[i:i+len(needle)] == needle {
			return true
		}
	}
	return false
}
