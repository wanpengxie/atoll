package gateway

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"github.com/wanpengxie/ActOS/kernel/channel"
	kerneldaemonbus "github.com/wanpengxie/ActOS/kernel/daemonbus"
	"github.com/wanpengxie/ActOS/kernel/placement"
	"github.com/wanpengxie/ActOS/server/daemonbus"
)

const rollbackUnbindTimeout = 200 * time.Millisecond

type placementRollbackIntent struct {
	ChannelID       channel.ID
	CreateRequestID placement.CreateRequestID
	OwnerEpoch      placement.OwnerEpoch
	DaemonID        placement.DaemonID
	ConnectionEpoch placement.ConnectionEpoch
	Reason          string
	Attempts        int
}

func (a *App) persistRollbackIntent(
	ctx context.Context,
	conn *daemonbus.Connection,
	p placement.Placement,
	reason string,
) error {
	if conn == nil || p.ChannelID == "" || p.CreateRequestID == "" || p.OwnerEpoch <= 0 {
		return nil
	}
	now := time.Now().UnixMilli()
	tx, err := a.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("gateway: persist rollback intent tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.ExecContext(ctx, `
			INSERT INTO placement_rollback_intents (
				channel_id, create_request_id, owner_epoch, daemon_id,
				daemon_connection_epoch, reason, attempts, last_attempt_at,
				created_at, updated_at
			) VALUES (?, ?, ?, ?, ?, ?, 0, 0, ?, ?)
		ON CONFLICT(channel_id, create_request_id, owner_epoch) DO UPDATE SET
			daemon_id = excluded.daemon_id,
				daemon_connection_epoch = excluded.daemon_connection_epoch,
				reason = excluded.reason,
				updated_at = excluded.updated_at`,
		string(p.ChannelID), string(p.CreateRequestID), int64(p.OwnerEpoch),
		string(conn.DaemonID), int64(conn.ConnectionEpoch), reason, now, now,
	); err != nil {
		return fmt.Errorf("gateway: persist rollback intent: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `
			INSERT INTO placement_sagas (
				saga_id, channel_id, create_request_id, owner_epoch,
				daemon_id, daemon_connection_epoch, saga_kind, phase,
				sent_at, expected_ack_frame_kind, attempt_count, last_attempt_at,
				created_at, updated_at
			) VALUES (?, ?, ?, ?, ?, ?, 'rollback', 'sent', ?, ?, 0, 0, ?, ?)
			ON CONFLICT(saga_id) DO UPDATE SET
				daemon_id = excluded.daemon_id,
				daemon_connection_epoch = excluded.daemon_connection_epoch,
				phase = 'sent',
				terminal_status = '',
				abandonment_reason = '',
				updated_at = excluded.updated_at`,
		rollbackSagaID(p.ChannelID, p.CreateRequestID, p.OwnerEpoch),
		string(p.ChannelID), string(p.CreateRequestID), int64(p.OwnerEpoch),
		string(conn.DaemonID), int64(conn.ConnectionEpoch),
		now, string(kerneldaemonbus.FrameTypeControlUnbindChannelAck), now, now,
	); err != nil {
		return fmt.Errorf("gateway: persist rollback saga: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("gateway: persist rollback intent commit: %w", err)
	}
	return nil
}

func (a *App) deleteRollbackIntent(ctx context.Context, intent placementRollbackIntent) error {
	tx, err := a.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("gateway: delete rollback intent tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.ExecContext(ctx, `
			DELETE FROM placement_rollback_intents
			 WHERE channel_id = ? AND create_request_id = ? AND owner_epoch = ?`,
		string(intent.ChannelID), string(intent.CreateRequestID), int64(intent.OwnerEpoch),
	); err != nil {
		return fmt.Errorf("gateway: delete rollback intent: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `
			UPDATE placement_sagas
			   SET phase='completed',
			       terminal_status='completed',
			       abandonment_reason='',
			       updated_at=?
			 WHERE saga_id=?`,
		time.Now().UnixMilli(),
		rollbackSagaID(intent.ChannelID, intent.CreateRequestID, intent.OwnerEpoch),
	); err != nil {
		return fmt.Errorf("gateway: complete rollback saga: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("gateway: delete rollback intent commit: %w", err)
	}
	return nil
}

func (a *App) listRollbackIntentsForDaemon(ctx context.Context, daemonID placement.DaemonID) ([]placementRollbackIntent, error) {
	return a.listRollbackIntentsForDaemonQuery(ctx, daemonID, 0)
}

func (a *App) listRollbackIntentsForDaemonBeforeEpoch(
	ctx context.Context,
	daemonID placement.DaemonID,
	epoch placement.ConnectionEpoch,
) ([]placementRollbackIntent, error) {
	return a.listRollbackIntentsForDaemonQuery(ctx, daemonID, epoch)
}

func (a *App) listRollbackIntentsForDaemonQuery(
	ctx context.Context,
	daemonID placement.DaemonID,
	beforeEpoch placement.ConnectionEpoch,
) ([]placementRollbackIntent, error) {
	query := `
		SELECT channel_id, create_request_id, owner_epoch, daemon_id,
		       daemon_connection_epoch, reason, attempts
		  FROM placement_rollback_intents
		 WHERE daemon_id = ?`
	args := []any{string(daemonID)}
	if beforeEpoch > 0 {
		query += ` AND daemon_connection_epoch < ?`
		args = append(args, int64(beforeEpoch))
	}
	query += ` ORDER BY updated_at ASC`
	rows, err := a.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("gateway: list rollback intents: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var out []placementRollbackIntent
	for rows.Next() {
		var intent placementRollbackIntent
		var chID, reqID, daemon string
		var ownerEpoch, connEpoch int64
		if err := rows.Scan(&chID, &reqID, &ownerEpoch, &daemon, &connEpoch, &intent.Reason, &intent.Attempts); err != nil {
			return nil, fmt.Errorf("gateway: scan rollback intent: %w", err)
		}
		intent.ChannelID = channel.ID(chID)
		intent.CreateRequestID = placement.CreateRequestID(reqID)
		intent.OwnerEpoch = placement.OwnerEpoch(ownerEpoch)
		intent.DaemonID = placement.DaemonID(daemon)
		intent.ConnectionEpoch = placement.ConnectionEpoch(connEpoch)
		out = append(out, intent)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("gateway: rollback intent rows: %w", err)
	}
	return out, nil
}

func (a *App) incrementRollbackAttempt(ctx context.Context, intent placementRollbackIntent) error {
	now := time.Now().UnixMilli()
	tx, err := a.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("gateway: increment rollback attempt tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.ExecContext(ctx, `
			UPDATE placement_rollback_intents
			   SET attempts = attempts + 1,
			       last_attempt_at = ?,
			       updated_at = ?
			 WHERE channel_id = ? AND create_request_id = ? AND owner_epoch = ?`,
		now, now, string(intent.ChannelID), string(intent.CreateRequestID), int64(intent.OwnerEpoch),
	); err != nil {
		return fmt.Errorf("gateway: increment rollback attempt: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `
			UPDATE placement_sagas
			   SET attempt_count = attempt_count + 1,
			       last_attempt_at = ?,
			       updated_at = ?
			 WHERE saga_id=?`,
		now, now, rollbackSagaID(intent.ChannelID, intent.CreateRequestID, intent.OwnerEpoch),
	); err != nil {
		return fmt.Errorf("gateway: increment rollback saga attempt: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("gateway: increment rollback attempt commit: %w", err)
	}
	return nil
}

func rollbackSagaID(channelID channel.ID, createRequestID placement.CreateRequestID, ownerEpoch placement.OwnerEpoch) string {
	return fmt.Sprintf("rollback:%s:%s:%d", channelID, createRequestID, ownerEpoch)
}

func (a *App) rollbackIntentSuperseded(ctx context.Context, intent placementRollbackIntent) (bool, error) {
	p, ok, err := a.placements.Get(ctx, intent.ChannelID)
	if err != nil {
		return false, err
	}
	if !ok {
		return true, nil
	}
	return p.CreateRequestID != intent.CreateRequestID ||
		p.OwnerEpoch != intent.OwnerEpoch ||
		p.DaemonID != intent.DaemonID, nil
}

func (a *App) attemptRollbackUnbind(
	ctx context.Context,
	conn *daemonbus.Connection,
	intent placementRollbackIntent,
) error {
	if conn == nil || conn.IsClosed() {
		return nil
	}
	superseded, err := a.rollbackIntentSuperseded(ctx, intent)
	if err != nil {
		return err
	}
	if superseded {
		return a.deleteRollbackIntent(ctx, intent)
	}
	if err := a.incrementRollbackAttempt(ctx, intent); err != nil {
		return err
	}
	attemptCtx, cancel := context.WithTimeout(ctx, rollbackUnbindTimeout)
	defer cancel()
	ackFrame, err := conn.SendAndAwait(attemptCtx, kerneldaemonbus.FrameTypeControlUnbindChannel, kerneldaemonbus.UnbindChannelBody{
		ChannelID:  intent.ChannelID,
		OwnerEpoch: intent.OwnerEpoch,
		Reason:     kerneldaemonbus.UnbindChannelReasonAbandon,
	})
	if err != nil {
		return fmt.Errorf("gateway: rollback unbind send: %w", err)
	}
	if ackFrame.FrameKind != kerneldaemonbus.FrameTypeControlUnbindChannelAck {
		return fmt.Errorf("gateway: rollback unbind unexpected ack frame %s", ackFrame.FrameKind)
	}
	var ack kerneldaemonbus.UnbindChannelAckBody
	if err := json.Unmarshal(ackFrame.Payload, &ack); err != nil {
		return fmt.Errorf("gateway: rollback unbind ack decode: %w", err)
	}
	if ack.ChannelID != intent.ChannelID || ack.OwnerEpoch != intent.OwnerEpoch {
		return fmt.Errorf("gateway: rollback unbind ack mismatch for %s", intent.ChannelID)
	}
	switch {
	case ack.Result == kerneldaemonbus.UnbindChannelReleased:
		return a.deleteRollbackIntent(ctx, intent)
	case ack.Result == kerneldaemonbus.UnbindChannelRejected &&
		(ack.Reason == kerneldaemonbus.UnbindChannelRejectAlreadyReleased ||
			ack.Reason == kerneldaemonbus.UnbindChannelRejectOwnerEpochStale):
		return a.deleteRollbackIntent(ctx, intent)
	default:
		return fmt.Errorf("gateway: rollback unbind rejected: %s", ack.Reason)
	}
}

func (a *App) retryRollbackIntentsForDaemon(ctx context.Context, conn *daemonbus.Connection) error {
	if conn == nil {
		return nil
	}
	intents, err := a.listRollbackIntentsForDaemon(ctx, conn.DaemonID)
	if err != nil {
		return err
	}
	for _, intent := range intents {
		if err := a.attemptRollbackUnbind(ctx, conn, intent); err != nil {
			return err
		}
	}
	return nil
}

func (a *App) retryRollbackIntentsForDaemonBeforeEpoch(ctx context.Context, conn *daemonbus.Connection) error {
	if conn == nil {
		return nil
	}
	intents, err := a.listRollbackIntentsForDaemonBeforeEpoch(
		ctx,
		conn.DaemonID,
		placement.ConnectionEpoch(conn.ConnectionEpoch),
	)
	if err != nil {
		return err
	}
	for _, intent := range intents {
		if err := a.attemptRollbackUnbind(ctx, conn, intent); err != nil {
			return err
		}
	}
	return nil
}

func (a *App) retryRollbackIntentsForRegisteredDaemon(conn *daemonbus.Connection) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := a.retryRollbackIntentsForDaemonBeforeEpoch(ctx, conn); err != nil {
		pkgLogger.Warn().Err(err).
			Str("event", "placement.rollback_retry_failed").
			Str("daemon_id", string(conn.DaemonID)).
			Msg("placement rollback retry failed after daemon register")
	}
}

func (a *App) reclaimRollback(
	ctx context.Context,
	conn *daemonbus.Connection,
	reserved placement.Placement,
	reason string,
) error {
	rollbackCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Second)
	defer cancel()
	if err := a.persistRollbackIntent(rollbackCtx, conn, reserved, reason); err != nil {
		return err
	}
	if _, err := a.placements.OrphanCreating(rollbackCtx, reserved.ChannelID, reserved.CreateRequestID); err != nil {
		return err
	}
	intent := placementRollbackIntent{
		ChannelID:       reserved.ChannelID,
		CreateRequestID: reserved.CreateRequestID,
		OwnerEpoch:      reserved.OwnerEpoch,
		DaemonID:        conn.DaemonID,
		ConnectionEpoch: placement.ConnectionEpoch(conn.ConnectionEpoch),
		Reason:          reason,
	}
	return a.attemptRollbackUnbind(rollbackCtx, conn, intent)
}

func (a *App) rollbackIntentCount(ctx context.Context, channelID channel.ID) (int, error) {
	var n int
	err := a.db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM placement_rollback_intents WHERE channel_id = ?`,
		string(channelID),
	).Scan(&n)
	if err != nil && err != sql.ErrNoRows {
		return 0, err
	}
	return n, nil
}
