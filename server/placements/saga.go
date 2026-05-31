package placements

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/wanpengxie/ActOS/framework/multiuser/placement"
	"github.com/wanpengxie/ActOS/kernel/channel"
)

type SagaKind string

const (
	SagaKindBootstrapReserve SagaKind = "bootstrap_reserve"
	SagaKindReclaimReserve   SagaKind = "reclaim_reserve"
	SagaKindRollback         SagaKind = "rollback"
)

type SagaPhase string

const (
	SagaPhaseSent            SagaPhase = "sent"
	SagaPhaseAwaitingAck     SagaPhase = "awaiting_ack"
	SagaPhasePartialTakeover SagaPhase = "partial_takeover"
	SagaPhaseCompleted       SagaPhase = "completed"
	SagaPhaseAbandoned       SagaPhase = "abandoned"
)

type PlacementSaga struct {
	SagaID                string
	ChannelID             channel.ID
	CreateRequestID       placement.CreateRequestID
	OwnerEpoch            placement.OwnerEpoch
	DaemonID              placement.DaemonID
	DaemonConnectionEpoch placement.ConnectionEpoch
	SagaKind              SagaKind
	Phase                 SagaPhase
	SentAt                int64
	ExpectedAckFrameKind  string
	TerminalStatus        string
	AbandonmentReason     string
	AttemptCount          int
	LastAttemptAt         int64
	CreatedAt             int64
	UpdatedAt             int64
}

type StartSagaInput struct {
	SagaID                string
	ChannelID             channel.ID
	CreateRequestID       placement.CreateRequestID
	OwnerEpoch            placement.OwnerEpoch
	DaemonID              placement.DaemonID
	DaemonConnectionEpoch placement.ConnectionEpoch
	SagaKind              SagaKind
	Phase                 SagaPhase
	SentAt                int64
	ExpectedAckFrameKind  string
	NowMs                 int64
}

func SagaID(kind SagaKind, channelID channel.ID, createRequestID placement.CreateRequestID, ownerEpoch placement.OwnerEpoch) string {
	return fmt.Sprintf("%s:%s:%s:%d", kind, channelID, createRequestID, ownerEpoch)
}

func (s *SQLStore) StartSaga(ctx context.Context, in StartSagaInput) (PlacementSaga, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return PlacementSaga{}, fmt.Errorf("placements: StartSaga tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	out, err := s.startSagaTx(ctx, tx, in)
	if err != nil {
		return PlacementSaga{}, err
	}
	if err := tx.Commit(); err != nil {
		return PlacementSaga{}, fmt.Errorf("placements: StartSaga commit: %w", err)
	}
	return out, nil
}

func (s *SQLStore) startSagaTx(ctx context.Context, tx *sql.Tx, in StartSagaInput) (PlacementSaga, error) {
	if in.SagaID == "" {
		in.SagaID = SagaID(in.SagaKind, in.ChannelID, in.CreateRequestID, in.OwnerEpoch)
	}
	if in.Phase == "" {
		in.Phase = SagaPhaseSent
	}
	if in.SentAt == 0 {
		in.SentAt = in.NowMs
	}
	if in.NowMs == 0 {
		in.NowMs = in.SentAt
	}
	if in.ChannelID == "" || in.SagaKind == "" {
		return PlacementSaga{}, errors.New("placements: StartSaga channel_id and saga_kind required")
	}
	_, err := tx.ExecContext(ctx, `
			INSERT INTO placement_sagas (
			saga_id, channel_id, create_request_id, owner_epoch,
			daemon_id, daemon_connection_epoch, saga_kind, phase,
			sent_at, expected_ack_frame_kind, created_at, updated_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(saga_id) DO UPDATE SET
			phase = excluded.phase,
			sent_at = excluded.sent_at,
			expected_ack_frame_kind = excluded.expected_ack_frame_kind,
			daemon_id = excluded.daemon_id,
			daemon_connection_epoch = excluded.daemon_connection_epoch,
			terminal_status = '',
			abandonment_reason = '',
			updated_at = excluded.updated_at`,
		in.SagaID, string(in.ChannelID), string(in.CreateRequestID), int64(in.OwnerEpoch),
		string(in.DaemonID), int64(in.DaemonConnectionEpoch), string(in.SagaKind), string(in.Phase),
		in.SentAt, in.ExpectedAckFrameKind, in.NowMs, in.NowMs,
	)
	if err != nil {
		return PlacementSaga{}, fmt.Errorf("placements: StartSaga: %w", err)
	}
	out, ok, err := getSagaTx(ctx, tx, in.SagaID)
	if err != nil {
		return PlacementSaga{}, err
	}
	if !ok {
		return PlacementSaga{}, fmt.Errorf("placements: StartSaga %q vanished", in.SagaID)
	}
	return out, nil
}

func (s *SQLStore) MarkSagaPhase(ctx context.Context, sagaID string, phase SagaPhase, nowMs int64) error {
	_, err := s.db.ExecContext(ctx,
		`UPDATE placement_sagas SET phase=?, updated_at=? WHERE saga_id=? AND phase NOT IN ('completed','abandoned')`,
		string(phase), nowMs, sagaID,
	)
	if err != nil {
		return fmt.Errorf("placements: MarkSagaPhase: %w", err)
	}
	return nil
}

func (s *SQLStore) CompleteSaga(ctx context.Context, sagaID, terminalStatus string, nowMs int64) error {
	_, err := s.db.ExecContext(ctx,
		`UPDATE placement_sagas
		    SET phase='completed', terminal_status=?, abandonment_reason='', updated_at=?
		  WHERE saga_id=? AND phase != 'abandoned'`,
		terminalStatus, nowMs, sagaID,
	)
	if err != nil {
		return fmt.Errorf("placements: CompleteSaga: %w", err)
	}
	return nil
}

func (s *SQLStore) AbandonSaga(ctx context.Context, sagaID, reason string, nowMs int64) error {
	_, err := s.db.ExecContext(ctx,
		`UPDATE placement_sagas
		    SET phase='abandoned', terminal_status='abandoned', abandonment_reason=?, updated_at=?
		  WHERE saga_id=? AND phase != 'completed'`,
		reason, nowMs, sagaID,
	)
	if err != nil {
		return fmt.Errorf("placements: AbandonSaga: %w", err)
	}
	return nil
}

func (s *SQLStore) GetSaga(ctx context.Context, sagaID string) (PlacementSaga, bool, error) {
	row := s.db.QueryRowContext(ctx, `SELECT
			saga_id, channel_id, create_request_id, owner_epoch, daemon_id,
			daemon_connection_epoch, saga_kind, phase, sent_at,
			expected_ack_frame_kind, terminal_status, abandonment_reason,
			attempt_count, last_attempt_at, created_at, updated_at
		FROM placement_sagas WHERE saga_id=?`, sagaID)
	out, err := scanSaga(row)
	if errors.Is(err, sql.ErrNoRows) {
		return PlacementSaga{}, false, nil
	}
	if err != nil {
		return PlacementSaga{}, false, err
	}
	return out, true, nil
}

func getSagaTx(ctx context.Context, tx *sql.Tx, sagaID string) (PlacementSaga, bool, error) {
	row := tx.QueryRowContext(ctx, `SELECT
			saga_id, channel_id, create_request_id, owner_epoch, daemon_id,
			daemon_connection_epoch, saga_kind, phase, sent_at,
			expected_ack_frame_kind, terminal_status, abandonment_reason,
			attempt_count, last_attempt_at, created_at, updated_at
		FROM placement_sagas WHERE saga_id=?`, sagaID)
	out, err := scanSaga(row)
	if errors.Is(err, sql.ErrNoRows) {
		return PlacementSaga{}, false, nil
	}
	if err != nil {
		return PlacementSaga{}, false, err
	}
	return out, true, nil
}

func (s *SQLStore) SagaForCreateRequest(ctx context.Context, kind SagaKind, channelID channel.ID, createRequestID placement.CreateRequestID) (PlacementSaga, bool, error) {
	row := s.db.QueryRowContext(ctx, `SELECT
			saga_id, channel_id, create_request_id, owner_epoch, daemon_id,
			daemon_connection_epoch, saga_kind, phase, sent_at,
			expected_ack_frame_kind, terminal_status, abandonment_reason,
			attempt_count, last_attempt_at, created_at, updated_at
		FROM placement_sagas
		WHERE saga_kind=? AND channel_id=? AND create_request_id=?
		ORDER BY created_at DESC LIMIT 1`, string(kind), string(channelID), string(createRequestID))
	out, err := scanSaga(row)
	if errors.Is(err, sql.ErrNoRows) {
		return PlacementSaga{}, false, nil
	}
	if err != nil {
		return PlacementSaga{}, false, err
	}
	return out, true, nil
}

func (s *SQLStore) AbandonTimedOutSagas(ctx context.Context, cutoffSentAt int64, reason string, nowMs int64) error {
	_, err := s.db.ExecContext(ctx, `
		UPDATE placement_sagas
		   SET phase='abandoned',
		       terminal_status='timeout',
		       abandonment_reason=?,
		       updated_at=?
 WHERE phase IN ('sent','awaiting_ack','partial_takeover')
   AND saga_kind != 'rollback'
   AND sent_at > 0
   AND sent_at <= ?`,
		reason, nowMs, cutoffSentAt,
	)
	if err != nil {
		return fmt.Errorf("placements: AbandonTimedOutSagas: %w", err)
	}
	return nil
}

type sagaScanner interface {
	Scan(dest ...any) error
}

func scanSaga(row sagaScanner) (PlacementSaga, error) {
	var out PlacementSaga
	var chID, reqID, daemonID, kind, phase string
	var ownerEpoch, connEpoch int64
	err := row.Scan(
		&out.SagaID, &chID, &reqID, &ownerEpoch, &daemonID,
		&connEpoch, &kind, &phase, &out.SentAt,
		&out.ExpectedAckFrameKind, &out.TerminalStatus, &out.AbandonmentReason,
		&out.AttemptCount, &out.LastAttemptAt, &out.CreatedAt, &out.UpdatedAt,
	)
	if err != nil {
		return PlacementSaga{}, err
	}
	out.ChannelID = channel.ID(chID)
	out.CreateRequestID = placement.CreateRequestID(reqID)
	out.OwnerEpoch = placement.OwnerEpoch(ownerEpoch)
	out.DaemonID = placement.DaemonID(daemonID)
	out.DaemonConnectionEpoch = placement.ConnectionEpoch(connEpoch)
	out.SagaKind = SagaKind(kind)
	out.Phase = SagaPhase(phase)
	return out, nil
}
