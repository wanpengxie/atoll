package catalog

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/wanpengxie/ActOS/kernel/channel"
)

const (
	memberTransitionKindAdd     = "add"
	memberTransitionKindRemove  = "remove"
	memberTransitionMaxBatch    = 100
	memberTransitionMaxAttempts = 10
	memberTransitionSweepLimit  = 100
	memberTransitionBaseDelay   = 60 * time.Second
	memberTransitionMaxDelay    = time.Hour
)

type memberTransition struct {
	ID                    int64
	ChannelID             string
	UserID                string
	MemberActorID         string
	Role                  string
	Kind                  string
	Attempts              int
	SubscriptionRevokedAt int64
}

func (s *Service) enqueueMemberTransitionTx(ctx context.Context, tx *sql.Tx, m ChannelMember, kind string, now int64) error {
	_, err := tx.ExecContext(ctx, `
		INSERT INTO member_transition_outbox (
			channel_id, user_id, member_actor_id, role, transition_kind,
			attempts, last_attempt_at, next_attempt_at, subscription_revoked_at,
			created_at, updated_at
			) VALUES (?, ?, ?, ?, ?, 0, 0, 0, 0, ?, ?)
			ON CONFLICT DO UPDATE SET
				user_id = excluded.user_id,
				role = excluded.role,
				next_attempt_at = 0,
				terminal_status = '',
				abandoned_at = 0,
				abandonment_reason = '',
				updated_at = excluded.updated_at`,
		m.ChannelID, m.UserID, m.MemberActorID, m.Role, kind, now, now,
	)
	if err != nil {
		return fmt.Errorf("enqueue member transition: %w", err)
	}
	return nil
}

func (s *Service) ProcessDueMemberTransitions(ctx context.Context, limit int) (int, error) {
	if limit <= 0 || limit > memberTransitionMaxBatch {
		limit = memberTransitionMaxBatch
	}
	if err := s.sweepMemberTransitions(ctx); err != nil {
		return 0, err
	}
	now := s.nowMs()
	rows, err := s.db.QueryContext(ctx, `
		SELECT id, channel_id, user_id, member_actor_id, role, transition_kind,
		       attempts, subscription_revoked_at
		  FROM member_transition_outbox
		 WHERE terminal_status = ''
		   AND (next_attempt_at = 0 OR next_attempt_at <= ?)
		 ORDER BY next_attempt_at ASC, id ASC
		 LIMIT ?`,
		now, limit,
	)
	if err != nil {
		return 0, fmt.Errorf("list member transitions: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var transitions []memberTransition
	for rows.Next() {
		var tr memberTransition
		if err := rows.Scan(
			&tr.ID, &tr.ChannelID, &tr.UserID, &tr.MemberActorID, &tr.Role,
			&tr.Kind, &tr.Attempts, &tr.SubscriptionRevokedAt,
		); err != nil {
			return 0, fmt.Errorf("scan member transition: %w", err)
		}
		transitions = append(transitions, tr)
	}
	if err := rows.Err(); err != nil {
		return 0, fmt.Errorf("member transition rows: %w", err)
	}

	var processed int
	var firstErr error
	for _, tr := range transitions {
		if tr.Attempts >= memberTransitionMaxAttempts {
			if err := s.abandonMemberTransition(ctx, tr, "member_transition_max_attempts"); err != nil && firstErr == nil {
				firstErr = err
			}
			continue
		}
		if err := s.processMemberTransition(ctx, tr); err != nil {
			if firstErr == nil {
				firstErr = err
			}
			if tr.Attempts+1 >= memberTransitionMaxAttempts {
				if updateErr := s.abandonMemberTransition(ctx, tr, "member_transition_max_attempts: "+err.Error()); updateErr != nil && firstErr == err {
					firstErr = updateErr
				}
			} else {
				if updateErr := s.deferMemberTransition(ctx, tr, err.Error()); updateErr != nil && firstErr == err {
					firstErr = updateErr
				}
			}
			continue
		}
		processed++
	}
	return processed, firstErr
}

func (s *Service) processMemberTransition(ctx context.Context, tr memberTransition) error {
	if tr.Kind == memberTransitionKindRemove && tr.SubscriptionRevokedAt == 0 && s.subscriptionRevoker != nil {
		s.subscriptionRevoker.RevokeChannelUser(channel.ID(tr.ChannelID), tr.UserID)
		if _, err := s.db.ExecContext(ctx,
			`UPDATE member_transition_outbox SET subscription_revoked_at = ?, updated_at = ? WHERE id = ?`,
			s.nowMs(), s.nowMs(), tr.ID,
		); err != nil {
			return fmt.Errorf("mark subscription revoked: %w", err)
		}
	}
	if s.placementHook == nil {
		return s.deleteMemberTransition(ctx, tr.ID)
	}
	switch tr.Kind {
	case memberTransitionKindAdd:
		err := s.placementHook.OnChannelMembersChanged(ctx, tr.ChannelID, []ChannelMember{{
			ChannelID:     tr.ChannelID,
			UserID:        tr.UserID,
			MemberActorID: tr.MemberActorID,
			Role:          tr.Role,
		}}, nil)
		if err != nil {
			return fmt.Errorf("mirror member add: %w", err)
		}
	case memberTransitionKindRemove:
		if err := s.placementHook.OnChannelMembersChanged(ctx, tr.ChannelID, nil, []string{tr.MemberActorID}); err != nil {
			return fmt.Errorf("mirror member remove: %w", err)
		}
	default:
		return fmt.Errorf("unknown member transition kind %q", tr.Kind)
	}
	return s.deleteMemberTransition(ctx, tr.ID)
}

func (s *Service) deleteMemberTransition(ctx context.Context, id int64) error {
	if _, err := s.db.ExecContext(ctx, `DELETE FROM member_transition_outbox WHERE id = ?`, id); err != nil {
		return fmt.Errorf("delete member transition: %w", err)
	}
	return nil
}

func (s *Service) deferMemberTransition(ctx context.Context, tr memberTransition, reason string) error {
	now := s.nowMs()
	next := now + memberTransitionBackoff(tr.Attempts+1).Milliseconds()
	_, err := s.db.ExecContext(ctx, `
		UPDATE member_transition_outbox
		   SET attempts = ?,
		       last_attempt_at = ?,
		       next_attempt_at = ?,
		       updated_at = ?
		 WHERE id = ? AND terminal_status = ''`,
		tr.Attempts+1, now, next, now, tr.ID,
	)
	if err != nil {
		return fmt.Errorf("defer member transition after %s: %w", reason, err)
	}
	return nil
}

func (s *Service) abandonMemberTransition(ctx context.Context, tr memberTransition, reason string) error {
	now := s.nowMs()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("abandon member transition begin: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	attempts := tr.Attempts
	if attempts < memberTransitionMaxAttempts {
		attempts = memberTransitionMaxAttempts
	}
	res, err := tx.ExecContext(ctx, `
		UPDATE member_transition_outbox
		   SET terminal_status = 'abandoned',
		       abandonment_reason = ?,
		       abandoned_at = ?,
		       attempts = ?,
		       updated_at = ?
		 WHERE id = ? AND terminal_status = ''`,
		reason, now, attempts, now, tr.ID,
	)
	if err != nil {
		return fmt.Errorf("abandon member transition: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("abandon member transition rows: %w", err)
	}
	if n == 0 {
		return nil
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO member_transition_audit_events (
			transition_id, channel_id, member_actor_id, transition_kind,
			event, reason, attempts, created_at
		) VALUES (?, ?, ?, ?, 'member_transition_abandoned', ?, ?, ?)`,
		tr.ID, tr.ChannelID, tr.MemberActorID, tr.Kind, reason, attempts, now,
	); err != nil {
		return fmt.Errorf("log member transition abandon audit: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("abandon member transition commit: %w", err)
	}
	return nil
}

func (s *Service) sweepMemberTransitions(ctx context.Context) error {
	now := s.nowMs()
	cutoff := now - memberTransitionMaxDelay.Milliseconds()
	rows, err := s.db.QueryContext(ctx, `
		SELECT id, channel_id, user_id, member_actor_id, role, transition_kind,
		       attempts, subscription_revoked_at
		  FROM member_transition_outbox
		 WHERE terminal_status = ''
		   AND attempts >= ?
		   AND (last_attempt_at = 0 OR last_attempt_at <= ?)
		 ORDER BY last_attempt_at ASC, id ASC
		 LIMIT ?`,
		memberTransitionMaxAttempts, cutoff, memberTransitionSweepLimit,
	)
	if err != nil {
		return fmt.Errorf("sweep member transitions: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var transitions []memberTransition
	for rows.Next() {
		var tr memberTransition
		if err := rows.Scan(
			&tr.ID, &tr.ChannelID, &tr.UserID, &tr.MemberActorID, &tr.Role,
			&tr.Kind, &tr.Attempts, &tr.SubscriptionRevokedAt,
		); err != nil {
			return fmt.Errorf("scan member transition sweep: %w", err)
		}
		transitions = append(transitions, tr)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("member transition sweep rows: %w", err)
	}
	for _, tr := range transitions {
		if err := s.abandonMemberTransition(ctx, tr, "member_transition_max_attempts"); err != nil {
			return err
		}
	}
	return nil
}

func memberTransitionBackoff(attempts int) time.Duration {
	if attempts < 0 {
		attempts = 0
	}
	delay := memberTransitionBaseDelay
	for i := 0; i < attempts; i++ {
		if delay >= memberTransitionMaxDelay/2 {
			return memberTransitionMaxDelay
		}
		delay *= 2
	}
	if delay > memberTransitionMaxDelay {
		return memberTransitionMaxDelay
	}
	return delay
}

func (s *Service) PendingMemberTransitionCount(ctx context.Context) (int, error) {
	var n int
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM member_transition_outbox WHERE terminal_status = ''`).Scan(&n); err != nil {
		return 0, err
	}
	return n, nil
}
