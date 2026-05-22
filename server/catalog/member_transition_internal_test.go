package catalog

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/wanpengxie/ActOS/server/store"
)

func TestMemberTransition_MaxAttemptsAbandoned(t *testing.T) {
	ctx := context.Background()
	svc, db := newMemberTransitionTestService(t)
	svc.SetPlacementHook(memberTransitionFailHook{})
	now := time.UnixMilli(10_000)
	svc.WithClock(func() time.Time { return now })

	id := insertMemberTransitionForTest(t, db, 9, 0)
	if _, err := svc.ProcessDueMemberTransitions(ctx, 10); err == nil {
		t.Fatal("ProcessDueMemberTransitions err=nil want mirror failure before abandon")
	}

	var status, reason string
	var attempts int
	if err := db.QueryRowContext(ctx,
		`SELECT terminal_status, abandonment_reason, attempts FROM member_transition_outbox WHERE id = ?`,
		id,
	).Scan(&status, &reason, &attempts); err != nil {
		t.Fatalf("query transition: %v", err)
	}
	if status != "abandoned" || attempts != memberTransitionMaxAttempts {
		t.Fatalf("status=%q attempts=%d want abandoned attempts=%d", status, attempts, memberTransitionMaxAttempts)
	}
	if reason == "" {
		t.Fatal("abandonment_reason empty")
	}
	if n, err := svc.PendingMemberTransitionCount(ctx); err != nil || n != 0 {
		t.Fatalf("pending count=%d err=%v want 0", n, err)
	}
}

func TestMemberTransition_AbandonAuditEventLogged(t *testing.T) {
	ctx := context.Background()
	svc, db := newMemberTransitionTestService(t)
	svc.WithClock(func() time.Time { return time.UnixMilli(20_000) })
	id := insertMemberTransitionForTest(t, db, memberTransitionMaxAttempts, 0)

	if err := svc.abandonMemberTransition(ctx, memberTransition{
		ID:            id,
		ChannelID:     "ch-member-transition",
		UserID:        "u2",
		MemberActorID: "user:u2",
		Role:          "member",
		Kind:          memberTransitionKindAdd,
		Attempts:      memberTransitionMaxAttempts,
	}, "member_transition_max_attempts"); err != nil {
		t.Fatalf("abandonMemberTransition: %v", err)
	}

	var event, reason string
	if err := db.QueryRowContext(ctx,
		`SELECT event, reason FROM member_transition_audit_events WHERE transition_id = ?`,
		id,
	).Scan(&event, &reason); err != nil {
		t.Fatalf("query audit event: %v", err)
	}
	if event != "member_transition_abandoned" || reason != "member_transition_max_attempts" {
		t.Fatalf("audit event=%q reason=%q", event, reason)
	}
}

func TestMemberTransition_SweepSelectsAbandonable(t *testing.T) {
	ctx := context.Background()
	svc, db := newMemberTransitionTestService(t)
	now := time.UnixMilli(100_000)
	svc.WithClock(func() time.Time { return now })
	oldAttempt := now.Add(-2 * memberTransitionMaxDelay).UnixMilli()
	id := insertMemberTransitionForTest(t, db, memberTransitionMaxAttempts, oldAttempt)

	if err := svc.sweepMemberTransitions(ctx); err != nil {
		t.Fatalf("sweepMemberTransitions: %v", err)
	}
	var status string
	if err := db.QueryRowContext(ctx,
		`SELECT terminal_status FROM member_transition_outbox WHERE id = ?`,
		id,
	).Scan(&status); err != nil {
		t.Fatalf("query transition: %v", err)
	}
	if status != "abandoned" {
		t.Fatalf("terminal_status=%q want abandoned", status)
	}
}

func newMemberTransitionTestService(t *testing.T) (*Service, *sql.DB) {
	t.Helper()
	ctx := context.Background()
	db, err := store.Open(ctx, filepath.Join(t.TempDir(), "server.db"), store.OpenOptions{})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return NewService(db), db
}

func insertMemberTransitionForTest(t *testing.T, db *sql.DB, attempts int, lastAttemptAt int64) int64 {
	t.Helper()
	ctx := context.Background()
	res, err := db.ExecContext(ctx, `
		INSERT INTO member_transition_outbox (
			channel_id, user_id, member_actor_id, role, transition_kind,
			attempts, last_attempt_at, next_attempt_at, subscription_revoked_at,
			created_at, updated_at
		) VALUES ('ch-member-transition', 'u2', 'user:u2', 'member', 'add',
		          ?, ?, 0, 0, 1, 1)`,
		attempts, lastAttemptAt,
	)
	if err != nil {
		t.Fatalf("insert transition: %v", err)
	}
	id, err := res.LastInsertId()
	if err != nil {
		t.Fatalf("LastInsertId: %v", err)
	}
	return id
}

type memberTransitionFailHook struct{}

func (memberTransitionFailHook) OnChannelCreated(context.Context, Channel, []ChannelMember) error {
	return nil
}

func (memberTransitionFailHook) OnChannelMembersChanged(context.Context, string, []ChannelMember, []string) error {
	return errors.New("daemon offline")
}
