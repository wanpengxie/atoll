package gateway

import (
	"context"
	"path/filepath"
	"strings"
	"testing"

	"github.com/wanpengxie/ActOS/kernel/channel"
	"github.com/wanpengxie/ActOS/kernel/placement"
	"github.com/wanpengxie/ActOS/server/store"
)

func TestRollbackIntent_SagaDivergence_DetectedByCASFailure(t *testing.T) {
	ctx := context.Background()
	db, err := store.Open(ctx, filepath.Join(t.TempDir(), "server.db"), store.OpenOptions{})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = db.Close() }()

	sagaID := "rollback:ch-diverged:req-diverged:1"
	if _, err := db.ExecContext(ctx, `
		INSERT INTO placement_sagas (
			saga_id, channel_id, create_request_id, owner_epoch,
			daemon_id, daemon_connection_epoch, saga_kind, phase,
			sent_at, expected_ack_frame_kind, terminal_status,
			created_at, updated_at
		) VALUES (?, 'ch-diverged', 'req-diverged', 1, 'd-diverged', 1,
		          'rollback', 'completed', 1, 'control.unbind_channel_ack',
		          'completed', 1, 1)`,
		sagaID,
	); err != nil {
		t.Fatalf("insert saga: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO placement_rollback_intents (
			saga_id, channel_id, create_request_id, owner_epoch, daemon_id,
			daemon_connection_epoch, reason, attempts, last_attempt_at,
			next_attempt_at, created_at, updated_at
		) VALUES (?, 'ch-diverged', 'req-diverged', 1, 'd-diverged', 1,
		          'test', 0, 0, 0, 1, 1)`,
		sagaID,
	); err != nil {
		t.Fatalf("insert intent: %v", err)
	}

	app := &App{db: db}
	err = app.incrementRollbackAttempt(ctx, placementRollbackIntent{
		SagaID:          sagaID,
		ChannelID:       channel.ID("ch-diverged"),
		CreateRequestID: placement.CreateRequestID("req-diverged"),
		OwnerEpoch:      placement.OwnerEpoch(1),
		DaemonID:        placement.DaemonID("d-diverged"),
		ConnectionEpoch: placement.ConnectionEpoch(1),
	})
	if err == nil || !strings.Contains(err.Error(), "increment rollback saga attempt") {
		t.Fatalf("incrementRollbackAttempt err=%v want saga CAS failure", err)
	}
}
