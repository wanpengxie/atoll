package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/wanpengxie/ActOS/kernel/channel"
	"github.com/wanpengxie/ActOS/kernel/placement"
)

// ChannelLockRow mirrors the channel_lock table row (launch-T3 — daemon
// side fencing fields per T1.4).
type ChannelLockRow struct {
	ChannelID    channel.ID
	FencingToken placement.FencingToken
	OwnerEpoch   placement.OwnerEpoch
	DaemonID     placement.DaemonID
	DaemonEpoch  placement.DaemonEpoch
	AcquiredAt   int64
	RefreshedAt  int64
	// ChannelType is the L4 channel-template key (M1.6-T5 phase-2). Empty
	// means "no template" — legacy group channels. Set by the daemon's
	// handleCreateChannel from the server-supplied CreateChannelRequest;
	// surfaced back into ChannelHooks on every (cold or hot) channel boot.
	ChannelType string
}

// ChannelLock owns the single-row channel_lock table.
//
// L1.4 + T3 semantics:
//   - At-most-one row per channel sqlite (row keyed by channel_id, but
//     since each channel has its own sqlite, the row is effectively
//     singleton).
//   - INSERT happens during create_channel ACK preparation (lifecycle/create).
//   - RefreshDaemon bumps daemon_epoch on every daemon process start
//     (regardless of whether ownership changes) so stale worker IPC
//     after daemon restart fails fence_check.
type ChannelLock struct {
	db *sql.DB
}

// NewChannelLock returns a *ChannelLock bound to a channel sqlite.
func NewChannelLock(db *sql.DB) *ChannelLock { return &ChannelLock{db: db} }

// Get returns the single channel_lock row.
// Returns ok=false when no row exists (channel never bootstrapped).
func (l *ChannelLock) Get(ctx context.Context) (ChannelLockRow, bool, error) {
	const q = `SELECT channel_id, fencing_token, owner_epoch, daemon_id, daemon_epoch,
	                 acquired_at, refreshed_at, channel_type
	            FROM channel_lock LIMIT 1`
	var row ChannelLockRow
	var (
		cid, did string
		ft       string
		oe       int64
		de       int64
		chType   string
	)
	err := l.db.QueryRowContext(ctx, q).Scan(&cid, &ft, &oe, &did, &de,
		&row.AcquiredAt, &row.RefreshedAt, &chType)
	if errors.Is(err, sql.ErrNoRows) {
		return ChannelLockRow{}, false, nil
	}
	if err != nil {
		return ChannelLockRow{}, false, fmt.Errorf("store: lock get: %w", err)
	}
	row.ChannelID = channel.ID(cid)
	row.FencingToken = placement.FencingToken(ft)
	row.OwnerEpoch = placement.OwnerEpoch(oe)
	row.DaemonID = placement.DaemonID(did)
	row.DaemonEpoch = placement.DaemonEpoch(de)
	row.ChannelType = chType
	return row, true, nil
}

// Insert creates the channel_lock row. Fails if a row already exists.
// Caller (lifecycle/create.go) handles the "exists with matching
// fencing_token" idempotent path BEFORE calling Insert.
func (l *ChannelLock) Insert(ctx context.Context, row ChannelLockRow) error {
	if row.ChannelID == "" {
		return errors.New("store: channel_lock insert: empty channel_id")
	}
	const q = `INSERT INTO channel_lock
	   (channel_id, fencing_token, owner_epoch, daemon_id, daemon_epoch, acquired_at, refreshed_at, channel_type)
	   VALUES (?, ?, ?, ?, ?, ?, ?, ?)`
	if _, err := l.db.ExecContext(ctx, q,
		string(row.ChannelID), string(row.FencingToken), int64(row.OwnerEpoch),
		string(row.DaemonID), int64(row.DaemonEpoch),
		row.AcquiredAt, row.RefreshedAt, row.ChannelType,
	); err != nil {
		return fmt.Errorf("store: channel_lock insert: %w", err)
	}
	return nil
}

// RefreshDaemon updates daemon_epoch + refreshed_at without changing
// fencing_token or owner_epoch. Called during boot phase 1 for every
// channel the daemon claims to own, so stale worker IPC fails
// fence_check immediately.
func (l *ChannelLock) RefreshDaemon(
	ctx context.Context,
	daemonEpoch placement.DaemonEpoch,
	refreshedAt int64,
) error {
	const q = `UPDATE channel_lock SET daemon_epoch=?, refreshed_at=?`
	if _, err := l.db.ExecContext(ctx, q, int64(daemonEpoch), refreshedAt); err != nil {
		return fmt.Errorf("store: channel_lock refresh daemon: %w", err)
	}
	return nil
}

// Takeover rotates the daemon ownership tuple during server-initiated
// reclaim. It UPDATEs the existing channel_lock row only; reclaim must
// never INSERT a new lock row because channel creation already happened.
func (l *ChannelLock) Takeover(ctx context.Context, row ChannelLockRow, previousOwnerEpoch placement.OwnerEpoch) error {
	if row.ChannelID == "" {
		return errors.New("store: channel_lock takeover: empty channel_id")
	}
	if row.FencingToken == "" {
		return errors.New("store: channel_lock takeover: empty fencing_token")
	}
	const q = `UPDATE channel_lock
	             SET fencing_token = ?,
	                 owner_epoch = ?,
	                 daemon_id = ?,
	                 daemon_epoch = ?,
	                 acquired_at = ?,
	                 refreshed_at = ?,
	                 channel_type = ?
	           WHERE channel_id = ?
	             AND owner_epoch = ?`
	res, err := l.db.ExecContext(ctx, q,
		string(row.FencingToken), int64(row.OwnerEpoch),
		string(row.DaemonID), int64(row.DaemonEpoch),
		row.AcquiredAt, row.RefreshedAt, row.ChannelType,
		string(row.ChannelID), int64(previousOwnerEpoch),
	)
	if err != nil {
		return fmt.Errorf("store: channel_lock takeover: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("store: channel_lock takeover rows: %w", err)
	}
	if n != 1 {
		return fmt.Errorf("store: channel_lock takeover lost CAS for %s", row.ChannelID)
	}
	return nil
}

// ValidateWrite is the fencing gate for non-tx callers (e.g.
// lifecycle/FencingChecker pre-flight at IPC boundary). It performs a
// standalone Get → compare. For sqlite-mutation paths the caller MUST
// instead use ValidateWriteTx so the check runs in the SAME transaction
// as the INSERT and a concurrent RefreshDaemon cannot slip between read
// and write (FIX-T6).
//
// Returns nil when the supplied fencing_token + daemon_epoch match the
// stored row; ErrFencingStale wrapper otherwise.
func (l *ChannelLock) ValidateWrite(
	ctx context.Context,
	fencingToken placement.FencingToken,
	daemonEpoch placement.DaemonEpoch,
) error {
	row, ok, err := l.Get(ctx)
	if err != nil {
		return err
	}
	return validateRow(row, ok, fencingToken, daemonEpoch)
}

// ValidateWriteTx is the in-transaction fencing gate. Every
// channel-local mutation (messages.Append, ledger.Reserve/Commit, etc.)
// MUST call this BEFORE the actual write, inside the same *sql.Tx, so
// the row-level lock semantics protect against a concurrent
// RefreshDaemon racing with the write.
//
// Returns ErrFencingStale when the channel_lock row is missing OR the
// fencing tuple does not match. The caller is expected to rollback the
// transaction and surface HarnessWorkerFencingStale to the principal.
func (l *ChannelLock) ValidateWriteTx(
	ctx context.Context,
	tx *sql.Tx,
	fencingToken placement.FencingToken,
	daemonEpoch placement.DaemonEpoch,
) error {
	if tx == nil {
		return errors.New("store: ValidateWriteTx nil tx")
	}
	const q = `SELECT fencing_token, daemon_epoch FROM channel_lock LIMIT 1`
	var ft string
	var de int64
	err := tx.QueryRowContext(ctx, q).Scan(&ft, &de)
	if errors.Is(err, sql.ErrNoRows) {
		return &FencingStaleError{Reason: "channel_lock row missing"}
	}
	if err != nil {
		return fmt.Errorf("store: validate_write_tx: %w", err)
	}
	row := ChannelLockRow{
		FencingToken: placement.FencingToken(ft),
		DaemonEpoch:  placement.DaemonEpoch(de),
	}
	return validateRow(row, true, fencingToken, daemonEpoch)
}

// validateRow is the shared compare used by both ValidateWrite (non-tx)
// and ValidateWriteTx (in-tx). Returns a typed FencingStaleError so
// callers can map to message.HarnessWorkerFencingStale.
func validateRow(
	row ChannelLockRow,
	ok bool,
	fencingToken placement.FencingToken,
	daemonEpoch placement.DaemonEpoch,
) error {
	if !ok {
		return &FencingStaleError{Reason: "channel_lock row missing"}
	}
	if row.FencingToken != fencingToken {
		return &FencingStaleError{
			HaveToken: row.FencingToken,
			GotToken:  fencingToken,
			HaveEpoch: row.DaemonEpoch,
			GotEpoch:  daemonEpoch,
			Reason:    fmt.Sprintf("fencing_token mismatch (have=%q got=%q)", row.FencingToken, fencingToken),
		}
	}
	if row.DaemonEpoch != daemonEpoch {
		return &FencingStaleError{
			HaveToken: row.FencingToken,
			GotToken:  fencingToken,
			HaveEpoch: row.DaemonEpoch,
			GotEpoch:  daemonEpoch,
			Reason:    fmt.Sprintf("daemon_epoch mismatch (have=%d got=%d)", row.DaemonEpoch, daemonEpoch),
		}
	}
	return nil
}

// FencingStaleError is the typed error returned by both ValidateWrite
// flavors when the caller's (token, epoch) tuple does not match the
// stored channel_lock row. Callers map this to
// message.HarnessWorkerFencingStale per L1 §10.3.1.
type FencingStaleError struct {
	HaveToken placement.FencingToken
	GotToken  placement.FencingToken
	HaveEpoch placement.DaemonEpoch
	GotEpoch  placement.DaemonEpoch
	Reason    string
}

// Error implements error.
func (e *FencingStaleError) Error() string {
	if e == nil {
		return ""
	}
	if e.Reason != "" {
		return "store: fencing stale: " + e.Reason
	}
	return fmt.Sprintf(
		"store: fencing stale (have token=%q epoch=%d, got token=%q epoch=%d)",
		e.HaveToken, e.HaveEpoch, e.GotToken, e.GotEpoch,
	)
}

// IsFencingStale reports whether err is (or wraps) a FencingStaleError.
func IsFencingStale(err error) bool {
	var fse *FencingStaleError
	return errors.As(err, &fse)
}
