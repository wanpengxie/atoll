package app

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/wanpengxie/atoll/platform/channelhost"
	"github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

// convergeTick paces the level backstop. Promptness comes from notify(): every
// realm authority write (decl edit/revoke, daemon delete) pokes the patrol
// after commit, so serving channels converge within milliseconds of a change.
// The tick only bounds how long a MISSED poke (crash between commit and
// notify, a channel that opened between passes) can stay stale.
const convergeTick = 30 * time.Second

// fanoutWorker is the realm's convergence patrol (fanout 轻形化终形): the realm
// registry is the single desired-state truth, publishing is ONE authority
// write + a poke, and this patrol walks every serving channel comparing what
// the channel runs against what the registry says it should run — delivering
// an apply/revoke frame only where they differ. There is no per-channel
// delivery ledger, no all-ack job terminal, and no "propagation complete"
// state: convergence is observed by reading channel truth, never proven by
// collecting receipts. Exactly-once rests on the observation gate (a converged
// channel produces zero deliveries) plus the channel's own anchor idempotency
// and render_seq stale guard — each delivery attempt mints a fresh ref; a
// result-unknown attempt is resolved by re-OBSERVING next pass, not by
// re-asking an old ref.
type fanoutWorker struct {
	app    *App
	ctx    context.Context
	cancel context.CancelFunc
	done   chan struct{}
	wake   chan struct{}
	once   sync.Once
}

func newFanoutWorker(a *App) *fanoutWorker {
	ctx, cancel := context.WithCancel(context.Background())
	return &fanoutWorker{app: a, ctx: ctx, cancel: cancel, done: make(chan struct{}), wake: make(chan struct{}, 1)}
}

func (w *fanoutWorker) start() { go w.run() }

func (w *fanoutWorker) notify() {
	select {
	case w.wake <- struct{}{}:
	default:
	}
}

func (w *fanoutWorker) close() {
	w.once.Do(w.cancel)
	t := time.NewTimer(10 * time.Second)
	defer t.Stop()
	select {
	case <-w.done:
	case <-t.C:
		w.app.logger.Error("fanout_worker_join_timeout")
	}
}

func (w *fanoutWorker) run() {
	defer close(w.done)
	// Startup pass: a poke lost to a crash (authority committed, notify never
	// fired) is recovered here before the first tick.
	w.converge()
	ticker := time.NewTicker(convergeTick)
	defer ticker.Stop()
	for {
		select {
		case <-w.ctx.Done():
			return
		case <-w.wake:
			w.converge()
		case <-ticker.C:
			w.converge()
		}
	}
}

// visitDeadline bounds ONE channel's in-visit work (View reads, realm reads,
// SysOp deliveries) so a stuck channel cannot starve the rest of the pass.
// The keyed channel-lock wait itself is not ctx-aware (known, R6-P2) — the
// deadline starts once the visit holds the lock.
const visitDeadline = 10 * time.Second

// converge is one full patrol pass. A channel that fails to converge is logged
// and left for the next pass — level semantics need no retry bookkeeping. The
// pass summary is a transient observation (log-borne), never a receipt.
func (w *fanoutWorker) converge() {
	started := time.Now()
	channelIDs, err := w.app.directoryChannelIDs(w.ctx)
	if err != nil {
		if !errors.Is(err, context.Canceled) {
			w.app.logger.Error("fanout.directory_read_failed", "err", err)
		}
		return
	}
	visited, failed := 0, 0
	corrections := 0
	for _, chID := range channelIDs {
		if w.ctx.Err() != nil {
			return
		}
		visited++
		n, err := w.convergeChannel(chID)
		corrections += n
		if err != nil && !errors.Is(err, context.Canceled) {
			failed++
			w.app.logger.Warn("fanout.converge_channel", "channel", chID, "err", err)
		}
	}
	if corrections > 0 || failed > 0 {
		w.app.logger.Info("fanout.pass", "channels", visited, "corrections", corrections,
			"failed", failed, "duration", time.Since(started))
	}
}

func (a *App) directoryChannelIDs(ctx context.Context) ([]channel.ID, error) {
	rows, err := a.db.QueryContext(ctx, `SELECT id FROM channels ORDER BY id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []channel.ID
	for rows.Next() {
		var id channel.ID
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		out = append(out, id)
	}
	return out, rows.Err()
}

// convergeChannel converges one channel inside its critical section — the same
// per-channel lock admission delivery and local edit hold, so a patrol
// delivery can never interleave with a local edit's judge→mint→deliver
// sequence (判断段与落账段恒同区). Returns the number of corrective deliveries
// attempted (the pass's transient mismatch count).
func (w *fanoutWorker) convergeChannel(chID channel.ID) (int, error) {
	release := w.app.channelLocks.lock(string(chID))
	defer release()
	visitCtx, cancel := context.WithTimeout(w.ctx, visitDeadline)
	defer cancel()
	bundle, ok := w.app.host.Acquire(chID)
	if !ok {
		// Not serving = zero delivery obligation. The channel converges when it
		// opens: the boot/open path lands it in the directory and the next pass
		// (or poke) walks it.
		return 0, nil
	}
	rows, err := bundle.View().ActiveActors(visitCtx)
	if err != nil {
		return 0, err
	}
	corrections := 0
	var errs []error
	seen := map[string]bool{}
	for _, row := range rows {
		if row.SourceDeclID == "" || seen[row.SourceDeclID] {
			continue
		}
		seen[row.SourceDeclID] = true
		n, err := w.convergeInstance(visitCtx, chID, bundle, row)
		corrections += n
		if err != nil {
			errs = append(errs, fmt.Errorf("decl %s: %w", row.SourceDeclID, err))
		}
	}
	n, err := w.convergeDaemons(visitCtx, chID, bundle)
	corrections += n
	if err != nil {
		errs = append(errs, err)
	}
	return corrections, errors.Join(errs...)
}

// convergeInstance compares one instance's running snapshot against the realm
// registry's desired rendering and delivers the difference: a definitively
// absent/soft-deleted declaration revokes the instance, a changed value
// applies a new version, and an equal digest is the observation gate — zero
// deliveries. Fail-closed: ANY realm read or render error skips this instance
// for the pass (never folded into "absent" — a control-plane fault must not
// amplify into a mass revoke).
func (w *fanoutWorker) convergeInstance(ctx context.Context, chID channel.ID, bundle channelhost.Bundle, current storespec.ActorControlRow) (int, error) {
	declID := current.SourceDeclID
	var class string
	var global sql.NullString
	var deleted sql.NullInt64
	err := w.app.db.QueryRowContext(ctx, `SELECT default_class,config_json,deleted_at FROM actor_decls WHERE id=?`, declID).Scan(&class, &global, &deleted)
	if errors.Is(err, sql.ErrNoRows) || (err == nil && deleted.Valid) {
		// Definitive absence only: ErrNoRows / a read soft-delete row are the
		// authority's own answers. Built-in protection mirrors finalize: a
		// sys:-prefixed declaration missing from the registry is never revoked.
		if strings.HasPrefix(declID, "sys:") {
			return 0, nil
		}
		_, err := bundle.SysOp().RevokeDeclTargets(ctx, channel.RevokeDeclRequest{Ref: convergeRef(chID), DeclID: declID})
		return 1, err
	}
	if err != nil {
		return 0, err
	}
	config := global.String
	var overlay sql.NullString
	err = w.app.db.QueryRowContext(ctx, `SELECT config_json FROM channel_decl_overlays WHERE channel_id=? AND decl_id=?`, string(chID), declID).Scan(&overlay)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return 0, err
	}
	if err == nil && overlay.Valid {
		// Whole-value masking: a channel-local customization shadows the
		// global value for this channel.
		config = overlay.String
	}
	// Realm declarations do not own channel placement or idle policy: desired
	// re-renders the value half (class/config) while retaining the channel's
	// current placement and TIdle.
	placement := channel.Placement{Kind: channel.PlacementKind(current.Placement.Kind), DesiredHost: current.Placement.Host}
	var rawConfig json.RawMessage
	if config != "" {
		rawConfig = json.RawMessage(config)
	}
	candidate, err := (channel.RenderedSnapshot{Class: class, Config: rawConfig, Placement: placement, TIdleMS: current.TIdle.Milliseconds(), RenderSeq: 1}).Seal()
	if err != nil {
		return 0, err
	}
	currentSnapshot, err := (channel.RenderedSnapshot{Class: current.Class, Config: current.Config, Placement: placement, TIdleMS: current.TIdle.Milliseconds(), RenderSeq: current.RenderSeq}).Seal()
	if err != nil {
		return 0, err
	}
	if candidate.Digest == currentSnapshot.Digest {
		return 0, nil
	}
	seq, err := w.mintRenderSeq(ctx, chID, declID, current.RenderSeq)
	if err != nil {
		return 0, err
	}
	candidate.RenderSeq = seq
	_, err = bundle.SysOp().ApplyDeclVersion(ctx, channel.ApplyDeclVersionRequest{
		Ref: convergeRef(chID), DeclID: declID, Rendered: candidate, Authority: channel.AuthorityRealm,
	})
	if err != nil {
		var operationErr *channel.OperationError
		if errors.As(err, &operationErr) && operationErr.Code == channel.ErrCodeRefConflict {
			// Structurally unreachable with fresh refs — a hit is a caller bug
			// signal, kept loud and re-raised every pass until fixed.
			w.app.logger.Error("fanout.ref_conflict", "channel", chID, "decl", declID, "err", err)
			return 1, nil
		}
	}
	return 1, err
}

// convergeDaemons revokes channel bindings whose daemon no longer exists in
// the realm registry (the daemon-delete authority write is the publish; this
// is its convergence half). Fail-closed like convergeInstance: a registry read
// error skips that binding for the pass.
func (w *fanoutWorker) convergeDaemons(ctx context.Context, chID channel.ID, bundle channelhost.Bundle) (int, error) {
	bound, err := bundle.View().ListBound(ctx)
	if err != nil {
		return 0, err
	}
	corrections := 0
	var errs []error
	for _, daemonID := range bound {
		var exists bool
		if err := w.app.db.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM daemons WHERE id=?)`, daemonID).Scan(&exists); err != nil {
			errs = append(errs, err)
			continue
		}
		if exists {
			continue
		}
		corrections++
		if _, err := bundle.SysOp().RevokeDaemon(ctx, channel.DaemonRequest{Ref: convergeRef(chID), DaemonID: daemonID}); err != nil {
			errs = append(errs, fmt.Errorf("daemon %s: %w", daemonID, err))
		}
	}
	return corrections, errors.Join(errs...)
}

// mintRenderSeq claims the next per-channel render seq above the instance's
// current one. Minted-but-undelivered seqs (a transient delivery failure) are
// harmless gaps in a monotonic counter.
func (w *fanoutWorker) mintRenderSeq(ctx context.Context, chID channel.ID, declID string, baseline int64) (int64, error) {
	tx, err := w.app.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `INSERT INTO decl_render_state(channel_id,decl_id,render_seq) VALUES (?,?,?) ON CONFLICT(channel_id,decl_id) DO UPDATE SET render_seq=MAX(render_seq,excluded.render_seq)`, string(chID), declID, baseline); err != nil {
		return 0, err
	}
	var seq int64
	if err := tx.QueryRowContext(ctx, `UPDATE decl_render_state SET render_seq=render_seq+1 WHERE channel_id=? AND decl_id=? RETURNING render_seq`, string(chID), declID).Scan(&seq); err != nil {
		return 0, err
	}
	return seq, tx.Commit()
}

// convergeRef mints a fresh anchored ref for ONE delivery attempt. The
// patrol's exactly-once story is the observation gate before each delivery,
// not ref reuse: re-asking an old ref after an unknown result is replaced by
// re-observing channel truth on the next pass.
func convergeRef(chID channel.ID) string {
	return channel.DerivedFanoutRef("fo:v1:"+uuid.NewString(), chID)
}
