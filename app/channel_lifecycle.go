package app

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"sync"
	"time"

	"github.com/wanpengxie/atoll/platform/channelhost"
	"github.com/wanpengxie/atoll/platform/channelspec"
	"github.com/wanpengxie/atoll/protocol/channel"
)

const (
	lifecycleTick     = 250 * time.Millisecond
	lifecycleFullScan = 30 * time.Second
)

// lifecycleWorker is a stateless convergence arm. Durable intent exists only
// in channels rows; all retry scheduling below is disposable process memory.
type lifecycleWorker struct {
	app    *App
	ctx    context.Context
	cancel context.CancelFunc
	wake   chan struct{}
	done   chan struct{}
	once   sync.Once

	startMu sync.Mutex
	started bool

	pokeMu sync.Mutex
	poked  map[channel.ID]struct{}

	retryMu   sync.Mutex
	attempts  map[channel.ID]int
	retryAt   map[channel.ID]time.Time
	permanent map[channel.ID]struct{}
}

func newLifecycleWorker(app *App) *lifecycleWorker {
	ctx, cancel := context.WithCancel(context.Background())
	return &lifecycleWorker{
		app: app, ctx: ctx, cancel: cancel,
		wake: make(chan struct{}, 1), done: make(chan struct{}),
		poked:    make(map[channel.ID]struct{}),
		attempts: make(map[channel.ID]int), retryAt: make(map[channel.ID]time.Time),
		permanent: make(map[channel.ID]struct{}),
	}
}

// start launches the arm. It runs AFTER assembly completes (App.Start, called
// from Run): construction must stay side-effect free — the boot full scan
// opens membranes and provisions physical stores, and it must never observe a
// half-assembled App (that ordering is what makes the post-New setter
// injections race-free by structure, not by per-field synchronization).
func (w *lifecycleWorker) start() {
	// One critical section carries idempotence and the close handshake at
	// once: the cancellation check and the started flip are atomic under
	// startMu, and close() reads started under the same lock after
	// cancelling — so either close observes started=true and joins done, or
	// this start observes the cancel and never spawns. A non-atomic pair
	// would let Start slip a worker past a Close that already returned.
	w.startMu.Lock()
	if w.started || w.ctx.Err() != nil {
		w.startMu.Unlock()
		return
	}
	w.started = true
	w.startMu.Unlock()
	go func() {
		defer close(w.done)
		ticker := time.NewTicker(lifecycleTick)
		defer ticker.Stop()
		full := time.NewTicker(lifecycleFullScan)
		defer full.Stop()
		w.fullScan()
		for {
			select {
			case <-w.ctx.Done():
				return
			case <-w.wake:
				w.lightScan()
			case <-ticker.C:
				w.lightScan()
			case <-full.C:
				w.fullScan()
			}
		}
	}()
}

func (w *lifecycleWorker) notify(id channel.ID) {
	if id == "" {
		return
	}
	w.pokeMu.Lock()
	w.poked[id] = struct{}{}
	w.pokeMu.Unlock()
	select {
	case w.wake <- struct{}{}:
	default:
	}
}

func (w *lifecycleWorker) close() {
	w.once.Do(func() {
		w.cancel()
		w.startMu.Lock()
		started := w.started
		w.startMu.Unlock()
		if started {
			<-w.done
		}
	})
}

func (w *lifecycleWorker) lightScan() {
	w.pokeMu.Lock()
	ids := w.poked
	w.poked = make(map[channel.ID]struct{})
	w.pokeMu.Unlock()
	for id := range ids {
		if !w.ready(id) {
			// Backoff not yet elapsed → keep for the next tick. Permanently
			// failed ids are dropped instead: retrying them is pointless and
			// re-queueing would spin the light scan forever.
			if !w.stopped(id) {
				w.retainRetry(id)
			}
			continue
		}
		w.app.convergeChannel(w.ctx, id)
		if w.retryPending(id) {
			w.retainRetry(id)
		}
	}
}

func (w *lifecycleWorker) stopped(id channel.ID) bool {
	w.retryMu.Lock()
	defer w.retryMu.Unlock()
	_, stopped := w.permanent[id]
	return stopped
}

func (w *lifecycleWorker) retainRetry(id channel.ID) {
	w.pokeMu.Lock()
	w.poked[id] = struct{}{}
	w.pokeMu.Unlock()
}

func (w *lifecycleWorker) fullScan() {
	rows, err := w.app.db.QueryContext(w.ctx, `SELECT id FROM channels ORDER BY id`)
	if err != nil {
		w.app.logger.Warn("channel desired scan failed", "err", err)
		return
	}
	var ids []channel.ID
	for rows.Next() {
		var id channel.ID
		if err := rows.Scan(&id); err != nil {
			_ = rows.Close()
			w.app.logger.Warn("channel desired scan failed", "err", err)
			return
		}
		ids = append(ids, id)
	}
	if err := rows.Close(); err != nil {
		w.app.logger.Warn("channel desired scan failed", "err", err)
		return
	}
	for _, id := range ids {
		w.app.convergeChannel(w.ctx, id)
	}
	entries, err := w.app.host.Census(w.ctx)
	if err != nil {
		w.app.logger.Warn("channel census failed", "err", err)
		return
	}
	for _, entry := range entries {
		var exists bool
		if err := w.app.db.QueryRowContext(w.ctx,
			`SELECT EXISTS(SELECT 1 FROM channels WHERE id=?)`, string(entry.ChannelID),
		).Scan(&exists); err != nil {
			w.app.logger.Warn("orphan desired check failed", "channel", entry.ChannelID, "err", err)
			continue
		}
		if exists {
			continue
		}
		release := w.app.channelLocks.lock(string(entry.ChannelID))
		err := w.app.host.Destroy(w.ctx, entry.ChannelID)
		release()
		if err != nil {
			w.app.logger.Warn("orphan channel cleanup failed", "channel", entry.ChannelID, "err", err)
		}
	}
}

type desiredChannel struct {
	ID       channel.ID
	Name     string
	Type     string
	Status   string
	Owner    string
	SpecJSON string
	Created  int64
	Parent   sql.NullString
}

func (a *App) loadDesiredChannel(ctx context.Context, id channel.ID) (desiredChannel, error) {
	var desired desiredChannel
	err := a.db.QueryRowContext(ctx, `SELECT id,name,type,status,owner_principal,spec_json,created_at,parent_id
		FROM channels WHERE id=?`, string(id)).
		Scan(&desired.ID, &desired.Name, &desired.Type, &desired.Status, &desired.Owner,
			&desired.SpecJSON, &desired.Created, &desired.Parent)
	return desired, err
}

// convergeChannel is also the create handler's bounded best-effort fast path.
// It changes only physical actual state and terminally removes a retired row.
func (a *App) convergeChannel(ctx context.Context, id channel.ID) {
	if a.lifecycle != nil && !a.lifecycle.ready(id) {
		return
	}
	release := a.channelLocks.lock(string(id))
	defer release()
	desired, err := a.loadDesiredChannel(ctx, id)
	if errors.Is(err, sql.ErrNoRows) {
		// The row is gone entirely: drop every trace, including a permanent
		// mark, so an externally removed channel leaks nothing in memory.
		a.resetLifecycleForStatusChange(id)
		return
	}
	if err != nil {
		a.deferLifecycle(id, err)
		return
	}
	switch desired.Status {
	case "present":
		err = a.convergePresent(ctx, desired)
	case "retiring":
		err = a.convergeRetiring(ctx, desired)
	default:
		err = errors.New("invalid desired channel status")
	}
	if err == nil {
		a.clearLifecycleRetry(id)
	}
}

func (a *App) convergePresent(ctx context.Context, desired desiredChannel) error {
	open := channelhost.OpenSpec{ChannelID: desired.ID, ExpectedType: desired.Type}
	err := a.host.Open(ctx, open)
	if err == nil {
		return nil
	}
	if !errors.Is(err, channelhost.ErrChannelNotFound) {
		return a.classifyLifecycleError(desired.ID, "open", err)
	}
	var spec channelhost.ProvisionSpec
	if err := json.Unmarshal([]byte(desired.SpecJSON), &spec); err != nil || spec.ChannelID != desired.ID {
		if err == nil {
			err = errors.New("provision spec channel mismatch")
		}
		return a.permanentLifecycle(desired.ID, "decode", err)
	}
	// ErrChannelNotFound is not itself proof of physical absence. Provision's
	// ErrServing/ErrChannelRetired guards run before its unpublished-image
	// cleanup and are the authoritative protection against destructive rebuild.
	if _, err := a.host.Provision(ctx, spec); err != nil {
		return a.classifyLifecycleError(desired.ID, "provision", err)
	}
	if err := a.host.Open(ctx, open); err != nil {
		return a.classifyLifecycleError(desired.ID, "open", err)
	}
	return nil
}

func (a *App) convergeRetiring(ctx context.Context, desired desiredChannel) error {
	if err := a.host.Destroy(ctx, desired.ID); err != nil {
		if errors.Is(err, channelhost.ErrInvalidChannelID) ||
			errors.Is(err, channelhost.ErrTombstoneExists) {
			return a.permanentLifecycle(desired.ID, "destroy", err)
		}
		return a.deferLifecycle(desired.ID, err)
	}
	res, err := a.db.ExecContext(ctx,
		`DELETE FROM channels WHERE id=? AND status='retiring'`, string(desired.ID))
	if err != nil {
		return a.deferLifecycle(desired.ID, err)
	}
	changed, err := res.RowsAffected()
	if err != nil {
		return a.deferLifecycle(desired.ID, err)
	}
	if changed != 0 {
		if err := a.relations.Apply(ctx, desired.ID, []channelspec.RelationDelta{{
			Kind: channelspec.RelationGone, ChannelID: desired.ID,
		}}); err != nil {
			a.logger.Warn("retired channel relation cleanup failed", "channel", desired.ID, "err", err)
		}
	}
	return nil
}

func (a *App) classifyLifecycleError(id channel.ID, phase string, err error) error {
	permanent := errors.Is(err, channelhost.ErrChannelRetired) ||
		errors.Is(err, channelhost.ErrInvalidChannelID) ||
		errors.Is(err, channelhost.ErrSchemaIncompatible) ||
		errors.Is(err, channelhost.ErrOwnerInvariant)
	if permanent {
		return a.permanentLifecycle(id, phase, err)
	}
	return a.deferLifecycle(id, err)
}

func (a *App) permanentLifecycle(id channel.ID, phase string, err error) error {
	a.logger.Error("channel convergence permanently failed", "channel", id, "phase", phase, "err", err)
	if a.lifecycle != nil {
		a.lifecycle.retryMu.Lock()
		a.lifecycle.permanent[id] = struct{}{}
		a.lifecycle.retryMu.Unlock()
	}
	return err
}

func (a *App) deferLifecycle(id channel.ID, err error) error {
	a.logger.Warn("channel convergence deferred", "channel", id, "err", err)
	if a.lifecycle != nil {
		a.lifecycle.retryMu.Lock()
		attempt := a.lifecycle.attempts[id] + 1
		a.lifecycle.attempts[id] = attempt
		a.lifecycle.retryAt[id] = time.Now().Add(backoff(attempt))
		a.lifecycle.retryMu.Unlock()
		a.lifecycle.notify(id)
	}
	return err
}

// pokeLifecycle nil-guards the arm like every other App-side accessor: test
// fixtures build App without a worker.
func (a *App) pokeLifecycle(id channel.ID) {
	if a.lifecycle != nil {
		a.lifecycle.notify(id)
	}
}

func (a *App) clearLifecycleRetry(id channel.ID) {
	if a.lifecycle == nil {
		return
	}
	a.lifecycle.retryMu.Lock()
	delete(a.lifecycle.attempts, id)
	delete(a.lifecycle.retryAt, id)
	a.lifecycle.retryMu.Unlock()
}

func (a *App) resetLifecycleForStatusChange(id channel.ID) {
	if a.lifecycle == nil {
		return
	}
	a.lifecycle.retryMu.Lock()
	delete(a.lifecycle.attempts, id)
	delete(a.lifecycle.retryAt, id)
	delete(a.lifecycle.permanent, id)
	a.lifecycle.retryMu.Unlock()
}

func (w *lifecycleWorker) ready(id channel.ID) bool {
	w.retryMu.Lock()
	defer w.retryMu.Unlock()
	if _, stopped := w.permanent[id]; stopped {
		return false
	}
	return !time.Now().Before(w.retryAt[id])
}

func (w *lifecycleWorker) retryPending(id channel.ID) bool {
	w.retryMu.Lock()
	defer w.retryMu.Unlock()
	if _, stopped := w.permanent[id]; stopped {
		return false
	}
	_, pending := w.attempts[id]
	return pending
}

func backoff(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	if attempt > 8 {
		attempt = 8
	}
	return time.Duration(1<<(attempt-1)) * lifecycleTick
}
