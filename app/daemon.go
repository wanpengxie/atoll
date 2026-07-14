package app

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"

	"github.com/wanpengxie/atoll/app/internal/middleware"
	"github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

// kickDaemonConverge drives the substrate half of a daemon revocation to
// convergence on ONE channel home: KickDaemon (close every link the home holds
// for computeID) is a hint, so it is executed at least once and then retried a
// bounded number of times until View.IsAttached reports the daemon gone. The
// unconditional first write matters because links register before attach
// publication: IsAttached is a convergence observation, never permission to
// skip the revocation command. A still-attached daemon after the budget is
// logged (not an error — the link teardown is best-effort). No-op if the home
// is not open in this process.
func (a *App) kickDaemonConverge(chID channel.ID, daemonID string) {
	home := a.getHome(chID)
	if home == nil {
		return
	}
	attempts := 0
	for attempts < 3 {
		home.KickDaemon(daemonID)
		attempts++
		if !home.View().IsAttached(daemonID) {
			break
		}
	}
	if home.View().IsAttached(daemonID) {
		a.logger.Warn("app: daemon kick did not converge", "channel", string(chID), "daemon", daemonID)
		return
	}
	// Convergence succeeded — the common case, previously silent. Without this
	// an explicit revocation ("who kicked which daemon off which channel and
	// when") is unreconstructable from slog; only the rare non-convergence
	// path had a trace.
	a.logger.Info("app: daemon kick converged", "channel", string(chID), "daemon", daemonID, "attempts", attempts)
}

// ---------------------------------------------------------------------------
// Daemon handlers
// ---------------------------------------------------------------------------

func (a *App) handleListDaemons(c *gin.Context) {
	userID := middleware.UserID(c)
	rows, err := a.db.QueryContext(c.Request.Context(),
		`SELECT id, name, created_at FROM daemons WHERE owner_id = ?`, userID,
	)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "query failed"})
		return
	}
	defer rows.Close()

	var result []gin.H
	for rows.Next() {
		var id, name string
		var createdAt int64
		if err := rows.Scan(&id, &name, &createdAt); err != nil {
			continue
		}
		chans := a.daemonAttachedChannels(c.Request.Context(), id)
		result = append(result, gin.H{
			"id": id, "name": name, "created_at": createdAt,
			"attached_channels": chans,
			// online = L1 link attachment: a live attach on ANY bound channel right
			// now, read-time from the platform View (out-of-band, no truth-log
			// write — UI polling must not pollute the log).
			"online": a.daemonOnline(channel.ID(""), chans, id),
		})
	}
	if result == nil {
		result = []gin.H{}
	}
	c.JSON(http.StatusOK, gin.H{"daemons": result})
}

// daemonOnline reports whether daemon id has a live link attach right now. It is
// online iff attached on any of its bound channels (or `only`, when non-empty).
// Read-time from each channel-home's View — derived, never a stored column.
func (a *App) daemonOnline(only channel.ID, boundChannels []string, daemonID string) bool {
	check := func(chID channel.ID) bool {
		h := a.getHome(chID)
		return h != nil && h.View().IsAttached(daemonID)
	}
	if only != "" {
		return check(only)
	}
	for _, ch := range boundChannels {
		if check(channel.ID(ch)) {
			return true
		}
	}
	return false
}

func (a *App) daemonAttachedChannels(ctx context.Context, daemonID string) []string {
	rows, err := a.db.QueryContext(ctx,
		`SELECT channel_id FROM daemon_channels WHERE daemon_id = ?`, daemonID,
	)
	if err != nil {
		return []string{}
	}
	defer rows.Close()
	var chans []string
	for rows.Next() {
		var chID string
		if err := rows.Scan(&chID); err == nil {
			chans = append(chans, chID)
		}
	}
	if chans == nil {
		chans = []string{}
	}
	return chans
}

func (a *App) handleCreateDaemon(c *gin.Context) {
	userID := middleware.UserID(c)
	var req struct {
		Name string `json:"name"`
	}
	if err := c.ShouldBindJSON(&req); err != nil || req.Name == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "name required"})
		return
	}

	daemonID := uuid.NewString()
	release := a.daemonLocks.lock(daemonID)
	defer release()
	apiKey := uuid.NewString() // plaintext, returned once
	keyHash := hashAPIKey(apiKey)
	now := time.Now().UnixMilli()

	_, err := a.db.ExecContext(c.Request.Context(),
		`INSERT INTO daemons (id, owner_id, name, api_key_hash, created_at) VALUES (?,?,?,?,?)`,
		daemonID, userID, req.Name, keyHash, now,
	)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "create daemon failed"})
		return
	}

	c.JSON(http.StatusCreated, gin.H{
		"id":      daemonID,
		"name":    req.Name,
		"api_key": apiKey, // returned only once
	})
}

// handleDeleteDaemon revokes a daemon (world-layer scope: the daemon key + its
// cross-channel identity, HTTP-legitimate). Full teardown, in order: delete the
// daemon_channels bindings (intent) INSIDE the tx with RETURNING channel_id — the
// kick set is the rows actually deleted, not a separate pre-tx read (a concurrent
// attach landing between a pre-read and the delete would else leave a residual port
// this delete never sees) → clear desired_host on every composition row placed on
// this daemon, keeping the pool row with placement UNCHANGED (a rehome is a separate
// 指派, never a migration) → delete the daemons row (revocation persisted) → run the
// KickDaemon convergence loop on each channel whose binding was just deleted so live
// links fall silent.
func (a *App) handleDeleteDaemon(c *gin.Context) {
	daemonID := c.Param("id")
	release := a.daemonLocks.lock(daemonID)
	locked := true
	defer func() {
		if locked {
			release()
		}
	}()
	userID := middleware.UserID(c)
	ctx := c.Request.Context()

	var owner string
	err := a.db.QueryRowContext(ctx, `SELECT owner_id FROM daemons WHERE id = ?`, daemonID).Scan(&owner)
	if err == sql.ErrNoRows || (err == nil && owner != userID) {
		c.JSON(http.StatusNotFound, gin.H{"error": "daemon not found"})
		return
	}
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "delete failed"})
		return
	}

	targetSet := map[string]struct{}{}
	rows, err := a.db.QueryContext(ctx, `SELECT channel_id FROM daemon_channels WHERE daemon_id=?`, daemonID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "delete failed"})
		return
	}
	for rows.Next() {
		var ch string
		if err := rows.Scan(&ch); err == nil {
			targetSet[ch] = struct{}{}
		}
	}
	_ = rows.Close()
	a.mu.RLock()
	homes := make(map[channel.ID]interface {
		Composition(context.Context) ([]storespec.CompositionRecord, error)
	}, len(a.homes))
	for id, h := range a.homes {
		homes[id] = h
	}
	a.mu.RUnlock()
	for id, h := range homes {
		composition, err := h.Composition(ctx)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "delete failed"})
			return
		}
		for _, row := range composition {
			if row.DesiredHost == daemonID {
				targetSet[string(id)] = struct{}{}
			}
		}
	}
	var targetIDs []string
	for id := range targetSet {
		targetIDs = append(targetIDs, id)
	}
	sort.Strings(targetIDs)
	targets := make([]daemonFanoutTarget, 0, len(targetIDs))
	for _, id := range targetIDs {
		targets = append(targets, daemonFanoutTarget{ChannelID: id})
	}
	targetsJSON, _ := json.Marshal(targets)

	tx, err := a.db.BeginTx(ctx, nil)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "delete failed"})
		return
	}
	defer func() { _ = tx.Rollback() }() // no-op after a successful Commit
	if _, err := tx.ExecContext(ctx, `DELETE FROM daemon_channels WHERE daemon_id=?`, daemonID); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "delete failed"})
		return
	}
	if _, err := tx.ExecContext(ctx,
		`DELETE FROM daemons WHERE id = ? AND owner_id = ?`, daemonID, userID); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "delete failed"})
		return
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO daemon_revoke_jobs(daemon_id,op,targets_json,created_at) VALUES (?,?,?,?)`, daemonID, "delete", string(targetsJSON), time.Now().UnixMilli()); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "delete failed"})
		return
	}
	if err := tx.Commit(); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "delete failed"})
		return
	}
	// The durable authority decision is complete. Do not hold the daemon
	// authority lock while Kick seals and joins admitted actor handshakes: an
	// already-admitted handshake may itself be waiting to revalidate under this
	// lock. Releasing here lets it observe the deleted binding and fail before
	// publication, while the voluntary link teardown remains quiet.
	release()
	locked = false

	// Only now that the revocation is durable do we Kick the live links to silence.
	// Info marks this as an explicit, deliberate revocation (the daemon's key
	// itself was just deleted) — distinct from "home closing" (platform.home's
	// own bulk teardown) as a source of the same links dying.
	a.logger.Info("app: daemon delete kicking live links", "daemon", daemonID, "channels", targetIDs)
	for _, ch := range targetIDs {
		a.kickDaemonConverge(channel.ID(ch), daemonID)
	}
	if a.fanout != nil {
		a.fanout.notify()
	}
	c.JSON(http.StatusOK, gin.H{"ok": true})
}

func (a *App) handleListChannelDaemons(c *gin.Context) {
	chID, ok := a.requireChannelAccess(c)
	if !ok {
		return
	}
	rows, err := a.db.QueryContext(c.Request.Context(),
		`SELECT d.id, d.name, d.created_at
		 FROM daemons d
		 JOIN daemon_channels dc ON d.id = dc.daemon_id
		 WHERE dc.channel_id = ?`, chID,
	)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "query failed"})
		return
	}
	defer rows.Close()

	var result []gin.H
	for rows.Next() {
		var id, name string
		var createdAt int64
		if err := rows.Scan(&id, &name, &createdAt); err != nil {
			continue
		}
		result = append(result, gin.H{
			"id": id, "name": name, "created_at": createdAt,
			// online = L1 link attachment on THIS channel, read-time from View.
			"online": a.daemonOnline(channel.ID(chID), nil, id),
		})
	}
	if result == nil {
		result = []gin.H{}
	}
	c.JSON(http.StatusOK, gin.H{"daemons": result})
}

func (a *App) handleAttachDaemons(c *gin.Context) {
	chID, ok := a.requireChannelAccess(c)
	if !ok {
		return
	}
	var req struct {
		DaemonIDs []string `json:"daemon_ids"`
	}
	if err := c.ShouldBindJSON(&req); err != nil || len(req.DaemonIDs) == 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "daemon_ids required"})
		return
	}

	userID := middleware.UserID(c)
	for _, did := range req.DaemonIDs {
		release := a.daemonLocks.lock(did)
		var ownerID string
		err := a.db.QueryRowContext(c.Request.Context(),
			`SELECT owner_id FROM daemons WHERE id = ?`, did,
		).Scan(&ownerID)
		if err != nil || ownerID != userID {
			release()
			c.JSON(http.StatusForbidden, gin.H{"error": "daemon not found or not owned by you"})
			return
		}
		_, _ = a.db.ExecContext(c.Request.Context(),
			`INSERT OR IGNORE INTO daemon_channels (daemon_id, channel_id) VALUES (?,?)`,
			did, chID,
		)
		release()
	}
	c.JSON(http.StatusOK, gin.H{"ok": true})
}

// handleDetachDaemon unbinds ONE daemon from ONE channel.收口 order: delete the
// binding (intent) → run the KickDaemon convergence loop on this channel's home so
// the live link drops (not left to natural link death) → clear desired_host on this
// channel's rows placed on the daemon, keeping the pool row with placement
// UNCHANGED (rehome = a later指派, never a migration here).
func (a *App) handleDetachDaemon(c *gin.Context) {
	chID, ok := a.requireChannelAccess(c)
	if !ok {
		return
	}
	daemonID := c.Param("id")
	release := a.daemonLocks.lock(daemonID)
	locked := true
	defer func() {
		if locked {
			release()
		}
	}()
	ctx := c.Request.Context()

	targetsJSON, _ := json.Marshal([]daemonFanoutTarget{{ChannelID: chID}})
	tx, err := a.db.BeginTx(ctx, nil)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "detach failed"})
		return
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx,
		`DELETE FROM daemon_channels WHERE daemon_id = ? AND channel_id = ?`,
		daemonID, chID); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "detach failed"})
		return
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO daemon_revoke_jobs(daemon_id,op,targets_json,created_at) VALUES (?,?,?,?)`, daemonID, "detach", string(targetsJSON), time.Now().UnixMilli()); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "detach failed"})
		return
	}
	if err := tx.Commit(); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "detach failed"})
		return
	}
	// Binding deletion is now authoritative. Release before Kick for the same
	// daemon-lock -> admitted-handshake join ordering used by full revocation.
	release()
	locked = false
	// Info marks this as an explicit, deliberate revocation (a single-channel
	// detach request) — same distinguishing purpose as handleDeleteDaemon's
	// own kick-start marker.
	a.logger.Info("app: daemon detach kicking live link", "channel", string(chID), "daemon", daemonID)
	a.kickDaemonConverge(channel.ID(chID), daemonID)
	if a.fanout != nil {
		a.fanout.notify()
	}
	c.JSON(http.StatusOK, gin.H{"ok": true})
}

// ---------------------------------------------------------------------------
// Auth helper: single path for compute connections
// ---------------------------------------------------------------------------

// authAndResolve verifies the API key, resolves the daemon ID, and checks that
// the daemon is bound to the requested channel. This is the single auth path
// for compute connections -- fleet never does auth itself.
func (a *App) authAndResolve(apiKey string, chID channel.ID) (string, error) {
	keyHash := hashAPIKey(apiKey)
	var daemonID string
	err := a.db.QueryRow(
		`SELECT id FROM daemons WHERE api_key_hash = ?`, keyHash,
	).Scan(&daemonID)
	if err != nil {
		return "", fmt.Errorf("invalid api key")
	}

	// Verify daemon-channel binding.
	var count int
	err = a.db.QueryRow(
		`SELECT COUNT(*) FROM daemon_channels WHERE daemon_id = ? AND channel_id = ?`,
		daemonID, string(chID),
	).Scan(&count)
	if err != nil || count == 0 {
		return "", fmt.Errorf("daemon not bound to channel")
	}

	return daemonID, nil
}

func (a *App) handleCreateAndAttachDaemon(c *gin.Context) {
	chID, ok := a.requireChannelAccess(c)
	if !ok {
		return
	}
	userID := middleware.UserID(c)
	var req struct {
		Name string `json:"name"`
	}
	if err := c.ShouldBindJSON(&req); err != nil || req.Name == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "name required"})
		return
	}

	daemonID := uuid.NewString()
	release := a.daemonLocks.lock(daemonID)
	defer release()
	apiKey := uuid.NewString()
	keyHash := hashAPIKey(apiKey)
	now := time.Now().UnixMilli()

	_, err := a.db.ExecContext(c.Request.Context(),
		`INSERT INTO daemons (id, owner_id, name, api_key_hash, created_at) VALUES (?,?,?,?,?)`,
		daemonID, userID, req.Name, keyHash, now,
	)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "create daemon failed"})
		return
	}
	_, _ = a.db.ExecContext(c.Request.Context(),
		`INSERT OR IGNORE INTO daemon_channels (daemon_id, channel_id) VALUES (?,?)`,
		daemonID, string(chID),
	)

	c.JSON(http.StatusCreated, gin.H{
		"id":      daemonID,
		"name":    req.Name,
		"api_key": apiKey,
	})
}

func hashAPIKey(key string) string {
	h := sha256.Sum256([]byte(key))
	return hex.EncodeToString(h[:])
}
