package app

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"

	"github.com/wanpengxie/atoll/app/internal/middleware"
	"github.com/wanpengxie/atoll/protocol/channel"
)

// kickDaemonConverge drives the substrate half of a daemon revocation to
// convergence on ONE channel home: KickDaemon (close every link the home holds
// for computeID) is a hint, so it is retried a bounded number of times until
// View.IsAttached reports the daemon gone, and a still-attached daemon after the
// budget is logged (not an error — the link teardown is best-effort). No-op if the
// home is not open in this process.
func (a *App) kickDaemonConverge(chID channel.ID, daemonID string) {
	home := a.getHome(chID)
	if home == nil {
		return
	}
	for i := 0; i < 3 && home.View().IsAttached(daemonID); i++ {
		home.KickDaemon(daemonID)
	}
	if home.View().IsAttached(daemonID) {
		a.logger.Warn("app: daemon kick did not converge", "channel", string(chID), "daemon", daemonID)
	}
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
// cross-channel identity, HTTP-legitimate). Full teardown, in order: capture the
// bound channels → delete the daemon_channels bindings (intent) → clear
// desired_host on every composition row placed on this daemon, keeping the pool row
// with placement UNCHANGED (a rehome is a separate指派, never a migration) → delete
// the daemons row (revocation persisted) → run the KickDaemon convergence loop on
// each formerly-bound home so live links fall silent.
func (a *App) handleDeleteDaemon(c *gin.Context) {
	daemonID := c.Param("id")
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
	bound := a.daemonAttachedChannels(ctx, daemonID)

	if _, err := a.db.ExecContext(ctx,
		`DELETE FROM daemon_channels WHERE daemon_id = ?`, daemonID); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "delete failed"})
		return
	}
	// Clear desired_host on this daemon's rows (placement恒不变; the pool row stays,
	// no daemon claims it, the live cell is simply absent until a re-指派).
	_, _ = a.db.ExecContext(ctx,
		`UPDATE channel_actors SET desired_host = '' WHERE desired_host = ?`, daemonID)
	_, _ = a.db.ExecContext(ctx,
		`DELETE FROM daemons WHERE id = ? AND owner_id = ?`, daemonID, userID)

	for _, ch := range bound {
		a.kickDaemonConverge(channel.ID(ch), daemonID)
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
		var ownerID string
		err := a.db.QueryRowContext(c.Request.Context(),
			`SELECT owner_id FROM daemons WHERE id = ?`, did,
		).Scan(&ownerID)
		if err != nil || ownerID != userID {
			c.JSON(http.StatusForbidden, gin.H{"error": "daemon not found or not owned by you"})
			return
		}
		_, _ = a.db.ExecContext(c.Request.Context(),
			`INSERT OR IGNORE INTO daemon_channels (daemon_id, channel_id) VALUES (?,?)`,
			did, chID,
		)
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
	ctx := c.Request.Context()

	if _, err := a.db.ExecContext(ctx,
		`DELETE FROM daemon_channels WHERE daemon_id = ? AND channel_id = ?`,
		daemonID, chID); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "detach failed"})
		return
	}
	a.kickDaemonConverge(channel.ID(chID), daemonID)
	_, _ = a.db.ExecContext(ctx,
		`UPDATE channel_actors SET desired_host = '' WHERE channel_id = ? AND desired_host = ?`,
		chID, daemonID)
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
