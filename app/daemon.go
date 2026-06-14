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

	"github.com/wanpengxie/ActOS/app/internal/middleware"
	"github.com/wanpengxie/ActOS/protocol/channel"
)

// ---------------------------------------------------------------------------
// Daemon handlers
// ---------------------------------------------------------------------------

func (a *App) handleListDaemons(c *gin.Context) {
	userID := middleware.UserID(c)
	rows, err := a.db.QueryContext(c.Request.Context(),
		`SELECT id, name, status, hostname, platform, last_heartbeat, created_at
		 FROM daemons WHERE owner_id = ?`, userID,
	)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "query failed"})
		return
	}
	defer rows.Close()

	var result []gin.H
	for rows.Next() {
		var id, name, status string
		var hostname, plat sql.NullString
		var lastHB sql.NullInt64
		var createdAt int64
		if err := rows.Scan(&id, &name, &status, &hostname, &plat, &lastHB, &createdAt); err != nil {
			continue
		}
		d := gin.H{
			"id": id, "name": name, "status": status, "created_at": createdAt,
		}
		if hostname.Valid {
			d["hostname"] = hostname.String
		}
		if plat.Valid {
			d["platform"] = plat.String
		}
		if lastHB.Valid {
			d["last_heartbeat"] = lastHB.Int64
		}
		d["attached_channels"] = a.daemonAttachedChannels(c.Request.Context(), id)
		result = append(result, d)
	}
	if result == nil {
		result = []gin.H{}
	}
	c.JSON(http.StatusOK, gin.H{"daemons": result})
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

func (a *App) handleDeleteDaemon(c *gin.Context) {
	daemonID := c.Param("id")
	userID := middleware.UserID(c)

	res, err := a.db.ExecContext(c.Request.Context(),
		`DELETE FROM daemons WHERE id = ? AND owner_id = ?`, daemonID, userID,
	)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "delete failed"})
		return
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		c.JSON(http.StatusNotFound, gin.H{"error": "daemon not found"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"ok": true})
}

func (a *App) handleListChannelDaemons(c *gin.Context) {
	chID, ok := a.requireChannelAccess(c)
	if !ok {
		return
	}
	rows, err := a.db.QueryContext(c.Request.Context(),
		`SELECT d.id, d.name, d.status, d.created_at
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
		var id, name, status string
		var createdAt int64
		if err := rows.Scan(&id, &name, &status, &createdAt); err != nil {
			continue
		}
		result = append(result, gin.H{
			"id": id, "name": name, "status": status, "created_at": createdAt,
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

func (a *App) handleDetachDaemon(c *gin.Context) {
	chID, ok := a.requireChannelAccess(c)
	if !ok {
		return
	}
	daemonID := c.Param("id")

	_, err := a.db.ExecContext(c.Request.Context(),
		`DELETE FROM daemon_channels WHERE daemon_id = ? AND channel_id = ?`,
		daemonID, chID,
	)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "detach failed"})
		return
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
