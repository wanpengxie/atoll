package app

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"

	"github.com/wanpengxie/atoll/app/internal/middleware"
	"github.com/wanpengxie/atoll/protocol/channel"
)

// ---------------------------------------------------------------------------
// Daemon handlers
// ---------------------------------------------------------------------------

func (a *App) handleListDaemons(c *gin.Context) {
	userID := middleware.UserID(c)
	rows, err := a.db.QueryContext(c.Request.Context(),
		`SELECT id, name, created_at FROM daemons WHERE owner_id = ? AND deleted_at IS NULL`, userID,
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
			"online": a.daemonOnline(c.Request.Context(), "", id),
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
func (a *App) daemonOnline(ctx context.Context, only channel.ID, daemonID string) bool {
	check := func(chID channel.ID) bool {
		bundle, ok := a.host.Acquire(chID)
		return ok && bundle.View().IsAttached(daemonID)
	}
	if only != "" {
		return check(only)
	}
	ids, _ := a.directoryChannelIDs(ctx)
	for _, ch := range ids {
		if check(ch) {
			return true
		}
	}
	return false
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

// handleDeleteDaemon commits a permanent realm tombstone. Channel Homes pull
// that value and detach their own bindings; this handler only observes serving
// channels for a bounded convenience response.
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
	var deletedAt sql.NullInt64
	err := a.db.QueryRowContext(ctx, `SELECT owner_id,deleted_at FROM daemons WHERE id = ?`, daemonID).Scan(&owner, &deletedAt)
	if err == sql.ErrNoRows || (err == nil && owner != userID) {
		c.JSON(http.StatusNotFound, gin.H{"error": "daemon not found"})
		return
	}
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "delete failed"})
		return
	}

	if !deletedAt.Valid {
		if _, err := a.db.ExecContext(ctx,
			`UPDATE daemons SET deleted_at=? WHERE id=? AND owner_id=? AND deleted_at IS NULL`, time.Now().UnixMilli(), daemonID, userID); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "delete failed"})
			return
		}
	}
	release()
	locked = false
	a.pokeAllChannels(ctx)
	convergence := "convergence_pending"
	deadline := time.Now().Add(time.Second)
	for {
		if a.daemonConvergenceObservedClear(ctx, daemonID) {
			convergence = "observed_clear"
			break
		}
		if time.Now().After(deadline) || ctx.Err() != nil {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	c.JSON(http.StatusOK, gin.H{"ok": true, "authority_committed": true, "convergence": convergence})
}

func (a *App) daemonConvergenceObservedClear(ctx context.Context, daemonID string) bool {
	ids, err := a.directoryChannelIDs(ctx)
	if err != nil {
		return false
	}
	for _, chID := range ids {
		bundle, serving := a.host.Acquire(chID)
		if !serving {
			continue
		}
		bound, err := bundle.View().IsBound(ctx, daemonID)
		if err != nil || bound || bundle.View().IsAttached(daemonID) {
			return false
		}
	}
	return true
}

func (a *App) handleListChannelDaemons(c *gin.Context) {
	chID, ok := a.requireChannelAccess(c)
	if !ok {
		return
	}
	bundle, available := a.host.Acquire(channel.ID(chID))
	if !available {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "channel unavailable"})
		return
	}
	rows, err := a.db.QueryContext(c.Request.Context(), `SELECT id,name,created_at FROM daemons WHERE owner_id=? AND deleted_at IS NULL ORDER BY id`, middleware.UserID(c))
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
		bound, err := bundle.View().IsBound(c.Request.Context(), id)
		if err != nil || !bound {
			continue
		}
		result = append(result, gin.H{
			"id": id, "name": name, "created_at": createdAt,
			"online": a.daemonOnline(c.Request.Context(), channel.ID(chID), id),
		})
	}
	if result == nil {
		result = []gin.H{}
	}
	c.JSON(http.StatusOK, gin.H{"daemons": result})
}

func (a *App) handleAttachDaemon(c *gin.Context) {
	chID, ok := a.requireChannelAccess(c)
	if !ok {
		return
	}
	var req struct {
		DaemonID string `json:"daemon_id"`
	}
	if err := c.ShouldBindJSON(&req); err != nil || strings.TrimSpace(req.DaemonID) == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "daemon_id required"})
		return
	}
	req.DaemonID = strings.TrimSpace(req.DaemonID)
	userID := middleware.UserID(c)
	var ownerID string
	err := a.db.QueryRowContext(c.Request.Context(), `SELECT owner_id FROM daemons WHERE id = ? AND deleted_at IS NULL`, req.DaemonID).Scan(&ownerID)
	if err != nil || ownerID != userID {
		c.JSON(http.StatusForbidden, gin.H{"error": "daemon not found or not owned by you"})
		return
	}
	intent := struct {
		DaemonID string `json:"daemon_id"`
	}{req.DaemonID}
	record, _, err := a.admission.submit(c.Request.Context(), admissionCommand{
		ChannelID: channel.ID(chID), Op: "attach", Owner: principalAdmissionOwner(userID), IdempotencyKey: c.GetHeader("Idempotency-Key"), Intent: intent,
		BuildRequest: func(ref string) any { return channel.DaemonRequest{Ref: ref, DaemonID: req.DaemonID} },
	})
	respondAdmissionRecord(c, record, err, http.StatusOK)
}

// handleDetachDaemon records one exact channel operation. The membrane commits
// binding removal and instance cleanup together, then kicks any live link as a
// post-commit convergence hint.
func (a *App) handleDetachDaemon(c *gin.Context) {
	chID, ok := a.requireChannelAccess(c)
	if !ok {
		return
	}
	daemonID := c.Param("id")
	ctx := c.Request.Context()
	var owner string
	if err := a.db.QueryRowContext(ctx, `SELECT owner_id FROM daemons WHERE id=?`, daemonID).Scan(&owner); err != nil || owner != middleware.UserID(c) {
		c.JSON(http.StatusForbidden, gin.H{"error": "daemon not found or not owned by you"})
		return
	}
	intent := struct {
		DaemonID string `json:"daemon_id"`
	}{daemonID}
	record, _, err := a.admission.submit(ctx, admissionCommand{
		ChannelID: channel.ID(chID), Op: "detach", Owner: principalAdmissionOwner(owner), IdempotencyKey: c.GetHeader("Idempotency-Key"), Intent: intent,
		BuildRequest: func(ref string) any { return channel.DaemonRequest{Ref: ref, DaemonID: daemonID} },
	})
	respondAdmissionRecord(c, record, err, http.StatusOK)
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
		`SELECT id FROM daemons WHERE api_key_hash = ? AND deleted_at IS NULL`, keyHash,
	).Scan(&daemonID)
	if err != nil {
		return "", fmt.Errorf("invalid api key")
	}

	bundle, ok := a.host.Acquire(chID)
	if !ok {
		return "", fmt.Errorf("channel unavailable")
	}
	bound, err := bundle.View().IsBound(context.Background(), daemonID)
	if err != nil || !bound {
		return "", fmt.Errorf("daemon not bound to channel")
	}

	return daemonID, nil
}

func hashAPIKey(key string) string {
	h := sha256.Sum256([]byte(key))
	return hex.EncodeToString(h[:])
}
