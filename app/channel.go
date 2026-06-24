package app

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"

	"github.com/wanpengxie/ActOS/app/internal/middleware"
	"github.com/wanpengxie/ActOS/protocol/actor"
	"github.com/wanpengxie/ActOS/protocol/channel"
)

// ---------------------------------------------------------------------------
// Channel handlers
// ---------------------------------------------------------------------------

func (a *App) handleListChannels(c *gin.Context) {
	wsID := c.Param("wsID")
	userID := middleware.UserID(c)
	if !a.isWorkspaceMember(c.Request.Context(), wsID, userID) {
		c.JSON(http.StatusForbidden, gin.H{"error": "not a workspace member"})
		return
	}
	rows, err := a.db.QueryContext(c.Request.Context(),
		`SELECT id, workspace_id, name, type, created_at FROM channels WHERE workspace_id = ?`, wsID,
	)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "query failed"})
		return
	}
	defer rows.Close()

	var result []gin.H
	for rows.Next() {
		var id, workspaceID, name, chType string
		var createdAt int64
		if err := rows.Scan(&id, &workspaceID, &name, &chType, &createdAt); err != nil {
			continue
		}
		result = append(result, gin.H{
			"id": id, "workspace_id": workspaceID, "name": name,
			"type": chType, "created_at": createdAt,
		})
	}
	if result == nil {
		result = []gin.H{}
	}
	c.JSON(http.StatusOK, gin.H{"channels": result})
}

func (a *App) handleCreateChannel(c *gin.Context) {
	wsID := c.Param("wsID")
	userID := middleware.UserID(c)
	if !a.isWorkspaceMember(c.Request.Context(), wsID, userID) {
		c.JSON(http.StatusForbidden, gin.H{"error": "not a workspace member"})
		return
	}
	var req struct {
		Name string `json:"name"`
		Type string `json:"type"`
	}
	if err := c.ShouldBindJSON(&req); err != nil || req.Name == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "name required"})
		return
	}
	if req.Type == "" {
		req.Type = "group"
	}
	validTypes := map[string]bool{"group": true, "xhs-creator": true}
	if !validTypes[req.Type] {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid channel type"})
		return
	}

	chID := uuid.NewString()
	dbPath := filepath.Join(a.channelDBDir, chID+".db")
	now := time.Now().UnixMilli()

	// Create the channel row + seed its DESIRED composition (channel_actors) + the
	// default_agent pointer in ONE tx. channel_actors is the canonical writer for
	// "what this channel runs"; default_agent is a name-agnostic pointer into it,
	// defaulting to the agent:boost fallback instance (actor-instance-model §7/§8).
	// One tx → never a half-seeded channel (§8·C, no three-write drift).
	tx, err := a.db.BeginTx(c.Request.Context(), nil)
	if err != nil {
		a.logger.Error("create channel: begin tx", "channel", chID, "err", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "internal error"})
		return
	}
	_, err = tx.ExecContext(c.Request.Context(),
		`INSERT INTO channels (id, workspace_id, name, type, db_path, default_agent, created_at) VALUES (?,?,?,?,?,?,?)`,
		chID, wsID, req.Name, req.Type, dbPath, string(defaultAgentInstanceID), now,
	)
	if err == nil {
		_, err = tx.ExecContext(c.Request.Context(),
			`INSERT INTO channel_actors (channel_id, instance_id, class, placement) VALUES (?,?,?,?)`,
			chID, string(defaultAgentInstanceID), defaultBoostLooper, placementServer,
		)
	}
	if err != nil {
		_ = tx.Rollback()
		a.logger.Error("create channel: seed", "channel", chID, "err", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "internal error"})
		return
	}
	if err := tx.Commit(); err != nil {
		a.logger.Error("create channel: commit", "channel", chID, "err", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "internal error"})
		return
	}

	home, err := a.createHome(channel.ID(chID), dbPath)
	if err != nil {
		// Roll back: delete the orphaned channel row.
		_, _ = a.db.ExecContext(c.Request.Context(),
			`DELETE FROM channels WHERE id = ?`, chID)
		a.logger.Error("create channel: init home", "channel", chID, "err", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "internal error"})
		return
	}

	actorID := actor.ActorID("user:" + userID)
	// Register the creating user as a presence-less channel member (a human is a
	// member but has no cell — Spawn with nil impl is membership-only).
	if mErr := home.Spawn(c.Request.Context(), actorID, actor.KindHuman, nil); mErr != nil {
		a.logger.Warn("app: channel membership insert failed", "channel", chID, "err", mErr.Error())
	}

	c.JSON(http.StatusCreated, gin.H{
		"id": chID, "workspace_id": wsID, "name": req.Name,
		"type": req.Type, "created_at": now,
	})
}

func (a *App) handleGetChannel(c *gin.Context) {
	chID, ok := a.requireChannelAccess(c)
	if !ok {
		return
	}
	var id, workspaceID, name, chType, defaultAgent string
	var createdAt int64
	err := a.db.QueryRowContext(c.Request.Context(),
		`SELECT id, workspace_id, name, type, COALESCE(default_agent, ''), created_at FROM channels WHERE id = ?`, chID,
	).Scan(&id, &workspaceID, &name, &chType, &defaultAgent, &createdAt)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "channel not found"})
		return
	}
	c.JSON(http.StatusOK, gin.H{
		"id": id, "workspace_id": workspaceID, "name": name,
		"type": chType, "default_agent": defaultAgent, "created_at": createdAt,
	})
}

func (a *App) handleDeleteChannel(c *gin.Context) {
	chID, ok := a.requireChannelAccess(c)
	if !ok {
		return
	}

	var dbPath string
	err := a.db.QueryRowContext(c.Request.Context(),
		`SELECT db_path FROM channels WHERE id = ?`, string(chID),
	).Scan(&dbPath)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "channel not found"})
		return
	}

	// Close the in-memory channel home (stops links, delivery tap, cells, stores).
	cID := channel.ID(chID)
	a.mu.Lock()
	if home, exists := a.homes[cID]; exists {
		_ = home.Close()
		delete(a.homes, cID)
	}
	a.forgetHumans(cID)
	a.mu.Unlock()

	// Remove daemon bindings, then the channel row.
	_, _ = a.db.ExecContext(c.Request.Context(),
		`DELETE FROM daemon_channels WHERE channel_id = ?`, string(chID))
	_, _ = a.db.ExecContext(c.Request.Context(),
		`DELETE FROM channels WHERE id = ?`, string(chID))

	// Remove the per-channel sqlite file.
	_ = os.Remove(dbPath)

	c.JSON(http.StatusOK, gin.H{"ok": true})
}

func (a *App) handleListChannelMembers(c *gin.Context) {
	chID, ok := a.requireChannelAccess(c)
	if !ok {
		return
	}
	var wsID string
	_ = a.db.QueryRowContext(c.Request.Context(),
		`SELECT workspace_id FROM channels WHERE id = ?`, chID,
	).Scan(&wsID)

	rows, err := a.db.QueryContext(c.Request.Context(),
		`SELECT wm.user_id, wm.role, u.email, u.display_name
		 FROM workspace_members wm
		 JOIN users u ON u.id = wm.user_id
		 WHERE wm.workspace_id = ?`, wsID,
	)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "query failed"})
		return
	}
	defer rows.Close()

	var result []gin.H
	for rows.Next() {
		var userID, role, email, displayName string
		if err := rows.Scan(&userID, &role, &email, &displayName); err != nil {
			continue
		}
		result = append(result, gin.H{
			"user_id": userID, "role": role, "email": email, "display_name": displayName,
		})
	}
	if result == nil {
		result = []gin.H{}
	}
	c.JSON(http.StatusOK, gin.H{"members": result})
}

func (a *App) handleListActors(c *gin.Context) {
	chID, ok := a.requireChannelAccess(c)
	if !ok {
		return
	}
	home := a.getHome(channel.ID(chID))
	if home == nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "channel not loaded"})
		return
	}

	actors, err := home.View().ListActors(c.Request.Context())
	if err != nil {
		a.logger.Error("list actors", "channel", chID, "err", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "internal error"})
		return
	}

	var result []gin.H
	for _, rec := range actors {
		result = append(result, gin.H{
			"id": string(rec.ID), "kind": string(rec.Kind),
			"binding": string(rec.Binding), "created_at": rec.CreatedAt,
		})
	}
	if result == nil {
		result = []gin.H{}
	}
	c.JSON(http.StatusOK, gin.H{"channel_id": chID, "actors": result})
}

// ---------------------------------------------------------------------------
// Message handlers
// ---------------------------------------------------------------------------

func (a *App) handleCursor(c *gin.Context) {
	chID, ok := a.requireChannelAccess(c)
	if !ok {
		return
	}
	home := a.getHome(channel.ID(chID))
	if home == nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "channel not loaded"})
		return
	}
	seq, err := home.View().MaxSeq(c.Request.Context())
	if err != nil {
		a.logger.Error("cursor: max seq", "channel", chID, "err", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "internal error"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"last_received_seq": seq})
}

func (a *App) handleListMessages(c *gin.Context) {
	chID, ok := a.requireChannelAccess(c)
	if !ok {
		return
	}
	home := a.getHome(channel.ID(chID))
	if home == nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "channel not loaded"})
		return
	}

	afterStr := c.DefaultQuery("after", "0")
	after, _ := strconv.ParseInt(afterStr, 10, 64)
	limitStr := c.DefaultQuery("limit", "100")
	limit, _ := strconv.Atoi(limitStr)
	if limit <= 0 || limit > 1000 {
		limit = 100
	}

	rows, err := home.View().ReadAfterSeq(c.Request.Context(), after, limit)
	if err != nil {
		a.logger.Error("list messages", "channel", chID, "err", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "internal error"})
		return
	}

	result := make([]gin.H, 0, len(rows))
	for _, r := range rows {
		result = append(result, gin.H{
			"seq":         r.Seq,
			"is_terminal": r.IsTerminal,
			"envelope":    r.Envelope,
		})
	}
	c.JSON(http.StatusOK, result)
}

// handleSendMessage commits a user-authored message to the channel. The user does
// NOT write truth directly — sealed-pen forbids an app-held write gate. Instead it
// hands the raw intent to the user's HUMAN write front-end (a home-scoped actor
// admitted with a pen welded to "user:<id>"), whose SubmitIntent resolves routing,
// builds the envelope, and commits through its welded pen — returning the
// WriteResult synchronously so this POST keeps its 201 message_id/seq.
func (a *App) handleSendMessage(c *gin.Context) {
	chID, ok := a.requireChannelAccess(c)
	if !ok {
		return
	}
	if home := a.getHome(channel.ID(chID)); home == nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "channel not loaded"})
		return
	}

	body, err := io.ReadAll(c.Request.Body)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "read body failed"})
		return
	}

	var req struct {
		ID         string          `json:"id"`
		Type       string          `json:"type"`
		Kind       string          `json:"kind"`
		Payload    json.RawMessage `json:"payload"`
		Audience   []string        `json:"audience"`
		Visibility string          `json:"visibility"`
		ParentID   string          `json:"parent_id"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid json"})
		return
	}

	userID := middleware.UserID(c)
	front, err := a.humanFor(c.Request.Context(), channel.ID(chID), userID)
	if err != nil {
		if errors.Is(err, errChannelNotLoaded) {
			c.JSON(http.StatusNotFound, gin.H{"error": "channel not loaded"})
			return
		}
		a.logger.Error("send message: admit human front", "channel", chID, "err", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "internal error"})
		return
	}

	res, err := front.SubmitIntent(c.Request.Context(), submitInput{
		ID:         req.ID,
		Type:       req.Type,
		Kind:       req.Kind,
		Payload:    req.Payload,
		Audience:   req.Audience,
		Visibility: req.Visibility,
		ParentID:   req.ParentID,
	})
	if err != nil {
		var rErr *routingError
		if errors.As(err, &rErr) {
			c.JSON(http.StatusServiceUnavailable, gin.H{
				"error":  "default agent unavailable",
				"detail": rErr.detail,
			})
			return
		}
		a.logger.Error("send message", "channel", chID, "err", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "internal error"})
		return
	}
	if !res.Accepted() {
		c.JSON(http.StatusUnprocessableEntity, gin.H{
			"error":  string(res.RejectReason),
			"detail": res.RejectDetail,
		})
		return
	}

	c.JSON(http.StatusCreated, gin.H{
		"message_id": string(res.MessageID),
		"seq":        res.Seq,
	})
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

type setDefaultAgentReq struct {
	InstanceID string `json:"instance_id"`
}

// handleSetDefaultAgent re-points (or clears) a channel's default_agent — the
// §7.2 "user repoints the brain" entry point (install a daemon agent and make it
// default, or fail back to agent:boost). The pointer may only target an instance
// already in the channel's composition (channel_actors); an empty instance_id
// clears it (→ group-chat when there is no boost floor). Takes effect on the next
// routed message; it does not hot-reconfigure live cells.
func (a *App) handleSetDefaultAgent(c *gin.Context) {
	chID, ok := a.requireChannelAccess(c)
	if !ok {
		return
	}
	var req setDefaultAgentReq
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "bad request"})
		return
	}
	inst := strings.TrimSpace(req.InstanceID)
	if inst != "" {
		has, err := a.channelHasInstance(c.Request.Context(), chID, inst)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "internal error"})
			return
		}
		if !has {
			c.JSON(http.StatusBadRequest, gin.H{"error": "instance not in channel composition"})
			return
		}
	}
	var val any // empty instance_id → NULL (clear the pointer)
	if inst != "" {
		val = inst
	}
	if _, err := a.db.ExecContext(c.Request.Context(),
		`UPDATE channels SET default_agent = ? WHERE id = ?`, val, chID); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "internal error"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"channel_id": chID, "default_agent": inst})
}

// channelHasInstance reports whether instanceID is in the channel's composition
// (channel_actors) — used to validate a default_agent pointer and to resolve the
// agent:boost failover floor at routing time.
func (a *App) channelHasInstance(ctx context.Context, chID, instanceID string) (bool, error) {
	var n int
	if err := a.db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM channel_actors WHERE channel_id = ? AND instance_id = ?`,
		chID, instanceID).Scan(&n); err != nil {
		return false, err
	}
	return n > 0, nil
}
