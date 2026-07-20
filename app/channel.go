package app

import (
	"context"
	"database/sql"
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"

	"github.com/wanpengxie/atoll/app/internal/middleware"
	"github.com/wanpengxie/atoll/platform/home"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

func (a *App) channelDBPath(chID channel.ID) string {
	return filepath.Join(a.channelDBDir, string(chID)+".db")
}

// rollbackOpenedChannel is the pre-publication cleanup path. Published channel
// destruction is replaced by ChannelHost tombstoning in S2; this helper is only
// allowed to remove a never-published half-built database.
func (a *App) rollbackOpenedChannel(ctx context.Context, chID channel.ID) {
	if h := a.detachHome(chID); h != nil {
		_ = h.Close()
	}
	_, _ = a.db.ExecContext(context.WithoutCancel(ctx), `DELETE FROM channels WHERE id=?`, string(chID))
	path := a.channelDBPath(chID)
	_ = os.Remove(path)
	_ = os.Remove(path + "-wal")
	_ = os.Remove(path + "-shm")
}

func (a *App) seedOpenedChannel(ctx context.Context, h *home.Home, chID channel.ID, _ string, userID string, at int64) (actor.ActorID, actor.ActorID, error) {
	creatorID, err := h.AdmitChannelOwner(ctx, userID)
	if err != nil {
		a.rollbackOpenedChannel(ctx, chID)
		return "", "", err
	}
	boost, err := h.Declare(ctx, home.DeclareRequest{
		SourceDeclID: "sys:boost", Principal: defaultAgentPrincipal, Class: defaultBoostClass,
		Placement: storespec.NewServerPlacement(), MakeDefault: true,
		Kind: actor.KindAgent, CreatedAt: at,
	})
	if err != nil {
		a.rollbackOpenedChannel(ctx, chID)
		return "", "", err
	}
	return creatorID, boost.Row.ID, nil
}

func (a *App) handleListChannels(c *gin.Context) {
	query := `SELECT id,name,type,created_at,parent_id FROM channels`
	args := []any{}
	if parent, ok := c.GetQuery("parent_id"); ok {
		query += ` WHERE parent_id=?`
		args = append(args, parent)
	}
	query += ` ORDER BY created_at,id`
	rows, err := a.db.QueryContext(c.Request.Context(), query, args...)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "query failed"})
		return
	}
	defer rows.Close()
	result := make([]gin.H, 0)
	for rows.Next() {
		var id, name, channelType string
		var createdAt int64
		var parent sql.NullString
		if err := rows.Scan(&id, &name, &channelType, &createdAt, &parent); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "query failed"})
			return
		}
		result = append(result, channelJSON(id, name, channelType, createdAt, parent))
	}
	if err := rows.Err(); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "query failed"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"channels": result})
}

func channelJSON(id, name, channelType string, createdAt int64, parent sql.NullString) gin.H {
	row := gin.H{"id": id, "name": name, "type": channelType, "created_at": createdAt}
	if parent.Valid {
		row["parent_id"] = parent.String
	} else {
		row["parent_id"] = nil
	}
	return row
}

func (a *App) handleCreateChannel(c *gin.Context) {
	userID := middleware.UserID(c)
	var req struct {
		Name     string  `json:"name"`
		Type     string  `json:"type"`
		ParentID *string `json:"parent_id"`
	}
	if err := c.ShouldBindJSON(&req); err != nil || strings.TrimSpace(req.Name) == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "name required"})
		return
	}
	req.Name = strings.TrimSpace(req.Name)
	if req.Type == "" {
		req.Type = "group"
	}
	if req.Type != "group" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid channel type"})
		return
	}
	if req.ParentID != nil && !a.channelExists(c.Request.Context(), *req.ParentID) {
		c.JSON(http.StatusBadRequest, gin.H{"error": "parent channel not found"})
		return
	}
	var duplicate bool
	if err := a.db.QueryRowContext(c.Request.Context(), `SELECT EXISTS(SELECT 1 FROM channels WHERE name=?)`, req.Name).Scan(&duplicate); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "query failed"})
		return
	}
	if duplicate {
		c.JSON(http.StatusConflict, gin.H{"error": "channel name already exists"})
		return
	}

	chID := channel.ID(uuid.NewString())
	now := time.Now().UnixMilli()
	h, err := a.createHome(chID, a.channelDBPath(chID))
	if err != nil {
		a.logger.Error("create channel: init home", "channel", chID, "err", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "internal error"})
		return
	}
	creatorID, boostID, err := a.seedOpenedChannel(c.Request.Context(), h, chID, a.channelDBPath(chID), userID, now)
	if err != nil {
		a.logger.Error("create channel: genesis", "channel", chID, "err", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "internal error"})
		return
	}

	tx, err := a.db.BeginTx(c.Request.Context(), nil)
	if err != nil {
		a.rollbackOpenedChannel(c.Request.Context(), chID)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "internal error"})
		return
	}
	defer tx.Rollback()
	var parent any
	if req.ParentID != nil {
		parent = *req.ParentID
	}
	_, err = tx.ExecContext(c.Request.Context(), `INSERT INTO channels(id,name,type,created_at,parent_id) VALUES (?,?,?,?,?)`,
		string(chID), req.Name, req.Type, now, parent)
	if err == nil {
		_, err = tx.ExecContext(c.Request.Context(), `INSERT INTO principal_channels(principal,channel_id,actor_id,updated_at) VALUES (?,?,?,?)`,
			userID, string(chID), string(creatorID), now)
	}
	if err != nil {
		a.rollbackOpenedChannel(c.Request.Context(), chID)
		if strings.Contains(strings.ToLower(err.Error()), "unique") {
			c.JSON(http.StatusConflict, gin.H{"error": "channel name already exists"})
		} else {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "internal error"})
		}
		return
	}
	if err := tx.Commit(); err != nil {
		a.rollbackOpenedChannel(c.Request.Context(), chID)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "internal error"})
		return
	}
	if a.membershipPoke != nil {
		a.membershipPoke(userID)
	}
	parentValue := sql.NullString{}
	if req.ParentID != nil {
		parentValue = sql.NullString{String: *req.ParentID, Valid: true}
	}
	result := channelJSON(string(chID), req.Name, req.Type, now, parentValue)
	result["default_agent"] = string(boostID)
	result["creator_actor_id"] = string(creatorID)
	c.JSON(http.StatusCreated, result)
}

func (a *App) handleGetChannel(c *gin.Context) {
	chID, ok := a.requireChannelAccess(c)
	if !ok {
		return
	}
	var id, name, channelType string
	var createdAt int64
	var parent sql.NullString
	err := a.db.QueryRowContext(c.Request.Context(), `SELECT id,name,type,created_at,parent_id FROM channels WHERE id=?`, chID).
		Scan(&id, &name, &channelType, &createdAt, &parent)
	if errors.Is(err, sql.ErrNoRows) {
		c.JSON(http.StatusNotFound, gin.H{"error": "channel not found"})
		return
	}
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "query failed"})
		return
	}
	result := channelJSON(id, name, channelType, createdAt, parent)
	if h := a.getHome(channel.ID(chID)); h != nil {
		if value, found, err := h.DefaultAgent(c.Request.Context()); err == nil && found {
			result["default_agent"] = string(value)
		}
	}
	c.JSON(http.StatusOK, result)
}

func (a *App) handleDeleteChannel(c *gin.Context) {
	chID, ok := a.requireChannelAccess(c)
	if !ok {
		return
	}
	h := a.getHome(channel.ID(chID))
	if h == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "channel unavailable"})
		return
	}
	owner, found, err := h.View().OwnerPrincipal(c.Request.Context())
	if err != nil || !found {
		// S1 bridge for a pre-publication crash image: the creator projection is
		// the only row. S3 replaces this with the provision-job owner snapshot.
		err = a.db.QueryRowContext(c.Request.Context(), `SELECT principal FROM principal_channels WHERE channel_id=? ORDER BY updated_at LIMIT 1`, chID).Scan(&owner)
		if err != nil {
			c.JSON(http.StatusServiceUnavailable, gin.H{"error": "channel unavailable"})
			return
		}
	}
	caller := middleware.UserID(c)
	if owner != caller {
		a.logger.Warn("channel delete denied", "channel", chID, "requested_by", caller, "owner", owner)
		c.JSON(http.StatusForbidden, gin.H{"error": "channel owner required"})
		return
	}

	rows, err := a.db.QueryContext(c.Request.Context(), `SELECT principal FROM principal_channels WHERE channel_id=?`, chID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "delete failed"})
		return
	}
	var affected []string
	for rows.Next() {
		var principal string
		if err := rows.Scan(&principal); err == nil {
			affected = append(affected, principal)
		}
	}
	_ = rows.Close()
	tx, err := a.db.BeginTx(c.Request.Context(), nil)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "delete failed"})
		return
	}
	if _, err = tx.ExecContext(c.Request.Context(), `DELETE FROM principal_channels WHERE channel_id=?`, chID); err == nil {
		_, err = tx.ExecContext(c.Request.Context(), `DELETE FROM channels WHERE id=?`, chID)
	}
	if err != nil || tx.Commit() != nil {
		_ = tx.Rollback()
		c.JSON(http.StatusInternalServerError, gin.H{"error": "delete failed"})
		return
	}
	if detached := a.detachHome(channel.ID(chID)); detached != nil {
		_ = detached.Close()
	}
	_ = os.Remove(a.channelDBPath(channel.ID(chID))) // replaced by tombstone in S2
	for _, principal := range affected {
		if a.membershipPoke != nil {
			a.membershipPoke(principal)
		}
	}
	c.JSON(http.StatusAccepted, gin.H{"ok": true})
}

func (a *App) handleListCandidates(c *gin.Context) {
	if _, ok := a.requireChannelAccess(c); !ok {
		return
	}
	rows, err := a.db.QueryContext(c.Request.Context(), `SELECT id,email,display_name FROM users ORDER BY created_at,id`)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "query failed"})
		return
	}
	defer rows.Close()
	result := make([]gin.H, 0)
	for rows.Next() {
		var id, email string
		var display sql.NullString
		if err := rows.Scan(&id, &email, &display); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "query failed"})
			return
		}
		result = append(result, gin.H{"user_id": id, "email": email, "display_name": display.String})
	}
	c.JSON(http.StatusOK, gin.H{"candidates": result})
}
