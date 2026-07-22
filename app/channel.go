package app

import (
	"database/sql"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"

	"github.com/wanpengxie/atoll/app/internal/middleware"
	"github.com/wanpengxie/atoll/platform/channelhost"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
)

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
		var id, name, typ string
		var created int64
		var parent sql.NullString
		if err := rows.Scan(&id, &name, &typ, &created, &parent); err != nil {
			c.JSON(500, gin.H{"error": "query failed"})
			return
		}
		result = append(result, channelJSON(id, name, typ, created, parent))
	}
	if err := rows.Err(); err != nil {
		c.JSON(500, gin.H{"error": "query failed"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"channels": result})
}

func channelJSON(id, name, typ string, created int64, parent sql.NullString) gin.H {
	row := gin.H{"id": id, "name": name, "type": typ, "created_at": created}
	if parent.Valid {
		row["parent_id"] = parent.String
	} else {
		row["parent_id"] = nil
	}
	return row
}

func (a *App) handleCreateChannel(c *gin.Context) {
	caller := middleware.UserID(c)
	var req struct {
		Name     string  `json:"name"`
		Type     string  `json:"type"`
		ParentID *string `json:"parent_id"`
	}
	if err := c.ShouldBindJSON(&req); err != nil || strings.TrimSpace(req.Name) == "" {
		c.JSON(400, gin.H{"error": "name required"})
		return
	}
	req.Name = strings.TrimSpace(req.Name)
	if req.Type == "" {
		req.Type = "group"
	}
	if req.Type != "group" {
		c.JSON(400, gin.H{"error": "invalid channel type"})
		return
	}
	now := time.Now().UnixMilli()
	chID := channel.ID(uuid.NewString())
	operationID := "lc:" + uuid.NewString()
	snapshot, err := (channel.RenderedSnapshot{Class: defaultBoostClass, Config: json.RawMessage(`{}`), Placement: channel.Placement{Kind: channel.PlacementServer}}).Seal()
	if err != nil {
		c.JSON(500, gin.H{"error": "internal error"})
		return
	}
	realmSnapshot, err := (channel.RenderedSnapshot{Class: realmToolClass, Config: json.RawMessage(`{}`), Placement: channel.Placement{Kind: channel.PlacementServer}}).Seal()
	if err != nil {
		c.JSON(500, gin.H{"error": "internal error"})
		return
	}
	spec := channelhost.ProvisionSpec{ChannelID: chID, Type: req.Type, OwnerPrincipal: caller, CreatedAt: now,
		GenesisDeclarations: []channelhost.GenesisDeclaration{
			{DeclID: "sys:boost", Kind: actor.KindAgent, Rendered: snapshot},
			{DeclID: realmToolDeclID, Kind: actor.KindTool, Rendered: realmSnapshot},
		}, DefaultSourceDeclID: "sys:boost"}
	if req.ParentID != nil {
		spec.Origin = &channelhost.Origin{ParentChannelID: channel.ID(*req.ParentID), InitiatorPrincipal: caller}
	}
	raw, _ := json.Marshal(spec)
	tx, err := a.db.BeginTx(c.Request.Context(), nil)
	if err != nil {
		c.JSON(500, gin.H{"error": "create failed"})
		return
	}
	defer tx.Rollback()
	var conflict bool
	if err = tx.QueryRowContext(c.Request.Context(), `SELECT EXISTS(SELECT 1 FROM channels WHERE name=? UNION ALL SELECT 1 FROM channel_provision_jobs WHERE name=? AND done_at IS NULL AND dead_at IS NULL)`, req.Name, req.Name).Scan(&conflict); err != nil {
		c.JSON(500, gin.H{"error": "create failed"})
		return
	}
	if conflict {
		c.JSON(409, gin.H{"error": "channel name already exists"})
		return
	}
	if req.ParentID != nil {
		var exists bool
		if err = tx.QueryRowContext(c.Request.Context(), `SELECT EXISTS(SELECT 1 FROM channels WHERE id=?)`, *req.ParentID).Scan(&exists); err != nil || !exists {
			c.JSON(400, gin.H{"error": "parent channel not found"})
			return
		}
	}
	res, err := tx.ExecContext(c.Request.Context(), `INSERT INTO channel_provision_jobs(operation_id,channel_id,requested_by,name,type,owner_principal,spec_json,created_at) VALUES (?,?,?,?,?,?,?,?)`, operationID, string(chID), caller, req.Name, req.Type, caller, string(raw), now)
	if err != nil {
		if strings.Contains(strings.ToLower(err.Error()), "unique") {
			c.JSON(409, gin.H{"error": "channel name already exists"})
		} else {
			c.JSON(500, gin.H{"error": "create failed"})
		}
		return
	}
	jobID, _ := res.LastInsertId()
	if err = tx.Commit(); err != nil {
		c.JSON(500, gin.H{"error": "create failed"})
		return
	}
	_ = a.runProvisionJob(c.Request.Context(), jobID)
	var done, dead sql.NullInt64
	var code sql.NullString
	if err := a.db.QueryRowContext(c.Request.Context(), `SELECT done_at,dead_at,error_code FROM channel_provision_jobs WHERE job_id=?`, jobID).Scan(&done, &dead, &code); err != nil {
		c.JSON(500, gin.H{"error": "create failed", "operation_id": operationID})
		return
	}
	if done.Valid {
		a.respondCreatedChannel(c, string(chID))
		return
	}
	if dead.Valid {
		status := 500
		if code.String == "name_conflict" {
			status = 409
		}
		c.JSON(status, gin.H{"error": code.String, "operation_id": operationID})
		return
	}
	a.lifecycle.notify()
	c.JSON(http.StatusAccepted, gin.H{"operation_id": operationID, "status": "provisioning"})
}

func (a *App) respondCreatedChannel(c *gin.Context, id string) {
	var name, typ string
	var created int64
	var parent sql.NullString
	if err := a.db.QueryRowContext(c.Request.Context(), `SELECT name,type,created_at,parent_id FROM channels WHERE id=?`, id).Scan(&name, &typ, &created, &parent); err != nil {
		c.JSON(500, gin.H{"error": "projection failed"})
		return
	}
	row := channelJSON(id, name, typ, created, parent)
	if bundle, ok := a.host.Acquire(channel.ID(id)); ok {
		if value, found, err := bundle.View().DefaultAgent(c.Request.Context()); err == nil && found {
			row["default_agent"] = string(value)
		}
		if owner, found, err := bundle.View().ResolvePrincipal(c.Request.Context(), actor.KindHuman, middleware.UserID(c)); err == nil && found {
			row["creator_actor_id"] = string(owner)
		}
	}
	c.JSON(http.StatusCreated, row)
}

func (a *App) handleGetChannel(c *gin.Context) {
	chID := c.Param("chID")
	_, _, reason, gateErr := a.readSubject(c.Request.Context(), channel.ID(chID), middleware.UserID(c))
	if reason != observeAllowed || gateErr != nil {
		writeReadFailure(c, reason, gateErr)
		return
	}
	var id, name, typ string
	var created int64
	var parent sql.NullString
	err := a.db.QueryRowContext(c.Request.Context(), `SELECT id,name,type,created_at,parent_id FROM channels WHERE id=?`, chID).Scan(&id, &name, &typ, &created, &parent)
	if errors.Is(err, sql.ErrNoRows) {
		c.JSON(404, gin.H{"error": "channel not found"})
		return
	}
	if err != nil {
		c.JSON(500, gin.H{"error": "query failed"})
		return
	}
	row := channelJSON(id, name, typ, created, parent)
	if bundle, ok := a.host.Acquire(channel.ID(chID)); ok {
		if value, found, err := bundle.View().DefaultAgent(c.Request.Context()); err == nil && found {
			row["default_agent"] = string(value)
		}
	}
	c.JSON(200, row)
}

func (a *App) handleDeleteChannel(c *gin.Context) {
	chID, ok := a.requireChannelAccess(c)
	if !ok {
		return
	}
	caller := middleware.UserID(c)
	owner := ""
	if bundle, found := a.host.Acquire(channel.ID(chID)); found {
		owner, _, _ = bundle.View().OwnerPrincipal(c.Request.Context())
	}
	if owner == "" {
		_ = a.db.QueryRowContext(c.Request.Context(), `SELECT owner_principal FROM channel_provision_jobs WHERE channel_id=? ORDER BY job_id DESC LIMIT 1`, chID).Scan(&owner)
	}
	if owner == "" {
		c.JSON(503, gin.H{"error": "channel unavailable"})
		return
	}
	if owner != caller {
		a.logger.Warn("channel delete denied", "channel", chID, "requested_by", caller, "owner", owner)
		c.JSON(403, gin.H{"error": "channel owner required"})
		return
	}
	release := a.channelLocks.lock(chID)
	defer release()
	rows, err := a.db.QueryContext(c.Request.Context(), `SELECT principal FROM principal_channels WHERE channel_id=?`, chID)
	if err != nil {
		c.JSON(500, gin.H{"error": "delete failed"})
		return
	}
	var affected []string
	for rows.Next() {
		var p string
		if rows.Scan(&p) == nil {
			affected = append(affected, p)
		}
	}
	_ = rows.Close()
	op := "lc:" + uuid.NewString()
	now := time.Now().UnixMilli()
	tx, err := a.db.BeginTx(c.Request.Context(), nil)
	if err != nil {
		c.JSON(500, gin.H{"error": "delete failed"})
		return
	}
	defer tx.Rollback()
	_, err = tx.ExecContext(c.Request.Context(), `DELETE FROM principal_channels WHERE channel_id=?`, chID)
	if err == nil {
		_, err = tx.ExecContext(c.Request.Context(), `DELETE FROM channels WHERE id=?`, chID)
	}
	if err == nil {
		_, err = tx.ExecContext(c.Request.Context(), `UPDATE channel_provision_jobs SET error_code='superseded_by_destroy',last_error='superseded by destroy',dead_at=? WHERE channel_id=? AND done_at IS NULL AND dead_at IS NULL`, now, chID)
	}
	var jobID int64
	if err == nil {
		res, e := tx.ExecContext(c.Request.Context(), `INSERT INTO channel_destroy_jobs(operation_id,channel_id,requested_by,created_at) VALUES (?,?,?,?)`, op, chID, caller, now)
		err = e
		if e == nil {
			jobID, _ = res.LastInsertId()
		}
	}
	if err != nil || tx.Commit() != nil {
		c.JSON(500, gin.H{"error": "delete failed"})
		return
	}
	for _, p := range affected {
		if a.membershipPoke != nil {
			a.membershipPoke(p)
		}
	}
	_ = a.runDestroyJobLocked(c.Request.Context(), jobID)
	var done sql.NullInt64
	if err := a.db.QueryRowContext(c.Request.Context(), `SELECT done_at FROM channel_destroy_jobs WHERE job_id=?`, jobID).Scan(&done); err != nil {
		// The logical delete is already committed. Preserve the lifecycle rule:
		// never turn a durable pending intent into a post-commit 500.
		a.logger.Warn("channel destroy status read failed", "operation", op, "channel", chID, "err", err)
	}
	if done.Valid {
		c.JSON(http.StatusOK, gin.H{"operation_id": op, "status": "done"})
		return
	}
	a.lifecycle.notify()
	c.JSON(http.StatusAccepted, gin.H{"operation_id": op, "status": "destroying"})
}

func (a *App) handleListCandidates(c *gin.Context) {
	if _, ok := a.requireChannelAccess(c); !ok {
		return
	}
	rows, err := a.db.QueryContext(c.Request.Context(), `SELECT id,email,display_name FROM users ORDER BY created_at,id`)
	if err != nil {
		c.JSON(500, gin.H{"error": "query failed"})
		return
	}
	defer rows.Close()
	result := make([]gin.H, 0)
	for rows.Next() {
		var id, email string
		var display sql.NullString
		if rows.Scan(&id, &email, &display) != nil {
			c.JSON(500, gin.H{"error": "query failed"})
			return
		}
		result = append(result, gin.H{"user_id": id, "email": email, "display_name": display.String})
	}
	c.JSON(200, gin.H{"candidates": result})
}
