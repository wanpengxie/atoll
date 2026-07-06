package app

import (
	"context"
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"

	"github.com/wanpengxie/atoll/app/internal/middleware"
	"github.com/wanpengxie/atoll/platform"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
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
	validTypes := map[string]bool{"group": true}
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
	// defaulting to the agent:boost fallback instance.
	// One tx → never a half-seeded channel (no three-write drift).
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

	// The two seeding Admits are REQUIRED stages of the create transaction, not
	// best-effort: a channel whose creator is not a member, or whose seeded boost
	// intent row has no matching membership (filtered to a never-embodied dead row
	// under desired=intent∩membership), is a half-built channel. On either failure,
	// tear the whole thing down — close the home and roll back the channel row —
	// and return 5xx, so the caller sees a clean failure it can retry, never a
	// silent 201 over a broken channel.
	admit := func(id actor.ActorID, kind actor.Kind) error {
		if a.seedAdmitFailHook != nil {
			if err := a.seedAdmitFailHook(); err != nil {
				return err
			}
		}
		return home.Admit(c.Request.Context(), id, kind)
	}
	rollback := func(stage string, err error) {
		cID := channel.ID(chID)
		a.mu.Lock()
		if h, ok := a.homes[cID]; ok {
			_ = h.Close()
			delete(a.homes, cID)
		}
		a.mu.Unlock()
		_, _ = a.db.ExecContext(c.Request.Context(), `DELETE FROM channels WHERE id = ?`, chID)
		a.logger.Error("create channel: "+stage, "channel", chID, "err", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "internal error"})
	}

	actorID := actor.ActorID("user:" + userID)
	// Membrane law: the creator is a member. Admit is the pure-membership动词 —
	// the creating user's not→member edge (§4.6). No cell here (the human is
	// embodied by the ring / subjectgate, never welded at this call site).
	if mErr := admit(actorID, actor.KindHuman); mErr != nil {
		rollback("creator admit", mErr)
		return
	}
	// Template seeding is a PAIR: the seeded boost composition intent row (written
	// in the tx above) + its durable membership admission. Idempotent (re-Admit is
	// a no-op-shaped upsert), and it pokes the ring so boost embodies without
	// waiting a tick.
	if mErr := admit(defaultAgentInstanceID, actor.KindAgent); mErr != nil {
		rollback("boost admit", mErr)
		return
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

// handleListWorkspaceMembers lists the WORKSPACE roster reachable through this
// channel (workspace_members JOIN users) — a world-layer / subject-domain
// projection (HTTP legitimate), NOT the channel's actor census. Named honestly:
// "who is in the workspace", not "who is in the channel". The channel's real
// roster (its admitted actors) is served by handleListActors (/actors), backed by
// the in-gate sysactor actor.list; the two are different questions and must not be
// conflated (A11).
func (a *App) handleListWorkspaceMembers(c *gin.Context) {
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
	home := a.homeOrError(c, channel.ID(chID))
	if home == nil {
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
	home := a.homeOrError(c, channel.ID(chID))
	if home == nil {
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
	home := a.homeOrError(c, channel.ID(chID))
	if home == nil {
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

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

type setDefaultAgentReq struct {
	InstanceID string `json:"instance_id"`
}

// handleSetDefaultAgent re-points (or clears) a channel's default_agent — the
// entry point for "user repoints the brain" (install a daemon agent and make it
// default, or fail back to agent:boost). It is an HTTP垫片 (NP-1=c): it replays the
// session user through the door (channel.set_default_agent, audience=[system]), so
// the pointer-validation + write live in the executor and the action lands 笔为
// user:X in the log. The pointer may only target an instance already in the
// channel's composition; an empty instance_id clears it.
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
	payload, _ := json.Marshal(instancePayload{InstanceID: strings.TrimSpace(req.InstanceID)})
	r, err := a.submitControlThroughDoor(c.Request.Context(), chID, middleware.UserID(c),
		platform.TypeSetDefaultAgent, payload)
	a.finishControlShim(c, r, err, func(body map[string]any) (int, any) {
		da, _ := body["default_agent"].(string)
		return http.StatusOK, gin.H{"channel_id": chID, "default_agent": da}
	})
}

// handleRemoveActor is the HTTP垫片 for the channel-internal removal半 (红线11): a
// member removes an actor from THIS channel by replaying through the door
// (channel.remove_actor, audience=[system]). It is distinct from the world-layer
// agent soft-delete (handleDeleteAgent, DELETE /agents/:agentID): that de-registers
// a cross-channel identity and cascades via a system-authored mirror; this is one
// member removing one composition member from one channel, 笔为 user:X.
func (a *App) handleRemoveActor(c *gin.Context) {
	chID, ok := a.requireChannelAccess(c)
	if !ok {
		return
	}
	inst := strings.TrimSpace(c.Param("instanceID"))
	if inst == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "instance_id required"})
		return
	}
	payload, _ := json.Marshal(instancePayload{InstanceID: inst})
	r, err := a.submitControlThroughDoor(c.Request.Context(), chID, middleware.UserID(c),
		platform.TypeRemoveActor, payload)
	a.finishControlShim(c, r, err, func(body map[string]any) (int, any) {
		removed, _ := body["removed"].(string)
		return http.StatusOK, gin.H{"channel_id": chID, "removed": removed}
	})
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
