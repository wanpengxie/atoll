package app

import (
	"encoding/json"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"

	"github.com/wanpengxie/ActOS/app/internal/middleware"
	"github.com/wanpengxie/ActOS/protocol/actor"
	"github.com/wanpengxie/ActOS/protocol/channel"
	"github.com/wanpengxie/ActOS/protocol/message"
	"github.com/wanpengxie/ActOS/runtime/harness"
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

	_, err := a.db.ExecContext(c.Request.Context(),
		`INSERT INTO channels (id, workspace_id, name, type, db_path, created_at) VALUES (?,?,?,?,?,?)`,
		chID, wsID, req.Name, req.Type, dbPath, now,
	)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "create channel failed"})
		return
	}

	home, err := a.createHome(channel.ID(chID), dbPath)
	if err != nil {
		// Roll back: delete the orphaned channel row.
		_, _ = a.db.ExecContext(c.Request.Context(),
			`DELETE FROM channels WHERE id = ?`, chID)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "init channel home failed: " + err.Error()})
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

func (a *App) handleBindChannel(c *gin.Context) {
	chID := c.Param("chID")
	var req struct {
		DaemonID string `json:"daemon_id"`
	}
	if err := c.ShouldBindJSON(&req); err != nil || req.DaemonID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "daemon_id required"})
		return
	}

	userID := middleware.UserID(c)
	var ownerID string
	err := a.db.QueryRowContext(c.Request.Context(),
		`SELECT owner_id FROM daemons WHERE id = ?`, req.DaemonID,
	).Scan(&ownerID)
	if err != nil || ownerID != userID {
		c.JSON(http.StatusForbidden, gin.H{"error": "daemon not found or not owned by you"})
		return
	}

	_, err = a.db.ExecContext(c.Request.Context(),
		`INSERT OR IGNORE INTO daemon_channels (daemon_id, channel_id) VALUES (?,?)`,
		req.DaemonID, chID,
	)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "bind failed"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"ok": true})
}

func (a *App) handleGetChannel(c *gin.Context) {
	chID, ok := a.requireChannelAccess(c)
	if !ok {
		return
	}
	var id, workspaceID, name, chType string
	var createdAt int64
	err := a.db.QueryRowContext(c.Request.Context(),
		`SELECT id, workspace_id, name, type, created_at FROM channels WHERE id = ?`, chID,
	).Scan(&id, &workspaceID, &name, &chType, &createdAt)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "channel not found"})
		return
	}
	c.JSON(http.StatusOK, gin.H{
		"id": id, "workspace_id": workspaceID, "name": name,
		"type": chType, "created_at": createdAt,
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
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
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
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
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
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
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

func (a *App) handleSendMessage(c *gin.Context) {
	chID, ok := a.requireChannelAccess(c)
	if !ok {
		return
	}
	home := a.getHome(channel.ID(chID))
	if home == nil {
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
	senderID := actor.ActorID("user:" + userID)

	audience := make([]actor.ActorID, 0, len(req.Audience))
	for _, a := range req.Audience {
		audience = append(audience, actor.ActorID(a))
	}

	// Product decision: no explicit audience → send to channel's agent (kind=request).
	kind := message.Kind(req.Kind)
	if len(audience) == 0 {
		actors, _ := home.View().ListActors(c.Request.Context())
		var agents []actor.ActorID
		for _, a := range actors {
			if a.Kind == actor.KindAgent {
				agents = append(agents, a.ID)
			}
		}
		if len(agents) == 0 {
			c.JSON(http.StatusUnprocessableEntity, gin.H{"error": "no agent in this channel"})
			return
		}
		if len(agents) > 1 {
			c.JSON(http.StatusUnprocessableEntity, gin.H{"error": "multiple agents in channel, specify audience"})
			return
		}
		audience = agents
		kind = message.KindRequest
	}

	gw := homeGateway(channel.ID(chID), home)
	channelID := channel.ID(chID)

	// App layer owns product decisions: sender kind, TTL, envelope shape.
	env := a.newClientEnvelope(channelID, senderID, req.ID, req.Type, kind, req.Payload, audience)
	if req.Visibility != "" {
		env.Visibility = message.Visibility(req.Visibility)
	}
	if req.ParentID != "" {
		env.ParentID = message.ID(req.ParentID)
	}

	ctx := harness.CtxWithCaller(c.Request.Context(), harness.CallerContext{
		ActorID:   senderID,
		ChannelID: channelID,
	})
	res, err := gw.SendMessage(ctx, env)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
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

// clientRequestTTLMs is the default TTL for client-sent messages (product
// decision, lives in app layer).
const clientRequestTTLMs int64 = 30_000

// newClientEnvelope builds a message.Envelope from client-provided fields,
// filling in defaults (ID, Kind, TTL, Sender with KindHuman). All product
// decisions (sender kind, TTL) live here in the app layer, not in platform.
func (a *App) newClientEnvelope(
	chID channel.ID,
	senderID actor.ActorID,
	msgID string,
	msgType string,
	kind message.Kind,
	payload []byte,
	audience []actor.ActorID,
) *message.Envelope {
	now := time.Now().UnixMilli()
	exp := now + clientRequestTTLMs

	envID := message.ID(msgID)
	if envID == "" {
		envID = message.ID(uuid.NewString())
	}
	if kind == "" {
		kind = message.KindRequest
	}

	aud := make(message.Audience, 0, len(audience))
	aud = append(aud, audience...)

	return &message.Envelope{
		ID:        envID,
		TS:        now,
		ChannelID: chID,
		Kind:      kind,
		Type:      msgType,
		Sender:    message.Sender{Kind: actor.KindHuman, ID: senderID},
		Audience:  aud,
		Payload:   payload,
		ExpiresAt: &exp,
	}
}
