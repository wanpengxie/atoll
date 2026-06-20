package app

import (
	"context"
	"database/sql"
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"

	"github.com/wanpengxie/ActOS/app/internal/middleware"
	"github.com/wanpengxie/ActOS/platform"
	"github.com/wanpengxie/ActOS/protocol/actor"
	"github.com/wanpengxie/ActOS/protocol/channel"
	"github.com/wanpengxie/ActOS/registry"
	"github.com/wanpengxie/ActOS/runtime/harness"
)

// agents.go is the §五 创建与控制 face (agent-spec §五): a direct API over the
// `agents` declaration table + `channel_actors` composition — the front-end UI's
// CRUD for a user's agents (create / introduce-to-channel / edit config / restart
// / soft-delete). It writes the tables directly (declaration data, NOT actor
// messages); changes take effect when the cell is (re)built, never via live hot
// update (Spawn replaces — agent-spec §五).

// spawnAgentInstance builds ONE agent instance from its declaration + per-channel
// row and spawns it live. Spawn REPLACES an existing cell (one actor, one owner),
// so this doubles as restart = rebuild (new config) + Spawn (the looper resumes
// from its state slot). Mirrors spawnComposition's per-instance block.
func (a *App) spawnAgentInstance(chID channel.ID, home *platform.Home, instanceID, class, channelCfg, state, looper, globalCfg string) error {
	sessionsRoot := filepath.Join(filepath.Dir(a.channelDBDir), "agent-sessions")
	dir := filepath.Join(sessionsRoot, pathSafe(string(chID)), pathSafe(instanceID))
	if err := os.MkdirAll(dir, 0o755); err != nil {
		a.logger.Warn("app: session dir", "channel", string(chID), "instance", instanceID, "err", err.Error())
		dir = ""
	}
	inst := instanceID
	store := func(blob json.RawMessage) error {
		_, err := a.db.Exec(
			`UPDATE channel_actors SET state = ? WHERE channel_id = ? AND instance_id = ?`,
			string(blob), string(chID), inst)
		return err
	}
	cfg := mergeConfig(globalCfg, channelCfg)
	if class == "agent" {
		// DSN: pack agents.looper into the opaque config blob. A non-object config
		// is a hard error — refuse to build the looper (agent-spec §三: config is a
		// knobs container; a non-object payload would be silently dropped).
		withL, werr := withLooper(cfg, looper)
		if werr != nil {
			return werr
		}
		cfg = withL
	}
	decl, err := registry.Build(class, registry.InstanceSpec{
		ID:     actor.ActorID(inst),
		Config: cfg,
	}, registry.Deps{
		ChannelID: chID,
		Logger:    a.logger,
		State: registry.StateSlot{
			Dir:   dir,
			Seed:  json.RawMessage(state),
			Store: store,
		},
	})
	if err != nil {
		return err
	}
	pen := &callerStampedWriter{inner: home.Gate(), caller: harness.CallerContext{
		ActorID: decl.ID, ChannelID: chID,
	}}
	impl := decl.Factory(pen)
	return home.Spawn(context.Background(), decl.ID, decl.Kind, impl)
}

type createAgentReq struct {
	Name   string          `json:"name"`
	Looper string          `json:"looper"`
	Config json.RawMessage `json:"config"`
}

// isJSONObject reports whether raw is a JSON object — the only shape an agent
// config may take, since the looper key (DSN) rides INSIDE it. null / array /
// scalar → false. Empty raw is the caller's responsibility (an absent config is
// fine; only a present-but-non-object config is rejected).
func isJSONObject(raw json.RawMessage) bool {
	var m map[string]json.RawMessage
	if err := json.Unmarshal(raw, &m); err != nil {
		return false
	}
	return m != nil // JSON null unmarshals without error but leaves m nil
}

// handleCreateAgent inserts a global agent declaration owned by the current user.
func (a *App) handleCreateAgent(c *gin.Context) {
	userID := middleware.UserID(c)
	var req createAgentReq
	if err := c.ShouldBindJSON(&req); err != nil || strings.TrimSpace(req.Name) == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "name required"})
		return
	}
	looper := strings.TrimSpace(req.Looper)
	if looper == "" {
		looper = "go-kimi"
	}
	id := uuid.NewString()
	now := time.Now().UnixMilli()
	cfg := ""
	if len(req.Config) > 0 {
		if !isJSONObject(req.Config) {
			c.JSON(http.StatusBadRequest, gin.H{"error": "config must be a JSON object"})
			return
		}
		cfg = string(req.Config)
	}
	if _, err := a.db.ExecContext(c.Request.Context(),
		`INSERT INTO agents (id, name, owner, looper, config_json, created_at, updated_at) VALUES (?,?,?,?,?,?,?)`,
		id, strings.TrimSpace(req.Name), userID, looper, cfg, now, now); err != nil {
		a.logger.Error("create agent", "err", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "internal error"})
		return
	}
	c.JSON(http.StatusCreated, gin.H{
		"id": id, "name": strings.TrimSpace(req.Name), "looper": looper,
		"owner": userID, "created_at": now,
	})
}

// handleListAgents lists the current user's (non-deleted) agents.
func (a *App) handleListAgents(c *gin.Context) {
	userID := middleware.UserID(c)
	rows, err := a.db.QueryContext(c.Request.Context(),
		`SELECT id, name, looper, created_at, updated_at FROM agents WHERE owner = ? AND deleted_at IS NULL ORDER BY created_at`,
		userID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "internal error"})
		return
	}
	defer rows.Close()
	out := []gin.H{}
	for rows.Next() {
		var id, name, looper string
		var ca, ua int64
		if err := rows.Scan(&id, &name, &looper, &ca, &ua); err != nil {
			continue
		}
		out = append(out, gin.H{"id": id, "name": name, "looper": looper, "created_at": ca, "updated_at": ua})
	}
	c.JSON(http.StatusOK, gin.H{"agents": out})
}

type updateAgentReq struct {
	Name   *string         `json:"name"`
	Config json.RawMessage `json:"config"`
}

// handleUpdateAgent edits an agent's name / global config (declaration data). It
// does NOT hot-update a live cell — the new config is read on the next restart.
func (a *App) handleUpdateAgent(c *gin.Context) {
	userID := middleware.UserID(c)
	agentID := c.Param("agentID")
	var req updateAgentReq
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "bad request"})
		return
	}
	var count int
	if err := a.db.QueryRowContext(c.Request.Context(),
		`SELECT COUNT(*) FROM agents WHERE id = ? AND owner = ? AND deleted_at IS NULL`,
		agentID, userID).Scan(&count); err != nil || count == 0 {
		c.JSON(http.StatusNotFound, gin.H{"error": "agent not found"})
		return
	}
	now := time.Now().UnixMilli()
	if req.Name != nil {
		_, _ = a.db.ExecContext(c.Request.Context(),
			`UPDATE agents SET name = ?, updated_at = ? WHERE id = ? AND owner = ?`,
			strings.TrimSpace(*req.Name), now, agentID, userID)
	}
	if len(req.Config) > 0 {
		if !isJSONObject(req.Config) {
			c.JSON(http.StatusBadRequest, gin.H{"error": "config must be a JSON object"})
			return
		}
		_, _ = a.db.ExecContext(c.Request.Context(),
			`UPDATE agents SET config_json = ?, updated_at = ? WHERE id = ? AND owner = ?`,
			string(req.Config), now, agentID, userID)
	}
	c.JSON(http.StatusOK, gin.H{"updated": agentID, "note": "takes effect on restart"})
}

// handleDeleteAgent soft-deletes an agent and removes it from every channel's
// composition (it disappears from channels). Live cells are NOT force-killed —
// membership gone = no re-route / no rebuild, the cell stops lazily (agent-spec §五).
func (a *App) handleDeleteAgent(c *gin.Context) {
	userID := middleware.UserID(c)
	agentID := c.Param("agentID")
	now := time.Now().UnixMilli()
	res, err := a.db.ExecContext(c.Request.Context(),
		`UPDATE agents SET deleted_at = ?, updated_at = ? WHERE id = ? AND owner = ? AND deleted_at IS NULL`,
		now, now, agentID, userID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "internal error"})
		return
	}
	if n, _ := res.RowsAffected(); n == 0 {
		c.JSON(http.StatusNotFound, gin.H{"error": "agent not found"})
		return
	}
	instanceID := "agent:" + agentID
	_, _ = a.db.ExecContext(c.Request.Context(),
		`DELETE FROM channel_actors WHERE instance_id = ?`, instanceID)
	// Clear any channel whose default_agent pointed at the deleted instance — a
	// removed agent must not keep receiving a channel's default traffic (default
	// routing keys off channels.default_agent; the live cell still stops lazily,
	// agent-spec §五). [codex P1]
	_, _ = a.db.ExecContext(c.Request.Context(),
		`UPDATE channels SET default_agent = NULL WHERE default_agent = ?`, instanceID)
	c.JSON(http.StatusOK, gin.H{"deleted": agentID})
}

type introduceAgentReq struct {
	AgentID     string `json:"agent_id"`
	Placement   string `json:"placement"`
	MakeDefault bool   `json:"make_default"`
}

// handleIntroduceAgent adds an agent to a channel's composition (channel_actors)
// and spawns it live when server-placed. Intent persists even if the live spawn
// fails (e.g. the claude CLI is absent) — mirrors spawnComposition's tolerance.
func (a *App) handleIntroduceAgent(c *gin.Context) {
	chID, ok := a.requireChannelAccess(c)
	if !ok {
		return
	}
	userID := middleware.UserID(c)
	var req introduceAgentReq
	if err := c.ShouldBindJSON(&req); err != nil || strings.TrimSpace(req.AgentID) == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "agent_id required"})
		return
	}
	var looper, gcfg string
	err := a.db.QueryRowContext(c.Request.Context(),
		`SELECT looper, COALESCE(config_json, '') FROM agents WHERE id = ? AND owner = ? AND deleted_at IS NULL`,
		req.AgentID, userID).Scan(&looper, &gcfg)
	if err == sql.ErrNoRows {
		c.JSON(http.StatusNotFound, gin.H{"error": "agent not found"})
		return
	}
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "internal error"})
		return
	}
	placement := strings.TrimSpace(req.Placement)
	if placement == "" {
		placement = placementServer
	}
	instanceID := "agent:" + req.AgentID
	if _, err := a.db.ExecContext(c.Request.Context(),
		`INSERT OR IGNORE INTO channel_actors (channel_id, instance_id, class, placement) VALUES (?,?,?,?)`,
		chID, instanceID, "agent", placement); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "internal error"})
		return
	}
	if req.MakeDefault {
		_, _ = a.db.ExecContext(c.Request.Context(),
			`UPDATE channels SET default_agent = ? WHERE id = ?`, instanceID, chID)
	}
	live := false
	if placement == placementServer {
		if home := a.getHome(channel.ID(chID)); home != nil {
			if serr := a.spawnAgentInstance(channel.ID(chID), home, instanceID, "agent", "", "", looper, gcfg); serr != nil {
				a.logger.Warn("introduce agent: spawn", "channel", chID, "instance", instanceID, "err", serr.Error())
			} else {
				live = true
			}
		}
	}
	c.JSON(http.StatusCreated, gin.H{
		"channel_id": chID, "instance_id": instanceID,
		"placement": placement, "looper": looper, "live": live,
	})
}

// handleRestartAgent rebuilds + respawns the agent's server-placed cells in every
// channel it is in. Spawn replaces the old cell; the rebuilt cell reads the
// latest config (declaration + per-channel) and the looper resumes from the state
// slot (agent-spec §五: 改表后不活体热更新；生效 = 重建).
func (a *App) handleRestartAgent(c *gin.Context) {
	userID := middleware.UserID(c)
	agentID := c.Param("agentID")
	var looper, gcfg string
	err := a.db.QueryRowContext(c.Request.Context(),
		`SELECT looper, COALESCE(config_json, '') FROM agents WHERE id = ? AND owner = ? AND deleted_at IS NULL`,
		agentID, userID).Scan(&looper, &gcfg)
	if err == sql.ErrNoRows {
		c.JSON(http.StatusNotFound, gin.H{"error": "agent not found"})
		return
	}
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "internal error"})
		return
	}
	instanceID := "agent:" + agentID
	type loc struct{ chID, cfg, state string }
	var locs []loc
	rows, qerr := a.db.QueryContext(c.Request.Context(),
		`SELECT channel_id, COALESCE(config_json, ''), COALESCE(state, '') FROM channel_actors WHERE instance_id = ? AND placement = ?`,
		instanceID, placementServer)
	if qerr != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "internal error"})
		return
	}
	for rows.Next() {
		var l loc
		if err := rows.Scan(&l.chID, &l.cfg, &l.state); err == nil {
			locs = append(locs, l)
		}
	}
	rows.Close()

	restarted := 0
	for _, l := range locs {
		home := a.getHome(channel.ID(l.chID))
		if home == nil {
			continue
		}
		if serr := a.spawnAgentInstance(channel.ID(l.chID), home, instanceID, "agent", l.cfg, l.state, looper, gcfg); serr != nil {
			a.logger.Warn("restart agent: spawn", "channel", l.chID, "instance", instanceID, "err", serr.Error())
			continue
		}
		restarted++
	}
	c.JSON(http.StatusOK, gin.H{"agent": agentID, "restarted": restarted})
}
