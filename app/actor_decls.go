package app

import (
	"encoding/json"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"

	"github.com/wanpengxie/atoll/app/internal/middleware"
	"github.com/wanpengxie/atoll/platform/home"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
)

// actor_decls.go is the create-and-control face: a direct API over the
// `actor_decls` declaration table + channel-local composition — the front-end
// UI's CRUD for a user's declared actor instances (create / introduce-to-channel
// / edit config / restart / soft-delete). The declaration layer is kind-neutral:
// one row = identity + class + config + owner + visibility, for agents and tools
// alike. It writes the tables directly (declaration data, NOT actor messages);
// changes take effect when the cell is (re)built, never via live hot update
// (Restart requests a fresh incarnation).

type createDeclReq struct {
	Name string `json:"name"`
	// Class = the declaration's DEFAULT engine class (stored as
	// actor_decls.default_class); a per-channel engine may override it at
	// introduce time.
	Class  string          `json:"class"`
	Config json.RawMessage `json:"config"`
}

// isJSONObject reports whether raw is a JSON object — the only shape a declared
// instance's config (persona/skills + engine knobs) may take. null / array /
// scalar → false. Empty raw is the caller's responsibility (an absent config is
// fine; only a present-but-non-object config is rejected).
func isJSONObject(raw json.RawMessage) bool {
	var m map[string]json.RawMessage
	if err := json.Unmarshal(raw, &m); err != nil {
		return false
	}
	return m != nil // JSON null unmarshals without error but leaves m nil
}

// handleCreateDecl inserts a global actor-instance declaration owned by the
// current user.
func (a *App) handleCreateDecl(c *gin.Context) {
	userID := middleware.UserID(c)
	var req createDeclReq
	if err := c.ShouldBindJSON(&req); err != nil || strings.TrimSpace(req.Name) == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "name required"})
		return
	}
	class := strings.TrimSpace(req.Class)
	if class == "" {
		class = "go-kimi"
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
		`INSERT INTO actor_decls (id, name, owner, default_class, config_json, created_at, updated_at) VALUES (?,?,?,?,?,?,?)`,
		id, strings.TrimSpace(req.Name), userID, class, cfg, now, now); err != nil {
		a.logger.Error("create decl", "err", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "internal error"})
		return
	}
	c.JSON(http.StatusCreated, gin.H{
		"id": id, "name": strings.TrimSpace(req.Name), "class": class,
		"owner": userID, "created_at": now,
	})
}

// handleListDecls lists the current user's (non-deleted) declarations.
func (a *App) handleListDecls(c *gin.Context) {
	userID := middleware.UserID(c)
	rows, err := a.db.QueryContext(c.Request.Context(),
		`SELECT id, name, default_class, created_at, updated_at FROM actor_decls WHERE owner = ? AND deleted_at IS NULL ORDER BY created_at`,
		userID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "internal error"})
		return
	}
	defer rows.Close()
	out := []gin.H{}
	for rows.Next() {
		var id, name, class string
		var ca, ua int64
		if err := rows.Scan(&id, &name, &class, &ca, &ua); err != nil {
			continue
		}
		out = append(out, gin.H{"id": id, "name": name, "class": class, "created_at": ca, "updated_at": ua})
	}
	c.JSON(http.StatusOK, gin.H{"decls": out})
}

type updateDeclReq struct {
	Name   *string         `json:"name"`
	Config json.RawMessage `json:"config"`
}

// handleUpdateDecl edits a declaration's name / global config (declaration data).
// It does NOT hot-update a live cell — the new config is read on the next restart.
func (a *App) handleUpdateDecl(c *gin.Context) {
	userID := middleware.UserID(c)
	declID := c.Param("declID")
	var req updateDeclReq
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "bad request"})
		return
	}
	var count int
	if err := a.db.QueryRowContext(c.Request.Context(),
		`SELECT COUNT(*) FROM actor_decls WHERE id = ? AND owner = ? AND deleted_at IS NULL`,
		declID, userID).Scan(&count); err != nil || count == 0 {
		c.JSON(http.StatusNotFound, gin.H{"error": "decl not found"})
		return
	}
	now := time.Now().UnixMilli()
	if req.Name != nil {
		_, _ = a.db.ExecContext(c.Request.Context(),
			`UPDATE actor_decls SET name = ?, updated_at = ? WHERE id = ? AND owner = ?`,
			strings.TrimSpace(*req.Name), now, declID, userID)
	}
	if len(req.Config) > 0 {
		if !isJSONObject(req.Config) {
			c.JSON(http.StatusBadRequest, gin.H{"error": "config must be a JSON object"})
			return
		}
		_, _ = a.db.ExecContext(c.Request.Context(),
			`UPDATE actor_decls SET config_json = ?, updated_at = ? WHERE id = ? AND owner = ?`,
			string(req.Config), now, declID, userID)
	}
	c.JSON(http.StatusOK, gin.H{"updated": declID, "note": "takes effect on restart"})
}

// handleDeleteDecl is the WORLD-LAYER half of a declared instance's death (C6): a
// cross-channel identity de-registration, HTTP-legitimate. It (1) soft-deletes the
// actor_decls declaration (the world-layer fact), then (2) projects that fact into
// every channel the instance was in — the world→channel cascade. The per-channel
// removal is NOT an orphan table DELETE that leaves the live cell a zombie (病灶
// #6): it goes through the Home control-plane machine seam (Home.Remove = despawn
// → dereg cascade → system-authored mirror), so the live cell dies + membership
// clears + a system-authored removal lands in the channel log. Order (红线 3):
// intent row first (the ring won't re-mint), then Home.Remove.
func (a *App) handleDeleteDecl(c *gin.Context) {
	userID := middleware.UserID(c)
	declID := c.Param("declID")
	ctx := c.Request.Context()
	release := a.declLocks.lock(declID)
	defer release()

	a.mu.RLock()
	homes := make(map[channel.ID]*home.Home, len(a.homes))
	for id, h := range a.homes {
		homes[id] = h
	}
	a.mu.RUnlock()
	var targets []declFanoutTarget
	for id, h := range homes {
		rows, err := h.Composition(ctx)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "channel composition unavailable"})
			return
		}
		for _, row := range rows {
			if row.DeclID == declID {
				targets = append(targets, declFanoutTarget{ChannelID: string(id), InstanceID: string(row.InstanceID)})
			}
		}
	}
	targetsJSON, err := json.Marshal(targets)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "internal error"})
		return
	}
	now := time.Now().UnixMilli()
	tx, err := a.db.BeginTx(ctx, nil)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "internal error"})
		return
	}
	defer tx.Rollback()
	res, err := tx.ExecContext(ctx,
		`UPDATE actor_decls SET deleted_at = ?, updated_at = ? WHERE id = ? AND owner = ? AND deleted_at IS NULL`,
		now, now, declID, userID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "internal error"})
		return
	}
	if n, _ := res.RowsAffected(); n == 0 {
		c.JSON(http.StatusNotFound, gin.H{"error": "decl not found"})
		return
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO decl_fanout_jobs(decl_id,op,initiator,targets_json,created_at) VALUES (?,?,?,?,?)`, declID, "delete", userID, string(targetsJSON), now); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "internal error"})
		return
	}
	if err := tx.Commit(); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "internal error"})
		return
	}
	if a.fanout != nil {
		a.fanout.notify()
	}
	c.JSON(http.StatusOK, gin.H{"deleted": declID, "queued": len(targets)})
}

type introduceActorReq struct {
	DeclID      string `json:"decl_id"`
	Placement   string `json:"placement"`
	DesiredHost string `json:"desired_host"`
	MakeDefault bool   `json:"make_default"`
	// Class optionally OVERRIDES the declaration's default_class for THIS channel
	// (per-channel runtime engine). Empty = use the declaration's default_class.
	// Effective engine = Class ?? actor_decls.default_class, resolved here (eager)
	// into the channel-local composition class.
	Class string `json:"class"`
}

// handleIntroduceActor is the HTTP垫片 for the add half of composition CRUD
// (POST /api/channels/:chID/actors — introduce a declared instance into a
// channel). It replays the session user through the door
// (channel.introduce_actor, audience=[system]) — the SAME chain a frame drives
// (red line 11: no control ability reachable only via HTTP). Ref-eligibility
// (二型律), ClassKind precheck, intent write + Admit + poke all live in the
// executor behind the gate; the request + terminal land 笔为 user:X in the log. A
// non-member session user is refused by the door's户籍校验 (膜律: 严禁 Admit 兜底
// — the UI引导 "先加入频道").
func (a *App) handleIntroduceActor(c *gin.Context) {
	chID, ok := a.requireChannelAccess(c)
	if !ok {
		return
	}
	var req introduceActorReq
	if err := c.ShouldBindJSON(&req); err != nil || strings.TrimSpace(req.DeclID) == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "decl_id required"})
		return
	}
	payload, _ := json.Marshal(introducePayload{
		DeclID: req.DeclID, Placement: req.Placement, DesiredHost: req.DesiredHost,
		MakeDefault: req.MakeDefault, Class: req.Class,
	})
	r, err := a.submitControlThroughDoor(c.Request.Context(), chID, middleware.UserID(c),
		home.TypeIntroduceActor, payload)
	a.finishControlRequest(c, r, err, func(body map[string]any) (int, any) {
		body["channel_id"] = chID
		status := http.StatusOK
		if created, _ := body["created"].(bool); created {
			status = http.StatusCreated
		}
		return status, body
	})
}

// handleRestartDecl restarts the declared instance's server-placed cells in every
// channel the CALLER is a member of. It is an HTTP垫片 (NP-1=c): world-layer
// ownership scopes WHICH channels (the caller's own declaration), but each
// per-channel restart is replayed through the door (channel.restart_actor,
// audience=[system]) — no direct embodiment mutation from HTTP (红线11). The per-channel
// authority is the door's member check; a channel the caller is not a member of
// is skipped (膜律). Restart is原地换脑 (A-P14): editing config
// does not hot-update a live cell; taking effect needs a rebuild.
func (a *App) handleRestartDecl(c *gin.Context) {
	userID := middleware.UserID(c)
	declID := c.Param("declID")
	ctx := c.Request.Context()
	release := a.declLocks.lock(declID)
	defer release()
	// World-layer scope: the caller owns this declaration (enumerate ITS channels).
	// The per-channel restart authority is the door's member check, not ownership.
	var owned int
	if err := a.db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM actor_decls WHERE id = ? AND owner = ? AND deleted_at IS NULL`,
		declID, userID).Scan(&owned); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "internal error"})
		return
	}
	if owned == 0 {
		c.JSON(http.StatusNotFound, gin.H{"error": "decl not found"})
		return
	}
	var targets []declFanoutTarget
	a.mu.RLock()
	homes := make(map[channel.ID]*home.Home, len(a.homes))
	for id, h := range a.homes {
		homes[id] = h
	}
	a.mu.RUnlock()
	for id, h := range homes {
		_, member, err := h.ResolvePrincipal(ctx, actor.KindHuman, userID)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "internal error"})
			return
		}
		if !member {
			continue
		}
		rows, err := h.Composition(ctx)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "internal error"})
			return
		}
		for _, row := range rows {
			if row.DeclID == declID {
				targets = append(targets, declFanoutTarget{ChannelID: string(id), InstanceID: string(row.InstanceID)})
			}
		}
	}
	targetsJSON, err := json.Marshal(targets)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "internal error"})
		return
	}
	res, err := a.db.ExecContext(ctx, `INSERT INTO decl_fanout_jobs(decl_id,op,initiator,targets_json,created_at) VALUES (?,?,?,?,?)`, declID, "restart", userID, string(targetsJSON), time.Now().UnixMilli())
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "internal error"})
		return
	}
	jobID, _ := res.LastInsertId()
	if a.fanout != nil {
		a.fanout.notify()
	}
	c.JSON(http.StatusOK, gin.H{"decl_id": declID, "restarted": len(targets), "job_id": jobID})
}
