package app

import (
	"database/sql"
	"encoding/json"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"

	"github.com/wanpengxie/atoll/app/internal/middleware"
	"github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/registry"
)

// actor_decls.go is the declaration registry's global-value face: the front-end
// CRUD for a user's blueprints (create / inspect / edit / soft-delete). Channel
// introduction and removal are structural operations; channel-local config lives
// in the declaration-keyed overlay API. The declaration layer is kind-neutral:
// one row = identity + class + config + owner + visibility, for agents and tools
// alike. It writes realm current values directly, then pokes serving channels;
// each Home pulls and applies the resolved snapshot during reconcile.

type createDeclReq struct {
	Name string `json:"name"`
	// Class = the declaration's DEFAULT engine class (stored as
	// actor_decls.default_class); a per-channel engine may override it at
	// introduce time.
	Class      string          `json:"class"`
	Config     json.RawMessage `json:"config"`
	Visibility string          `json:"visibility"`
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
	// Same gate as realmOps.CreateDeclaration (realm_ops.go): an unknown class is
	// rejected, and realm-tool is reserved — composition.go builds a real realm
	// boundary tool for class=="realm-tool", so a forged realm-tool declaration
	// would smuggle a membrane entry past the "remove realm-tool = close it"
	// sovereignty switch. The default "go-kimi" is a registered class and passes.
	if _, ok, err := a.declarationClassKind(c.Request.Context(), class); err != nil || !ok {
		c.JSON(http.StatusBadRequest, gin.H{"error": "unknown or reserved class"})
		return
	}
	visibility := strings.TrimSpace(req.Visibility)
	if visibility == "" {
		visibility = "private"
	}
	if visibility != "public" && visibility != "private" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "visibility must be public or private"})
		return
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
	if err := registry.ValidateConfig(class, req.Config); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "config_invalid"})
		return
	}
	if _, err := a.db.ExecContext(c.Request.Context(),
		`INSERT INTO actor_decls (id, name, owner, default_class, config_json, created_at, updated_at, visibility) VALUES (?,?,?,?,?,?,?,?)`,
		id, strings.TrimSpace(req.Name), userID, class, cfg, now, now, visibility); err != nil {
		a.logger.Error("create decl", "err", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "internal error"})
		return
	}
	c.JSON(http.StatusCreated, gin.H{
		"id": id, "name": strings.TrimSpace(req.Name), "class": class,
		"owner": userID, "visibility": visibility, "created_at": now,
	})
}

// handleListDecls lists every public declaration plus the current principal's
// private declarations. Visibility is a realm roster policy; ownership is not a
// prerequisite for inspecting a public declaration.
func (a *App) handleListDecls(c *gin.Context) {
	userID := middleware.UserID(c)
	rows, err := a.db.QueryContext(c.Request.Context(),
		`SELECT id, name, owner, default_class, visibility, created_at, updated_at FROM actor_decls WHERE (visibility = 'public' OR owner = ?) AND deleted_at IS NULL ORDER BY created_at`,
		userID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "internal error"})
		return
	}
	defer rows.Close()
	out := []gin.H{}
	for rows.Next() {
		var id, name, owner, class, visibility string
		var ca, ua int64
		if err := rows.Scan(&id, &name, &owner, &class, &visibility, &ca, &ua); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "internal error"})
			return
		}
		out = append(out, gin.H{"id": id, "name": name, "owner": owner, "class": class, "visibility": visibility, "created_at": ca, "updated_at": ua})
	}
	if err := rows.Err(); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "internal error"})
		return
	}
	if err := rows.Close(); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "internal error"})
		return
	}

	// Project instance identity only. A channel's declaration version is a local
	// history/order fence, not realm value identity and not part of this API DTO.
	for _, decl := range out {
		declID := decl["id"].(string)
		instances := make([]gin.H, 0)
		indexed, err := a.relations.InstancesOf(c.Request.Context(), declID)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "declaration instances unavailable"})
			return
		}
		for _, instance := range indexed {
			instances = append(instances, gin.H{
				"channel_id": string(instance.ChannelID), "instance_id": string(instance.ActorID),
			})
		}
		decl["instances"] = instances
	}
	c.JSON(http.StatusOK, gin.H{"decls": out})
}

type updateDeclReq struct {
	Name       *string         `json:"name"`
	Class      *string         `json:"class"`
	Config     json.RawMessage `json:"config"`
	Visibility *string         `json:"visibility"`
}

// handleUpdateDecl writes the global declaration value. Channel Homes pull the
// new class/config after commit; class changes must remain within one actor kind.
func (a *App) handleUpdateDecl(c *gin.Context) {
	userID := middleware.UserID(c)
	declID := c.Param("declID")
	var req updateDeclReq
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "bad request"})
		return
	}
	now := time.Now().UnixMilli()
	tx, err := a.db.BeginTx(c.Request.Context(), nil)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "internal error"})
		return
	}
	defer tx.Rollback()
	var currentClass string
	var currentConfig sql.NullString
	if err := tx.QueryRowContext(c.Request.Context(), `SELECT default_class,config_json FROM actor_decls WHERE `+ownedDeclarationWhere+` AND deleted_at IS NULL`, declID, userID).Scan(&currentClass, &currentConfig); err != nil {
		if err == sql.ErrNoRows {
			c.JSON(http.StatusNotFound, gin.H{"error": "decl not found"})
		} else {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "internal error"})
		}
		return
	}
	finalClass := currentClass
	if req.Class != nil {
		finalClass = strings.TrimSpace(*req.Class)
	}
	var finalConfig json.RawMessage
	if currentConfig.Valid && currentConfig.String != "" {
		finalConfig = json.RawMessage(currentConfig.String)
	}
	if len(req.Config) > 0 {
		finalConfig = req.Config
	}
	if err := registry.ValidateConfig(finalClass, finalConfig); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "config_invalid"})
		return
	}
	if req.Class != nil {
		sameKind, classErr := a.declarationClassTransition(c.Request.Context(), currentClass, finalClass)
		if classErr != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "class registry unavailable"})
			return
		}
		if !sameKind {
			c.JSON(http.StatusBadRequest, gin.H{"error": "class must remain within the declaration kind"})
			return
		}
		if _, err := tx.ExecContext(c.Request.Context(),
			`UPDATE actor_decls SET default_class=?,updated_at=? WHERE `+ownedDeclarationWhere,
			finalClass, now, declID, userID); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "internal error"})
			return
		}
	}
	if req.Name != nil {
		if _, err := tx.ExecContext(c.Request.Context(),
			`UPDATE actor_decls SET name = ?, updated_at = ? WHERE `+ownedDeclarationWhere,
			strings.TrimSpace(*req.Name), now, declID, userID); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "internal error"})
			return
		}
	}
	if req.Visibility != nil {
		visibility := strings.TrimSpace(*req.Visibility)
		if visibility != "public" && visibility != "private" {
			c.JSON(http.StatusBadRequest, gin.H{"error": "visibility must be public or private"})
			return
		}
		if _, err := tx.ExecContext(c.Request.Context(),
			`UPDATE actor_decls SET visibility = ?, updated_at = ? WHERE `+ownedDeclarationWhere,
			visibility, now, declID, userID); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "internal error"})
			return
		}
	}
	if len(req.Config) > 0 {
		if !isJSONObject(req.Config) {
			c.JSON(http.StatusBadRequest, gin.H{"error": "config must be a JSON object"})
			return
		}
		canonical, err := channel.CanonicalJSON(req.Config)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid config"})
			return
		}
		if _, err := tx.ExecContext(c.Request.Context(),
			`UPDATE actor_decls SET config_json = ?, updated_at = ? WHERE `+ownedDeclarationWhere,
			string(canonical), now, declID, userID); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "internal error"})
			return
		}
	}
	if err := tx.Commit(); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "internal error"})
		return
	}
	a.pokeAllChannels(c.Request.Context())
	c.JSON(http.StatusOK, gin.H{"updated": declID})
}

// handleDeleteDecl marks supply stopped. Existing channel instances retain
// their last local snapshot until an explicit Remove operation ends them.
func (a *App) handleDeleteDecl(c *gin.Context) {
	userID := middleware.UserID(c)
	declID := c.Param("declID")
	ctx := c.Request.Context()
	now := time.Now().UnixMilli()
	tx, err := a.db.BeginTx(ctx, nil)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "internal error"})
		return
	}
	defer tx.Rollback()
	res, err := tx.ExecContext(ctx,
		`UPDATE actor_decls SET deleted_at = ?, updated_at = ? WHERE `+ownedDeclarationWhere+` AND deleted_at IS NULL`,
		now, now, declID, userID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "internal error"})
		return
	}
	if n, _ := res.RowsAffected(); n == 0 {
		c.JSON(http.StatusNotFound, gin.H{"error": "decl not found"})
		return
	}
	if err := tx.Commit(); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "internal error"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"deleted": declID})
}
