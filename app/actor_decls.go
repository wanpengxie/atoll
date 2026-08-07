package app

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"

	"github.com/wanpengxie/atoll/app/contract"
	"github.com/wanpengxie/atoll/app/internal/middleware"
	"github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/registry"
)

var errDeclarationConflict = errors.New("declaration id conflicts with existing value")

// createDeclarationCore is the transport-free declaration creation path used
// by both HTTP and local-node convergence. A caller-provided stable id is
// idempotently reusable only when every declaration value agrees.
func (a *App) createDeclarationCore(ctx context.Context, id, name, owner, class string, config json.RawMessage, visibility string) (contract.Declaration, error) {
	id = strings.TrimSpace(id)
	name = strings.TrimSpace(name)
	owner = strings.TrimSpace(owner)
	class = strings.TrimSpace(class)
	visibility = strings.TrimSpace(visibility)
	if id == "" || name == "" || owner == "" || class == "" {
		return contract.Declaration{}, errors.New("id, name, owner, and class required")
	}
	if visibility == "" {
		visibility = "private"
	}
	if visibility != "public" && visibility != "private" {
		return contract.Declaration{}, errors.New("visibility must be public or private")
	}
	if _, ok, err := a.declarationClassKind(ctx, class); err != nil || !ok {
		return contract.Declaration{}, fmt.Errorf("unknown or reserved class %q", class)
	}
	if len(config) > 0 && !isJSONObject(config) {
		return contract.Declaration{}, errors.New("config must be a JSON object")
	}
	if err := registry.ValidateConfig(class, config); err != nil {
		return contract.Declaration{}, err
	}
	cfg := ""
	if len(config) > 0 {
		var object map[string]any
		if err := json.Unmarshal(config, &object); err != nil {
			return contract.Declaration{}, errors.New("config must be a JSON object")
		}
		canonical, err := json.Marshal(object)
		if err != nil {
			return contract.Declaration{}, err
		}
		cfg = string(canonical)
	}
	var existing contract.Declaration
	var existingCfg string
	var deleted sql.NullInt64
	err := a.db.QueryRowContext(ctx, `SELECT id,name,owner,default_class,config_json,visibility,created_at,deleted_at FROM actor_decls WHERE id=?`, id).Scan(&existing.ID, &existing.Name, &existing.Owner, &existing.Class, &existingCfg, &existing.Visibility, &existing.CreatedAt, &deleted)
	if err == nil {
		if deleted.Valid || existing.Name != name || existing.Owner != owner || existing.Class != class || existingCfg != cfg || existing.Visibility != visibility {
			return contract.Declaration{}, errDeclarationConflict
		}
		existing.Instances = []contract.DeclarationInstance{}
		return existing, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return contract.Declaration{}, err
	}
	now := time.Now().UnixMilli()
	if _, err := a.db.ExecContext(ctx, `INSERT INTO actor_decls (id,name,owner,default_class,config_json,created_at,updated_at,visibility) VALUES (?,?,?,?,?,?,?,?)`, id, name, owner, class, cfg, now, now, visibility); err != nil {
		return contract.Declaration{}, err
	}
	return contract.Declaration{ID: id, Name: name, Owner: owner, Class: class, Visibility: visibility, CreatedAt: now, Instances: []contract.DeclarationInstance{}}, nil
}

// actor_decls.go is the declaration registry's global-value face: the front-end
// CRUD for a user's blueprints (create / inspect / edit / soft-delete). Channel
// introduction and removal are structural operations; channel-local config lives
// in the declaration-keyed overlay API. The declaration layer is kind-neutral:
// one row = identity + class + config + owner + visibility, for agents and tools
// alike. It writes realm current values directly, then pokes serving channels;
// each Home pulls and applies the resolved snapshot during reconcile.

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
	var req contract.DeclarationCreateRequest
	if !decodeRequest(c, &req) {
		return
	}
	if strings.TrimSpace(req.Name) == "" {
		writeAPIError(c, http.StatusBadRequest, contract.CodeInvalidRequest, "name required")
		return
	}
	class := strings.TrimSpace(req.Class)
	if class == "" {
		writeAPIError(c, http.StatusBadRequest, contract.CodeInvalidRequest, "class required")
		return
	}
	// Same gate as realmOps.CreateDeclaration (realm_ops.go): an unknown class is
	// rejected, and realm-tool is reserved — composition.go builds a real realm
	// boundary tool for class=="realm-tool", so a forged realm-tool declaration
	// would smuggle a membrane entry past the "remove realm-tool = close it"
	// sovereignty switch.
	if _, ok, err := a.declarationClassKind(c.Request.Context(), class); err != nil || !ok {
		writeAPIError(c, http.StatusBadRequest, contract.CodeUnknownClass, "unknown or reserved class")
		return
	}
	visibility := strings.TrimSpace(req.Visibility)
	if visibility == "" {
		visibility = "private"
	}
	if visibility != "public" && visibility != "private" {
		writeAPIError(c, http.StatusBadRequest, contract.CodeInvalidRequest, "visibility must be public or private")
		return
	}
	id := uuid.NewString()
	decl, err := a.createDeclarationCore(c.Request.Context(), id, req.Name, userID, class, req.Config, visibility)
	if err != nil {
		a.logger.Error("create decl", "err", err)
		writeAPIError(c, http.StatusBadRequest, contract.CodeConfigInvalid, "invalid declaration")
		return
	}
	c.JSON(http.StatusCreated, decl)
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
		writeAPIError(c, http.StatusInternalServerError, contract.CodeInternal, "internal error")
		return
	}
	defer rows.Close()
	out := []contract.Declaration{}
	for rows.Next() {
		var id, name, owner, class, visibility string
		var ca, ua int64
		if err := rows.Scan(&id, &name, &owner, &class, &visibility, &ca, &ua); err != nil {
			writeAPIError(c, http.StatusInternalServerError, contract.CodeInternal, "internal error")
			return
		}
		out = append(out, contract.Declaration{
			ID: id, Name: name, Owner: owner, Class: class, Visibility: visibility,
			CreatedAt: ca, UpdatedAt: ua, Instances: []contract.DeclarationInstance{},
		})
	}
	if err := rows.Err(); err != nil {
		writeAPIError(c, http.StatusInternalServerError, contract.CodeInternal, "internal error")
		return
	}
	if err := rows.Close(); err != nil {
		writeAPIError(c, http.StatusInternalServerError, contract.CodeInternal, "internal error")
		return
	}

	// Project instance identity only. A channel's declaration version is a local
	// history/order fence, not realm value identity and not part of this API DTO.
	for i := range out {
		declID := out[i].ID
		instances := make([]contract.DeclarationInstance, 0)
		indexed, err := a.relations.InstancesOf(c.Request.Context(), declID)
		if err != nil {
			writeAPIError(c, http.StatusInternalServerError, contract.CodeInternal, "declaration instances unavailable")
			return
		}
		for _, instance := range indexed {
			instances = append(instances, contract.DeclarationInstance{
				ChannelID: string(instance.ChannelID), InstanceID: string(instance.ActorID),
			})
		}
		out[i].Instances = instances
	}
	c.JSON(http.StatusOK, contract.DeclarationList{Declarations: out})
}

// handleUpdateDecl writes the global declaration value. Channel Homes pull the
// new class/config after commit; class changes must remain within one actor kind.
func (a *App) handleUpdateDecl(c *gin.Context) {
	userID := middleware.UserID(c)
	declID := c.Param("declID")
	var req contract.DeclarationUpdateRequest
	if !decodeRequest(c, &req) {
		return
	}
	now := time.Now().UnixMilli()
	tx, err := a.db.BeginTx(c.Request.Context(), nil)
	if err != nil {
		writeAPIError(c, http.StatusInternalServerError, contract.CodeInternal, "internal error")
		return
	}
	defer tx.Rollback()
	var currentClass string
	var currentConfig sql.NullString
	if err := tx.QueryRowContext(c.Request.Context(), `SELECT default_class,config_json FROM actor_decls WHERE `+ownedDeclarationWhere+` AND deleted_at IS NULL`, declID, userID).Scan(&currentClass, &currentConfig); err != nil {
		if err == sql.ErrNoRows {
			writeAPIError(c, http.StatusNotFound, contract.CodeDeclNotFound, "declaration not found")
		} else {
			writeAPIError(c, http.StatusInternalServerError, contract.CodeInternal, "internal error")
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
		if errors.Is(err, registry.ErrUnknownClass) {
			writeAPIError(c, http.StatusBadRequest, contract.CodeUnknownClass, "unknown or reserved class")
			return
		}
		writeAPIError(c, http.StatusBadRequest, contract.CodeConfigInvalid, "invalid config")
		return
	}
	if req.Class != nil {
		sameKind, classErr := a.declarationClassTransition(c.Request.Context(), currentClass, finalClass)
		if classErr != nil {
			writeAPIError(c, http.StatusInternalServerError, contract.CodeInternal, "class registry unavailable")
			return
		}
		if !sameKind {
			writeAPIError(c, http.StatusBadRequest, contract.CodeInvalidRequest, "class must remain within the declaration kind")
			return
		}
		if _, err := tx.ExecContext(c.Request.Context(),
			`UPDATE actor_decls SET default_class=?,updated_at=? WHERE `+ownedDeclarationWhere,
			finalClass, now, declID, userID); err != nil {
			writeAPIError(c, http.StatusInternalServerError, contract.CodeInternal, "internal error")
			return
		}
	}
	if req.Name != nil {
		if _, err := tx.ExecContext(c.Request.Context(),
			`UPDATE actor_decls SET name = ?, updated_at = ? WHERE `+ownedDeclarationWhere,
			strings.TrimSpace(*req.Name), now, declID, userID); err != nil {
			writeAPIError(c, http.StatusInternalServerError, contract.CodeInternal, "internal error")
			return
		}
	}
	if req.Visibility != nil {
		visibility := strings.TrimSpace(*req.Visibility)
		if visibility != "public" && visibility != "private" {
			writeAPIError(c, http.StatusBadRequest, contract.CodeInvalidRequest, "visibility must be public or private")
			return
		}
		if _, err := tx.ExecContext(c.Request.Context(),
			`UPDATE actor_decls SET visibility = ?, updated_at = ? WHERE `+ownedDeclarationWhere,
			visibility, now, declID, userID); err != nil {
			writeAPIError(c, http.StatusInternalServerError, contract.CodeInternal, "internal error")
			return
		}
	}
	if len(req.Config) > 0 {
		if !isJSONObject(req.Config) {
			writeAPIError(c, http.StatusBadRequest, contract.CodeConfigInvalid, "config must be a JSON object")
			return
		}
		canonical, err := channel.CanonicalJSON(req.Config)
		if err != nil {
			writeAPIError(c, http.StatusBadRequest, contract.CodeConfigInvalid, "invalid config")
			return
		}
		if _, err := tx.ExecContext(c.Request.Context(),
			`UPDATE actor_decls SET config_json = ?, updated_at = ? WHERE `+ownedDeclarationWhere,
			string(canonical), now, declID, userID); err != nil {
			writeAPIError(c, http.StatusInternalServerError, contract.CodeInternal, "internal error")
			return
		}
	}
	if err := tx.Commit(); err != nil {
		writeAPIError(c, http.StatusInternalServerError, contract.CodeInternal, "internal error")
		return
	}
	a.pokeAllChannels(c.Request.Context())
	c.JSON(http.StatusOK, contract.DeclarationMutation{Updated: declID})
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
		writeAPIError(c, http.StatusInternalServerError, contract.CodeInternal, "internal error")
		return
	}
	defer tx.Rollback()
	res, err := tx.ExecContext(ctx,
		`UPDATE actor_decls SET deleted_at = ?, updated_at = ? WHERE `+ownedDeclarationWhere+` AND deleted_at IS NULL`,
		now, now, declID, userID)
	if err != nil {
		writeAPIError(c, http.StatusInternalServerError, contract.CodeInternal, "internal error")
		return
	}
	if n, _ := res.RowsAffected(); n == 0 {
		writeAPIError(c, http.StatusNotFound, contract.CodeDeclNotFound, "declaration not found")
		return
	}
	if err := tx.Commit(); err != nil {
		writeAPIError(c, http.StatusInternalServerError, contract.CodeInternal, "internal error")
		return
	}
	c.JSON(http.StatusOK, contract.DeclarationMutation{Deleted: declID})
}
