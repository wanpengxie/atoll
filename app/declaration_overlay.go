package app

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/wanpengxie/atoll/app/internal/middleware"
	"github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/registry"
)

type declarationOverlayRequest struct {
	Config json.RawMessage `json:"config"`
}

func (a *App) handlePutDeclarationOverlay(c *gin.Context) {
	var req declarationOverlayRequest
	if err := c.ShouldBindJSON(&req); err != nil || !isJSONObject(req.Config) {
		c.JSON(http.StatusBadRequest, gin.H{"error": "config must be a JSON object"})
		return
	}
	canonical, err := channel.CanonicalJSON(req.Config)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid config"})
		return
	}
	a.writeDeclarationOverlay(c, canonical, false)
}

func (a *App) handleDeleteDeclarationOverlay(c *gin.Context) {
	a.writeDeclarationOverlay(c, nil, true)
}

// writeDeclarationOverlay keeps the declaration authorization facts and the
// overlay mutation in one realm transaction. Membership is channel truth and
// is deliberately checked before entering the realm transaction.
func (a *App) writeDeclarationOverlay(c *gin.Context, config json.RawMessage, clear bool) {
	chID, ok := a.requireChannelMember(c)
	if !ok {
		return
	}
	declID := c.Param("declID")
	principal := middleware.UserID(c)
	tx, err := a.db.BeginTx(c.Request.Context(), nil)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "internal error"})
		return
	}
	defer tx.Rollback()

	var owner, visibility, class string
	var global sql.NullString
	var deletedAt sql.NullInt64
	if err := tx.QueryRowContext(c.Request.Context(),
		`SELECT owner,visibility,default_class,config_json,deleted_at FROM actor_decls WHERE id=?`, declID).
		Scan(&owner, &visibility, &class, &global, &deletedAt); err != nil {
		if err == sql.ErrNoRows {
			c.JSON(http.StatusNotFound, gin.H{"error": "declaration not found"})
		} else {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "internal error"})
		}
		return
	}
	if deletedAt.Valid {
		c.JSON(http.StatusConflict, gin.H{"error": "declaration is deleted"})
		return
	}
	if !declarationVisibleTo(visibility, owner, principal) {
		c.JSON(http.StatusForbidden, gin.H{"error": "declaration owner required"})
		return
	}
	validated := config
	if clear && global.Valid {
		validated = json.RawMessage(global.String)
	}
	if err := registry.ValidateConfig(class, validated); err != nil {
		if errors.Is(err, registry.ErrUnknownClass) {
			// The decl's persisted class no longer exists in this binary —
			// a different ailment from an invalid config value.
			c.JSON(http.StatusBadRequest, gin.H{"error": "unknown or reserved class"})
			return
		}
		status := http.StatusBadRequest
		code := "config_invalid"
		if clear {
			status = http.StatusConflict
			code = "config_invalid_on_clear"
		}
		c.JSON(status, gin.H{"error": code})
		return
	}
	if clear {
		_, err = tx.ExecContext(c.Request.Context(),
			`DELETE FROM channel_decl_overlays WHERE channel_id=? AND decl_id=?`, chID, declID)
	} else {
		_, err = tx.ExecContext(c.Request.Context(), `INSERT INTO channel_decl_overlays(channel_id,decl_id,config_json,updated_at)
			VALUES(?,?,?,?) ON CONFLICT(channel_id,decl_id) DO UPDATE SET config_json=excluded.config_json,updated_at=excluded.updated_at`,
			chID, declID, string(config), time.Now().UnixMilli())
	}
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "internal error"})
		return
	}
	if err := tx.Commit(); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "internal error"})
		return
	}
	a.host.Poke(channel.ID(chID))
	c.JSON(http.StatusOK, gin.H{"updated": declID})
}

func (a *App) pokeAllChannels(ctx context.Context) {
	ids, err := a.directoryChannelIDs(ctx)
	if err != nil {
		a.logger.Warn("declaration poke directory read failed", "error", err)
		return
	}
	for _, id := range ids {
		a.host.Poke(id)
	}
}
