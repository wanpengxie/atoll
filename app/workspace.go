package app

import (
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
)

// ---------------------------------------------------------------------------
// Workspace handlers
// ---------------------------------------------------------------------------

func (a *App) handleListWorkspaces(c *gin.Context) {
	userID := getUserID(c)
	rows, err := a.db.QueryContext(c.Request.Context(),
		`SELECT w.id, w.owner_id, w.name, w.created_at
		 FROM workspaces w
		 JOIN workspace_members wm ON w.id = wm.workspace_id
		 WHERE wm.user_id = ?`, userID,
	)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "query failed"})
		return
	}
	defer rows.Close()

	var result []gin.H
	for rows.Next() {
		var id, ownerID, name string
		var createdAt int64
		if err := rows.Scan(&id, &ownerID, &name, &createdAt); err != nil {
			continue
		}
		result = append(result, gin.H{
			"id": id, "owner_id": ownerID, "name": name, "created_at": createdAt,
		})
	}
	if result == nil {
		result = []gin.H{}
	}
	c.JSON(http.StatusOK, result)
}

func (a *App) handleCreateWorkspace(c *gin.Context) {
	userID := getUserID(c)
	var req struct {
		Name string `json:"name"`
	}
	if err := c.ShouldBindJSON(&req); err != nil || req.Name == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "name required"})
		return
	}

	wsID := uuid.NewString()
	now := time.Now().UnixMilli()

	tx, err := a.db.BeginTx(c.Request.Context(), nil)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "tx failed"})
		return
	}
	defer tx.Rollback()

	_, err = tx.ExecContext(c.Request.Context(),
		`INSERT INTO workspaces (id, owner_id, name, created_at) VALUES (?,?,?,?)`,
		wsID, userID, req.Name, now,
	)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "create workspace failed"})
		return
	}

	_, err = tx.ExecContext(c.Request.Context(),
		`INSERT INTO workspace_members (workspace_id, user_id, role) VALUES (?,?,?)`,
		wsID, userID, "owner",
	)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "add member failed"})
		return
	}

	if err := tx.Commit(); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "commit failed"})
		return
	}

	c.JSON(http.StatusCreated, gin.H{
		"id": wsID, "owner_id": userID, "name": req.Name, "created_at": now,
	})
}
