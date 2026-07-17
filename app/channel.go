package app

import (
	"context"
	"net/http"
	"os"
	"path/filepath"
	"sort"
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

// ---------------------------------------------------------------------------
// Channel handlers
// ---------------------------------------------------------------------------

// rollbackOpenedChannel removes every artifact owned by an opened-but-incomplete
// channel. The Home handle is detached under a.mu and closed after the lock is gone.
func (a *App) rollbackOpenedChannel(ctx context.Context, chID channel.ID, dbPath string) {
	if h := a.detachHome(chID); h != nil {
		_ = h.Close()
	}
	_, _ = a.db.ExecContext(context.WithoutCancel(ctx), `DELETE FROM channels WHERE id = ?`, string(chID))
	_ = os.Remove(dbPath)
}

// seedOpenedChannel performs the required post-open genesis writes against the real
// Home and owns rollback on failure. It has no injectable callback: tests exercise
// failure by closing the Home, which is the same failure surface production handles.
func (a *App) seedOpenedChannel(ctx context.Context, h *home.Home, chID channel.ID, dbPath, userID string, at int64) (actor.ActorID, actor.ActorID, error) {
	creatorID, err := h.AdmitChannelOwner(ctx, userID)
	if err != nil {
		a.rollbackOpenedChannel(ctx, chID, dbPath)
		return "", "", err
	}
	boost, err := h.Declare(ctx, home.DeclareRequest{
		SourceDeclID: "sys:boost", Principal: defaultAgentPrincipal, Class: defaultBoostClass,
		Placement: storespec.NewServerPlacement(), MakeDefault: true,
		Kind: actor.KindAgent, CreatedAt: at,
	})
	if err != nil {
		a.rollbackOpenedChannel(ctx, chID, dbPath)
		return "", "", err
	}
	return creatorID, boost.Row.ID, nil
}

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

	// Open the substrate first so both genesis principals can be admitted and
	// yield their authoritative minted ids before app desired truth is written.
	home, err := a.createHome(channel.ID(chID), dbPath)
	if err != nil {
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
	rollback := func(stage string, err error) {
		// 锁纪律 (连接模型勘误期 §3.2 P1-6): 摘把手 under a.mu, Close OUTSIDE it — a
		// home.Close held under a.mu blocks every concurrent getHome / resolver read.
		a.rollbackOpenedChannel(c.Request.Context(), channel.ID(chID), dbPath)
		a.logger.Error("create channel: "+stage, "channel", chID, "err", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "internal error"})
	}

	// Membrane law: the creator is the unique channel owner. Genesis uses the
	// dedicated owner admission; ordinary membership remains role-neutral. No cell here (the human is
	// embodied by the ring / subjectgate, never welded at this call site).
	creatorID, boostID, mErr := a.seedOpenedChannel(c.Request.Context(), home, channel.ID(chID), dbPath, userID, now)
	if mErr != nil {
		a.logger.Error("create channel: genesis seed", "channel", chID, "err", mErr)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "internal error"})
		return
	}

	// Write directory, desired composition, and default pointer once with the
	// minted id. No placeholder/repair window exists.
	tx, err := a.db.BeginTx(c.Request.Context(), nil)
	if err != nil {
		rollback("begin tx", err)
		return
	}
	_, err = tx.ExecContext(c.Request.Context(),
		`INSERT INTO channels (id, workspace_id, name, type, db_path, created_at) VALUES (?,?,?,?,?,?)`,
		chID, wsID, req.Name, req.Type, dbPath, now)
	if err != nil {
		_ = tx.Rollback()
		rollback("seed", err)
		return
	}
	if err := tx.Commit(); err != nil {
		rollback("commit", err)
		return
	}
	// 目录行提交后补一次 poke (连接模型勘误期 P1-4, 六轮终审): the two Admits above
	// (creator + boost) each fired a membership-change poke SYNCHRONOUSLY, inside
	// home.Admit, BEFORE this transaction committed — at that instant the resolver
	// (EntitlementSnapshot enumerates the `channels` directory table) could not
	// possibly see this channel yet, so a session poked at that moment re-resolves
	// into the SAME stale answer it already had. Poke again now that the directory
	// row is actually visible, so the creator's live session (if any) subscribes
	// within ≤下一泵轮 instead of waiting the T_sweep 30s backstop.
	if a.membershipPoke != nil {
		a.membershipPoke(userID)
	}

	c.JSON(http.StatusCreated, gin.H{
		"id": chID, "workspace_id": wsID, "name": req.Name,
		"type": req.Type, "created_at": now, "default_agent": string(boostID),
		"creator_actor_id": string(creatorID),
	})
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
	defaultAgent := ""
	if h := a.getHome(channel.ID(chID)); h != nil {
		if value, ok, derr := h.DefaultAgent(c.Request.Context()); derr == nil && ok {
			defaultAgent = string(value)
		}
	}
	c.JSON(http.StatusOK, gin.H{
		"id": id, "workspace_id": workspaceID, "name": name,
		"type": chType, "default_agent": defaultAgent, "created_at": createdAt,
	})
}

// handleDeleteChannel tears a channel down. The authority is WORLD-LAYER: a
// workspace member (requireChannelAccess) may delete it, judged entirely from the
// app-db directory — it NEVER consults channel-internal membership or requires a
// live/complete home. This is deliberate: a半成品 channel (a crash between
// createChannel's app-db commit and the creator's Admit left the app row + an empty
// channel-db membership, so the channel has NO members at all) must stay deletable.
// The home Close below is a no-op when the home is absent from the map, and the row/
// file deletes proceed regardless of whether membership was ever admitted.
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

	// Stabilize the affected per-daemon lock set, then acquire SQLite's writer
	// reservation before the in-transaction recheck. An attach-daemon writer for
	// a previously unseen daemon cannot slip a new binding between recheck and
	// delete: its INSERT waits behind this transaction and then observes no channel.
	ctx := c.Request.Context()
	for {
		rows, qerr := a.db.QueryContext(ctx, `SELECT daemon_id FROM daemon_channels WHERE channel_id=? ORDER BY daemon_id`, string(chID))
		if qerr != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "delete failed"})
			return
		}
		var ids []string
		for rows.Next() {
			var id string
			if err := rows.Scan(&id); err == nil {
				ids = append(ids, id)
			}
		}
		_ = rows.Close()
		sort.Strings(ids)
		var releases []func()
		for _, id := range ids {
			releases = append(releases, a.daemonLocks.lock(id))
		}
		releaseAll := func() {
			for i := len(releases) - 1; i >= 0; i-- {
				releases[i]()
			}
		}

		tx, err := a.db.BeginTx(ctx, nil)
		if err != nil {
			releaseAll()
			c.JSON(http.StatusInternalServerError, gin.H{"error": "delete failed"})
			return
		}
		// Force the deferred SQLite transaction into its write phase before the
		// lock-set recheck.
		if _, err := tx.ExecContext(ctx, `UPDATE channels SET created_at=created_at WHERE id=?`, string(chID)); err != nil {
			_ = tx.Rollback()
			releaseAll()
			c.JSON(http.StatusInternalServerError, gin.H{"error": "delete failed"})
			return
		}
		checkRows, err := tx.QueryContext(ctx, `SELECT daemon_id FROM daemon_channels WHERE channel_id=? ORDER BY daemon_id`, string(chID))
		if err != nil {
			_ = tx.Rollback()
			releaseAll()
			c.JSON(http.StatusInternalServerError, gin.H{"error": "delete failed"})
			return
		}
		var check []string
		for checkRows.Next() {
			var id string
			if err := checkRows.Scan(&id); err == nil {
				check = append(check, id)
			}
		}
		_ = checkRows.Close()
		sort.Strings(check)
		if strings.Join(ids, "\x00") != strings.Join(check, "\x00") {
			_ = tx.Rollback()
			releaseAll()
			continue
		}
		_, writeErr := tx.ExecContext(ctx, `DELETE FROM daemon_channels WHERE channel_id=?`, string(chID))
		if writeErr == nil {
			_, writeErr = tx.ExecContext(ctx, `DELETE FROM channels WHERE id=?`, string(chID))
		}
		if writeErr != nil || tx.Commit() != nil {
			_ = tx.Rollback()
			releaseAll()
			c.JSON(http.StatusInternalServerError, gin.H{"error": "delete failed"})
			return
		}
		releaseAll()
		break
	}

	// Directory intent is gone; now detach and close the in-memory universe
	// outside a.mu.
	if h := a.detachHome(channel.ID(chID)); h != nil {
		_ = h.Close()
	}

	// Remove the per-channel sqlite file.
	_ = os.Remove(dbPath)

	c.JSON(http.StatusOK, gin.H{"ok": true})
}

// handleListWorkspaceMembers lists the WORKSPACE roster reachable through this
// channel (workspace_members JOIN users) — a world-layer / subject-domain
// projection (HTTP legitimate), NOT the channel's actor census. Named honestly:
// "who is in the workspace", not "who is in the channel". Channel actor truth
// is available only through the canonical in-gate subject protocol; the two
// questions must not be conflated (A11).
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
