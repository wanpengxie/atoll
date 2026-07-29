package app

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"

	"github.com/wanpengxie/atoll/app/internal/middleware"
	"github.com/wanpengxie/atoll/platform/channelhost"
	"github.com/wanpengxie/atoll/platform/channelspec"
	"github.com/wanpengxie/atoll/protocol/channel"
)

// ---------------------------------------------------------------------------
// Daemon handlers
// ---------------------------------------------------------------------------

func (a *App) handleListDaemons(c *gin.Context) {
	userID := middleware.UserID(c)
	rows, err := a.db.QueryContext(c.Request.Context(),
		`SELECT id, name, api_key, created_at FROM daemons WHERE owner_id = ? AND deleted_at IS NULL`, userID,
	)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "query failed"})
		return
	}
	defer rows.Close()

	var result []gin.H
	for rows.Next() {
		var id, name, apiKey string
		var createdAt int64
		if err := rows.Scan(&id, &name, &apiKey, &createdAt); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "query failed"})
			return
		}
		online, err := a.daemonOnline(c.Request.Context(), "", id)
		if err != nil {
			c.JSON(http.StatusServiceUnavailable, gin.H{"error": "daemon status unavailable"})
			return
		}
		result = append(result, gin.H{
			"id": id, "name": name, "api_key": apiKey, "created_at": createdAt,
			"online": online,
		})
	}
	if err := rows.Err(); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "query failed"})
		return
	}
	if result == nil {
		result = []gin.H{}
	}
	c.JSON(http.StatusOK, gin.H{"daemons": result})
}

// daemonOnline reports whether daemon id has a live link attach right now. It is
// online iff attached on any of its bound channels (or `only`, when non-empty).
// Read-time from each channel-home's View — derived, never a stored column.
func (a *App) daemonOnline(ctx context.Context, only channel.ID, daemonID string) (bool, error) {
	check := func(chID channel.ID) (bool, error) {
		release := a.channelLocks.lock(string(chID))
		defer release()
		bundle, ok := a.host.Acquire(chID)
		if !ok {
			return false, nil
		}
		bound, err := bundle.View().IsBound(ctx, daemonID)
		if err != nil {
			return false, nil
		}
		if err := a.relations.ReconcileBinding(ctx, chID, daemonID, bound); err != nil {
			a.logger.Warn("daemon binding relation repair failed", "channel", chID, "daemon", daemonID, "err", err)
		}
		if !bound {
			return false, nil
		}
		return bundle.View().IsAttached(daemonID), nil
	}
	if only != "" {
		return check(only)
	}
	ids, err := a.relations.BindingsOf(ctx, daemonID)
	if err != nil {
		return false, err
	}
	for _, ch := range ids {
		online, err := check(ch)
		if err != nil {
			continue
		}
		if online {
			return true, nil
		}
	}
	return false, nil
}

func (a *App) directoryChannelIDs(ctx context.Context) ([]channel.ID, error) {
	rows, err := a.db.QueryContext(ctx, `SELECT id FROM channels WHERE status='present' ORDER BY id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []channel.ID
	for rows.Next() {
		var id channel.ID
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		out = append(out, id)
	}
	return out, rows.Err()
}

func (a *App) handleCreateDaemon(c *gin.Context) {
	userID := middleware.UserID(c)
	var req struct {
		Name string `json:"name"`
	}
	if err := c.ShouldBindJSON(&req); err != nil || req.Name == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "name required"})
		return
	}

	daemonID := uuid.NewString()
	release := a.daemonLocks.lock(daemonID)
	defer release()
	apiKey := uuid.NewString()
	now := time.Now().UnixMilli()

	_, err := a.db.ExecContext(c.Request.Context(),
		`INSERT INTO daemons (id, owner_id, name, api_key, created_at) VALUES (?,?,?,?,?)`,
		daemonID, userID, req.Name, apiKey, now,
	)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "create daemon failed"})
		return
	}

	c.JSON(http.StatusCreated, gin.H{
		"id":      daemonID,
		"name":    req.Name,
		"api_key": apiKey,
	})
}

// handleDeleteDaemon commits a permanent realm tombstone. Channel Homes pull
// that value and detach their own bindings; this handler only observes serving
// channels for a bounded convenience response.
func (a *App) handleDeleteDaemon(c *gin.Context) {
	daemonID := c.Param("id")
	release := a.daemonLocks.lock(daemonID)
	locked := true
	defer func() {
		if locked {
			release()
		}
	}()
	userID := middleware.UserID(c)
	ctx := c.Request.Context()

	var owner string
	var deletedAt sql.NullInt64
	err := a.db.QueryRowContext(ctx, `SELECT owner_id,deleted_at FROM daemons WHERE id = ?`, daemonID).Scan(&owner, &deletedAt)
	if err == sql.ErrNoRows || (err == nil && owner != userID) {
		c.JSON(http.StatusNotFound, gin.H{"error": "daemon not found"})
		return
	}
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "delete failed"})
		return
	}

	if !deletedAt.Valid {
		if _, err := a.db.ExecContext(ctx,
			`UPDATE daemons SET deleted_at=? WHERE id=? AND owner_id=? AND deleted_at IS NULL`, time.Now().UnixMilli(), daemonID, userID); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "delete failed"})
			return
		}
	}
	release()
	locked = false
	a.pokeAllChannels(ctx)
	convergence := "convergence_pending"
	deadline := time.Now().Add(time.Second)
	for {
		if a.daemonConvergenceObservedClear(ctx, daemonID) {
			convergence = "observed_clear"
			break
		}
		if time.Now().After(deadline) || ctx.Err() != nil {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	c.JSON(http.StatusOK, gin.H{"ok": true, "authority_committed": true, "convergence": convergence})
}

func (a *App) daemonConvergenceObservedClear(ctx context.Context, daemonID string) bool {
	ids, err := a.directoryChannelIDs(ctx)
	if err != nil {
		return false
	}
	for _, chID := range ids {
		bundle, serving := a.host.Acquire(chID)
		if !serving {
			continue
		}
		bound, err := bundle.View().IsBound(ctx, daemonID)
		if err != nil || bound || bundle.View().IsAttached(daemonID) {
			return false
		}
	}
	return true
}

func (a *App) handleListChannelDaemons(c *gin.Context) {
	chID, ok := a.requireChannelMember(c)
	if !ok {
		return
	}
	bundle, available := a.host.Acquire(channel.ID(chID))
	if !available {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "channel unavailable"})
		return
	}
	// A channel roster is channel-scoped, not viewer-owned. Any member who can
	// read the channel sees every daemon currently bound to it, regardless of
	// which member registered that daemon in the realm.
	rows, err := a.db.QueryContext(c.Request.Context(), `SELECT id,name,created_at FROM daemons WHERE deleted_at IS NULL ORDER BY id`)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "query failed"})
		return
	}
	defer rows.Close()

	var result []gin.H
	for rows.Next() {
		var id, name string
		var createdAt int64
		if err := rows.Scan(&id, &name, &createdAt); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "query failed"})
			return
		}
		bound, err := bundle.View().IsBound(c.Request.Context(), id)
		if err != nil {
			c.JSON(http.StatusServiceUnavailable, gin.H{"error": "channel bindings unavailable"})
			return
		}
		if !bound {
			continue
		}
		result = append(result, gin.H{
			"id": id, "name": name, "created_at": createdAt,
			"online": bundle.View().IsAttached(id),
		})
	}
	if err := rows.Err(); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "query failed"})
		return
	}
	if result == nil {
		result = []gin.H{}
	}
	c.JSON(http.StatusOK, gin.H{"daemons": result})
}

func (a *App) handleAttachDaemon(c *gin.Context) {
	chID := channel.ID(c.Param("chID"))
	var req struct {
		DaemonID string `json:"daemon_id"`
	}
	if err := c.ShouldBindJSON(&req); err != nil || strings.TrimSpace(req.DaemonID) == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "daemon_id required"})
		return
	}
	req.DaemonID = strings.TrimSpace(req.DaemonID)
	userID := middleware.UserID(c)
	outcome, err := forwardSysop(c.Request.Context(), a, chID, sysopForward[channelspec.BindingResult]{
		Predicate: func(bundle channelhost.Bundle) (channelspec.BindingResult, bool, error) {
			bound, err := bundle.View().IsBound(c.Request.Context(), req.DaemonID)
			if err != nil || !bound {
				return channelspec.BindingResult{}, false, err
			}
			var deleted sql.NullInt64
			err = a.db.QueryRowContext(c.Request.Context(),
				`SELECT deleted_at FROM daemons WHERE id=?`, req.DaemonID).Scan(&deleted)
			if err != nil && !errors.Is(err, sql.ErrNoRows) {
				return channelspec.BindingResult{}, false, err
			}
			return channelspec.BindingResult{Bound: true}, err == nil && !deleted.Valid, nil
		},
		Qualify: func(bundle channelhost.Bundle) error {
			var owner string
			var deleted sql.NullInt64
			err := a.db.QueryRowContext(c.Request.Context(),
				`SELECT owner_id,deleted_at FROM daemons WHERE id=?`, req.DaemonID).
				Scan(&owner, &deleted)
			if errors.Is(err, sql.ErrNoRows) || (err == nil && owner != userID) {
				return &sysopGateError{Status: http.StatusForbidden, Code: "forbidden"}
			}
			if err != nil {
				return &sysopUnknownError{cause: err}
			}
			if deleted.Valid {
				return &sysopGateError{Status: http.StatusNotFound, Code: string(sysopCodeDaemonNotFound)}
			}
			return memberGate(c.Request.Context(), bundle, userID)
		},
		Invoke: func(sys channelhost.SysOp, ref string) (channelspec.BindingResult, error) {
			return sys.AttachDaemon(c.Request.Context(), channelspec.DaemonRequest{Ref: ref, DaemonID: req.DaemonID})
		},
	})
	if err != nil {
		writeSysopError(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"bound": outcome.Value.Bound, "changed": outcome.Changed})
}

// handleDetachDaemon records one exact channel operation. The membrane commits
// binding removal and instance cleanup together, then kicks any live link as a
// post-commit convergence hint.
func (a *App) handleDetachDaemon(c *gin.Context) {
	chID := channel.ID(c.Param("chID"))
	daemonID := c.Param("id")
	ctx := c.Request.Context()
	principal := middleware.UserID(c)
	outcome, err := forwardSysop(ctx, a, chID, sysopForward[channelspec.BindingResult]{
		Predicate: func(bundle channelhost.Bundle) (channelspec.BindingResult, bool, error) {
			bound, err := bundle.View().IsBound(ctx, daemonID)
			return channelspec.BindingResult{Bound: false}, !bound, err
		},
		Qualify: func(bundle channelhost.Bundle) error {
			if err := memberGate(ctx, bundle, principal); err != nil {
				return err
			}
			var owner string
			if err := a.db.QueryRowContext(ctx,
				`SELECT owner_id FROM daemons WHERE id=?`, daemonID).Scan(&owner); err != nil || owner != principal {
				if err != nil && !errors.Is(err, sql.ErrNoRows) {
					return &sysopUnknownError{cause: err}
				}
				return &sysopGateError{Status: http.StatusForbidden, Code: "forbidden"}
			}
			return nil
		},
		Invoke: func(sys channelhost.SysOp, ref string) (channelspec.BindingResult, error) {
			return sys.DetachDaemon(ctx, channelspec.DaemonRequest{Ref: ref, DaemonID: daemonID})
		},
	})
	if err != nil {
		writeSysopError(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{
		"bound": outcome.Value.Bound, "changed": outcome.Changed,
		"cleared_instances": outcome.Value.ClearedInstances,
	})
}

// ---------------------------------------------------------------------------
// Auth helper: single path for compute connections
// ---------------------------------------------------------------------------

// authAndResolve verifies the API key, resolves the daemon ID, and checks that
// the daemon is bound to the requested channel. This is the single auth path
// for compute connections -- fleet never does auth itself.
func (a *App) authAndResolve(apiKey string, chID channel.ID) (string, error) {
	var daemonID string
	err := a.db.QueryRow(
		`SELECT id FROM daemons WHERE api_key = ? AND deleted_at IS NULL`, apiKey,
	).Scan(&daemonID)
	if err != nil {
		return "", fmt.Errorf("invalid api key")
	}

	bundle, ok := a.host.Acquire(chID)
	if !ok {
		return "", fmt.Errorf("channel unavailable")
	}
	bound, err := bundle.View().IsBound(context.Background(), daemonID)
	if err != nil || !bound {
		return "", fmt.Errorf("daemon not bound to channel")
	}

	return daemonID, nil
}
