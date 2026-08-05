package app

import (
	"context"
	"database/sql"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"

	"github.com/wanpengxie/atoll/app/contract"
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
		writeAPIError(c, http.StatusInternalServerError, contract.CodeInternal, "query failed")
		return
	}
	defer rows.Close()

	result := make([]contract.Daemon, 0)
	for rows.Next() {
		var id, name, apiKey string
		var createdAt int64
		if err := rows.Scan(&id, &name, &apiKey, &createdAt); err != nil {
			writeAPIError(c, http.StatusInternalServerError, contract.CodeInternal, "query failed")
			return
		}
		online, err := a.daemonOnline(c.Request.Context(), id)
		if err != nil {
			writeAPIError(c, http.StatusServiceUnavailable, contract.CodeUnavailable, "daemon status unavailable")
			return
		}
		result = append(result, contract.Daemon{ID: id, Name: name, APIKey: apiKey, CreatedAt: createdAt, Online: &online})
	}
	if err := rows.Err(); err != nil {
		writeAPIError(c, http.StatusInternalServerError, contract.CodeInternal, "query failed")
		return
	}
	c.JSON(http.StatusOK, contract.DaemonList{Daemons: result})
}

func (a *App) daemonOnline(_ context.Context, stringID string) (bool, error) {
	return a.daemonHost.DaemonOnline(stringID), nil
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
	var req contract.CreateDaemonRequest
	if !decodeRequest(c, &req) {
		return
	}
	if req.Name == "" {
		writeAPIError(c, http.StatusBadRequest, contract.CodeInvalidRequest, "name required")
		return
	}

	daemonID, apiKey, err := a.createDaemonRow(c.Request.Context(), userID, req.Name)
	if err != nil {
		writeAPIError(c, http.StatusInternalServerError, contract.CodeInternal, "create daemon failed")
		return
	}

	c.JSON(http.StatusCreated, contract.Daemon{ID: daemonID, Name: req.Name, APIKey: apiKey})
}

// createDaemonRow is the transport-free daemon-mint core shared by the HTTP
// handler and ProvisionHome (app 代 mint key —— daemon 仪式内化的机制半).
func (a *App) createDaemonRow(ctx context.Context, ownerID, name string) (string, string, error) {
	daemonID := uuid.NewString()
	release := a.daemonLocks.lock(daemonID)
	defer release()
	apiKey := uuid.NewString()
	now := time.Now().UnixMilli()
	_, err := a.db.ExecContext(ctx,
		`INSERT INTO daemons (id, owner_id, name, api_key, created_at) VALUES (?,?,?,?,?)`,
		daemonID, ownerID, name, apiKey, now,
	)
	if err != nil {
		return "", "", err
	}
	return daemonID, apiKey, nil
}

// handleDeleteDaemon commits the realm tombstone and immediately revokes the
// device carrier. Channel-local bindings are intentionally retained.
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
		writeAPIError(c, http.StatusNotFound, contract.CodeDaemonNotFound, "daemon not found")
		return
	}
	if err != nil {
		writeAPIError(c, http.StatusInternalServerError, contract.CodeInternal, "delete failed")
		return
	}

	if !deletedAt.Valid {
		if _, err := a.db.ExecContext(ctx,
			`UPDATE daemons SET deleted_at=? WHERE id=? AND owner_id=? AND deleted_at IS NULL`, time.Now().UnixMilli(), daemonID, userID); err != nil {
			writeAPIError(c, http.StatusInternalServerError, contract.CodeInternal, "delete failed")
			return
		}
	}
	release()
	locked = false
	a.daemonHost.RevokeDaemon(daemonID)
	c.JSON(http.StatusOK, contract.DaemonDeletion{
		OK: true, AuthorityCommitted: true, Convergence: "revoked",
		Diagnostics: a.daemonHost.Diagnostics(daemonID),
	})
}

func (a *App) handleListChannelDaemons(c *gin.Context) {
	chID, ok := a.requireChannelMember(c)
	if !ok {
		return
	}
	bundle, available := a.host.Acquire(channel.ID(chID))
	if !available {
		writeAPIError(c, http.StatusServiceUnavailable, contract.CodeChannelUnavailable, "channel unavailable")
		return
	}
	// A channel roster is channel-scoped, not viewer-owned. Any member who can
	// read the channel sees every daemon currently bound to it, regardless of
	// which member registered that daemon in the realm.
	rows, err := a.db.QueryContext(c.Request.Context(), `SELECT id,name,created_at FROM daemons WHERE deleted_at IS NULL ORDER BY id`)
	if err != nil {
		writeAPIError(c, http.StatusInternalServerError, contract.CodeInternal, "query failed")
		return
	}
	defer rows.Close()

	result := make([]contract.Daemon, 0)
	for rows.Next() {
		var id, name string
		var createdAt int64
		if err := rows.Scan(&id, &name, &createdAt); err != nil {
			writeAPIError(c, http.StatusInternalServerError, contract.CodeInternal, "query failed")
			return
		}
		bound, err := bundle.View().IsBound(c.Request.Context(), id)
		if err != nil {
			writeAPIError(c, http.StatusServiceUnavailable, contract.CodeChannelUnavailable, "channel bindings unavailable")
			return
		}
		if !bound {
			continue
		}
		online := a.daemonHost.LaneAttached(id, string(chID))
		result = append(result, contract.Daemon{ID: id, Name: name, CreatedAt: createdAt, Online: &online})
	}
	if err := rows.Err(); err != nil {
		writeAPIError(c, http.StatusInternalServerError, contract.CodeInternal, "query failed")
		return
	}
	c.JSON(http.StatusOK, contract.DaemonList{Daemons: result})
}

func (a *App) handleAttachDaemon(c *gin.Context) {
	chID := channel.ID(c.Param("chID"))
	var req contract.AttachDaemonRequest
	if !decodeRequest(c, &req) {
		return
	}
	if strings.TrimSpace(req.DaemonID) == "" {
		writeAPIError(c, http.StatusBadRequest, contract.CodeInvalidRequest, "daemon_id required")
		return
	}
	req.DaemonID = strings.TrimSpace(req.DaemonID)
	userID := middleware.UserID(c)
	outcome, err := a.attachDaemonCore(c.Request.Context(), userID, chID, req.DaemonID)
	if err != nil {
		writeSysopError(c, err)
		return
	}
	c.JSON(http.StatusOK, contract.DaemonBinding{Bound: outcome.Value.Bound, Changed: outcome.Changed})
}

// attachDaemonCore is the transport-free attach verb shared by the HTTP handler
// and ProvisionHome. Same gates, same sysop forward — provisioning never gets a
// side door around the membrane.
func (a *App) attachDaemonCore(ctx context.Context, principal string, chID channel.ID, daemonID string) (sysopOutcome[channelspec.BindingResult], error) {
	return forwardSysop(ctx, a, chID, sysopForward[channelspec.BindingResult]{
		Predicate: func(bundle channelhost.Bundle) (channelspec.BindingResult, bool, error) {
			bound, err := bundle.View().IsBound(ctx, daemonID)
			if err != nil || !bound {
				return channelspec.BindingResult{}, false, err
			}
			var deleted sql.NullInt64
			err = a.db.QueryRowContext(ctx,
				`SELECT deleted_at FROM daemons WHERE id=?`, daemonID).Scan(&deleted)
			if err != nil && !errors.Is(err, sql.ErrNoRows) {
				return channelspec.BindingResult{}, false, err
			}
			return channelspec.BindingResult{Bound: true}, err == nil && !deleted.Valid, nil
		},
		Qualify: func(bundle channelhost.Bundle) error {
			// Same in-gate order as detach: membership first, then the daemon
			// checks — the two verbs are one family and order differences read
			// as intent.
			if err := memberGate(ctx, bundle, principal); err != nil {
				return err
			}
			var owner string
			var deleted sql.NullInt64
			err := a.db.QueryRowContext(ctx,
				`SELECT owner_id,deleted_at FROM daemons WHERE id=?`, daemonID).
				Scan(&owner, &deleted)
			if errors.Is(err, sql.ErrNoRows) || (err == nil && owner != principal) {
				return &sysopGateError{Status: http.StatusForbidden, Code: "forbidden"}
			}
			if err != nil {
				return &sysopUnknownError{cause: err}
			}
			if deleted.Valid {
				return &sysopGateError{Status: http.StatusNotFound, Code: string(sysopCodeDaemonNotFound)}
			}
			return nil
		},
		Invoke: func(sys channelhost.SysOp, ref string) (channelspec.BindingResult, error) {
			return sys.AttachDaemon(ctx, channelspec.DaemonRequest{Ref: ref, DaemonID: daemonID})
		},
	})
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
	c.JSON(http.StatusOK, contract.DaemonBinding{
		Bound: outcome.Value.Bound, Changed: outcome.Changed,
		ClearedInstances: actorIDStrings(outcome.Value.ClearedInstances),
	})
}

// ---------------------------------------------------------------------------
// Credential resolver: transport authentication only. Binding is deliberately
// absent; channel authority is evaluated by daemonhost's per-coordinate scan.
// ---------------------------------------------------------------------------

func (a *App) resolveDaemonCredential(ctx context.Context, apiKey string) (string, int) {
	var daemonID string
	err := a.db.QueryRowContext(ctx,
		`SELECT id FROM daemons WHERE api_key = ? AND deleted_at IS NULL`, apiKey,
	).Scan(&daemonID)
	if errors.Is(err, sql.ErrNoRows) {
		return "", http.StatusUnauthorized
	}
	if err != nil {
		return "", http.StatusServiceUnavailable
	}
	return daemonID, http.StatusOK
}
