package app

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"modernc.org/sqlite"

	"github.com/wanpengxie/atoll/app/internal/middleware"
	"github.com/wanpengxie/atoll/platform/channelhost"
	"github.com/wanpengxie/atoll/platform/channelspec"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
)

func (a *App) handleListChannels(c *gin.Context) {
	query := `SELECT id,name,type,status,owner_principal,created_at,parent_id FROM channels WHERE status='present'`
	args := []any{}
	if parent, ok := c.GetQuery("parent_id"); ok {
		query += ` AND parent_id=?`
		args = append(args, parent)
	}
	query += ` ORDER BY created_at,id`
	rows, err := a.db.QueryContext(c.Request.Context(), query, args...)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "query failed"})
		return
	}
	defer rows.Close()
	result := make([]gin.H, 0)
	for rows.Next() {
		var id, name, typ, status, owner string
		var created int64
		var parent sql.NullString
		if err := rows.Scan(&id, &name, &typ, &status, &owner, &created, &parent); err != nil {
			c.JSON(500, gin.H{"error": "query failed"})
			return
		}
		result = append(result, channelJSON(id, name, typ, status, owner, created, parent))
	}
	if err := rows.Err(); err != nil {
		c.JSON(500, gin.H{"error": "query failed"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"channels": result})
}

func channelJSON(id, name, typ, status, owner string, created int64, parent sql.NullString) gin.H {
	row := gin.H{
		"id": id, "name": name, "type": typ, "status": status,
		"owner_principal": owner, "created_at": created,
	}
	if parent.Valid {
		row["parent_id"] = parent.String
	} else {
		row["parent_id"] = nil
	}
	return row
}

// handleCreateChannel is the create acceptance gate. 201 means "desired
// accepted, physical convergence bounded" — not "genesis aligned"; the
// stateless arm brings the physical side up after the answer.
func (a *App) handleCreateChannel(c *gin.Context) {
	caller := middleware.UserID(c)
	var req struct {
		Name     string  `json:"name"`
		Type     string  `json:"type"`
		ParentID *string `json:"parent_id"`
	}
	if err := c.ShouldBindJSON(&req); err != nil || strings.TrimSpace(req.Name) == "" {
		c.JSON(400, gin.H{"error": "name required"})
		return
	}
	req.Name = strings.TrimSpace(req.Name)
	if req.Type == "" {
		req.Type = "group"
	}
	if req.Type != "group" {
		c.JSON(400, gin.H{"error": "invalid channel type"})
		return
	}
	now := time.Now().UnixMilli()
	chID := channel.ID(uuid.NewString())
	snapshot, err := (channelspec.RenderedSnapshot{Class: defaultBoostClass, Config: json.RawMessage(`{}`), Placement: channel.Placement{Kind: channel.PlacementServer}}).Seal()
	if err != nil {
		c.JSON(500, gin.H{"error": "internal error"})
		return
	}
	realmSnapshot, err := (channelspec.RenderedSnapshot{Class: realmToolClass, Config: json.RawMessage(`{}`), Placement: channel.Placement{Kind: channel.PlacementServer}}).Seal()
	if err != nil {
		c.JSON(500, gin.H{"error": "internal error"})
		return
	}
	spec := channelhost.ProvisionSpec{ChannelID: chID, Type: req.Type, OwnerPrincipal: caller, CreatedAt: now,
		GenesisDeclarations: []channelhost.GenesisDeclaration{
			{DeclID: "sys:boost", Kind: actor.KindAgent, Rendered: snapshot},
			{DeclID: realmToolDeclID, Kind: actor.KindTool, Rendered: realmSnapshot},
		}}
	if req.ParentID != nil {
		spec.Origin = &channelhost.Origin{ParentChannelID: channel.ID(*req.ParentID), InitiatorPrincipal: caller}
	}
	raw, err := json.Marshal(spec)
	if err != nil {
		c.JSON(500, gin.H{"error": "internal error"})
		return
	}
	a.createMu.Lock()
	accepted, changed, conflict, parentMissing, err := a.acceptCreateChannel(
		c.Request.Context(), desiredChannel{
			ID: chID, Name: req.Name, Type: req.Type, Status: "present",
			Owner: caller, SpecJSON: string(raw), Created: now,
			Parent: nullableParent(req.ParentID),
		},
	)
	a.createMu.Unlock()
	if err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "create unavailable", "retry": "safe"})
		return
	}
	if parentMissing {
		c.JSON(http.StatusConflict, gin.H{"error": "parent_not_present"})
		return
	}
	if conflict {
		c.JSON(http.StatusConflict, gin.H{"error": "channel name already exists"})
		return
	}
	row := channelJSON(
		string(accepted.ID), accepted.Name, accepted.Type, accepted.Status,
		accepted.Owner, accepted.Created, accepted.Parent,
	)
	row["changed"] = changed
	status := http.StatusOK
	if changed {
		status = http.StatusCreated
		// A freshly accepted row must not inherit lifecycle state from any
		// earlier life of this ID (stale permanent marks would silently block
		// convergence forever).
		a.resetLifecycleForStatusChange(accepted.ID)
		a.convergeChannel(c.Request.Context(), accepted.ID)
	}
	// Poke on replays too: an idempotent re-submit is the caller's strongest
	// "hurry up" signal for a channel whose physical side may still be
	// converging, and a poke only buys timeliness.
	a.pokeLifecycle(accepted.ID)
	c.JSON(status, row)
}

func nullableParent(parent *string) sql.NullString {
	if parent == nil {
		return sql.NullString{}
	}
	return sql.NullString{String: *parent, Valid: true}
}

func sameParent(a, b sql.NullString) bool {
	return a.Valid == b.Valid && (!a.Valid || a.String == b.String)
}

func (a *App) acceptCreateChannel(ctx context.Context, requested desiredChannel) (desiredChannel, bool, bool, bool, error) {
	var (
		accepted     desiredChannel
		changed      bool
		conflict     bool
		parentAbsent bool
		err          error
	)
	for attempt := 0; attempt < 2; attempt++ {
		accepted, changed, conflict, parentAbsent, err = a.acceptCreateChannelOnce(ctx, requested)
		if err == nil || !isSQLiteBusy(err) {
			break
		}
	}
	return accepted, changed, conflict, parentAbsent, err
}

func (a *App) acceptCreateChannelOnce(ctx context.Context, requested desiredChannel) (desiredChannel, bool, bool, bool, error) {
	tx, err := a.db.BeginTx(ctx, nil)
	if err != nil {
		return desiredChannel{}, false, false, false, err
	}
	defer tx.Rollback()
	var present desiredChannel
	err = tx.QueryRowContext(ctx, `SELECT id,name,type,status,owner_principal,spec_json,created_at,parent_id
		FROM channels WHERE name=? AND status='present'`, requested.Name).
		Scan(&present.ID, &present.Name, &present.Type, &present.Status, &present.Owner,
			&present.SpecJSON, &present.Created, &present.Parent)
	if err == nil {
		if present.Owner == requested.Owner && present.Type == requested.Type && sameParent(present.Parent, requested.Parent) {
			return present, false, false, false, nil
		}
		return desiredChannel{}, false, true, false, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return desiredChannel{}, false, false, false, err
	}
	if requested.Parent.Valid {
		var exists bool
		if err := tx.QueryRowContext(ctx, `SELECT EXISTS(
			SELECT 1 FROM channels WHERE id=? AND status='present')`,
			requested.Parent.String).Scan(&exists); err != nil {
			return desiredChannel{}, false, false, false, err
		}
		if !exists {
			return desiredChannel{}, false, false, true, nil
		}
	}
	res, err := tx.ExecContext(ctx, `INSERT INTO channels(
		id,name,type,status,owner_principal,spec_json,created_at,parent_id)
		VALUES (?,?,?,?,?,?,?,?)
		ON CONFLICT(name) WHERE status='present' DO NOTHING`,
		string(requested.ID), requested.Name, requested.Type, requested.Status,
		requested.Owner, requested.SpecJSON, requested.Created,
		nullStringValue(requested.Parent))
	if err != nil {
		return desiredChannel{}, false, false, false, err
	}
	inserted, err := res.RowsAffected()
	if err != nil {
		return desiredChannel{}, false, false, false, err
	}
	if inserted == 0 {
		_ = tx.Rollback()
		var winner desiredChannel
		err := a.db.QueryRowContext(ctx, `SELECT id,name,type,status,owner_principal,spec_json,created_at,parent_id
			FROM channels WHERE name=? AND status='present'`, requested.Name).
			Scan(&winner.ID, &winner.Name, &winner.Type, &winner.Status, &winner.Owner,
				&winner.SpecJSON, &winner.Created, &winner.Parent)
		if err != nil {
			return desiredChannel{}, false, false, false, err
		}
		if winner.Owner == requested.Owner && winner.Type == requested.Type && sameParent(winner.Parent, requested.Parent) {
			return winner, false, false, false, nil
		}
		return desiredChannel{}, false, true, false, nil
	}
	if err := tx.Commit(); err != nil {
		return desiredChannel{}, false, false, false, err
	}
	return requested, true, false, false, nil
}

func nullStringValue(value sql.NullString) any {
	if value.Valid {
		return value.String
	}
	return nil
}

func isSQLiteBusy(err error) bool {
	var sqliteErr *sqlite.Error
	// SQLite extended result codes retain the primary result code in the low
	// byte, so this covers BUSY, BUSY_RECOVERY, BUSY_SNAPSHOT and BUSY_TIMEOUT.
	return errors.As(err, &sqliteErr) && sqliteErr.Code()&0xff == 5
}

func (a *App) handleGetChannel(c *gin.Context) {
	chID := c.Param("chID")
	var id, name, typ, status, owner string
	var created int64
	var parent sql.NullString
	err := a.db.QueryRowContext(c.Request.Context(), `SELECT id,name,type,status,owner_principal,created_at,parent_id
		FROM channels WHERE id=? AND status='present'`, chID).
		Scan(&id, &name, &typ, &status, &owner, &created, &parent)
	if errors.Is(err, sql.ErrNoRows) {
		c.JSON(404, gin.H{"error": "channel not found"})
		return
	}
	if err != nil {
		c.JSON(500, gin.H{"error": "query failed"})
		return
	}
	row := channelJSON(id, name, typ, status, owner, created, parent)
	if bundle, ok := a.host.Acquire(channel.ID(chID)); ok {
		if value, found, err := bundle.View().DefaultAgent(c.Request.Context()); err == nil && found {
			row["default_agent"] = string(value)
		}
	}
	c.JSON(200, row)
}

func (a *App) handleDeleteChannel(c *gin.Context) {
	chID := c.Param("chID")
	caller := middleware.UserID(c)
	release := a.channelLocks.lock(chID)
	defer release()
	accepted, err := a.acceptDeleteChannel(c.Request.Context(), channel.ID(chID), caller)
	if err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "delete unavailable", "retry": "safe"})
		return
	}
	switch accepted {
	case deleteAlreadyRetired:
		c.JSON(http.StatusOK, gin.H{"status": "retiring", "changed": false})
		return
	case deleteRowAbsent:
		// The predicate "must not exist" already holds; claiming a status for
		// a row that never existed would be a false statement.
		c.JSON(http.StatusOK, gin.H{"changed": false})
		return
	case deleteForbidden:
		c.JSON(http.StatusForbidden, gin.H{"error": "channel owner required"})
		return
	case deleteAccepted:
		// Fall through to the destructive tail below.
	default:
		// Fail closed: an outcome this switch does not know must never fall
		// into the destructive accepted tail.
		c.JSON(http.StatusInternalServerError, gin.H{"error": "unknown delete outcome"})
		return
	}
	// Read the affected roster through the relation module (it owns every
	// write and provides the read API) before the Gone event deletes it.
	affected, aerr := a.relations.PrincipalsOf(c.Request.Context(), channel.ID(chID))
	if aerr != nil {
		a.logger.Warn("affected principal read failed; gateway kick degraded", "channel", chID, "err", aerr)
	}
	if err := a.relations.Apply(c.Request.Context(), channel.ID(chID), []channelspec.RelationDelta{{
		Kind: channelspec.RelationGone, ChannelID: channel.ID(chID),
	}}); err != nil {
		a.logger.Warn("channel relation retirement event failed", "channel", chID, "err", err)
	}
	for _, principal := range affected {
		if a.membershipPoke != nil {
			a.membershipPoke(principal)
		}
	}
	a.resetLifecycleForStatusChange(channel.ID(chID))
	a.pokeLifecycle(channel.ID(chID))
	c.JSON(http.StatusOK, gin.H{"status": "retiring", "changed": true})
}

type deleteOutcome uint8

const (
	deleteAccepted deleteOutcome = iota + 1
	deleteAlreadyRetired
	deleteRowAbsent
	deleteForbidden
)

func (a *App) acceptDeleteChannel(ctx context.Context, id channel.ID, caller string) (deleteOutcome, error) {
	var (
		accepted deleteOutcome
		err      error
	)
	for attempt := 0; attempt < 2; attempt++ {
		accepted, err = a.acceptDeleteChannelOnce(ctx, id, caller)
		if err == nil || !isSQLiteBusy(err) {
			break
		}
	}
	return accepted, err
}

func (a *App) acceptDeleteChannelOnce(ctx context.Context, id channel.ID, caller string) (deleteOutcome, error) {
	tx, err := a.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()
	res, err := tx.ExecContext(ctx, `UPDATE channels SET status='retiring'
		WHERE id=? AND status='present' AND owner_principal=?`, string(id), caller)
	if err != nil {
		return 0, err
	}
	changed, err := res.RowsAffected()
	if err != nil {
		return 0, err
	}
	if changed == 0 {
		var status, owner string
		err := tx.QueryRowContext(ctx,
			`SELECT status,owner_principal FROM channels WHERE id=?`, string(id)).Scan(&status, &owner)
		if errors.Is(err, sql.ErrNoRows) {
			return deleteRowAbsent, nil
		}
		if err == nil && status == "retiring" {
			return deleteAlreadyRetired, nil
		}
		if err != nil {
			return 0, err
		}
		return deleteForbidden, nil
	}
	if err := tx.Commit(); err != nil {
		return 0, err
	}
	return deleteAccepted, nil
}

func (a *App) handleListCandidates(c *gin.Context) {
	if _, ok := a.requireChannelAccess(c); !ok {
		return
	}
	rows, err := a.db.QueryContext(c.Request.Context(), `SELECT id,email,display_name FROM users ORDER BY created_at,id`)
	if err != nil {
		c.JSON(500, gin.H{"error": "query failed"})
		return
	}
	defer rows.Close()
	result := make([]gin.H, 0)
	for rows.Next() {
		var id, email string
		var display sql.NullString
		if rows.Scan(&id, &email, &display) != nil {
			c.JSON(500, gin.H{"error": "query failed"})
			return
		}
		result = append(result, gin.H{"user_id": id, "email": email, "display_name": display.String})
	}
	c.JSON(200, gin.H{"candidates": result})
}
