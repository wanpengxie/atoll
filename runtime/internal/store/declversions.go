package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

const declaredControlColumns = `r.actor_id, r.actor_kind, r.principal, r.role,
    COALESCE(r.actor_binding,''), r.created_at, r.current_decl_version,
    d.class, d.config_json, d.t_idle_ms, d.placement, d.desired_host,
    r.source_decl_id`

type controlScanner interface{ Scan(...any) error }

func scanDeclaredControl(s controlScanner) (storespec.ActorControlRow, error) {
	var row storespec.ActorControlRow
	var rawKind, rawRole, rawBinding, placement string
	var config []byte
	var idleMS int64
	if err := s.Scan(&row.ID, &rawKind, &row.Principal, &rawRole, &rawBinding,
		&row.CreatedAt, &row.CurrentDeclVersion, &row.Class, &config, &idleMS,
		&placement, &row.Placement.Host, &row.SourceDeclID); err != nil {
		return storespec.ActorControlRow{}, err
	}
	kind, ok := actor.ParseKind(rawKind)
	if !ok {
		return storespec.ActorControlRow{}, fmt.Errorf("store: actor %q invalid kind %q", row.ID, rawKind)
	}
	row.Kind = kind
	role, ok := storespec.ParseActorRole(rawRole)
	if !ok {
		return storespec.ActorControlRow{}, fmt.Errorf("store: actor %q invalid role %q", row.ID, rawRole)
	}
	row.Role = role
	if rawBinding != "" {
		binding, ok := actor.ParseBinding(rawBinding)
		if !ok {
			return storespec.ActorControlRow{}, fmt.Errorf("store: actor %q invalid binding %q", row.ID, rawBinding)
		}
		row.Binding = binding
	}
	if idleMS < 0 {
		return storespec.ActorControlRow{}, fmt.Errorf("store: actor %q negative t_idle_ms", row.ID)
	}
	row.TIdle = time.Duration(idleMS) * time.Millisecond
	row.Placement.Kind = storespec.PlacementKind(placement)
	if err := row.Placement.Validate(); err != nil {
		return storespec.ActorControlRow{}, fmt.Errorf("store: actor %q placement: %w", row.ID, err)
	}
	if config != nil {
		row.Config = append([]byte(nil), config...)
	}
	row.Sponsor = actor.SystemActorID
	return row, nil
}

func (r *actorRegistry) LookupDeclaredActive(ctx context.Context, id actor.ActorID) (storespec.ActorControlRow, bool, error) {
	const q = `SELECT ` + declaredControlColumns + `
		FROM actor_registry r JOIN actor_decl_versions d
		  ON d.actor_id=r.actor_id AND d.version=r.current_decl_version
		WHERE r.actor_id=? AND r.deregistered_at IS NULL`
	row, err := scanDeclaredControl(r.db.QueryRowContext(ctx, q, string(id)))
	if errors.Is(err, sql.ErrNoRows) {
		return storespec.ActorControlRow{}, false, nil
	}
	if err != nil {
		return storespec.ActorControlRow{}, false, fmt.Errorf("store: lookup declared actor %q: %w", id, err)
	}
	return row, true, nil
}

func (r *actorRegistry) ListDeclaredActive(ctx context.Context) ([]storespec.ActorControlRow, error) {
	const q = `SELECT ` + declaredControlColumns + `
		FROM actor_registry r JOIN actor_decl_versions d
		  ON d.actor_id=r.actor_id AND d.version=r.current_decl_version
		WHERE r.deregistered_at IS NULL ORDER BY r.actor_id`
	rows, err := r.db.QueryContext(ctx, q)
	if err != nil {
		return nil, fmt.Errorf("store: list declared actors: %w", err)
	}
	defer rows.Close()
	var out []storespec.ActorControlRow
	for rows.Next() {
		row, err := scanDeclaredControl(rows)
		if err != nil {
			return nil, fmt.Errorf("store: list declared actors scan: %w", err)
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: list declared actors rows: %w", err)
	}
	return out, nil
}

func validateAdmitBundle(in storespec.AdmitBundle) error {
	if _, ok := actor.ParseKind(string(in.Kind)); !ok {
		return fmt.Errorf("store: invalid actor kind %q", in.Kind)
	}
	if in.Class == "" || in.CreatedAt <= 0 || in.TIdle < 0 {
		return errors.New("store: declared admission requires class, timestamp, and non-negative idle")
	}
	if err := in.Placement.Validate(); err != nil {
		return err
	}
	if in.Kind == actor.KindHuman && in.Principal == "" {
		return errors.New("store: human admission principal required")
	}
	if in.Kind == actor.KindHuman && in.SourceDeclID != "" {
		return errors.New("store: human admission cannot carry declaration source")
	}
	if in.Kind != actor.KindHuman && in.Principal != "" {
		return errors.New("store: only human admissions may carry a login principal")
	}
	if (in.Kind == actor.KindAgent || in.Kind == actor.KindTool) && in.SourceDeclID == "" {
		return errors.New("store: declaration-backed admission source required")
	}
	if in.Kind == actor.KindSystem && in.SourceDeclID != "" {
		return errors.New("store: system admission cannot carry declaration source")
	}
	if in.ID == actor.SystemActorID && in.Kind != actor.KindSystem {
		return errors.New("store: system id requires system kind")
	}
	if role, ok := storespec.ParseActorRole(string(in.Role)); !ok || role == storespec.RoleOwner && in.Kind != actor.KindHuman {
		return errors.New("store: invalid declared actor role")
	}
	return validateMemberIdentity(in.ID, in.Kind, in.Binding)
}

// AdmitDeclared atomically creates the durable identity row, decl@v1, and
// registration mirror. Existing active (kind,principal) admissions converge
// only when their decl row is complete; a half row fails loudly.
func (r *actorRegistry) AdmitDeclared(ctx context.Context, in storespec.AdmitBundle) (storespec.DeclAdmissionResult, error) {
	mintID := in.ID == ""
	if mintID {
		// Temporary non-empty value lets the shared closed-set validator run;
		// the transaction chooses the first never-used timestamp suffix below.
		in.ID = actor.ActorID("pending")
	}
	if err := validateAdmitBundle(in); err != nil {
		return storespec.DeclAdmissionResult{}, err
	}
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return storespec.DeclAdmissionResult{}, fmt.Errorf("store: declared admission begin: %w", err)
	}
	defer tx.Rollback()

	if in.SourceDeclID != "" {
		var existing actor.ActorID
		err := tx.QueryRowContext(ctx, `SELECT actor_id FROM actor_registry
			WHERE source_decl_id=? AND deregistered_at IS NULL`, in.SourceDeclID).Scan(&existing)
		if err == nil {
			var count int
			if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM actor_decl_versions WHERE actor_id=? AND version=1`, string(existing)).Scan(&count); err != nil {
				return storespec.DeclAdmissionResult{}, err
			}
			if count != 1 {
				return storespec.DeclAdmissionResult{}, fmt.Errorf("store: active actor %q missing decl@v1", existing)
			}
			return storespec.DeclAdmissionResult{ID: existing}, nil
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return storespec.DeclAdmissionResult{}, err
		}
	} else if in.Principal != "" {
		var existing actor.ActorID
		err := tx.QueryRowContext(ctx, `SELECT actor_id FROM actor_registry
			WHERE actor_kind=? AND principal=? AND deregistered_at IS NULL`, string(in.Kind), in.Principal).Scan(&existing)
		if err == nil {
			var count int
			if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM actor_decl_versions WHERE actor_id=? AND version=1`, string(existing)).Scan(&count); err != nil {
				return storespec.DeclAdmissionResult{}, err
			}
			if count != 1 {
				return storespec.DeclAdmissionResult{}, fmt.Errorf("store: active actor %q missing decl@v1", existing)
			}
			return storespec.DeclAdmissionResult{ID: existing}, nil
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return storespec.DeclAdmissionResult{}, err
		}
	}
	if mintID {
		seed := in.Principal
		if seed == "" {
			seed = in.SourceDeclID
		}
		in.ID, err = mintActorIDTx(ctx, tx, in.Kind, seed, in.CreatedAt)
		if err != nil {
			return storespec.DeclAdmissionResult{}, err
		}
	}

	var exists, active int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*), COALESCE(SUM(deregistered_at IS NULL),0) FROM actor_registry WHERE actor_id=?`, string(in.ID)).Scan(&exists, &active); err != nil {
		return storespec.DeclAdmissionResult{}, err
	}
	if exists != 0 {
		if active == 0 {
			return storespec.DeclAdmissionResult{}, fmt.Errorf("store: actor id %q was already ended", in.ID)
		}
		var count int
		if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM actor_decl_versions WHERE actor_id=? AND version=1`, string(in.ID)).Scan(&count); err != nil {
			return storespec.DeclAdmissionResult{}, err
		}
		if count != 1 {
			return storespec.DeclAdmissionResult{}, fmt.Errorf("store: active actor %q missing decl@v1", in.ID)
		}
		return storespec.DeclAdmissionResult{ID: in.ID}, nil
	}

	rendered := channel.RenderedSnapshot{
		Class: in.Class, Config: in.Config, TIdleMS: in.TIdle.Milliseconds(),
		Placement: channel.Placement{Kind: channel.PlacementKind(in.Placement.Kind), DesiredHost: in.Placement.Host},
	}
	if err := insertDeclaredTx(ctx, tx, in.ID, in.Kind, in.Principal, in.SourceDeclID, in.Binding, rendered, in.Role, in.CreatedAt); err != nil {
		return storespec.DeclAdmissionResult{}, fmt.Errorf("store: declared actor insert %q: %w", in.ID, err)
	}
	if _, err := appendTx(ctx, tx, actorRegisteredEnvelope(r.channelID, in.ID, in.Kind, in.Binding, in.CreatedAt), false); err != nil {
		return storespec.DeclAdmissionResult{}, fmt.Errorf("store: declared actor mirror %q: %w", in.ID, err)
	}
	if err := tx.Commit(); err != nil {
		return storespec.DeclAdmissionResult{}, fmt.Errorf("store: declared admission commit: %w", err)
	}
	if r.onCommit != nil {
		r.onCommit()
	}
	return storespec.DeclAdmissionResult{ID: in.ID, Created: true}, nil
}

func (r *actorRegistry) ExistsEver(ctx context.Context, id actor.ActorID) (bool, error) {
	return r.Exists(ctx, id)
}

var (
	_ storespec.DeclAdmissionStore    = (*actorRegistry)(nil)
	_ storespec.DeclaredControlReader = (*actorRegistry)(nil)
	_ storespec.DurableHistory        = (*actorRegistry)(nil)
)
