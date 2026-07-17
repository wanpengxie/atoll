package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

const declaredControlColumns = `r.actor_id, r.actor_kind, r.principal,
	COALESCE(r.actor_binding,''), r.created_at, r.current_decl_version,
	d.class, d.config_json, d.t_idle_ms, d.placement, d.desired_host,
	d.source_decl_id`

type controlScanner interface{ Scan(...any) error }

func scanDeclaredControl(s controlScanner) (storespec.ActorControlRow, error) {
	var row storespec.ActorControlRow
	var rawKind, rawBinding, placement string
	var config []byte
	var idleMS int64
	if err := s.Scan(&row.ID, &rawKind, &row.Principal, &rawBinding,
		&row.CreatedAt, &row.CurrentDeclVersion, &row.Class, &config, &idleMS,
		&placement, &row.Placement.Host, &row.SourceDeclID); err != nil {
		return storespec.ActorControlRow{}, err
	}
	kind, ok := actor.ParseKind(rawKind)
	if !ok {
		return storespec.ActorControlRow{}, fmt.Errorf("store: actor %q invalid kind %q", row.ID, rawKind)
	}
	row.Kind = kind
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

func (r *actorRegistry) LookupDeclaredVersion(ctx context.Context, id actor.ActorID, version int64) (storespec.ActorControlRow, bool, error) {
	const q = `SELECT r.actor_id, r.actor_kind, r.principal,
		COALESCE(r.actor_binding,''), r.created_at, d.version,
		d.class, d.config_json, d.t_idle_ms, d.placement, d.desired_host,
		d.source_decl_id
		FROM actor_registry r JOIN actor_decl_versions d ON d.actor_id=r.actor_id
		WHERE r.actor_id=? AND d.version=?`
	row, err := scanDeclaredControl(r.db.QueryRowContext(ctx, q, string(id), version))
	if errors.Is(err, sql.ErrNoRows) {
		return storespec.ActorControlRow{}, false, nil
	}
	if err != nil {
		return storespec.ActorControlRow{}, false, fmt.Errorf("store: lookup actor %q decl v%d: %w", id, version, err)
	}
	return row, true, nil
}

func (r *actorRegistry) LatestDeclaredVersion(ctx context.Context, id actor.ActorID) (storespec.ActorControlRow, bool, error) {
	const q = `SELECT r.actor_id, r.actor_kind, r.principal,
		COALESCE(r.actor_binding,''), r.created_at, d.version,
		d.class, d.config_json, d.t_idle_ms, d.placement, d.desired_host,
		d.source_decl_id
		FROM actor_registry r JOIN actor_decl_versions d ON d.actor_id=r.actor_id
		WHERE r.actor_id=? ORDER BY d.version DESC LIMIT 1`
	row, err := scanDeclaredControl(r.db.QueryRowContext(ctx, q, string(id)))
	if errors.Is(err, sql.ErrNoRows) {
		return storespec.ActorControlRow{}, false, nil
	}
	if err != nil {
		return storespec.ActorControlRow{}, false, fmt.Errorf("store: latest actor %q decl: %w", id, err)
	}
	return row, true, nil
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
	if in.Kind != actor.KindSystem && in.Principal == "" {
		return errors.New("store: declared admission principal required")
	}
	if in.ID == actor.SystemActorID && in.Kind != actor.KindSystem {
		return errors.New("store: system id requires system kind")
	}
	return validateMemberIdentity(in.ID, in.Kind, in.Binding)
}

// AdmitDeclared atomically creates the durable identity row, decl@v1, and
// registration mirror. Existing active (kind,principal) admissions converge
// only when their decl row is complete; a half row fails loudly.
func (r *actorRegistry) AdmitDeclared(ctx context.Context, in storespec.AdmitBundle) (storespec.AdmitResult, error) {
	mintID := in.ID == ""
	if mintID {
		// Temporary non-empty value lets the shared closed-set validator run;
		// the transaction chooses the first never-used timestamp suffix below.
		in.ID = actor.ActorID("pending")
	}
	if err := validateAdmitBundle(in); err != nil {
		return storespec.AdmitResult{}, err
	}
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return storespec.AdmitResult{}, fmt.Errorf("store: declared admission begin: %w", err)
	}
	defer tx.Rollback()

	if in.Principal != "" {
		var existing actor.ActorID
		err := tx.QueryRowContext(ctx, `SELECT actor_id FROM actor_registry
			WHERE actor_kind=? AND principal=? AND deregistered_at IS NULL`, string(in.Kind), in.Principal).Scan(&existing)
		if err == nil {
			var count int
			if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM actor_decl_versions WHERE actor_id=? AND version=1`, string(existing)).Scan(&count); err != nil {
				return storespec.AdmitResult{}, err
			}
			if count != 1 {
				return storespec.AdmitResult{}, fmt.Errorf("store: active actor %q missing decl@v1", existing)
			}
			return storespec.AdmitResult{ID: existing}, nil
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return storespec.AdmitResult{}, err
		}
	}
	if mintID {
		in.ID = ""
		for attempt := int64(0); attempt < 1000; attempt++ {
			candidate := actor.ActorID(fmt.Sprintf("%s:%s:%d", in.Kind, in.Principal, in.CreatedAt+attempt))
			var count int
			if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM actor_registry WHERE actor_id=?`, string(candidate)).Scan(&count); err != nil {
				return storespec.AdmitResult{}, err
			}
			if count == 0 {
				in.ID = candidate
				break
			}
		}
		if in.ID == "" {
			return storespec.AdmitResult{}, errors.New("store: unable to mint unique actor id")
		}
	}

	var exists, active int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*), COALESCE(SUM(deregistered_at IS NULL),0) FROM actor_registry WHERE actor_id=?`, string(in.ID)).Scan(&exists, &active); err != nil {
		return storespec.AdmitResult{}, err
	}
	if exists != 0 {
		if active == 0 {
			return storespec.AdmitResult{}, fmt.Errorf("store: actor id %q was already ended", in.ID)
		}
		var count int
		if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM actor_decl_versions WHERE actor_id=? AND version=1`, string(in.ID)).Scan(&count); err != nil {
			return storespec.AdmitResult{}, err
		}
		if count != 1 {
			return storespec.AdmitResult{}, fmt.Errorf("store: active actor %q missing decl@v1", in.ID)
		}
		return storespec.AdmitResult{ID: in.ID}, nil
	}

	if _, err := tx.ExecContext(ctx, `INSERT INTO actor_registry
		(actor_id, actor_kind, principal, actor_binding, created_at, current_decl_version, deregistered_at)
		VALUES (?,?,?,?,?,1,NULL)`, string(in.ID), string(in.Kind), in.Principal,
		nullableBinding(in.Binding), in.CreatedAt); err != nil {
		return storespec.AdmitResult{}, fmt.Errorf("store: declared actor insert %q: %w", in.ID, err)
	}
	var config any
	if in.Config != nil {
		config = string(in.Config)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO actor_decl_versions
		(actor_id,version,class,config_json,placement,desired_host,t_idle_ms,source_decl_id,created_at)
		VALUES (?,1,?,?,?,?,?,?,?)`, string(in.ID), in.Class, config, string(in.Placement.Kind),
		in.Placement.Host, in.TIdle.Milliseconds(), in.SourceDeclID, in.CreatedAt); err != nil {
		return storespec.AdmitResult{}, fmt.Errorf("store: declared actor decl insert %q: %w", in.ID, err)
	}
	if _, err := appendTx(ctx, tx, actorRegisteredEnvelope(r.channelID, in.ID, in.Kind, in.Binding, in.CreatedAt), false); err != nil {
		return storespec.AdmitResult{}, fmt.Errorf("store: declared actor mirror %q: %w", in.ID, err)
	}
	if err := tx.Commit(); err != nil {
		return storespec.AdmitResult{}, fmt.Errorf("store: declared admission commit: %w", err)
	}
	if r.onCommit != nil {
		r.onCommit()
	}
	return storespec.AdmitResult{ID: in.ID, Created: true}, nil
}

func (r *actorRegistry) ExistsEver(ctx context.Context, id actor.ActorID) (bool, error) {
	return r.Exists(ctx, id)
}

func (r *actorRegistry) EditDeclared(ctx context.Context, in storespec.DeclEditBundle) (storespec.ActorControlRow, error) {
	if in.ActorID == "" || in.Class == "" || in.CreatedAt <= 0 || in.TIdle < 0 || in.Placement.Validate() != nil {
		return storespec.ActorControlRow{}, errors.New("store: invalid declaration edit")
	}
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return storespec.ActorControlRow{}, err
	}
	defer tx.Rollback()
	var active int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM actor_registry WHERE actor_id=? AND deregistered_at IS NULL`, string(in.ActorID)).Scan(&active); err != nil {
		return storespec.ActorControlRow{}, err
	}
	if active != 1 {
		return storespec.ActorControlRow{}, storespec.ErrMemberInactive
	}
	var version int64
	if err := tx.QueryRowContext(ctx, `SELECT COALESCE(MAX(version),0)+1 FROM actor_decl_versions WHERE actor_id=?`, string(in.ActorID)).Scan(&version); err != nil {
		return storespec.ActorControlRow{}, err
	}
	var config any
	if in.Config != nil {
		config = string(in.Config)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO actor_decl_versions
		(actor_id,version,class,config_json,placement,desired_host,t_idle_ms,source_decl_id,created_at)
		VALUES (?,?,?,?,?,?,?,?,?)`, string(in.ActorID), version, in.Class, config, string(in.Placement.Kind), in.Placement.Host, in.TIdle.Milliseconds(), in.SourceDeclID, in.CreatedAt); err != nil {
		return storespec.ActorControlRow{}, err
	}
	row, err := scanDeclaredControl(tx.QueryRowContext(ctx, `SELECT r.actor_id,r.actor_kind,r.principal,COALESCE(r.actor_binding,''),r.created_at,d.version,d.class,d.config_json,d.t_idle_ms,d.placement,d.desired_host,d.source_decl_id FROM actor_registry r JOIN actor_decl_versions d ON d.actor_id=r.actor_id WHERE r.actor_id=? AND d.version=?`, string(in.ActorID), version))
	if err != nil {
		return storespec.ActorControlRow{}, err
	}
	if err := tx.Commit(); err != nil {
		return storespec.ActorControlRow{}, err
	}
	return row, nil
}

func (r *actorRegistry) ApplyDeclaredVersion(ctx context.Context, id actor.ActorID, version int64) (storespec.ActorControlRow, bool, error) {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return storespec.ActorControlRow{}, false, err
	}
	defer tx.Rollback()
	res, err := tx.ExecContext(ctx, `UPDATE actor_registry SET current_decl_version=? WHERE actor_id=? AND deregistered_at IS NULL AND EXISTS (SELECT 1 FROM actor_decl_versions WHERE actor_id=? AND version=?)`, version, string(id), string(id), version)
	if err != nil {
		return storespec.ActorControlRow{}, false, err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return storespec.ActorControlRow{}, false, err
	}
	if n == 0 {
		return storespec.ActorControlRow{}, false, nil
	}
	row, err := scanDeclaredControl(tx.QueryRowContext(ctx, `SELECT `+declaredControlColumns+` FROM actor_registry r JOIN actor_decl_versions d ON d.actor_id=r.actor_id AND d.version=r.current_decl_version WHERE r.actor_id=?`, string(id)))
	if err != nil {
		return storespec.ActorControlRow{}, false, err
	}
	if err := tx.Commit(); err != nil {
		return storespec.ActorControlRow{}, false, err
	}
	return row, true, nil
}

var (
	_ storespec.DeclAdmissionStore    = (*actorRegistry)(nil)
	_ storespec.DeclaredControlReader = (*actorRegistry)(nil)
	_ storespec.DurableHistory        = (*actorRegistry)(nil)
	_ storespec.DeclVersionStore      = (*actorRegistry)(nil)
)
