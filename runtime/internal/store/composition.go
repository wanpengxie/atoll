package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

type compositionStore struct {
	db  *sql.DB
	reg *actorRegistry
}

func newCompositionStore(db *sql.DB, reg *actorRegistry) *compositionStore {
	return &compositionStore{db: db, reg: reg}
}

const compositionColumns = `instance_id, decl_id, principal, class,
	COALESCE(config_json,''), placement, desired_host, is_default, restart_epoch`

type compositionScanner interface{ Scan(...any) error }

func scanComposition(s compositionScanner) (storespec.CompositionRecord, error) {
	var r storespec.CompositionRecord
	var placement string
	var isDefault int
	if err := s.Scan(&r.InstanceID, &r.DeclID, &r.Principal, &r.Class,
		&r.ConfigJSON, &placement, &r.DesiredHost, &isDefault, &r.Epoch); err != nil {
		return storespec.CompositionRecord{}, err
	}
	switch storespec.Placement(placement) {
	case storespec.PlacementServer, storespec.PlacementDaemon:
		r.Placement = storespec.Placement(placement)
	default:
		return storespec.CompositionRecord{}, fmt.Errorf("store: invalid composition placement %q", placement)
	}
	r.IsDefault = isDefault == 1
	return r, nil
}

func (s *compositionStore) LookupComposition(ctx context.Context, id actor.ActorID) (storespec.CompositionRecord, bool, error) {
	r, err := scanComposition(s.db.QueryRowContext(ctx,
		`SELECT `+compositionColumns+` FROM channel_composition WHERE instance_id=?`, string(id)))
	if errors.Is(err, sql.ErrNoRows) {
		return storespec.CompositionRecord{}, false, nil
	}
	if err != nil {
		return storespec.CompositionRecord{}, false, fmt.Errorf("store: lookup composition %q: %w", id, err)
	}
	return r, true, nil
}

func (s *compositionStore) LookupCompositionPrincipal(ctx context.Context, principal string) (storespec.CompositionRecord, bool, error) {
	r, err := scanComposition(s.db.QueryRowContext(ctx,
		`SELECT `+compositionColumns+` FROM channel_composition WHERE principal=?`, principal))
	if errors.Is(err, sql.ErrNoRows) {
		return storespec.CompositionRecord{}, false, nil
	}
	if err != nil {
		return storespec.CompositionRecord{}, false, fmt.Errorf("store: lookup composition principal %q: %w", principal, err)
	}
	return r, true, nil
}

func (s *compositionStore) ListComposition(ctx context.Context) ([]storespec.CompositionRecord, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT `+compositionColumns+` FROM channel_composition ORDER BY instance_id`)
	if err != nil {
		return nil, fmt.Errorf("store: list composition: %w", err)
	}
	defer rows.Close()
	var out []storespec.CompositionRecord
	for rows.Next() {
		r, err := scanComposition(rows)
		if err != nil {
			return nil, fmt.Errorf("store: list composition scan: %w", err)
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

func (s *compositionStore) DefaultComposition(ctx context.Context) (actor.ActorID, bool, error) {
	var id actor.ActorID
	err := s.db.QueryRowContext(ctx, `SELECT instance_id FROM channel_composition WHERE is_default=1`).Scan(&id)
	if errors.Is(err, sql.ErrNoRows) {
		return "", false, nil
	}
	if err != nil {
		return "", false, fmt.Errorf("store: default composition: %w", err)
	}
	return id, true, nil
}

func validateCompositionIntroduce(in storespec.CompositionIntroduce) error {
	if strings.TrimSpace(in.DeclID) == "" || strings.TrimSpace(in.Principal) == "" || strings.TrimSpace(in.Class) == "" {
		return errors.New("store: composition decl_id, principal, and class are required")
	}
	if in.At == 0 {
		return errors.New("store: composition introduce timestamp required")
	}
	if _, ok := actor.ParseKind(string(in.Kind)); !ok || (in.Kind != actor.KindAgent && in.Kind != actor.KindTool) {
		return fmt.Errorf("store: composition kind %q outside agent/tool domain", in.Kind)
	}
	switch in.Placement {
	case storespec.PlacementServer:
		if in.DesiredHost != "" {
			return errors.New("store: server composition cannot carry desired_host")
		}
	case storespec.PlacementDaemon:
	default:
		return fmt.Errorf("store: invalid composition placement %q", in.Placement)
	}
	return nil
}

func (s *compositionStore) IntroduceComposition(ctx context.Context, in storespec.CompositionIntroduce) (storespec.CompositionRecord, bool, bool, error) {
	if err := validateCompositionIntroduce(in); err != nil {
		return storespec.CompositionRecord{}, false, false, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return storespec.CompositionRecord{}, false, false, err
	}
	defer tx.Rollback()

	existing, err := scanComposition(tx.QueryRowContext(ctx,
		`SELECT `+compositionColumns+` FROM channel_composition WHERE principal=?`, in.Principal))
	created := errors.Is(err, sql.ErrNoRows)
	if err != nil && !created {
		return storespec.CompositionRecord{}, false, false, err
	}

	var id actor.ActorID
	admitted := false
	configChanged := false
	if created {
		id, admitted, err = s.ensureActivePrincipalTx(ctx, tx, in.Kind, in.Principal, in.At)
		if err != nil {
			return storespec.CompositionRecord{}, false, false, err
		}
		var cfg any
		if in.ConfigJSON != nil {
			cfg = *in.ConfigJSON
		}
		_, err = tx.ExecContext(ctx, `INSERT INTO channel_composition
			(instance_id,decl_id,principal,class,config_json,placement,desired_host,is_default)
			VALUES (?,?,?,?,?,?,?,?)`, string(id), in.DeclID, in.Principal, in.Class, cfg,
			string(in.Placement), in.DesiredHost, 0)
	} else {
		id = existing.InstanceID
		// A half-written/inactive registry side is repaired with the freshly
		// minted active id while immutable class/placement remain frozen.
		var active int
		qerr := tx.QueryRowContext(ctx, `SELECT 1 FROM actor_registry WHERE actor_id=? AND deregistered_at IS NULL`, string(id)).Scan(&active)
		if errors.Is(qerr, sql.ErrNoRows) {
			var repaired bool
			id, repaired, err = s.ensureActivePrincipalTx(ctx, tx, in.Kind, in.Principal, in.At)
			admitted = admitted || repaired
			if err == nil {
				_, err = tx.ExecContext(ctx, `UPDATE channel_composition SET instance_id=? WHERE principal=?`, string(id), in.Principal)
			}
		} else if qerr != nil {
			err = qerr
		}
		if err == nil && in.ConfigJSON != nil {
			res, uerr := tx.ExecContext(ctx, `UPDATE channel_composition SET config_json=? WHERE principal=? AND COALESCE(config_json,'')<>?`, *in.ConfigJSON, in.Principal, *in.ConfigJSON)
			err = uerr
			if uerr == nil {
				n, _ := res.RowsAffected()
				configChanged = n > 0
			}
		}
		if err == nil && in.MakeDefault {
			err = setDefaultTx(ctx, tx, id)
		}
	}
	if err != nil {
		return storespec.CompositionRecord{}, false, false, err
	}
	if in.MakeDefault && created {
		if err := setDefaultTx(ctx, tx, id); err != nil {
			return storespec.CompositionRecord{}, false, false, err
		}
	}
	if err := tx.Commit(); err != nil {
		return storespec.CompositionRecord{}, false, false, err
	}
	if admitted && s.reg.onCommit != nil {
		s.reg.onCommit()
	}
	r, ok, err := s.LookupComposition(ctx, id)
	if err != nil || !ok {
		return storespec.CompositionRecord{}, false, false, err
	}
	return r, created, configChanged, nil
}

func (s *compositionStore) ensureActivePrincipalTx(ctx context.Context, tx *sql.Tx, kind actor.Kind, principal string, at int64) (actor.ActorID, bool, error) {
	var id actor.ActorID
	err := tx.QueryRowContext(ctx, `SELECT actor_id FROM actor_registry WHERE actor_kind=? AND principal=? AND deregistered_at IS NULL`, string(kind), principal).Scan(&id)
	if err == nil {
		return id, false, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return "", false, err
	}
	for n := int64(0); n < 1000; n++ {
		id = actor.ActorID(fmt.Sprintf("%s:%s:%d", kind, principal, at+n))
		res, err := tx.ExecContext(ctx, `INSERT OR IGNORE INTO actor_registry
			(actor_id,actor_kind,principal,actor_binding,host,created_at,deregistered_at)
			VALUES (?,?,?,NULL,'',?,NULL)`, string(id), string(kind), principal, at+n)
		if err != nil {
			return "", false, err
		}
		changed, err := res.RowsAffected()
		if err != nil {
			return "", false, err
		}
		if changed == 0 {
			if err := tx.QueryRowContext(ctx, `SELECT actor_id FROM actor_registry WHERE actor_kind=? AND principal=? AND deregistered_at IS NULL`, string(kind), principal).Scan(&id); err == nil {
				return id, false, nil
			}
			continue
		}
		add := storespec.MemberActorAdd{ID: id, Kind: kind, At: at + n}
		if _, err := appendTx(ctx, tx, actorRegisteredEnvelope(s.reg.channelID, add), false); err != nil {
			return "", false, err
		}
		return id, true, nil
	}
	return "", false, errors.New("store: unable to mint composition actor id")
}

func setDefaultTx(ctx context.Context, tx *sql.Tx, id actor.ActorID) error {
	if _, err := tx.ExecContext(ctx, `UPDATE channel_composition SET is_default=0 WHERE is_default=1`); err != nil {
		return err
	}
	if id == "" {
		return nil
	}
	res, err := tx.ExecContext(ctx, `UPDATE channel_composition SET is_default=1 WHERE instance_id=?`, string(id))
	if err != nil {
		return err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n != 1 {
		return storespec.ErrCompositionNotFound
	}
	return nil
}

func (s *compositionStore) SetDefaultComposition(ctx context.Context, id actor.ActorID) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err := setDefaultTx(ctx, tx, id); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *compositionStore) RemoveComposition(ctx context.Context, id actor.ActorID, at int64) (bool, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer tx.Rollback()
	res, err := tx.ExecContext(ctx, `DELETE FROM channel_composition WHERE instance_id=?`, string(id))
	if err != nil {
		return false, err
	}
	n, err := res.RowsAffected()
	if err != nil || n == 0 {
		return false, err
	}
	changed, counts, err := s.reg.applyMemberRemoveTx(ctx, tx, storespec.MemberActorRemove{ID: id, At: at})
	if err != nil {
		return false, err
	}
	if changed {
		if _, err := appendTx(ctx, tx, actorDeregisteredEnvelope(s.reg.channelID, storespec.MemberActorRemove{ID: id, At: at}, counts), false); err != nil {
			return false, err
		}
	}
	if err := tx.Commit(); err != nil {
		return false, err
	}
	if changed && s.reg.onCommit != nil {
		s.reg.onCommit()
	}
	return true, nil
}

func (s *compositionStore) RestartComposition(ctx context.Context, id actor.ActorID) (int64, error) {
	var epoch int64
	err := s.db.QueryRowContext(ctx, `UPDATE channel_composition SET restart_epoch=restart_epoch+1 WHERE instance_id=? RETURNING restart_epoch`, string(id)).Scan(&epoch)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, storespec.ErrCompositionNotFound
	}
	return epoch, err
}

func (s *compositionStore) ApplyRestartComposition(ctx context.Context, jobID int64, id actor.ActorID, at int64) (int64, bool, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, false, err
	}
	defer tx.Rollback()
	res, err := tx.ExecContext(ctx, `INSERT OR IGNORE INTO restart_applied(job_id,instance_id,applied_at) SELECT ?,?,? WHERE EXISTS(SELECT 1 FROM channel_composition WHERE instance_id=?)`, jobID, string(id), at, string(id))
	if err != nil {
		return 0, false, err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return 0, false, err
	}
	if n == 1 {
		if _, err := tx.ExecContext(ctx, `UPDATE channel_composition SET restart_epoch=restart_epoch+1 WHERE instance_id=?`, string(id)); err != nil {
			return 0, false, err
		}
	}
	var epoch int64
	if err := tx.QueryRowContext(ctx, `SELECT restart_epoch FROM channel_composition WHERE instance_id=?`, string(id)).Scan(&epoch); errors.Is(err, sql.ErrNoRows) {
		return 0, false, storespec.ErrCompositionNotFound
	} else if err != nil {
		return 0, false, err
	}
	if err := tx.Commit(); err != nil {
		return 0, false, err
	}
	return epoch, n == 1, nil
}

func (s *compositionStore) RevokeDaemonTarget(ctx context.Context, daemonID string) ([]actor.ActorID, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	rows, err := tx.QueryContext(ctx, `SELECT instance_id FROM channel_composition WHERE desired_host=? ORDER BY instance_id`, daemonID)
	if err != nil {
		return nil, err
	}
	var ids []actor.ActorID
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			_ = rows.Close()
			return nil, err
		}
		ids = append(ids, actor.ActorID(id))
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE channel_composition SET desired_host='' WHERE desired_host=?`, daemonID); err != nil {
		return nil, err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE actor_registry SET host='' WHERE host=? AND deregistered_at IS NULL`, daemonID); err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return ids, nil
}

func (s *compositionStore) MarkCompositionMigrated(ctx context.Context, at int64) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `UPDATE actor_registry
		SET actor_binding=(SELECT CASE c.placement WHEN 'daemon' THEN ? ELSE ? END FROM channel_composition c WHERE c.instance_id=actor_registry.actor_id)
		WHERE deregistered_at IS NULL AND EXISTS(SELECT 1 FROM channel_composition c WHERE c.instance_id=actor_registry.actor_id)`,
		string(actor.BindingRuntimeInboundViaRelay), string(actor.BindingEmbedded)); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE actor_registry SET host=''
		WHERE deregistered_at IS NULL AND host<>'' AND NOT EXISTS(
			SELECT 1 FROM channel_composition c WHERE c.instance_id=actor_registry.actor_id
			AND c.placement='daemon' AND c.desired_host=actor_registry.host)`); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `INSERT OR IGNORE INTO composition_migrated(one_row,migrated_at) VALUES (1,?)`, at); err != nil {
		return err
	}
	return tx.Commit()
}
