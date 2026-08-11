package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/channel"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

// actorRegistry owns the durable half of the actor record store: the
// actor_registry table and nothing else. It appends no message, clears no
// belonging and holds no business policy — a record verb touches records only.
type actorRegistry struct {
	db        *sql.DB
	channelID channel.ID
	onCommit  func()
}

func newActorRegistry(db *sql.DB, channelID channel.ID, onCommit func()) *actorRegistry {
	return &actorRegistry{db: db, channelID: channelID, onCommit: onCommit}
}

const actorRecordColumns = `actor_id, actor_kind, principal, source_decl_id,
    class, config_json, placement, desired_host, created_at`

type recordScanner interface{ Scan(...any) error }

func scanActorRecord(s recordScanner) (storespec.ActorRecord, error) {
	var record storespec.ActorRecord
	var rawKind, placement string
	var config []byte
	if err := s.Scan(&record.ID, &rawKind, &record.Principal, &record.SourceDeclID,
		&record.Definition.Class, &config, &placement, &record.Placement.Host,
		&record.CreatedAt); err != nil {
		return storespec.ActorRecord{}, err
	}
	kind, ok := actor.ParseKind(rawKind)
	if !ok {
		return storespec.ActorRecord{}, fmt.Errorf("store: actor %q invalid kind %q", record.ID, rawKind)
	}
	record.Kind = kind
	record.Placement.Kind = storespec.PlacementKind(placement)
	if err := record.Placement.Validate(); err != nil {
		return storespec.ActorRecord{}, fmt.Errorf("store: actor %q placement: %w", record.ID, err)
	}
	if config != nil {
		record.Definition.Config = append([]byte(nil), config...)
	}
	return record, nil
}

func (r *actorRegistry) LookupActive(
	ctx context.Context,
	id actor.ActorID,
) (storespec.ActorRecord, bool, error) {
	const q = `SELECT ` + actorRecordColumns + ` FROM actor_registry
		WHERE actor_id=? AND deregistered_at IS NULL`
	record, err := scanActorRecord(r.db.QueryRowContext(ctx, q, string(id)))
	if errors.Is(err, sql.ErrNoRows) {
		return storespec.ActorRecord{}, false, nil
	}
	if err != nil {
		return storespec.ActorRecord{}, false, fmt.Errorf("store: lookup actor %q: %w", id, err)
	}
	return record, true, nil
}

func (r *actorRegistry) ListActive(ctx context.Context) ([]storespec.ActorRecord, error) {
	const q = `SELECT ` + actorRecordColumns + ` FROM actor_registry
		WHERE deregistered_at IS NULL ORDER BY actor_id`
	rows, err := r.db.QueryContext(ctx, q)
	if err != nil {
		return nil, fmt.Errorf("store: list actors: %w", err)
	}
	defer rows.Close()
	var out []storespec.ActorRecord
	for rows.Next() {
		record, err := scanActorRecord(rows)
		if err != nil {
			return nil, fmt.Errorf("store: list actors scan: %w", err)
		}
		out = append(out, record)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: list actors rows: %w", err)
	}
	return out, nil
}

func (r *actorRegistry) LookupActivePrincipal(
	ctx context.Context,
	kind actor.Kind,
	principal string,
) (storespec.Record, bool, error) {
	const q = `SELECT actor_id, actor_kind, principal, created_at
	 FROM actor_registry WHERE actor_kind=? AND principal=? AND deregistered_at IS NULL`
	var rec storespec.Record
	var rawKind string
	err := r.db.QueryRowContext(ctx, q, string(kind), principal).Scan(
		&rec.ID, &rawKind, &rec.Principal, &rec.CreatedAt,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return storespec.Record{}, false, nil
	}
	if err != nil {
		return storespec.Record{}, false, fmt.Errorf("store: principal lookup: %w", err)
	}
	parsed, ok := actor.ParseKind(rawKind)
	if !ok {
		return storespec.Record{}, false, fmt.Errorf("store: actor %q invalid kind %q", rec.ID, rawKind)
	}
	rec.Kind = parsed
	return rec, true, nil
}

func validateDraft(in storespec.ActorDraft) error {
	if _, ok := actor.ParseKind(string(in.Kind)); !ok {
		return fmt.Errorf("store: invalid actor kind %q", in.Kind)
	}
	if in.Kind == actor.KindSystem {
		// The kernel is a constant, never a member: it has no record.
		return errors.New("store: the system kernel has no actor record")
	}
	if in.Definition.Class == "" || in.CreatedAt <= 0 {
		return errors.New("store: actor insert requires class and timestamp")
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
	if in.Kind == actor.KindTool && in.Principal != "" {
		return errors.New("store: tool admissions cannot carry a principal")
	}
	if (in.Kind == actor.KindAgent || in.Kind == actor.KindTool) && in.SourceDeclID == "" {
		return errors.New("store: declaration-backed admission source required")
	}
	return nil
}

// Insert is the whole declaration-class birth on ONE transaction bed: semantic
// key replay lookup + id mint (with tombstone avoidance) + row insert. There is
// no intermediate state — a crash at any point either leaves nothing (retry
// starts over) or leaves everything (retry recovers the id by semantic key).
func (r *actorRegistry) Insert(
	ctx context.Context,
	in storespec.ActorDraft,
) (storespec.ActorRecord, error) {
	if err := validateDraft(in); err != nil {
		return storespec.ActorRecord{}, err
	}
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return storespec.ActorRecord{}, fmt.Errorf("store: actor insert begin: %w", err)
	}
	defer tx.Rollback()

	lookupExisting := func(q string, args ...any) (storespec.ActorRecord, bool, error) {
		record, err := scanActorRecord(tx.QueryRowContext(ctx, q, args...))
		if errors.Is(err, sql.ErrNoRows) {
			return storespec.ActorRecord{}, false, nil
		}
		if err != nil {
			return storespec.ActorRecord{}, false, err
		}
		return record, true, nil
	}

	switch {
	case in.SourceDeclID != "":
		record, found, err := lookupExisting(`SELECT `+actorRecordColumns+` FROM actor_registry
			WHERE source_decl_id=? AND deregistered_at IS NULL`, in.SourceDeclID)
		if err != nil {
			return storespec.ActorRecord{}, err
		}
		if found {
			return record, nil
		}
	case in.Principal != "":
		record, found, err := lookupExisting(`SELECT `+actorRecordColumns+` FROM actor_registry
			WHERE actor_kind=? AND principal=? AND deregistered_at IS NULL`,
			string(in.Kind), in.Principal)
		if err != nil {
			return storespec.ActorRecord{}, err
		}
		if found {
			return record, nil
		}
	}

	// The id is minted here and only here, inside the transaction that inserts
	// the row. The draft has no id field to honour, so there is no second way in.
	id, err := mintActorIDTx(ctx, tx, in.Kind, seedFor(in), in.CreatedAt)
	if err != nil {
		return storespec.ActorRecord{}, err
	}

	var config any
	if in.Definition.Config != nil {
		config = string(in.Definition.Config)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO actor_registry
		(actor_id,actor_kind,principal,source_decl_id,class,config_json,placement,desired_host,created_at,deregistered_at)
		VALUES (?,?,?,?,?,?,?,?,?,NULL)`,
		string(id), string(in.Kind), in.Principal, in.SourceDeclID,
		in.Definition.Class, config, string(in.Placement.Kind), in.Placement.Host,
		in.CreatedAt); err != nil {
		return storespec.ActorRecord{}, fmt.Errorf("store: actor insert %q: %w", id, err)
	}
	if err := tx.Commit(); err != nil {
		return storespec.ActorRecord{}, fmt.Errorf("store: actor insert commit: %w", err)
	}
	if r.onCommit != nil {
		r.onCommit()
	}
	return storespec.ActorRecord{
		ID: id, Kind: in.Kind, Principal: in.Principal,
		SourceDeclID: in.SourceDeclID, CreatedAt: in.CreatedAt,
		Definition: in.Definition.Clone(), Placement: in.Placement,
	}, nil
}

// UpdateDefinition overwrites the current definition of one declaration-backed
// active row and returns the committed record. Placement is immutable and stays
// untouched. ONE transaction bed: the row is read and rewritten under the same
// tx, and the return value is composed from that in-tx read plus the new
// definition — every fallible step sits before the commit, nothing is read
// back after it (the ledger law: settled means nothing below can fail). A row
// with no declaration source (a human admission) has no declaration to change
// and is refused with a typed error — an operation verdict, not a species
// branch.
func (r *actorRegistry) UpdateDefinition(
	ctx context.Context,
	id actor.ActorID,
	def storespec.ActorDefinition,
) (storespec.ActorRecord, error) {
	if id == "" || def.Class == "" {
		return storespec.ActorRecord{}, storespec.ErrActorNotFound
	}
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return storespec.ActorRecord{}, fmt.Errorf("store: update definition begin: %w", err)
	}
	defer tx.Rollback()

	record, err := scanActorRecord(tx.QueryRowContext(ctx,
		`SELECT `+actorRecordColumns+` FROM actor_registry
		 WHERE actor_id=? AND deregistered_at IS NULL`, string(id)))
	if errors.Is(err, sql.ErrNoRows) {
		return storespec.ActorRecord{}, storespec.ErrActorNotFound
	}
	if err != nil {
		return storespec.ActorRecord{}, fmt.Errorf("store: update definition read %q: %w", id, err)
	}
	if record.SourceDeclID == "" {
		return storespec.ActorRecord{}, fmt.Errorf(
			"%w: %q", storespec.ErrNoDeclaration, id)
	}
	var config any
	if def.Config != nil {
		config = string(def.Config)
	}
	if _, err := tx.ExecContext(ctx,
		`UPDATE actor_registry SET class=?, config_json=?
		 WHERE actor_id=? AND deregistered_at IS NULL`,
		def.Class, config, string(id)); err != nil {
		return storespec.ActorRecord{}, fmt.Errorf("store: update definition %q: %w", id, err)
	}
	if err := tx.Commit(); err != nil {
		return storespec.ActorRecord{}, fmt.Errorf("store: update definition commit: %w", err)
	}
	if r.onCommit != nil {
		r.onCommit()
	}
	record.Definition = def.Clone()
	return record, nil
}

// Deregister is the monotonic termination latch. It is idempotent, applies to
// the whole set, and touches actor_registry alone — no message, no state, no
// timer, no grant, no routing pointer.
//
// There is deliberately NO kernel guard here. The kernel holds no row, so a
// termination aimed at it finds nothing to latch and is a natural no-op; a
// protection branch would itself concede that the kernel lives inside the
// lifecycle scope.
func (r *actorRegistry) Deregister(
	ctx context.Context,
	ids []actor.ActorID,
	at int64,
) error {
	if len(ids) == 0 {
		return nil
	}
	if at <= 0 {
		return errors.New("store: deregister requires a timestamp")
	}
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("store: deregister begin: %w", err)
	}
	defer tx.Rollback()
	for _, id := range ids {
		if id == "" {
			return fmt.Errorf("store: invalid deregister target %q", id)
		}
		if _, err := tx.ExecContext(ctx,
			`UPDATE actor_registry SET deregistered_at=? WHERE actor_id=? AND deregistered_at IS NULL`,
			at, string(id)); err != nil {
			return fmt.Errorf("store: deregister %q: %w", id, err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("store: deregister commit: %w", err)
	}
	if r.onCommit != nil {
		r.onCommit()
	}
	return nil
}

// seedFor is the naming input a birth id is derived from: the login principal
// for a human admission, the declaration for everything else. Exactly one of
// them is set — validateDraft has already refused a draft that carries the
// wrong one for its kind.
func seedFor(in storespec.ActorDraft) string {
	if in.Principal != "" {
		return in.Principal
	}
	return in.SourceDeclID
}

func mintActorIDTx(ctx context.Context, tx *sql.Tx, kind actor.Kind, seed string, at int64) (actor.ActorID, error) {
	for attempt := int64(0); attempt < 1000; attempt++ {
		candidate := actor.ActorID(fmt.Sprintf("%s:%s:%d", kind, seed, at+attempt))
		var used bool
		if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM actor_registry WHERE actor_id=?)`, string(candidate)).Scan(&used); err != nil {
			return "", err
		}
		if !used {
			return candidate, nil
		}
	}
	return "", errors.New("store: cannot mint actor id")
}

var (
	_ storespec.PrincipalRegistry  = (*actorRegistry)(nil)
	_ storespec.ActorRegistryStore = (*actorRegistry)(nil)
)
