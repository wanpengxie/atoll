package store

import (
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/wanpengxie/atoll/protocol/access"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/resource"
	"github.com/wanpengxie/atoll/runtime/resourcespec"
)

// resourceRegistry implements resourcespec.Registry over the channel-local
// sqlite (resources + resource_grants + resource_reservations +
// resource_tombstones), the plane-2 dual of actorRegistry: it is the
// object-existence + authorization-relation (R) truth the door consults and
// mutates, PLUS the create/delete outbox's server-side durable halves (期11
// spec §1.3). Bound to one channel database (access is channel-scoped).
type resourceRegistry struct {
	db *sql.DB
	// nowMs stamps created_at/reserved_at/deleted_at. Injectable (tests pin
	// it) — the rest of the registry is clock-free.
	nowMs func() int64
}

// clearActorGrantsTx removes only grants whose grantee axis names the actor.
// Resource ownership and members-scoped grants are orthogonal and survive.
// Returns RowsAffected (A4/C2 cascade telemetry) — the caller (actors.go's
// applyMemberRemoveTx) folds this into the deregistration mirror payload's
// grants_cleared field; store itself still never logs (§0 分工).
func clearActorGrantsTx(ctx context.Context, tx *sql.Tx, id actor.ActorID) (int64, error) {
	res, err := tx.ExecContext(ctx, `DELETE FROM resource_grants WHERE grantee_kind='actor' AND grantee=?`, string(id))
	if err != nil {
		return 0, fmt.Errorf("store: actor grants cascade clear %q: %w", id, err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("store: actor grants cascade clear rows-affected %q: %w", id, err)
	}
	return n, nil
}

func newResourceRegistry(db *sql.DB) *resourceRegistry {
	return &resourceRegistry{db: db, nowMs: func() int64 { return time.Now().UnixMilli() }}
}

// placementKindFor derives the public placement projection from the durable
// resource kind. placement_kind is intentionally not persisted: there is no
// independent placement choice in the current architecture.
func placementKindFor(kind resourcespec.ResourceKind) resourcespec.PlacementKind {
	if kind == resourcespec.KindFile {
		return resourcespec.PlacementDaemonLocal
	}
	return ""
}

// Resolve reads back existence + full routing/audit meta. kind is returned as
// the raw persisted value with NO closed-set parse (unlike actor_kind):
// ResourceKind is a runtime routing key, and
// whether a kind resolves to a registered driver is the door's question, not
// a poison-row guard here.
func (r *resourceRegistry) Resolve(ctx context.Context, id resource.ResourceID) (resourcespec.ResourceMeta, bool, error) {
	const q = `SELECT kind, placement_daemon_id, placement_coord, created_by, created_at, is_dir
	             FROM resources WHERE resource_id=?`
	var kind, placementDaemonID, placementCoord, createdBy string
	var createdAt int64
	var isDir bool
	err := r.db.QueryRowContext(ctx, q, string(id)).Scan(
		&kind, &placementDaemonID, &placementCoord, &createdBy, &createdAt, &isDir,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return resourcespec.ResourceMeta{}, false, nil
	}
	if err != nil {
		return resourcespec.ResourceMeta{}, false, fmt.Errorf("store: resource resolve %q: %w", id, err)
	}
	return resourcespec.ResourceMeta{
		Kind:              resourcespec.ResourceKind(kind),
		CreatedAt:         createdAt,
		PlacementKind:     placementKindFor(resourcespec.ResourceKind(kind)),
		PlacementDaemonID: placementDaemonID,
		PlacementCoord:    placementCoord,
		CreatedBy:         actor.ActorID(createdBy),
		Dir:               isDir,
	}, true, nil
}

// Create is op=create's atomic birth event for the IMMEDIATE-landing path
// (kv, and content-less file creates). See createResourceTx for the shared
// atomic-insert half CommitReservation also drives.
func (r *resourceRegistry) Create(ctx context.Context, id resource.ResourceID, kind resourcespec.ResourceKind, creator actor.ActorID, placementDaemonID string, placementCoord string, initial []byte) error {
	if id == "" {
		return errors.New("store: resource create: empty id")
	}
	if creator == "" {
		return errors.New("store: resource create: empty creator")
	}

	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("store: resource create begin: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	// Create is the kv / with-content-less immediate landing entry point;
	// direct Create is day-1 only ever kv (a file always routes through
	// ReserveCreate+CommitReservation), which is never directory-shaped —
	// so dir is unconditionally false here.
	if err := r.createResourceTx(ctx, tx, id, kind, creator, placementDaemonID, placementCoord, initial, false); err != nil {
		return err // resourcespec.ErrAlreadyExists or a wrapped infra error
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("store: resource create commit: %w", err)
	}
	return nil
}

// createResourceTx is Create's and CommitReservation's SHARED atomic-birth
// half: the existence row (all columns) + the creator's full-rights grant,
// inside the CALLER's transaction (Create starts+commits its own;
// CommitReservation folds it into the reservation-consuming transaction so
// landing and reservation-deletion stay one atomic event, 期11 spec §1.7).
// Returns resourcespec.ErrAlreadyExists on collision, decided INSIDE this
// INSERT ... ON CONFLICT DO NOTHING (the race window never has a
// resolve-then-create gap) — the caller decides what the collision means (a
// plain failure for direct Create; "lost the race" for CommitReservation).
func (r *resourceRegistry) createResourceTx(ctx context.Context, tx *sql.Tx, id resource.ResourceID, kind resourcespec.ResourceKind, creator actor.ActorID, placementDaemonID string, placementCoord string, initial []byte, dir bool) error {
	// The creator's grant is full object-rights: read/write/set/delete. set-right
	// is what makes the creator the controller (control = a full grant in R, no
	// separate owner column).
	ops, err := json.Marshal([]access.Operation{access.OpRead, access.OpWrite, access.OpSet, access.OpDelete})
	if err != nil {
		return fmt.Errorf("store: resource create marshal ops: %w", err)
	}

	res, err := tx.ExecContext(ctx,
		`INSERT INTO resources (resource_id, kind, bytes, placement_daemon_id, placement_coord, created_by, created_at, is_dir)
		   VALUES (?, ?, ?, ?, ?, ?, ?, ?)
		 ON CONFLICT(resource_id) DO NOTHING`,
		string(id), string(kind), initial, placementDaemonID, placementCoord, string(creator), r.nowMs(), dir,
	)
	if err != nil {
		return fmt.Errorf("store: resource create insert %q: %w", id, err)
	}
	// RowsAffected==0 means the id already existed (ON CONFLICT DO NOTHING) — the
	// collision verdict, decided inside the transaction. A RowsAffected FAILURE is
	// surfaced as its own error — never fabricated into an already_exists verdict.
	n, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("store: resource create rows-affected %q: %w", id, err)
	}
	if n == 0 {
		return resourcespec.ErrAlreadyExists
	}

	if _, err := tx.ExecContext(ctx,
		`INSERT INTO resource_grants (resource_id, grantee_kind, grantee, ops)
		   VALUES (?, ?, ?, ?)`,
		string(id), string(access.GranteeActor), string(creator), string(ops),
	); err != nil {
		return fmt.Errorf("store: resource create grant %q: %w", id, err)
	}
	return nil
}

// ReserveCreate is create-outbox's server-side write-ahead half (§1.7): a
// fresh reservation_id per call (uuid, so no collision to arbitrate — unlike
// resource_id, reservation identity is never client-proposed).
func (r *resourceRegistry) ReserveCreate(ctx context.Context, id resource.ResourceID, kind resourcespec.ResourceKind, creator actor.ActorID, placementDaemonID string, placementCoord string, dir bool) (string, error) {
	if id == "" {
		return "", errors.New("store: resource reserve-create: empty id")
	}
	if creator == "" {
		return "", errors.New("store: resource reserve-create: empty creator")
	}
	reservationID := uuid.NewString()
	now := r.nowMs()
	// last_progress_at seeds to the SAME stamp as reserved_at (期11 S1): a
	// freshly-minted reservation starts "alive as of right now", identical
	// to the pre-S1 reserved_at-only behavior until something bumps it —
	// SweepExpiredReservations judging a never-touched row is unchanged.
	if _, err := r.db.ExecContext(ctx,
		`INSERT INTO resource_reservations (reservation_id, resource_id, kind, placement_daemon_id, placement_coord, created_by, reserved_at, is_dir, last_progress_at)
		   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		reservationID, string(id), string(kind), placementDaemonID, placementCoord, string(creator), now, dir, now,
	); err != nil {
		return "", fmt.Errorf("store: resource reserve-create %q: %w", id, err)
	}
	return reservationID, nil
}

// CommitReservation is create-outbox's landing half (§1.7). See the
// resourcespec.Registry doc for the found/err contract.
func (r *resourceRegistry) CommitReservation(ctx context.Context, reservationID string) (bool, error) {
	if reservationID == "" {
		return false, errors.New("store: commit reservation: empty reservation id")
	}

	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return false, fmt.Errorf("store: commit reservation begin: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	var resourceID, kind, placementDaemonID, placementCoord, createdBy string
	var isDir bool
	err = tx.QueryRowContext(ctx,
		`SELECT resource_id, kind, placement_daemon_id, placement_coord, created_by, is_dir
		   FROM resource_reservations WHERE reservation_id=?`,
		reservationID,
	).Scan(&resourceID, &kind, &placementDaemonID, &placementCoord, &createdBy, &isDir)
	if errors.Is(err, sql.ErrNoRows) {
		// Already committed (an earlier replay landed and deleted it) or never
		// existed: Committed is level-triggered and MUST be replay-safe — a
		// clean no-op, not an error.
		if cerr := tx.Commit(); cerr != nil {
			return false, fmt.Errorf("store: commit reservation no-op commit: %w", cerr)
		}
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("store: commit reservation lookup %q: %w", reservationID, err)
	}

	createErr := r.createResourceTx(ctx, tx,
		resource.ResourceID(resourceID), resourcespec.ResourceKind(kind), actor.ActorID(createdBy),
		placementDaemonID, placementCoord, nil, isDir,
	)
	if createErr != nil && !errors.Is(createErr, resourcespec.ErrAlreadyExists) {
		return false, fmt.Errorf("store: commit reservation create %q: %w", resourceID, createErr)
	}

	// Either way (won or lost the race) THIS reservation is consumed — delete
	// it in the SAME transaction: the success path deletes on landing; the
	// losing side of a same-resource_id race deletes too, never left
	// dangling (§1.7's three-triggers account, trigger ①/②).
	if _, err := tx.ExecContext(ctx, `DELETE FROM resource_reservations WHERE reservation_id=?`, reservationID); err != nil {
		return false, fmt.Errorf("store: commit reservation delete %q: %w", reservationID, err)
	}

	if err := tx.Commit(); err != nil {
		return false, fmt.Errorf("store: commit reservation commit: %w", err)
	}

	if errors.Is(createErr, resourcespec.ErrAlreadyExists) {
		return true, resourcespec.ErrReservationLost
	}
	return true, nil
}

// ReservationDaemon reads back one reservation's placement_daemon_id only —
// the §4.7 sender-auth read (see resourcespec.Registry's doc). found=false
// when the row is already gone (committed/lost/swept), a clean non-error.
func (r *resourceRegistry) ReservationDaemon(ctx context.Context, reservationID string) (string, bool, error) {
	var daemonID string
	err := r.db.QueryRowContext(ctx,
		`SELECT placement_daemon_id FROM resource_reservations WHERE reservation_id=?`, reservationID,
	).Scan(&daemonID)
	if errors.Is(err, sql.ErrNoRows) {
		return "", false, nil
	}
	if err != nil {
		return "", false, fmt.Errorf("store: reservation daemon %q: %w", reservationID, err)
	}
	return daemonID, true, nil
}

// TombstoneDaemon is ReservationDaemon's delete-side mirror.
func (r *resourceRegistry) TombstoneDaemon(ctx context.Context, tombstoneID string) (string, bool, error) {
	var daemonID string
	err := r.db.QueryRowContext(ctx,
		`SELECT daemon_id FROM resource_tombstones WHERE tombstone_id=?`, tombstoneID,
	).Scan(&daemonID)
	if errors.Is(err, sql.ErrNoRows) {
		return "", false, nil
	}
	if err != nil {
		return "", false, fmt.Errorf("store: tombstone daemon %q: %w", tombstoneID, err)
	}
	return daemonID, true, nil
}

// ListReservationsByDaemon returns every pending reservation whose
// placement_daemon_id == daemonID, oldest first — §4.7 ReconcilePull's
// "挂起 reservation" answer, pre-filtered server-side (a daemon's
// ReconcilePull never sees another daemon's rows, §4.7's read-side
// extension of the sender-auth discipline).
func (r *resourceRegistry) ListReservationsByDaemon(ctx context.Context, daemonID string) ([]resourcespec.ReservationRow, error) {
	rows, err := r.db.QueryContext(ctx,
		`SELECT reservation_id, resource_id, kind, placement_daemon_id, placement_coord, created_by, reserved_at, last_progress_at
		   FROM resource_reservations WHERE placement_daemon_id=? ORDER BY reserved_at, reservation_id`,
		daemonID,
	)
	if err != nil {
		return nil, fmt.Errorf("store: list reservations by daemon %q: %w", daemonID, err)
	}
	defer func() { _ = rows.Close() }()

	var out []resourcespec.ReservationRow
	for rows.Next() {
		var row resourcespec.ReservationRow
		var resID, kind, createdBy string
		if err := rows.Scan(&row.ReservationID, &resID, &kind, &row.PlacementDaemonID, &row.PlacementCoord, &createdBy, &row.ReservedAt, &row.LastProgressAt); err != nil {
			return nil, fmt.Errorf("store: list reservations by daemon scan %q: %w", daemonID, err)
		}
		row.ResourceID = resource.ResourceID(resID)
		row.Kind = resourcespec.ResourceKind(kind)
		row.CreatedBy = actor.ActorID(createdBy)
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: list reservations by daemon rows %q: %w", daemonID, err)
	}
	return out, nil
}

// ListTombstonesByDaemon is ListReservationsByDaemon's delete-side mirror —
// §4.7 ReconcilePull's "待收 tombstone" answer.
func (r *resourceRegistry) ListTombstonesByDaemon(ctx context.Context, daemonID string) ([]resourcespec.TombstoneRow, error) {
	rows, err := r.db.QueryContext(ctx,
		`SELECT tombstone_id, resource_id, daemon_id, placement_coord, kind, deleted_at
		   FROM resource_tombstones WHERE daemon_id=? ORDER BY deleted_at, tombstone_id`,
		daemonID,
	)
	if err != nil {
		return nil, fmt.Errorf("store: list tombstones by daemon %q: %w", daemonID, err)
	}
	defer func() { _ = rows.Close() }()

	var out []resourcespec.TombstoneRow
	for rows.Next() {
		var row resourcespec.TombstoneRow
		var resID, kind string
		if err := rows.Scan(&row.TombstoneID, &resID, &row.DaemonID, &row.PlacementCoord, &kind, &row.DeletedAt); err != nil {
			return nil, fmt.Errorf("store: list tombstones by daemon scan %q: %w", daemonID, err)
		}
		row.ResourceID = resource.ResourceID(resID)
		row.Kind = resourcespec.ResourceKind(kind)
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: list tombstones by daemon rows %q: %w", daemonID, err)
	}
	return out, nil
}

// ListByPlacementDaemon returns every LANDED resource row whose
// placement_daemon_id == daemonID — §4.7 ReconcilePull's "应有资源清单"
// answer (the Scrubber's registry-side half of registry↔directory
// reconciliation). Grant projection is fetched per row exactly like List
// (grantsFor), for the same reason: a page of N resources should cost the
// caller no per-op round trip, even though the Scrubber does not currently
// consume Grants — the shape stays uniform with every other row-returning
// method rather than a bespoke narrower one.
func (r *resourceRegistry) ListByPlacementDaemon(ctx context.Context, daemonID string) ([]resourcespec.ResourceRow, error) {
	rows, err := r.db.QueryContext(ctx,
		`SELECT resource_id, kind, placement_daemon_id, placement_coord, created_by, created_at
		   FROM resources WHERE placement_daemon_id=? ORDER BY created_at, resource_id`,
		daemonID,
	)
	if err != nil {
		return nil, fmt.Errorf("store: list by placement daemon %q: %w", daemonID, err)
	}
	// Buffer + close before any nested grantsFor query — same single-connection
	// deadlock avoidance as List (see its comment).
	type baseRow struct {
		id                                                 resource.ResourceID
		kind, placementDaemonID, placementCoord, createdBy string
		createdAt                                          int64
	}
	var base []baseRow
	for rows.Next() {
		var b baseRow
		var id string
		if err := rows.Scan(&id, &b.kind, &b.placementDaemonID, &b.placementCoord, &b.createdBy, &b.createdAt); err != nil {
			_ = rows.Close()
			return nil, fmt.Errorf("store: list by placement daemon scan %q: %w", daemonID, err)
		}
		b.id = resource.ResourceID(id)
		base = append(base, b)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return nil, fmt.Errorf("store: list by placement daemon rows %q: %w", daemonID, err)
	}
	if err := rows.Close(); err != nil {
		return nil, fmt.Errorf("store: list by placement daemon close %q: %w", daemonID, err)
	}

	var out []resourcespec.ResourceRow
	for _, b := range base {
		grants, err := r.grantsFor(ctx, b.id)
		if err != nil {
			return nil, err
		}
		out = append(out, resourcespec.ResourceRow{
			ID: b.id,
			Meta: resourcespec.ResourceMeta{
				Kind:              resourcespec.ResourceKind(b.kind),
				CreatedAt:         b.createdAt,
				PlacementKind:     placementKindFor(resourcespec.ResourceKind(b.kind)),
				PlacementDaemonID: b.placementDaemonID,
				PlacementCoord:    b.placementCoord,
				CreatedBy:         actor.ActorID(b.createdBy),
			},
			Grants: grants,
		})
	}
	return out, nil
}

// SweepExpiredReservations deletes every resource_reservations row belonging
// to daemonID whose last_progress_at < cutoffMs, returning the swept rows
// (§1.7's third reservation-deletion trigger, 期11 S1-narrowed from
// reserved_at to last_progress_at — see ReservationRow.LastProgressAt's doc:
// the server ages out a reservation whose placement daemon has gone quiet
// long enough, not merely one that has been slow-but-alive since birth).
// Select-then-delete in one transaction (same shape as CommitReservation's
// own lookup-then-mutate), so a row cannot be swept out from under a
// Committed that lands concurrently — either this transaction's DELETE
// commits first (Committed's later CommitReservation then sees the row
// already gone, a clean no-op) or CommitReservation's own transaction
// commits first (this sweep's SELECT simply does not see it).
func (r *resourceRegistry) SweepExpiredReservations(ctx context.Context, daemonID string, cutoffMs int64) ([]resourcespec.ReservationRow, error) {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("store: sweep expired reservations begin: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	// Every inactive reservation is eligible for the age sweep; there is no
	// daemon-side landed phase or hidden completion replay.
	rows, err := tx.QueryContext(ctx,
		`SELECT reservation_id, resource_id, kind, placement_daemon_id, placement_coord, created_by, reserved_at, last_progress_at
		   FROM resource_reservations WHERE placement_daemon_id=? AND last_progress_at<? ORDER BY reserved_at, reservation_id`,
		daemonID, cutoffMs,
	)
	if err != nil {
		return nil, fmt.Errorf("store: sweep expired reservations select %q: %w", daemonID, err)
	}
	var out []resourcespec.ReservationRow
	for rows.Next() {
		var row resourcespec.ReservationRow
		var resID, kind, createdBy string
		if err := rows.Scan(&row.ReservationID, &resID, &kind, &row.PlacementDaemonID, &row.PlacementCoord, &createdBy, &row.ReservedAt, &row.LastProgressAt); err != nil {
			_ = rows.Close()
			return nil, fmt.Errorf("store: sweep expired reservations scan %q: %w", daemonID, err)
		}
		row.ResourceID = resource.ResourceID(resID)
		row.Kind = resourcespec.ResourceKind(kind)
		row.CreatedBy = actor.ActorID(createdBy)
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return nil, fmt.Errorf("store: sweep expired reservations rows %q: %w", daemonID, err)
	}
	if err := rows.Close(); err != nil {
		return nil, fmt.Errorf("store: sweep expired reservations close %q: %w", daemonID, err)
	}

	for _, row := range out {
		if _, err := tx.ExecContext(ctx, `DELETE FROM resource_reservations WHERE reservation_id=?`, row.ReservationID); err != nil {
			return nil, fmt.Errorf("store: sweep expired reservations delete %q: %w", row.ReservationID, err)
		}
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("store: sweep expired reservations commit: %w", err)
	}
	return out, nil
}

// TouchReservationsByCoords bumps last_progress_at=atMs for every currently-
// pending reservation belonging to daemonID WHOSE placement_coord is in
// coords (期11 review 收窄修复 — see resourcespec.Registry.
// TouchReservationsByCoords's doc for why "daemon in touch at all" is the
// wrong liveness granularity). A plain bulk UPDATE, not a select-then-mutate
// transaction: unlike SweepExpiredReservations this never deletes a row, so
// there is no race window to close — a reservation that lands
// (CommitReservation) or gets lost concurrently simply vanishes from this
// UPDATE's WHERE match, no different from any other concurrent writer racing
// a wide predicate.
//
// An empty coords touches ZERO rows — this is NOT "no filter" degrading to
// the old by-daemon behavior, it is the honest answer to "this daemon
// currently has no active writes" (a daemon with only abandoned reservations
// and nothing open must bump nothing, or age-sweep could never catch up).
func (r *resourceRegistry) TouchReservationsByCoords(ctx context.Context, daemonID string, coords []string, atMs int64) error {
	if len(coords) == 0 {
		return nil
	}
	placeholders := make([]string, len(coords))
	args := make([]any, 0, len(coords)+2)
	args = append(args, atMs, daemonID)
	for i, c := range coords {
		placeholders[i] = "?"
		args = append(args, c)
	}
	query := fmt.Sprintf(
		`UPDATE resource_reservations SET last_progress_at=? WHERE placement_daemon_id=? AND placement_coord IN (%s)`,
		strings.Join(placeholders, ","),
	)
	if _, err := r.db.ExecContext(ctx, query, args...); err != nil {
		return fmt.Errorf("store: touch reservations by coords %q: %w", daemonID, err)
	}
	return nil
}

// ActorAllows queries caller's DIRECT actor entry only. members late-binding is
// the door's job (it unions this with MembersAllow gated by a membership check),
// so this half never resolves the members set.
func (r *resourceRegistry) ActorAllows(ctx context.Context, caller actor.ActorID, id resource.ResourceID, op access.Operation) (bool, error) {
	return r.entryAllows(ctx, id, access.GranteeActor, string(caller), op)
}

// MembersAllow reports whether the object carries a members-kind entry granting
// op. It does NOT look at caller — whether caller is a current member is the
// door's membership check; the two halves union at the door (allow-only).
func (r *resourceRegistry) MembersAllow(ctx context.Context, id resource.ResourceID, op access.Operation) (bool, error) {
	return r.entryAllows(ctx, id, access.GranteeMembers, "", op)
}

// entryAllows fetches one grant entry (by the sum-form key) and reports whether
// its ops include op. An absent entry is a clean false, not an error.
func (r *resourceRegistry) entryAllows(ctx context.Context, id resource.ResourceID, kind access.GranteeKind, grantee string, op access.Operation) (bool, error) {
	const q = `SELECT ops FROM resource_grants WHERE resource_id=? AND grantee_kind=? AND grantee=?`
	var opsJSON string
	err := r.db.QueryRowContext(ctx, q, string(id), string(kind), grantee).Scan(&opsJSON)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("store: resource grant lookup %q/%s/%q: %w", id, kind, grantee, err)
	}
	ops, err := decodeOps(opsJSON)
	if err != nil {
		return false, fmt.Errorf("store: resource grant ops decode %q/%s/%q: %w", id, kind, grantee, err)
	}
	for _, o := range ops {
		if o == op {
			return true, nil
		}
	}
	return false, nil
}

// SetGrant implements op=set with chmod/setfacl SET semantics: it REPLACES the
// grantee's entry with g. g.Ops == ∅ REVOKES = deletes the row (an absent entry
// and an empty-ops entry must not be two states). The entry key is the full sum
// form (resource_id, grantee_kind, grantee). g has already passed the door's
// ValidateGrant, so the registry only stores (mirrors storespec's
// store-not-validate discipline).
func (r *resourceRegistry) SetGrant(ctx context.Context, id resource.ResourceID, g access.Grant) error {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("store: resource set-grant begin: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	if len(g.Ops) == 0 {
		if _, err := tx.ExecContext(ctx,
			`DELETE FROM resource_grants WHERE resource_id=? AND grantee_kind=? AND grantee=?`,
			string(id), string(g.GranteeKind), string(g.Grantee),
		); err != nil {
			return fmt.Errorf("store: resource revoke grant %q: %w", id, err)
		}
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("store: resource revoke grant commit: %w", err)
		}
		return nil
	}

	ops, err := json.Marshal(g.Ops)
	if err != nil {
		return fmt.Errorf("store: resource set-grant marshal ops: %w", err)
	}
	if _, err := tx.ExecContext(ctx,
		`INSERT INTO resource_grants (resource_id, grantee_kind, grantee, ops)
		   VALUES (?, ?, ?, ?)
		 ON CONFLICT(resource_id, grantee_kind, grantee) DO UPDATE SET ops=excluded.ops`,
		string(id), string(g.GranteeKind), string(g.Grantee), string(ops),
	); err != nil {
		return fmt.Errorf("store: resource set-grant %q: %w", id, err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("store: resource set-grant commit: %w", err)
	}
	return nil
}

// Delete removes the resource row + ALL its grants in one transaction. The
// byte-collection time-order is KIND-DEPENDENT (期11 spec §1 item 8):
//   - kv: bytes live inline, so removing the row removes the bytes — no
//     tombstone, same shape as before.
//   - file: ROW-FIRST-BYTES-LAST — this reads the row (kind + placement)
//     inside the SAME transaction, writes a resource_tombstones
//     row from those values, then deletes the resource row + grants. The
//     daemon-side Reclaimer (§4, a later addition) collects the bytes
//     asynchronously afterward. The invariant "a visible row always points
//     at valid bytes" holds throughout: the row disappears FIRST.
//
// supersedePendingReservationsTx is Delete's #C helper: inside the caller's
// transaction, for every still-pending reservation on id, write a
// resource_tombstones row for its coord (so the daemon's ordinary
// ReconcilePull Reclaimer collects the orphaned live/<coord> bytes — the SAME
// delete-outbox reclaim path a resource delete already uses, no new
// mechanism) then delete the reservation row. Reservations are always file
// kind, so the tombstone's kind is fixed. A reservation whose write
// handle is still open when this runs will, on its eventual Commit, hit
// CommitReservation's found=false no-op — the row is already gone here.
func (r *resourceRegistry) supersedePendingReservationsTx(ctx context.Context, tx *sql.Tx, id resource.ResourceID) error {
	rows, err := tx.QueryContext(ctx,
		`SELECT reservation_id, placement_daemon_id, placement_coord FROM resource_reservations WHERE resource_id=?`,
		string(id),
	)
	if err != nil {
		return fmt.Errorf("store: supersede reservations select %q: %w", id, err)
	}
	type pending struct{ reservationID, daemonID, coord string }
	var supers []pending
	for rows.Next() {
		var p pending
		if err := rows.Scan(&p.reservationID, &p.daemonID, &p.coord); err != nil {
			_ = rows.Close()
			return fmt.Errorf("store: supersede reservations scan %q: %w", id, err)
		}
		supers = append(supers, p)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return fmt.Errorf("store: supersede reservations rows %q: %w", id, err)
	}
	if err := rows.Close(); err != nil {
		return fmt.Errorf("store: supersede reservations close %q: %w", id, err)
	}

	for _, p := range supers {
		if _, err := tx.ExecContext(ctx,
			`INSERT INTO resource_tombstones (tombstone_id, resource_id, daemon_id, placement_coord, kind, deleted_at)
			   VALUES (?, ?, ?, ?, ?, ?)`,
			uuid.NewString(), string(id), p.daemonID, p.coord, string(resourcespec.KindFile), r.nowMs(),
		); err != nil {
			return fmt.Errorf("store: supersede reservations tombstone %q: %w", p.reservationID, err)
		}
		if _, err := tx.ExecContext(ctx, `DELETE FROM resource_reservations WHERE reservation_id=?`, p.reservationID); err != nil {
			return fmt.Errorf("store: supersede reservations delete %q: %w", p.reservationID, err)
		}
	}
	return nil
}

func (r *resourceRegistry) Delete(ctx context.Context, id resource.ResourceID) error {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("store: resource delete begin: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	// SUPERSEDED terminal (期11 review §2.5 #C): a Delete on this resource_id
	// also kills any still-pending reservation for the SAME id and reclaims its
	// coord. Without this, a write handle held open forever keeps its coord
	// "active" (TouchReservationsByCoords bumps it every ReconcilePull, so the
	// age-sweep never fires); across a delete/recreate cycle its straggler
	// Committed would later find the id free and silently REBUILD it with the
	// original creator/coord — a resurrection with no fresh OpCreate
	// authorization. Deleting the reservation row here is the "straggler 落地时
	//发现已被 delete，拒绝落 row" guard: CommitReservation's own lookup then
	// returns found=false (a clean replay-safe no-op), never re-landing the id.
	// Runs FIRST (even when the resource row itself is already gone — an
	// idempotent re-delete still supersedes any lingering reservation).
	if err := r.supersedePendingReservationsTx(ctx, tx, id); err != nil {
		return err
	}

	var kind, placementDaemonID, placementCoord string
	err = tx.QueryRowContext(ctx,
		`SELECT kind, placement_daemon_id, placement_coord FROM resources WHERE resource_id=?`,
		string(id),
	).Scan(&kind, &placementDaemonID, &placementCoord)
	if errors.Is(err, sql.ErrNoRows) {
		// Already gone: delete is idempotent, retryable, no cross-call
		// atomicity needed (only create does, for its controller-grab window).
		if cerr := tx.Commit(); cerr != nil {
			return fmt.Errorf("store: resource delete no-op commit: %w", cerr)
		}
		return nil
	}
	if err != nil {
		return fmt.Errorf("store: resource delete read %q: %w", id, err)
	}

	if kind == string(resourcespec.KindFile) {
		if _, err := tx.ExecContext(ctx,
			`INSERT INTO resource_tombstones (tombstone_id, resource_id, daemon_id, placement_coord, kind, deleted_at)
			   VALUES (?, ?, ?, ?, ?, ?)`,
			uuid.NewString(), string(id), placementDaemonID, placementCoord, kind, r.nowMs(),
		); err != nil {
			return fmt.Errorf("store: resource delete tombstone %q: %w", id, err)
		}
	}
	// kv: bytes live inline in the row itself — deleting the row IS deleting
	// the bytes, no tombstone.

	if _, err := tx.ExecContext(ctx, `DELETE FROM resource_grants WHERE resource_id=?`, string(id)); err != nil {
		return fmt.Errorf("store: resource delete grants %q: %w", id, err)
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM resources WHERE resource_id=?`, string(id)); err != nil {
		return fmt.Errorf("store: resource delete row %q: %w", id, err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("store: resource delete commit: %w", err)
	}
	return nil
}

// ClearTombstone deletes one resource_tombstones row (the Reclaimer's
// ReclaimAck closure, §4.7's fourth frame). found=false, err=nil when
// already gone — replay-safe like CommitReservation.
func (r *resourceRegistry) ClearTombstone(ctx context.Context, tombstoneID string) (bool, error) {
	if tombstoneID == "" {
		return false, errors.New("store: clear tombstone: empty tombstone id")
	}
	res, err := r.db.ExecContext(ctx, `DELETE FROM resource_tombstones WHERE tombstone_id=?`, tombstoneID)
	if err != nil {
		return false, fmt.Errorf("store: clear tombstone %q: %w", tombstoneID, err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("store: clear tombstone rows-affected %q: %w", tombstoneID, err)
	}
	return n > 0, nil
}

// List enumerates resources in stable (created_at, resource_id) order — a
// raw range scan, no grant filtering (the door's job, §3.7). See the
// resourcespec.Registry doc for the prefix/limit/cursor contract.
func (r *resourceRegistry) List(ctx context.Context, prefix string, limit int, cursor string) ([]resourcespec.ResourceRow, string, error) {
	if limit <= 0 {
		return nil, "", errors.New("store: resource list: limit must be positive")
	}

	var afterCreatedAt int64
	var afterID string
	if cursor != "" {
		var ok bool
		afterCreatedAt, afterID, ok = decodeListCursor(cursor)
		if !ok {
			return nil, "", fmt.Errorf("store: resource list: malformed cursor %q: %w", cursor, resourcespec.ErrMalformedCursor)
		}
	}

	q := `SELECT resource_id, kind, placement_daemon_id, placement_coord, created_by, created_at
	        FROM resources
	       WHERE (created_at > ? OR (created_at = ? AND resource_id > ?))`
	args := []any{afterCreatedAt, afterCreatedAt, afterID}
	if like, ok := likePrefix(prefix); ok {
		q += ` AND resource_id LIKE ? ESCAPE '\'`
		args = append(args, like)
	}
	q += ` ORDER BY created_at, resource_id LIMIT ?`
	args = append(args, limit)

	rows, err := r.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, "", fmt.Errorf("store: resource list query: %w", err)
	}

	// Buffer the base rows and CLOSE this cursor before firing any per-row
	// grantsFor query: the channel db is pinned to a single connection
	// (sqlite.go's SetMaxOpenConns(1), load-bearing for messages.go's in-tx
	// re-check), so a nested QueryContext while this cursor still holds it
	// would deadlock waiting for a connection the pool will never hand out.
	type baseRow struct {
		id                                                 resource.ResourceID
		kind, placementDaemonID, placementCoord, createdBy string
		createdAt                                          int64
	}
	var base []baseRow
	for rows.Next() {
		var b baseRow
		var id string
		if err := rows.Scan(&id, &b.kind, &b.placementDaemonID, &b.placementCoord, &b.createdBy, &b.createdAt); err != nil {
			_ = rows.Close()
			return nil, "", fmt.Errorf("store: resource list scan: %w", err)
		}
		b.id = resource.ResourceID(id)
		base = append(base, b)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return nil, "", fmt.Errorf("store: resource list rows: %w", err)
	}
	if err := rows.Close(); err != nil {
		return nil, "", fmt.Errorf("store: resource list close: %w", err)
	}

	var out []resourcespec.ResourceRow
	var lastCreatedAt int64
	var lastID string
	n := 0
	for _, b := range base {
		grants, err := r.grantsFor(ctx, b.id)
		if err != nil {
			return nil, "", err
		}
		out = append(out, resourcespec.ResourceRow{
			ID: b.id,
			Meta: resourcespec.ResourceMeta{
				Kind:              resourcespec.ResourceKind(b.kind),
				CreatedAt:         b.createdAt,
				PlacementKind:     placementKindFor(resourcespec.ResourceKind(b.kind)),
				PlacementDaemonID: b.placementDaemonID,
				PlacementCoord:    b.placementCoord,
				CreatedBy:         actor.ActorID(b.createdBy),
			},
			Grants: grants,
		})
		lastCreatedAt, lastID = b.createdAt, string(b.id)
		n++
	}

	nextCursor := ""
	if n == limit {
		// A full page was scanned — there MAY be more; the cursor encodes the
		// last SCANNED row's key (== last returned row's key here, since this
		// layer never filters), not "the last visible row" (that distinction
		// matters one layer up, at the door's grant-filtered projection, §3.7).
		nextCursor = encodeListCursor(lastCreatedAt, lastID)
	}
	return out, nextCursor, nil
}

// grantsFor fetches the FULL grant projection for one resource id — every
// persisted (grantee_kind, grantee) entry, ops included — so List's caller
// (the door) can compute effectiveOps without a second per-op query.
func (r *resourceRegistry) grantsFor(ctx context.Context, id resource.ResourceID) ([]access.Grant, error) {
	rows, err := r.db.QueryContext(ctx, `SELECT grantee_kind, grantee, ops FROM resource_grants WHERE resource_id=?`, string(id))
	if err != nil {
		return nil, fmt.Errorf("store: resource list grants %q: %w", id, err)
	}
	defer func() { _ = rows.Close() }()

	var out []access.Grant
	for rows.Next() {
		var granteeKind, grantee, opsJSON string
		if err := rows.Scan(&granteeKind, &grantee, &opsJSON); err != nil {
			return nil, fmt.Errorf("store: resource list grants scan %q: %w", id, err)
		}
		ops, err := decodeOps(opsJSON)
		if err != nil {
			return nil, fmt.Errorf("store: resource list grants decode %q: %w", id, err)
		}
		out = append(out, access.Grant{
			GranteeKind: access.GranteeKind(granteeKind),
			Grantee:     actor.ActorID(grantee),
			Ops:         ops,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: resource list grants rows %q: %w", id, err)
	}
	return out, nil
}

// decodeOps unmarshals the JSON-array ops column shared by every grant read
// path (entryAllows, grantsFor).
func decodeOps(opsJSON string) ([]access.Operation, error) {
	var ops []access.Operation
	if err := json.Unmarshal([]byte(opsJSON), &ops); err != nil {
		return nil, err
	}
	return ops, nil
}

// likePrefix turns a plain resource_id prefix into a LIKE pattern, escaping
// the two SQL wildcard characters (and the escape character itself) so a raw
// '%'/'_' in an id acts as a literal, never a glob. ok=false for the empty
// prefix (no filter).
func likePrefix(prefix string) (string, bool) {
	if prefix == "" {
		return "", false
	}
	var b strings.Builder
	for _, ch := range prefix {
		switch ch {
		case '\\', '%', '_':
			b.WriteByte('\\')
		}
		b.WriteRune(ch)
	}
	b.WriteByte('%')
	return b.String(), true
}

// encodeListCursor / decodeListCursor are List's OWN opaque pagination
// token: base64 of "<created_at>\x00<resource_id>" — the last SCANNED row's
// composite sort key. NUL-delimited (not a printable separator) so a
// resource_id containing any printable character round-trips exactly.
func encodeListCursor(createdAt int64, id string) string {
	raw := strconv.FormatInt(createdAt, 10) + "\x00" + id
	return base64.RawURLEncoding.EncodeToString([]byte(raw))
}

func decodeListCursor(cursor string) (createdAt int64, id string, ok bool) {
	raw, err := base64.RawURLEncoding.DecodeString(cursor)
	if err != nil {
		return 0, "", false
	}
	parts := strings.SplitN(string(raw), "\x00", 2)
	if len(parts) != 2 {
		return 0, "", false
	}
	n, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return 0, "", false
	}
	return n, parts[1], true
}

// kvDriver implements resourcespec.Driver for KindKV — the day-1 channel-scoped
// inline-byte driver, operating on the resources.bytes column.
type kvDriver struct {
	db *sql.DB
}

func newKVDriver(db *sql.DB) *kvDriver {
	return &kvDriver{db: db}
}

// Read returns the current bytes. found == false is resolved-but-empty (a LEGAL
// outcome, not a failure): a NULL bytes column — or, defensively, a vanished
// row — reads back as found=false with nil value. The row-vs-empty distinction
// the door cares about is drawn earlier, at Resolve (a missing row is
// resource_not_found before the driver is ever called).
func (d *kvDriver) Read(ctx context.Context, id resource.ResourceID) ([]byte, bool, error) {
	// bytes IS NULL is selected explicitly: a zero-length blob scans back as a
	// nil []byte just like NULL does, so the Go value alone cannot distinguish
	// present-but-empty (found=true, proto: legal and distinct) from no-operand
	// NULL (found=false).
	const q = `SELECT bytes, bytes IS NULL FROM resources WHERE resource_id=?`
	var raw []byte
	var isNull bool
	err := d.db.QueryRowContext(ctx, q, string(id)).Scan(&raw, &isNull)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, fmt.Errorf("store: kv read %q: %w", id, err)
	}
	if isNull {
		return nil, false, nil
	}
	if raw == nil {
		raw = []byte{}
	}
	return raw, true, nil
}

// Write overwrites existing content (PUT semantics, naturally idempotent). The
// door reaches Write only after Resolve confirms existence, but the row can die
// in the resolve→execute window (no transaction spans the door's stages), so a
// zero-row UPDATE is surfaced as an error — an honest driver_error verdict —
// rather than a silent success against a vanished resource.
func (d *kvDriver) Write(ctx context.Context, id resource.ResourceID, value []byte) error {
	res, err := d.db.ExecContext(ctx, `UPDATE resources SET bytes=? WHERE resource_id=?`, value, string(id))
	if err != nil {
		return fmt.Errorf("store: kv write %q: %w", id, err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("store: kv write rows-affected %q: %w", id, err)
	}
	if n == 0 {
		return fmt.Errorf("store: kv write %q: resource vanished mid-invocation", id)
	}
	return nil
}

// Delete is a no-op: KindKV bytes live INLINE in the resources row, so removing
// the row (Registry.Delete) removes the bytes too. This orchestration slot earns
// its keep only for a future external-byte driver whose bytes live outside the
// row; for inline kv there is nothing to remove here.
func (d *kvDriver) Delete(ctx context.Context, id resource.ResourceID) error {
	return nil
}

var (
	_ resourcespec.Registry       = (*resourceRegistry)(nil)
	_ resourcespec.ResourceOutbox = (*resourceRegistry)(nil)
	_ resourcespec.Driver         = (*kvDriver)(nil)
)
