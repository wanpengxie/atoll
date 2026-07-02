package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/wanpengxie/atoll/protocol/access"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/resource"
	"github.com/wanpengxie/atoll/runtime/resourcespec"
)

// resourceRegistry implements resourcespec.Registry over the channel-local
// sqlite (resources + resource_grants), the plane-2 dual of actorRegistry: it
// is the object-existence + authorization-relation (R) truth the door consults
// and mutates. Bound to one channel database (access is channel-scoped).
type resourceRegistry struct {
	db *sql.DB
	// nowMs stamps created_at. Injectable (tests pin it) — the rest of the
	// registry is clock-free.
	nowMs func() int64
}

func newResourceRegistry(db *sql.DB) *resourceRegistry {
	return &resourceRegistry{db: db, nowMs: func() int64 { return time.Now().UnixMilli() }}
}

// Resolve reads back existence + meta. kind is returned as the raw persisted
// value with NO closed-set parse (unlike actor_kind): ResourceKind is a runtime
// routing key, and whether a kind resolves to a registered driver is the door's
// question, not a poison-row guard here.
func (r *resourceRegistry) Resolve(ctx context.Context, id resource.ResourceID) (resourcespec.ResourceMeta, bool, error) {
	const q = `SELECT kind, created_at FROM resources WHERE resource_id=?`
	var kind string
	var createdAt int64
	err := r.db.QueryRowContext(ctx, q, string(id)).Scan(&kind, &createdAt)
	if errors.Is(err, sql.ErrNoRows) {
		return resourcespec.ResourceMeta{}, false, nil
	}
	if err != nil {
		return resourcespec.ResourceMeta{}, false, fmt.Errorf("store: resource resolve %q: %w", id, err)
	}
	return resourcespec.ResourceMeta{Kind: resourcespec.ResourceKind(kind), CreatedAt: createdAt}, true, nil
}

// Create is op=create's atomic birth event: the existence row + inline bytes AND
// the creator's full-rights actor entry in R, all in ONE transaction (creation
// IS ownership — no half-built window where the row exists without its
// controller grant). A colliding id returns ErrAlreadyExists: the INSERT ...
// ON CONFLICT DO NOTHING makes existence an atomic test-and-set, so the door
// never resolves-then-creates in two steps and no concurrent creator can grab
// control of an already-born id.
func (r *resourceRegistry) Create(ctx context.Context, id resource.ResourceID, kind resourcespec.ResourceKind, creator actor.ActorID, initial []byte) error {
	if id == "" {
		return errors.New("store: resource create: empty id")
	}
	if creator == "" {
		return errors.New("store: resource create: empty creator")
	}
	// The creator's grant is full object-rights: read/write/set/delete. set-right
	// is what makes the creator the controller (control = a full grant in R, no
	// separate owner column).
	ops, err := json.Marshal([]access.Operation{access.OpRead, access.OpWrite, access.OpSet, access.OpDelete})
	if err != nil {
		return fmt.Errorf("store: resource create marshal ops: %w", err)
	}

	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("store: resource create begin: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	res, err := tx.ExecContext(ctx,
		`INSERT INTO resources (resource_id, kind, bytes, created_at)
		   VALUES (?, ?, ?, ?)
		 ON CONFLICT(resource_id) DO NOTHING`,
		string(id), string(kind), initial, r.nowMs(),
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

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("store: resource create commit: %w", err)
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
	var ops []access.Operation
	if err := json.Unmarshal([]byte(opsJSON), &ops); err != nil {
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

// Delete removes the resource row + ALL its grants in one transaction. Grants
// (the FK child) go first, then the row (the FK parent), so foreign_keys=ON is
// satisfied at every step.
func (r *resourceRegistry) Delete(ctx context.Context, id resource.ResourceID) error {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("store: resource delete begin: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

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

// kvDriver implements resourcespec.Driver for KindKV — the day-1 channel-scoped
// inline-byte driver, operating on the resources.bytes column.
type kvDriver struct {
	db *sql.DB
}

func newKVDriver(db *sql.DB) *kvDriver {
	return &kvDriver{db: db}
}

// Read returns the current bytes. found==false is resolved-but-empty (a LEGAL
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
	_ resourcespec.Registry = (*resourceRegistry)(nil)
	_ resourcespec.Driver   = (*kvDriver)(nil)
)
