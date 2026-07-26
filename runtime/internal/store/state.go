package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/resource"
	"github.com/wanpengxie/atoll/runtime/resourcespec"
)

// stateStore implements resourcespec.StateStore over the actor_state table — the
// byte realizer for the ACTOR-SCOPED storage locus. It is the collapsed-branch
// dual of resourceRegistry+kvDriver: no R, no grants, no kind routing (day-1
// one mechanical shape). owner is a COORDINATE welded by the door at handle
// mint (reachable set ≡ {owner}); this store only persists (mirrors
// storespec's store-not-validate discipline), it never re-checks
// authorization. Bound to one channel database (access is channel-scoped).
type stateStore struct {
	db *sql.DB
	// nowMs stamps created_at. Injectable (tests pin it) — the rest of the
	// package takes caller-supplied instants (add.At / Deregister(at)); this
	// store's contract carries no timestamp, so the clock is construction
	// state, defaulting to wall time.
	nowMs func() int64
}

func newStateStore(db *sql.DB) *stateStore {
	return &stateStore{db: db, nowMs: func() int64 { return time.Now().UnixMilli() }}
}

// Create is the atomic birth: INSERT (owner, id, bytes). ON CONFLICT DO NOTHING
// makes existence a test-and-set within the race window, so a colliding
// (owner, id) returns ErrAlreadyExists (the SAME sentinel the channel-scoped
// tree uses — one collision vocabulary, two loci) rather than clobbering the
// living row. No grant row: R does not apply to this locus (the absence IS the
// scope law).
func (s *stateStore) Create(ctx context.Context, owner actor.ActorID, id resource.ResourceID, initial []byte) error {
	// No empty-owner/id guards: owner is a COORDINATE the door welds at mint
	// (store-not-validate, per the struct doc), and an empty id is rejected by
	// the door's ingress (checkResourceID) before the store is reached.
	// No membership re-judgement here: the verdict belongs at the door
	// (organs judge once at their real entrance), and the transaction is purely
	// mechanical. A second EXISTS check would be a second authority AND would
	// let an End racing an already-admitted call veto it a second time.
	res, err := s.db.ExecContext(ctx,
		`INSERT INTO actor_state (owner_id, resource_id, bytes, created_at)
		 VALUES (?, ?, ?, ?)
		 ON CONFLICT(owner_id, resource_id) DO NOTHING`,
		string(owner), string(id), initial, s.nowMs(),
	)
	if err != nil {
		return fmt.Errorf("store: actor_state create %q/%q: %w", owner, id, err)
	}
	// RowsAffected==0 means the (owner, id) already existed — the collision
	// verdict, decided inside the INSERT (test-and-set). The error is NOT
	// swallowed: a driver that cannot report affected rows must surface as a
	// store failure, never a fabricated already_exists.
	n, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("store: actor_state create rows-affected %q/%q: %w", owner, id, err)
	}
	if n == 0 {
		return resourcespec.ErrAlreadyExists
	}
	return nil
}

// Read returns the current bytes and whether the ROW exists. exists
// tracks EXISTENCE, not byte-nullness: an existing row whose bytes column is NULL
// is resolved-but-empty and reads back exists=true with value=nil (the door
// maps that to Found=false, uniform with the channel-scoped driver); an empty
// non-nil blob is a value (exists=true, value=[]byte{}). A missing row is
// exists=false (door → resource_not_found).
func (s *stateStore) Read(ctx context.Context, owner actor.ActorID, id resource.ResourceID) (value []byte, exists bool, err error) {
	// bytes IS NULL is selected explicitly: a zero-length blob scans back as a
	// nil []byte just like NULL does, so the Go value alone cannot distinguish
	// resolved-but-empty (NULL → value=nil) from an existing empty value ([]byte{}).
	const q = `SELECT bytes, bytes IS NULL FROM actor_state WHERE owner_id=? AND resource_id=?`
	var raw []byte
	var isNull bool
	err = s.db.QueryRowContext(ctx, q, string(owner), string(id)).Scan(&raw, &isNull)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, fmt.Errorf("store: actor_state read %q/%q: %w", owner, id, err)
	}
	if isNull {
		// Row exists, bytes NULL = resolved-but-empty.
		return nil, true, nil
	}
	if raw == nil {
		raw = []byte{}
	}
	return raw, true, nil
}

// Write overwrites an EXISTING row (PUT semantics, naturally idempotent). It
// never creates: birth is Create. exists=false when no row was hit (door →
// resource_not_found — honest about writing to something that is not there),
// mirroring kvDriver's zero-row surfacing.
func (s *stateStore) Write(ctx context.Context, owner actor.ActorID, id resource.ResourceID, value []byte) (exists bool, err error) {
	res, err := s.db.ExecContext(ctx,
		`UPDATE actor_state SET bytes=? WHERE owner_id=? AND resource_id=?`,
		value, string(owner), string(id),
	)
	if err != nil {
		return false, fmt.Errorf("store: actor_state write %q/%q: %w", owner, id, err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("store: actor_state write rows-affected %q/%q: %w", owner, id, err)
	}
	return n > 0, nil
}

// Delete removes the row; exists=false when no row was hit (door →
// resource_not_found; repeated delete is honestly not-found). This is the
// non-lossy "explicit delete" half; the OTHER death is scope-expiry (owner
// deregister → clearActorScopedTx, store-internal, not an op).
func (s *stateStore) Delete(ctx context.Context, owner actor.ActorID, id resource.ResourceID) (exists bool, err error) {
	res, err := s.db.ExecContext(ctx,
		`DELETE FROM actor_state WHERE owner_id=? AND resource_id=?`,
		string(owner), string(id),
	)
	if err != nil {
		return false, fmt.Errorf("store: actor_state delete %q/%q: %w", owner, id, err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("store: actor_state delete rows-affected %q/%q: %w", owner, id, err)
	}
	return n > 0, nil
}

// No deregistration cascade clears this table. A dead owner's rows are inert
// data: ActorIDs are never reused and every belonging is keyed by ActorID, so
// nobody but the dead can ever address them. Correctness lives at the admission
// gate, never in a delete; reclaiming the disk is an explicit batch management
// action, not lifecycle logic.

var _ resourcespec.StateStore = (*stateStore)(nil)
