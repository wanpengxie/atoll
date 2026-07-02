package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/wanpengxie/ActOS/protocol/actor"
	"github.com/wanpengxie/ActOS/protocol/resource"
	"github.com/wanpengxie/ActOS/runtime/resourcespec"
)

// stateStore implements resourcespec.StateStore over the actor_state table — the
// byte realizer for the ACTOR-SCOPED storage locus (forward §6 · §12.9 拍点 8.1).
// It is the collapsed-branch dual of resourceRegistry+kvDriver: no R, no grants,
// no kind routing (day-1 one mechanical shape). owner is a COORDINATE welded by
// the door at handle mint (reachable set ≡ {owner}); this store only persists
// (mirrors storespec's store-not-validate discipline), it never re-checks
// authorization. Bound to one channel database (access is channel-封).
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

// Read returns the current bytes and whether the ROW exists (present). present
// tracks EXISTENCE, not byte-nullness: a present row whose bytes column is NULL
// is resolved-but-empty and reads back present=true with value=nil (the door
// maps that to Found=false, uniform with the channel-scoped driver); an empty
// non-nil blob is a value (present=true, value=[]byte{}). A missing row is
// present=false (door → resource_not_found).
func (s *stateStore) Read(ctx context.Context, owner actor.ActorID, id resource.ResourceID) (value []byte, present bool, err error) {
	// bytes IS NULL is selected explicitly: a zero-length blob scans back as a
	// nil []byte just like NULL does, so the Go value alone cannot distinguish
	// resolved-but-empty (NULL → value=nil) from a present empty value ([]byte{}).
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
		// Row present, bytes NULL = resolved-but-empty.
		return nil, true, nil
	}
	if raw == nil {
		raw = []byte{}
	}
	return raw, true, nil
}

// Write overwrites an EXISTING row (PUT semantics, naturally idempotent). It
// never creates: birth is Create. present=false when no row was hit (door →
// resource_not_found — honest about writing to something that is not there),
// mirroring kvDriver's zero-row surfacing.
func (s *stateStore) Write(ctx context.Context, owner actor.ActorID, id resource.ResourceID, value []byte) (present bool, err error) {
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

// Delete removes the row; present=false when no row was hit (door →
// resource_not_found; repeated delete is honestly not-found). This is the
// non-lossy "explicit delete" half; the OTHER death is scope-expiry (owner
// deregister → clearActorScopedTx, store-internal, not an op).
func (s *stateStore) Delete(ctx context.Context, owner actor.ActorID, id resource.ResourceID) (present bool, err error) {
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

// clearActorScopedTx cascades the actor-scoped state locus: it deletes every
// actor_state row owned by owner, inside the SAME transaction that deregisters
// the actor (both dereg entry points in actors.go hang it there). This is the
// scope law (§10.12 row 3 / forward §6.5③): an actor's private persistent state
// 亡 with the actor (Erlang ETS private — owner 亡表随亡). Idempotent: a re-run
// over an already-cleared owner deletes zero rows. The channel-scoped resources
// table is deliberately NOT touched — those objects are non-lossy, outliving
// their creator and dying only on explicit delete / channel destroy.
// Store-internal substrate mechanism, never exposed as a plane-2 door verb
// (exposing it would be a bypass path around the door). It lives HERE beside the
// locus's other SQL so actor_state has exactly ONE author file — a future second
// actor-scoped mechanical shape edits this file and finds the cascade in it.
func clearActorScopedTx(ctx context.Context, tx *sql.Tx, owner actor.ActorID) error {
	if _, err := tx.ExecContext(ctx, `DELETE FROM actor_state WHERE owner_id=?`, string(owner)); err != nil {
		return fmt.Errorf("store: actor_state cascade clear %q: %w", owner, err)
	}
	return nil
}

var _ resourcespec.StateStore = (*stateStore)(nil)
