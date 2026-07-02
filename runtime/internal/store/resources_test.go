package store

// White-box tests for the access-plane store: resourceRegistry (R + existence)
// and kvDriver (inline bytes). Both are unexported and reachable only from
// inside the package — the same §4.5 confinement the message-log store relies
// on. They run over a real channel sqlite (ChannelLocalDDL), no fakes.

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/wanpengxie/ActOS/protocol/access"
	"github.com/wanpengxie/ActOS/protocol/actor"
	"github.com/wanpengxie/ActOS/protocol/resource"
)

// openResourceReg opens a fresh temp-dir channel sqlite with the full DDL
// (resources + resource_grants present, foreign_keys=ON) and returns a registry
// over it, registering cleanup.
func openResourceReg(t *testing.T) *resourceRegistry {
	t.Helper()
	ctx := context.Background()
	db, err := openSqlite(ctx, filepath.Join(t.TempDir(), "res.sqlite"), OpenOptions{}, ChannelLocalDDL)
	if err != nil {
		t.Fatalf("openSqlite: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return newResourceRegistry(db)
}

func kvOf(r *resourceRegistry) *kvDriver { return newKVDriver(r.db) }

// --- Create: atomicity (row + full grant + bytes) + collision sentinel -------

func TestResource_CreateWritesRowGrantBytes(t *testing.T) {
	ctx := context.Background()
	reg := openResourceReg(t)

	if err := reg.Create(ctx, "kv:doc", "kv", "actor:a", []byte("hello")); err != nil {
		t.Fatalf("Create: %v", err)
	}

	meta, ok, err := reg.Resolve(ctx, "kv:doc")
	if err != nil || !ok {
		t.Fatalf("Resolve ok=%v err=%v", ok, err)
	}
	if meta.Kind != "kv" {
		t.Errorf("kind=%q want kv", meta.Kind)
	}
	if meta.CreatedAt <= 0 {
		t.Errorf("created_at=%d must be stamped positive", meta.CreatedAt)
	}

	// Creator holds the full object-rights grant (read/write/set/delete).
	for _, op := range []access.Operation{access.OpRead, access.OpWrite, access.OpSet, access.OpDelete} {
		if !allowActor(t, reg, "actor:a", "kv:doc", op) {
			t.Errorf("creator must hold op=%s in its full grant", op)
		}
	}

	// Initial bytes are readable.
	val, found, err := kvOf(reg).Read(ctx, "kv:doc")
	if err != nil || !found {
		t.Fatalf("Read found=%v err=%v", found, err)
	}
	if string(val) != "hello" {
		t.Errorf("bytes=%q want hello", val)
	}
}

func TestResource_CreateCollisionSentinel(t *testing.T) {
	ctx := context.Background()
	reg := openResourceReg(t)

	if err := reg.Create(ctx, "kv:doc", "kv", "actor:a", []byte("v1")); err != nil {
		t.Fatalf("first Create: %v", err)
	}
	err := reg.Create(ctx, "kv:doc", "kv", "actor:b", []byte("v2"))
	if err == nil {
		t.Fatal("second Create on same id must collide")
	}
	if err.Error() != "resourcespec: resource already exists" {
		t.Errorf("collision err=%v want ErrAlreadyExists", err)
	}
	// The colliding create left the original untouched (no controller grab, no
	// byte overwrite).
	if val, _, _ := kvOf(reg).Read(ctx, "kv:doc"); string(val) != "v1" {
		t.Errorf("bytes after collision=%q want v1 (unchanged)", val)
	}
	if allowActor(t, reg, "actor:b", "kv:doc", access.OpRead) {
		t.Error("colliding creator b must NOT have grabbed any grant")
	}
}

func TestResource_CreateGuardsEmptyInputs(t *testing.T) {
	ctx := context.Background()
	reg := openResourceReg(t)
	if err := reg.Create(ctx, "", "kv", "actor:a", nil); err == nil {
		t.Error("Create with empty id must error")
	}
	if err := reg.Create(ctx, "kv:x", "kv", "", nil); err == nil {
		t.Error("Create with empty creator must error")
	}
}

// --- SetGrant: replace + revoke, and the two query halves --------------------

func TestResource_SetGrantReplaceAndRevoke(t *testing.T) {
	reg := openResourceReg(t)
	mustCreate(t, reg, "kv:doc", "actor:a")

	// Grant B read only.
	set(t, reg, "kv:doc", grantActor("actor:b", access.OpRead))
	if !allowActor(t, reg, "actor:b", "kv:doc", access.OpRead) {
		t.Error("B must have read after set")
	}
	if allowActor(t, reg, "actor:b", "kv:doc", access.OpWrite) {
		t.Error("B must NOT have write (grant was read-only)")
	}

	// Replace (not merge) with read+write.
	set(t, reg, "kv:doc", grantActor("actor:b", access.OpRead, access.OpWrite))
	if !allowActor(t, reg, "actor:b", "kv:doc", access.OpWrite) {
		t.Error("B must have write after replacing grant")
	}

	// Revoke: empty ops deletes the entry.
	set(t, reg, "kv:doc", grantActor("actor:b"))
	if allowActor(t, reg, "actor:b", "kv:doc", access.OpRead) {
		t.Error("B read must be gone after revoke (∅ ops)")
	}
}

func TestResource_MembersEntryQuerySplit(t *testing.T) {
	reg := openResourceReg(t)
	mustCreate(t, reg, "kv:doc", "actor:a")

	// A members entry (grantee empty) grants read.
	set(t, reg, "kv:doc", grantMembers(access.OpRead))

	// MembersAllow sees it; it does NOT look at any caller.
	if !allowMembers(t, reg, "kv:doc", access.OpRead) {
		t.Error("MembersAllow(read) must be true")
	}
	if allowMembers(t, reg, "kv:doc", access.OpWrite) {
		t.Error("MembersAllow(write) must be false (only read granted)")
	}

	// The actor-entry half is independent: B has no direct entry.
	if allowActor(t, reg, "actor:b", "kv:doc", access.OpRead) {
		t.Error("ActorAllows must NOT see the members entry (two separate halves)")
	}

	// Revoke the members entry.
	set(t, reg, "kv:doc", grantMembers())
	if allowMembers(t, reg, "kv:doc", access.OpRead) {
		t.Error("members entry must be gone after ∅ revoke")
	}
}

// --- Delete: cascades row + all grants ---------------------------------------

func TestResource_DeleteCascadesRowAndGrants(t *testing.T) {
	ctx := context.Background()
	reg := openResourceReg(t)
	mustCreate(t, reg, "kv:doc", "actor:a")
	set(t, reg, "kv:doc", grantActor("actor:b", access.OpRead))
	set(t, reg, "kv:doc", grantMembers(access.OpRead))

	if err := reg.Delete(ctx, "kv:doc"); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	if _, ok, _ := reg.Resolve(ctx, "kv:doc"); ok {
		t.Error("resource must not resolve after delete")
	}
	// All grants gone (creator, B, members).
	if allowActor(t, reg, "actor:a", "kv:doc", access.OpRead) {
		t.Error("creator grant must be cascaded away by Delete")
	}
	if allowActor(t, reg, "actor:b", "kv:doc", access.OpRead) {
		t.Error("B grant must be cascaded away by Delete")
	}
	if allowMembers(t, reg, "kv:doc", access.OpRead) {
		t.Error("members grant must be cascaded away by Delete")
	}

	// Delete is idempotent: deleting an absent id is a clean no-op.
	if err := reg.Delete(ctx, "kv:doc"); err != nil {
		t.Errorf("repeat Delete must be a no-op, got %v", err)
	}
}

// --- kvDriver: found semantics (empty bytes vs no row), write, no-op delete ---

func TestKVDriver_ReadFoundSemantics(t *testing.T) {
	ctx := context.Background()
	reg := openResourceReg(t)
	kv := kvOf(reg)

	// Row exists with NULL bytes (nil initial) → resolved-but-empty: found=false.
	if err := reg.Create(ctx, "kv:empty", "kv", "actor:a", nil); err != nil {
		t.Fatalf("Create empty: %v", err)
	}
	if _, ok, _ := reg.Resolve(ctx, "kv:empty"); !ok {
		t.Fatal("empty-bytes resource must still RESOLVE (the row exists)")
	}
	val, found, err := kv.Read(ctx, "kv:empty")
	if err != nil {
		t.Fatalf("Read empty: %v", err)
	}
	if found || val != nil {
		t.Errorf("NULL-bytes row: found=%v val=%q want found=false empty", found, val)
	}

	// No row at all → also found=false, but Resolve reports it as not-existing
	// (that is where the door draws not_found vs resolved-but-empty).
	if _, ok, _ := reg.Resolve(ctx, "kv:ghost"); ok {
		t.Error("absent id must not resolve")
	}
	if _, found, _ := kv.Read(ctx, "kv:ghost"); found {
		t.Error("absent id Read must be found=false")
	}

	// Row with content → found=true.
	if err := reg.Create(ctx, "kv:full", "kv", "actor:a", []byte("payload")); err != nil {
		t.Fatalf("Create full: %v", err)
	}
	val, found, _ = kv.Read(ctx, "kv:full")
	if !found || string(val) != "payload" {
		t.Errorf("content row: found=%v val=%q want true/payload", found, val)
	}

	// Present-but-empty ([]byte{}, proto: legal and distinct from nil) must read
	// back found=true with empty value — NOT collapse into the NULL case.
	if err := reg.Create(ctx, "kv:blank", "kv", "actor:a", []byte{}); err != nil {
		t.Fatalf("Create blank: %v", err)
	}
	val, found, err = kv.Read(ctx, "kv:blank")
	if err != nil {
		t.Fatalf("Read blank: %v", err)
	}
	if !found || len(val) != 0 {
		t.Errorf("empty-blob row: found=%v val=%q want found=true empty value", found, val)
	}
}

// Write against a row that vanished in the resolve→execute window must surface
// an error (→ driver_error verdict at the door), never a silent zero-row success.
func TestKVDriver_WriteVanishedRow(t *testing.T) {
	ctx := context.Background()
	reg := openResourceReg(t)
	kv := kvOf(reg)
	if err := kv.Write(ctx, "kv:gone", []byte("x")); err == nil {
		t.Fatal("Write on absent row must error, not silently succeed")
	}
}

func TestKVDriver_WriteOverwrites(t *testing.T) {
	ctx := context.Background()
	reg := openResourceReg(t)
	kv := kvOf(reg)
	mustCreate(t, reg, "kv:doc", "actor:a") // created with nil bytes

	if err := kv.Write(ctx, "kv:doc", []byte("v1")); err != nil {
		t.Fatalf("Write v1: %v", err)
	}
	if val, found, _ := kv.Read(ctx, "kv:doc"); !found || string(val) != "v1" {
		t.Errorf("after write: found=%v val=%q want true/v1", found, val)
	}
	// PUT overwrites (idempotent-on-content).
	if err := kv.Write(ctx, "kv:doc", []byte("v2")); err != nil {
		t.Fatalf("Write v2: %v", err)
	}
	if val, _, _ := kv.Read(ctx, "kv:doc"); string(val) != "v2" {
		t.Errorf("after overwrite: val=%q want v2", val)
	}
}

// kvDriver.Delete is a no-op: the bytes live inline, so the driver leaves the
// row and its bytes intact — it is Registry.Delete that removes them.
func TestKVDriver_DeleteIsNoop(t *testing.T) {
	ctx := context.Background()
	reg := openResourceReg(t)
	kv := kvOf(reg)
	if err := reg.Create(ctx, "kv:doc", "kv", "actor:a", []byte("keep")); err != nil {
		t.Fatalf("Create: %v", err)
	}
	if err := kv.Delete(ctx, "kv:doc"); err != nil {
		t.Fatalf("driver Delete: %v", err)
	}
	// Row + bytes untouched by the driver-level delete.
	if _, ok, _ := reg.Resolve(ctx, "kv:doc"); !ok {
		t.Error("kvDriver.Delete must NOT remove the row (no-op)")
	}
	if val, found, _ := kv.Read(ctx, "kv:doc"); !found || string(val) != "keep" {
		t.Errorf("kvDriver.Delete must leave bytes intact, got found=%v val=%q", found, val)
	}
}

// --- test helpers ------------------------------------------------------------

func mustCreate(t *testing.T, reg *resourceRegistry, id resource.ResourceID, creator actor.ActorID) {
	t.Helper()
	if err := reg.Create(context.Background(), id, "kv", creator, nil); err != nil {
		t.Fatalf("Create %q: %v", id, err)
	}
}

func set(t *testing.T, reg *resourceRegistry, id resource.ResourceID, g access.Grant) {
	t.Helper()
	if err := reg.SetGrant(context.Background(), id, g); err != nil {
		t.Fatalf("SetGrant %q %+v: %v", id, g, err)
	}
}

func grantActor(who actor.ActorID, ops ...access.Operation) access.Grant {
	return access.Grant{GranteeKind: access.GranteeActor, Grantee: who, Ops: ops}
}

func grantMembers(ops ...access.Operation) access.Grant {
	return access.Grant{GranteeKind: access.GranteeMembers, Ops: ops}
}

func allowActor(t *testing.T, reg *resourceRegistry, caller actor.ActorID, id resource.ResourceID, op access.Operation) bool {
	t.Helper()
	ok, err := reg.ActorAllows(context.Background(), caller, id, op)
	if err != nil {
		t.Fatalf("ActorAllows(%q,%q,%s): %v", caller, id, op, err)
	}
	return ok
}

func allowMembers(t *testing.T, reg *resourceRegistry, id resource.ResourceID, op access.Operation) bool {
	t.Helper()
	ok, err := reg.MembersAllow(context.Background(), id, op)
	if err != nil {
		t.Fatalf("MembersAllow(%q,%s): %v", id, op, err)
	}
	return ok
}
