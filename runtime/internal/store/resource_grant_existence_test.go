package store

// SetGrant's same-transaction existence gate, over the REAL SQL path.
//
// resourcespec.ErrResourceNotFound is a typed sentinel the door branches on, but
// its only prior coverage injected the error value into a fake registry — the
// `SELECT 1 FROM resources` check that actually produces it was never executed.
// These tests drive it through a real channel sqlite: a never-created id and an
// id whose resource was deleted, on both SetGrant halves (grant and revoke).

import (
	"context"
	"errors"
	"testing"

	"github.com/wanpengxie/atoll/protocol/access"
	"github.com/wanpengxie/atoll/runtime/resourcespec"
)

func TestResource_SetGrantOnAbsentResourceIsNotFound(t *testing.T) {
	ctx := context.Background()
	reg := openResourceReg(t)

	// A resource that never existed — the grant half (non-empty ops).
	err := reg.SetGrant(ctx, "kv:never", grantActor("actor:b", access.OpRead))
	if !errors.Is(err, resourcespec.ErrResourceNotFound) {
		t.Fatalf("SetGrant on absent resource = %v, want ErrResourceNotFound", err)
	}
	// The revoke half (empty ops) is gated by the SAME existence check: an
	// absent resource is not "already revoked".
	err = reg.SetGrant(ctx, "kv:never", grantActor("actor:b"))
	if !errors.Is(err, resourcespec.ErrResourceNotFound) {
		t.Fatalf("revoke on absent resource = %v, want ErrResourceNotFound", err)
	}
	// A members entry aimed at nothing is refused identically — the gate is on
	// the resource, not on the grantee shape.
	err = reg.SetGrant(ctx, "kv:never", grantMembers(access.OpRead))
	if !errors.Is(err, resourcespec.ErrResourceNotFound) {
		t.Fatalf("members SetGrant on absent resource = %v, want ErrResourceNotFound", err)
	}

	if n := countRows(t, reg.db, "resource_grants"); n != 0 {
		t.Fatalf("resource_grants rows = %d, want 0 (a refused SetGrant writes nothing)", n)
	}
}

func TestResource_SetGrantAfterDeleteIsNotFound(t *testing.T) {
	ctx := context.Background()
	reg := openResourceReg(t)
	createKV(t, reg, "kv:doc", "actor:a", nil)
	set(t, reg, "kv:doc", grantActor("actor:b", access.OpRead))

	if err := reg.Delete(ctx, "kv:doc"); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	// The live path and the sentinel path are the same call: once the row is
	// gone, the grant that worked a moment ago is refused with the sentinel.
	err := reg.SetGrant(ctx, "kv:doc", grantActor("actor:b", access.OpWrite))
	if !errors.Is(err, resourcespec.ErrResourceNotFound) {
		t.Fatalf("SetGrant after Delete = %v, want ErrResourceNotFound", err)
	}
	if n := countRows(t, reg.db, "resource_grants"); n != 0 {
		t.Fatalf("resource_grants rows = %d, want 0", n)
	}
}
