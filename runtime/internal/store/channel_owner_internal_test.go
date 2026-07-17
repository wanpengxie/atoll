package store

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

func TestActorRoleSQLConstraintsAndReadBoundaryFailClosed(t *testing.T) {
	ctx := context.Background()
	cs, err := OpenChannel(ctx, "role-read-boundary", filepath.Join(t.TempDir(), "channel.sqlite"), OpenOptions{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer cs.Close()
	admitted, err := cs.DeclAdmission.AdmitDeclared(ctx, storespec.AdmitBundle{
		Kind: actor.KindAgent, Principal: "agent", Class: "agent",
		Placement: storespec.NewServerPlacement(), CreatedAt: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := cs.db.ExecContext(ctx, `UPDATE actor_registry SET role='owner' WHERE actor_id=?`, admitted.ID); err == nil {
		t.Fatal("owner=>human SQL constraint accepted an agent owner")
	}
	if _, err := cs.db.ExecContext(ctx, `UPDATE actor_registry SET role='unknown' WHERE actor_id=?`, admitted.ID); err == nil {
		t.Fatal("role closed-set SQL constraint accepted an unknown role")
	}

	conn, err := cs.db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if _, err := conn.ExecContext(ctx, `PRAGMA ignore_check_constraints=ON`); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.ExecContext(ctx, `UPDATE actor_registry SET role='unknown' WHERE actor_id=?`, admitted.ID); err != nil {
		t.Fatal(err)
	}
	row, err := scanDeclaredControl(conn.QueryRowContext(ctx, `SELECT `+declaredControlColumns+`
		FROM actor_registry r JOIN actor_decl_versions d
		ON d.actor_id=r.actor_id AND d.version=r.current_decl_version WHERE r.actor_id=?`, admitted.ID))
	if err == nil {
		t.Fatalf("read boundary accepted invalid role in %+v", row)
	}
}
