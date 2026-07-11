package store

import (
	"context"
	"strings"
	"testing"

	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

func TestAdmitPrimaryKeyCollisionAdvancesTimestamp(t *testing.T) {
	f := openTimersFixture(t)
	ctx := context.Background()
	if err := f.reg.insertFixedID(ctx, storespec.Record{
		ID: "agent:alice:1000", Kind: actor.KindAgent, CreatedAt: 1,
	}); err != nil {
		t.Fatalf("seed collision: %v", err)
	}
	id, err := f.reg.Admit(ctx, actor.KindAgent, "alice", 1000)
	if err != nil {
		t.Fatalf("Admit: %v", err)
	}
	if id != "agent:alice:1001" {
		t.Fatalf("Admit id=%q want timestamp collision retry at 1001", id)
	}
}

func TestAdmitMirrorFailureRollsBackRegistryAndPreservesError(t *testing.T) {
	f := openTimersFixture(t)
	ctx := context.Background()
	if _, err := f.reg.db.ExecContext(ctx, `CREATE TRIGGER reject_actor_mirror BEFORE INSERT ON messages
		WHEN NEW.type = 'system.actor.registered' BEGIN SELECT RAISE(ABORT, 'mirror blocked'); END`); err != nil {
		t.Fatalf("create trigger: %v", err)
	}
	_, err := f.reg.Admit(ctx, actor.KindAgent, "alice", 1000)
	if err == nil || !strings.Contains(err.Error(), "mirror blocked") {
		t.Fatalf("Admit error=%v want original mirror failure", err)
	}
	if _, ok, lookupErr := f.reg.LookupActivePrincipal(ctx, actor.KindAgent, "alice"); lookupErr != nil || ok {
		t.Fatalf("registry survived failed mirror tx: ok=%v err=%v", ok, lookupErr)
	}
}
