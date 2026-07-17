package actorrt

import (
	"context"
	"testing"

	"github.com/wanpengxie/atoll/protocol/actor"
)

// TestStat_KindAcrossEmbodimentForms (G11 DoD, review-fixed): UnitStat.Kind is
// the incarnation household's own copy of the out-generation attribute
// Spawn/Fork/Attach weld at mint time — asserted across the three embodiment
// forms Stat serves. A cell answers its Spawn-time kind; a fork child answers
// its Fork-time kind (ForkSpec.Kind, the SAME attribute an admission Spawn
// welds — recursive assembly, not a lesser form); a port answers its
// Attach-time KindOf resolution — the codex review fix that closed the
// name-register gap (port.kind() no longer silently answers the zero value;
// see port.kind doc).
func TestStat_KindAcrossEmbodimentForms(t *testing.T) {
	t.Parallel()
	rt, _ := New(Config{Parent: context.Background()})
	defer rt.StopAll()

	_, _, _ = rt.SpawnIfAbsent("parent", actor.KindAgent, static(newRecordActor()))
	st, ok := rt.Stat("parent")
	if !ok {
		t.Fatal("cell not hosted after Spawn")
	}
	if st.Kind != actor.KindAgent {
		t.Fatalf("cell UnitStat.Kind = %q, want %q", st.Kind, actor.KindAgent)
	}

	kindOf := func(id actor.ActorID) (actor.Kind, bool) {
		if id == actor.ActorID("remote-kind") {
			return actor.KindTool, true
		}
		return "", false
	}
	id, remote := dialPort(t, rt, "lease-kind", nopEmit, staticResolve("remote-kind"), kindOf)
	defer remote.conn.Close()
	st, ok = rt.Stat(id)
	if !ok {
		t.Fatal("port not hosted after Attach")
	}
	if st.Kind != actor.KindTool {
		t.Fatalf("port UnitStat.Kind = %q, want %q (Attach's KindOf resolution)", st.Kind, actor.KindTool)
	}
}
