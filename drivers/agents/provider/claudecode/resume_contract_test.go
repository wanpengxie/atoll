package claudecode

import (
	"testing"

	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/registry"
)

// TestNewDecl_BuildContract pins claude's daemon-side build contract:
//
//   - build is SEED-OPTIONAL cold start: NewDecl succeeds with NO durable state
//     (fresh start every boot — the platform state slot was removed, resume is
//     down until period-10 rebuilds it on sys.State).
//   - build is CREDS-INDEPENDENT: no ANTHROPIC_API_KEY / login is read at BUILD
//     (the claude CLI carries its own auth at RUN time, see agent.go EnvKeys), so
//     a daemon builds the assigned claude with only the user's LOCAL login.
func TestNewDecl_BuildContract(t *testing.T) {
	base := registry.Deps{ChannelID: "c1", WorkspaceDir: t.TempDir()}
	spec := registry.InstanceSpec{ID: actor.ActorID("agent:rev")}

	decl, err := NewDecl(spec, base)
	if err != nil {
		t.Fatalf("NewDecl should build (cold start): %v", err)
	}
	if decl.Kind != actor.KindAgent {
		t.Fatalf("claude decl Kind = %v, want agent", decl.Kind)
	}
	if decl.ID != spec.ID {
		t.Fatalf("claude decl ID = %v, want %v", decl.ID, spec.ID)
	}
}

// TestNewDecl_RequiresChannelAndID pins WHY the daemon must build from an
// EXPLICIT assignment (not a blind "one of each" build with empty specs):
// claude refuses an empty id — exactly the fatal a blind "one of each" loop
// would hit once agents were compiled into the daemon.
func TestNewDecl_RequiresChannelAndID(t *testing.T) {
	if _, err := NewDecl(registry.InstanceSpec{ID: "agent:x"}, registry.Deps{}); err == nil {
		t.Fatal("NewDecl with empty ChannelID should error")
	}
	if _, err := NewDecl(registry.InstanceSpec{}, registry.Deps{ChannelID: "c1"}); err == nil {
		t.Fatal("NewDecl with empty instance id should error (no blind-build)")
	}
}
